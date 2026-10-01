"""sweeper 규칙의 SQL(API 설계 7장, 가이드 13.1).

대상 run은 SKIP LOCKED로 가져와 처리 중인 요청과 겹치지 않게 한다.
"""

from datetime import timedelta
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

LOCK_KEY = "load_control_sweeper"


async def try_lock(conn: AsyncConnection) -> bool:
    """트랜잭션 범위 advisory lock. 다른 sweeper가 실행 중이면 False."""
    row = (await conn.execute(text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
                              {"key": LOCK_KEY})).first()
    return bool(row and row[0])


async def runs_with_stale_partitions(conn: AsyncConnection, stale: timedelta) -> list[UUID]:
    """heartbeat가 끊긴 RUNNING 파티션이 있는 EXTRACTING run(잠금, 처리 중인 run은 건너뜀)."""
    rows = (await conn.execute(text("""
        SELECT r.run_id FROM nifi_ops.load_run r
         WHERE r.status = 'EXTRACTING'
           AND EXISTS (SELECT 1 FROM nifi_ops.load_partition p
                        WHERE p.run_id = r.run_id AND p.status = 'RUNNING'
                          AND p.heartbeat_at < clock_timestamp() - CAST(:stale AS interval))
         ORDER BY r.started_at
           FOR UPDATE OF r SKIP LOCKED
    """), {"stale": stale})).all()
    return [r[0] for r in rows]


async def stale_partitions(conn: AsyncConnection, run_id: UUID, stale: timedelta) -> list[tuple[str, int]]:
    """run의 stale 파티션과 지금까지의 시도 횟수."""
    rows = (await conn.execute(text("""
        SELECT partition_id, attempt_count FROM nifi_ops.load_partition
         WHERE run_id = :run_id AND status = 'RUNNING'
           AND heartbeat_at < clock_timestamp() - CAST(:stale AS interval)
         ORDER BY partition_id
           FOR UPDATE
    """), {"run_id": run_id, "stale": stale})).all()
    return [(str(r[0]), int(r[1])) for r in rows]


async def reset_for_reissue(conn: AsyncConnection, run_id: UUID, partition_id: str) -> bool:
    """RUNNING → RETRY, claim 초기화. 이전 Worker의 보고는 CLAIM_MISMATCH가 된다."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_partition
           SET status = 'RETRY', claim_token = NULL, worker_node = NULL,
               heartbeat_at = clock_timestamp(), error_code = 'HEARTBEAT_STALE',
               error_message = 'reset by sweeper for reissue'
         WHERE run_id = :run_id AND partition_id = :partition_id AND status = 'RUNNING'
        RETURNING partition_id
    """), {"run_id": run_id, "partition_id": partition_id})
    return result.first() is not None


async def time_out_partitions(conn: AsyncConnection, run_id: UUID, code: str) -> int:
    """run의 미완료 파티션을 모두 TIMED_OUT으로 바꾼다."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_partition
           SET status = 'TIMED_OUT', error_code = :code, completed_at = clock_timestamp()
         WHERE run_id = :run_id AND status IN ('PENDING', 'RUNNING', 'RETRY')
    """), {"run_id": run_id, "code": code})
    return result.rowcount


async def runs_past_deadline(conn: AsyncConnection, run_timeout: timedelta) -> list[UUID]:
    """recovery.run_timeout을 넘긴 CREATED/EXTRACTING run."""
    rows = (await conn.execute(text("""
        SELECT run_id FROM nifi_ops.load_run
         WHERE status IN ('CREATED', 'EXTRACTING')
           AND started_at < clock_timestamp() - CAST(:timeout AS interval)
         ORDER BY started_at
           FOR UPDATE SKIP LOCKED
    """), {"timeout": run_timeout})).all()
    return [r[0] for r in rows]


async def requeue_unacked_dispatches(conn: AsyncConnection, ack_timeout: timedelta) -> list[UUID]:
    """SENT 후 ACK가 없고 아직 기다리는 상태면 다시 보낸다(API 설계 4.2)."""
    rows = (await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch d
           SET status = 'PENDING', next_attempt_at = clock_timestamp(),
               last_error = 'ack timeout, requeued by sweeper'
          FROM nifi_ops.load_run r
         WHERE r.run_id = d.run_id
           AND d.status = 'SENT'
           AND d.sent_at < clock_timestamp() - CAST(:ack AS interval)
           AND ((d.dispatch_type = 'VALIDATE_RUN' AND r.status = 'EXTRACTED_VALIDATED')
                OR (d.dispatch_type = 'REISSUE_PARTITION' AND r.status = 'EXTRACTING'
                    AND EXISTS (SELECT 1 FROM nifi_ops.load_partition p
                                 WHERE p.run_id = d.run_id AND p.partition_id = d.partition_id
                                   AND p.status = 'RETRY')))
        RETURNING d.run_id
    """), {"ack": ack_timeout})).all()
    return [r[0] for r in rows]


async def alert_stale_runs(conn: AsyncConnection, stale: timedelta) -> int:
    """검증·게시 중 멈춘 run에 경보 이벤트를 남긴다. 같은 run에는 stale 기간마다 한 번만."""
    result = await conn.execute(text("""
        INSERT INTO nifi_ops.load_event (
            event_id, event_level, event_name, run_id, job_key, business_key,
            process_group, processor_name, message, details)
        SELECT gen_random_uuid(), 'ERROR', 'RUN_STALE_ALERT', r.run_id, r.job_key, r.business_key,
               'LOAD_CONTROL_API', 'sweeper', 'no progress in ' || r.status,
               jsonb_build_object('status', r.status, 'heartbeatAt', r.heartbeat_at)
          FROM nifi_ops.load_run r
         WHERE r.status IN ('STAGE_VALIDATING', 'PUBLISHED')
           AND r.heartbeat_at < clock_timestamp() - CAST(:stale AS interval)
           AND NOT EXISTS (SELECT 1 FROM nifi_ops.load_event e
                            WHERE e.run_id = r.run_id AND e.event_name = 'RUN_STALE_ALERT'
                              AND e.event_time > clock_timestamp() - CAST(:stale AS interval))
    """), {"stale": stale})
    return result.rowcount


async def stale_publishing_runs(conn: AsyncConnection, stale: timedelta) -> list[UUID]:
    """recovery.publish_stale 동안 게시 결과가 오지 않은 run."""
    rows = (await conn.execute(text("""
        SELECT run_id FROM nifi_ops.load_run
         WHERE status = 'PUBLISHING'
           AND publish_started_at < clock_timestamp() - CAST(:stale AS interval)
           FOR UPDATE SKIP LOCKED
    """), {"stale": stale})).all()
    return [r[0] for r in rows]


async def active_run_counts(conn: AsyncConnection) -> dict[str, int]:
    """상태별 활성 run 수(메트릭용)."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_run
         WHERE status IN ('CREATED', 'EXTRACTING', 'EXTRACTED_VALIDATED', 'STAGE_VALIDATING',
                          'STAGING_VALIDATED', 'PUBLISHING', 'PUBLISHED', 'PUBLISH_UNKNOWN')
         GROUP BY status
    """))).all()
    return {str(r[0]): int(r[1]) for r in rows}
