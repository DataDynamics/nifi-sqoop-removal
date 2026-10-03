"""sweeper 규칙의 SQL.

대상 run은 SKIP LOCKED로 가져와 처리 중인 요청과 겹치지 않게 한다.

sweeper(worker.sweeper)는 한 트랜잭션 안에서 이 함수들을 차례로 부른다. 먼저 try_lock으로 advisory lock을
얻어 여러 worker 중 하나만 실행되게 하고, 대상 run 행을 FOR UPDATE SKIP LOCKED로 잠근다. API 요청이
지금 잠그고 있는 run은 기다리지 않고 이번 회차에서 건너뛰며 다음 주기에 다시 본다.
stale 판정 시각은 모두 clock_timestamp()(실제 현재 시각) 기준이다.
"""

from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

# advisory lock 키. hashtext()로 정수 키로 바꿔 쓴다.
LOCK_KEY = "load_control_sweeper"


async def try_lock(conn: AsyncConnection) -> bool:
    """트랜잭션 범위 advisory lock. 다른 sweeper가 실행 중이면 False.

    pg_try_advisory_xact_lock은 기다리지 않고 바로 결과를 돌려주며, 잠금은 트랜잭션이 끝나면 자동으로
    풀린다(별도 unlock이 필요 없고, 프로세스가 죽어도 남지 않는다).
    """
    row = (await conn.execute(text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
                              {"key": LOCK_KEY})).first()
    return bool(row and row[0])


async def runs_with_stale_partitions(conn: AsyncConnection, stale: timedelta) -> list[UUID]:
    """heartbeat가 끊긴 RUNNING 파티션이 있는 EXTRACTING run(잠금, 처리 중인 run은 건너뜀).

    FOR UPDATE OF r는 load_run 행만 잠근다(EXISTS 안의 파티션은 잠그지 않음). 파티션 잠금은 이후
    stale_partitions가 run 잠금 뒤에 잡아 잠금 순서 load_run → load_partition을 지킨다.
    오래 시작된 run부터 돌려준다.
    """
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
    """run의 stale 파티션과 지금까지의 시도 횟수.

    RUNNING이면서 heartbeat_at이 stale보다 오래된 파티션 행을 FOR UPDATE로 잠그고
    (partition_id, attempt_count) 목록을 돌려준다. 호출자는 attempt_count로 재발행 한도(max_attempts)를 본다.
    run 행은 runs_with_stale_partitions가 이미 잠갔다.
    """
    rows = (await conn.execute(text("""
        SELECT partition_id, attempt_count FROM nifi_ops.load_partition
         WHERE run_id = :run_id AND status = 'RUNNING'
           AND heartbeat_at < clock_timestamp() - CAST(:stale AS interval)
         ORDER BY partition_id
           FOR UPDATE
    """), {"run_id": run_id, "stale": stale})).all()
    return [(str(r[0]), int(r[1])) for r in rows]


async def reset_for_reissue(conn: AsyncConnection, run_id: UUID, partition_id: str) -> bool:
    """RUNNING → RETRY, claim 초기화. 이전 Worker의 보고는 CLAIM_MISMATCH가 된다.

    claim_token·worker_node를 지워 이전 Worker가 늦게 보내는 chunk·실패 보고를 거부하게 한다. heartbeat_at을
    지금으로 바꾸고 error_code에 HEARTBEAT_STALE을 남긴다. WHERE status = 'RUNNING'이 CAS 조건이며 바꿨으면
    True. 이후 새 Worker가 claim하면 RETRY → RUNNING이 되고 이 재발행 dispatch가 ACK된다.
    """
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
    """run의 미완료 파티션을 모두 TIMED_OUT으로 바꾼다.

    PENDING·RUNNING·RETRY 파티션에 code(PARTITION_STALE, REISSUE_ATTEMPTS_EXHAUSTED, RUN_TIMEOUT 등)와
    completed_at을 남긴다. claim_token은 지우지 않는다. 이후 Worker의 chunk 보고는 run이 끝난 상태라
    판정 없이 기록만 되고, 실패 보고는 파티션이 RUNNING이 아니라 409가 된다. 바꾼 행 수를 돌려준다.
    호출자는 run 행을 이미 잠근 상태다.
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_partition
           SET status = 'TIMED_OUT', error_code = :code, completed_at = clock_timestamp()
         WHERE run_id = :run_id AND status IN ('PENDING', 'RUNNING', 'RETRY')
    """), {"run_id": run_id, "code": code})
    return result.rowcount


async def runs_past_deadline(conn: AsyncConnection, run_timeout: timedelta) -> list[UUID]:
    """recovery.run_timeout을 넘긴 CREATED/EXTRACTING run.

    started_at 기준으로 run_timeout이 지난 run 행을 FOR UPDATE SKIP LOCKED로 잠가 돌려준다. heartbeat가
    살아 있어도 전체 추출 시간이 한도를 넘으면 대상이 된다. 호출자가 TIMED_OUT으로 바꾼다.
    """
    rows = (await conn.execute(text("""
        SELECT run_id FROM nifi_ops.load_run
         WHERE status IN ('CREATED', 'EXTRACTING')
           AND started_at < clock_timestamp() - CAST(:timeout AS interval)
         ORDER BY started_at
           FOR UPDATE SKIP LOCKED
    """), {"timeout": run_timeout})).all()
    return [r[0] for r in rows]


@dataclass(frozen=True, slots=True)
class UnackedDispatch:
    """requeue_unacked_dispatches가 처리한 dispatch 한 건. status는 처리 뒤 상태(PENDING 또는 DEAD)다."""

    dispatch_id: UUID
    run_id: UUID
    dispatch_type: str
    partition_id: str | None
    attempt_count: int
    status: str


async def requeue_unacked_dispatches(conn: AsyncConnection, ack_timeout: timedelta,
                                     max_attempts: int) -> list[UnackedDispatch]:
    """SENT 후 ACK가 없고 아직 기다리는 상태면 다시 보내고, 시도를 다 쓴 것은 DEAD로 바꾼다.

    SENT로 기록된 지 ack_timeout이 지났는데 ACKED가 아닌 dispatch가 대상이다.
    - attempt_count < max_attempts: PENDING으로 되돌리고 next_attempt_at을 지금으로 둔다(다시 보냄).
    - attempt_count >= max_attempts: DEAD로 바꾼다. NiFi가 202로 받기만 하고 flow를 시작하지 않는 상태
      (Job PG 정지, PG-05 → Job PG 연결 막힘 등)가 이어지면 무한히 다시 보내는 대신 운영자에게 알린다.
      dispatcher의 max_attempts 확인은 전송 실패 경로에만 있으므로 여기서 막아야 한다.
    attempt_count는 초기화하지 않는다(재전송도 한도에 포함). 처리한 행 목록을 돌려준다.
    """
    # run이 아직 그 호출을 기다리는 상태일 때만 되돌린다.
    # - VALIDATE_RUN: run이 EXTRACTED_VALIDATED일 때(검증이 시작되면 STAGE_VALIDATING이 되어 빠진다).
    # - REISSUE_PARTITION: run이 EXTRACTING이고 그 파티션이 아직 RETRY일 때(claim되면 RUNNING이 되어 빠진다).
    # 이미 의미 없어진 호출을 다시 보내지 않기 위해서다.
    rows = (await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch d
           SET status = CASE WHEN d.attempt_count >= :max_attempts THEN 'DEAD' ELSE 'PENDING' END,
               next_attempt_at = clock_timestamp(),
               last_error = CASE WHEN d.attempt_count >= :max_attempts
                                 THEN 'no ack after ' || d.attempt_count || ' attempts (ack timeout)'
                                 ELSE 'ack timeout, requeued by sweeper' END
          FROM nifi_ops.load_run r
         WHERE r.run_id = d.run_id
           AND d.status = 'SENT'
           AND d.sent_at < clock_timestamp() - CAST(:ack AS interval)
           AND ((d.dispatch_type = 'VALIDATE_RUN' AND r.status = 'EXTRACTED_VALIDATED')
                OR (d.dispatch_type = 'REISSUE_PARTITION' AND r.status = 'EXTRACTING'
                    AND EXISTS (SELECT 1 FROM nifi_ops.load_partition p
                                 WHERE p.run_id = d.run_id AND p.partition_id = d.partition_id
                                   AND p.status = 'RETRY')))
        RETURNING d.dispatch_id, d.run_id, d.dispatch_type, d.partition_id, d.attempt_count, d.status
    """), {"ack": ack_timeout, "max_attempts": max_attempts})).mappings().all()
    # RETURNING은 UPDATE 뒤의 값을 돌려주므로 status로 되돌림·DEAD를 구분한다.
    return [UnackedDispatch(**dict(m)) for m in rows]


async def alert_stale_runs(conn: AsyncConnection, stale: timedelta) -> int:
    """검증·게시 중 멈춘 run에 경보 이벤트를 남긴다. 같은 run에는 stale 기간마다 한 번만.

    STAGE_VALIDATING(검증 진행 중)·PUBLISHED(target 검증 대기)에서 heartbeat가 stale보다 오래된 run에
    RUN_STALE_ALERT(ERROR) 이벤트를 INSERT ... SELECT로 넣는다. 상태는 바꾸지 않는다(사람이 판단할 일).
    NOT EXISTS 조건으로 최근 stale 기간 안에 같은 경보가 있으면 다시 넣지 않는다. 넣은 행 수를 돌려준다.
    """
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
    """recovery.publish_stale 동안 게시 결과가 오지 않은 run.

    PUBLISHING에서 publish_started_at이 stale보다 오래된 run 행을 FOR UPDATE SKIP LOCKED로 잠가 돌려준다.
    호출자는 PUBLISH_UNKNOWN으로 바꾼다. INSERT OVERWRITE가 실제로 반영됐는지 알 수 없으므로 자동으로
    재시도하거나 실패로 확정하지 않고 운영자 확인에 맡긴다.
    """
    rows = (await conn.execute(text("""
        SELECT run_id FROM nifi_ops.load_run
         WHERE status = 'PUBLISHING'
           AND publish_started_at < clock_timestamp() - CAST(:stale AS interval)
           FOR UPDATE SKIP LOCKED
    """), {"stale": stale})).all()
    return [r[0] for r in rows]


async def active_run_counts(conn: AsyncConnection) -> dict[str, int]:
    """상태별 활성 run 수(메트릭용). 0건인 상태는 결과에 없다(호출자가 0으로 채운다)."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_run
         WHERE status IN ('CREATED', 'EXTRACTING', 'EXTRACTED_VALIDATED', 'STAGE_VALIDATING',
                          'STAGING_VALIDATED', 'PUBLISHING', 'PUBLISHED', 'PUBLISH_UNKNOWN')
         GROUP BY status
    """))).all()
    return {str(r[0]): int(r[1]) for r in rows}
