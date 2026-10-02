"""outbox(load_dispatch) SQL. 상태: PENDING → SENT → ACKED, 실패 누적 시 DEAD."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

CHANNEL = "load_dispatch"


@dataclass(frozen=True, slots=True)
class DispatchRow:
    """조회 API에 보여 줄 dispatch 요약."""

    dispatch_id: UUID
    dispatch_type: str
    partition_id: str | None
    status: str
    attempt_count: int
    sent_at: datetime | None
    acked_at: datetime | None


@dataclass(frozen=True, slots=True)
class LeasedDispatch:
    """lease로 선점한, 지금 보낼 dispatch."""

    dispatch_id: UUID
    run_id: UUID
    dispatch_type: str
    partition_id: str | None
    attempt_count: int
    job_key: str


async def _notify(conn: AsyncConnection, run_id: UUID) -> None:
    # NOTIFY는 commit될 때만 전달된다. rollback된 예약은 dispatcher를 깨우지 않는다.
    await conn.execute(text("SELECT pg_notify(:channel, :payload)"),
                       {"channel": CHANNEL, "payload": str(run_id)})


async def enqueue_validation(conn: AsyncConnection, run_id: UUID) -> bool:
    """검증 호출을 outbox에 예약하고 dispatcher를 깨운다.

    uq_load_dispatch_validate가 run당 1행을 보장하는 마지막 방어선이다.
    """
    inserted = (await conn.execute(text("""
        INSERT INTO nifi_ops.load_dispatch (dispatch_id, run_id, dispatch_type)
        VALUES (:dispatch_id, :run_id, 'VALIDATE_RUN')
        ON CONFLICT (run_id) WHERE dispatch_type = 'VALIDATE_RUN' DO NOTHING
        RETURNING dispatch_id
    """), {"dispatch_id": uuid.uuid4(), "run_id": run_id})).first()
    await _notify(conn, run_id)
    return inserted is not None


async def enqueue_reissue(conn: AsyncConnection, run_id: UUID, partition_id: str) -> UUID:
    """stale 파티션 재발행 요청을 outbox에 넣는다(sweeper REISSUE 모드)."""
    dispatch_id = uuid.uuid4()
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_dispatch (dispatch_id, run_id, dispatch_type, partition_id)
        VALUES (:dispatch_id, :run_id, 'REISSUE_PARTITION', :partition_id)
    """), {"dispatch_id": dispatch_id, "run_id": run_id, "partition_id": partition_id})
    await _notify(conn, run_id)
    return dispatch_id


async def list_for_run(conn: AsyncConnection, run_id: UUID) -> list[DispatchRow]:
    """run의 dispatch 목록(조회 API용)."""
    rows = (await conn.execute(text("""
        SELECT dispatch_id, dispatch_type, partition_id, status, attempt_count, sent_at, acked_at
          FROM nifi_ops.load_dispatch WHERE run_id = :run_id ORDER BY created_at
    """), {"run_id": run_id})).mappings().all()
    return [DispatchRow(**dict(m)) for m in rows]


async def get_for_update(conn: AsyncConnection, dispatch_id: UUID) -> tuple[UUID, str, str] | None:
    """dispatch 행을 잠그고 (run_id, type, status)를 돌려준다."""
    row = (await conn.execute(text("""
        SELECT run_id, dispatch_type, status FROM nifi_ops.load_dispatch
         WHERE dispatch_id = :dispatch_id FOR UPDATE
    """), {"dispatch_id": dispatch_id})).first()
    return (row[0], str(row[1]), str(row[2])) if row else None


async def lease_due(conn: AsyncConnection, *, batch: int, lease: timedelta) -> list[LeasedDispatch]:
    """전송할 행을 lease로 선점한다. 전송 동안 트랜잭션을 열어 두지 않기 위해서다."""
    rows = (await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch d
           SET attempt_count   = d.attempt_count + 1,
               next_attempt_at = clock_timestamp() + CAST(:lease AS interval)
          FROM nifi_ops.load_run r
         WHERE r.run_id = d.run_id
           AND d.dispatch_id IN (
                SELECT dispatch_id
                  FROM nifi_ops.load_dispatch
                 WHERE status = 'PENDING' AND next_attempt_at <= clock_timestamp()
                 ORDER BY next_attempt_at
                 LIMIT :batch
                 FOR UPDATE SKIP LOCKED)
        RETURNING d.dispatch_id, d.run_id, d.dispatch_type, d.partition_id, d.attempt_count, r.job_key
    """), {"batch": batch, "lease": lease})).mappings().all()
    return [LeasedDispatch(**dict(m)) for m in rows]


async def mark_sent(conn: AsyncConnection, dispatch_id: UUID, http_status: int) -> bool:
    """NiFi가 2xx로 받았음을 기록한다. PENDING일 때만 바꾼다."""
    # PENDING일 때만 바꾼다. NiFi가 202 직후 /validation/start를 먼저 호출해 이미 ACKED일 수 있다.
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'SENT', sent_at = clock_timestamp(), last_http_status = :http_status,
               last_error = NULL
         WHERE dispatch_id = :dispatch_id AND status = 'PENDING'
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id, "http_status": http_status})
    return result.first() is not None


async def schedule_retry(conn: AsyncConnection, dispatch_id: UUID, *, http_status: int | None,
                         error: str, delay: timedelta) -> None:
    """전송 실패: delay 뒤에 다시 보내도록 next_attempt_at을 미룬다."""
    await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET next_attempt_at = clock_timestamp() + CAST(:delay AS interval),
               last_http_status = :http_status, last_error = :error
         WHERE dispatch_id = :dispatch_id AND status = 'PENDING'
    """), {"dispatch_id": dispatch_id, "delay": delay, "http_status": http_status,
           "error": error[:2000]})


async def mark_dead(conn: AsyncConnection, dispatch_id: UUID, *, http_status: int | None,
                    error: str) -> bool:
    """더 보내지 않는다. 운영자가 resend로 되살릴 수 있다."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'DEAD', last_http_status = :http_status, last_error = :error
         WHERE dispatch_id = :dispatch_id AND status = 'PENDING'
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id, "http_status": http_status, "error": error[:2000]})
    return result.first() is not None


async def ack(conn: AsyncConnection, dispatch_id: UUID) -> bool:
    """검증 flow가 /validation/start로 실제 시작을 알렸다."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'ACKED', acked_at = clock_timestamp()
         WHERE dispatch_id = :dispatch_id AND status IN ('PENDING', 'SENT', 'DEAD')
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id})
    return result.first() is not None


async def ack_reissue(conn: AsyncConnection, run_id: UUID, partition_id: str) -> None:
    """재발행된 파티션이 claim되면 해당 REISSUE dispatch를 ACK로 본다."""
    await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'ACKED', acked_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id
           AND dispatch_type = 'REISSUE_PARTITION' AND status IN ('PENDING', 'SENT', 'DEAD')
    """), {"run_id": run_id, "partition_id": partition_id})


async def resend(conn: AsyncConnection, run_id: UUID, dispatch_id: UUID) -> bool:
    """운영자 재전송: DEAD 또는 SENT를 PENDING으로 되돌리고 시도 횟수를 초기화한다."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'PENDING', attempt_count = 0, next_attempt_at = clock_timestamp(),
               last_error = NULL
         WHERE dispatch_id = :dispatch_id AND run_id = :run_id AND status IN ('DEAD', 'SENT')
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id, "run_id": run_id})
    if result.first() is None:
        return False
    await _notify(conn, run_id)
    return True


async def build_body(conn: AsyncConnection, d: LeasedDispatch) -> dict[str, Any]:
    """API→NiFi 호출 본문. 재발행은 Worker 실행에 필요한 값을 모두 담는다."""
    body: dict[str, Any] = {"runId": str(d.run_id), "dispatchId": str(d.dispatch_id)}
    if d.dispatch_type != "REISSUE_PARTITION":
        return body
    m = (await conn.execute(text("""
        SELECT r.business_key, r.snapshot_scn, r.hdfs_run_path, p.lower_bound, p.upper_bound,
               p.upper_inclusive, p.is_null_partition, p.expected_row_count
          FROM nifi_ops.load_partition p JOIN nifi_ops.load_run r ON r.run_id = p.run_id
         WHERE p.run_id = :run_id AND p.partition_id = :partition_id
    """), {"run_id": d.run_id, "partition_id": d.partition_id})).mappings().one()

    def num(v: object) -> str | None:
        return None if v is None else str(v)

    body.update({
        "partitionId": d.partition_id, "businessKey": m["business_key"],
        "snapshotScn": num(m["snapshot_scn"]), "hdfsRunPath": m["hdfs_run_path"],
        "lowerBound": num(m["lower_bound"]), "upperBound": num(m["upper_bound"]),
        "upperInclusive": m["upper_inclusive"], "isNullPartition": m["is_null_partition"],
        "expectedRowCount": m["expected_row_count"]})
    return body


async def backlog(conn: AsyncConnection) -> dict[str, int]:
    """상태별 미완료 dispatch 수(메트릭용)."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_dispatch
         WHERE status IN ('PENDING', 'SENT', 'DEAD') GROUP BY status
    """))).all()
    counts = {"PENDING": 0, "SENT": 0, "DEAD": 0}
    counts.update({str(r[0]): int(r[1]) for r in rows})
    return counts
