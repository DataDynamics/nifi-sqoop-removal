import uuid
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

CHANNEL = "load_dispatch"


@dataclass(frozen=True, slots=True)
class DispatchRow:
    dispatch_id: UUID
    dispatch_type: str
    partition_id: str | None
    status: str
    attempt_count: int
    sent_at: datetime | None
    acked_at: datetime | None


async def enqueue_validation(conn: AsyncConnection, run_id: UUID) -> bool:
    """검증 호출을 outbox에 예약하고 dispatcher를 깨운다(API 설계 4장, 9.6).

    pg_notify는 commit될 때만 전달되므로 rollback된 완료가 dispatcher를 깨우지 않는다.
    uq_load_dispatch_validate가 run당 1행을 보장하는 마지막 방어선이다.
    """
    inserted = (await conn.execute(text("""
        INSERT INTO nifi_ops.load_dispatch (dispatch_id, run_id, dispatch_type)
        VALUES (:dispatch_id, :run_id, 'VALIDATE_RUN')
        ON CONFLICT (run_id) WHERE dispatch_type = 'VALIDATE_RUN' DO NOTHING
        RETURNING dispatch_id
    """), {"dispatch_id": uuid.uuid4(), "run_id": run_id})).first()
    await conn.execute(text("SELECT pg_notify(:channel, :payload)"),
                       {"channel": CHANNEL, "payload": str(run_id)})
    return inserted is not None


async def list_for_run(conn: AsyncConnection, run_id: UUID) -> list[DispatchRow]:
    rows = (await conn.execute(text("""
        SELECT dispatch_id, dispatch_type, partition_id, status, attempt_count, sent_at, acked_at
          FROM nifi_ops.load_dispatch WHERE run_id = :run_id ORDER BY created_at
    """), {"run_id": run_id})).mappings().all()
    return [DispatchRow(**dict(m)) for m in rows]
