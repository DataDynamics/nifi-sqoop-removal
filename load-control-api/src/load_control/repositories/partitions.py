from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


@dataclass(frozen=True, slots=True)
class PartitionRow:
    run_id: UUID
    partition_id: str
    lower_bound: Decimal | None
    upper_bound: Decimal | None
    upper_inclusive: bool
    is_null_partition: bool
    status: str
    expected_row_count: int
    actual_row_count: int | None
    file_count: int | None
    attempt_count: int
    claim_token: UUID | None
    worker_node: str | None
    error_code: str | None


_COLUMNS = """run_id, partition_id, lower_bound, upper_bound, upper_inclusive, is_null_partition,
       status, expected_row_count, actual_row_count, file_count, attempt_count, claim_token,
       worker_node, error_code"""


async def insert_many(conn: AsyncConnection, rows: list[dict[str, Any]]) -> None:
    """manifest 일괄 등록. 0건 파티션은 바로 SUCCESS(actual=0)로 넣는다(API 설계 3.4)."""
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_partition (
            run_id, partition_id, lower_bound, upper_bound, upper_inclusive, is_null_partition,
            status, expected_row_count, actual_row_count, file_count, fragment_count, completed_at)
        VALUES (
            :run_id, :partition_id, :lower_bound, :upper_bound, :upper_inclusive, :is_null_partition,
            CASE WHEN :expected_row_count = 0 THEN 'SUCCESS' ELSE 'PENDING' END,
            :expected_row_count,
            CASE WHEN :expected_row_count = 0 THEN 0 END,
            CASE WHEN :expected_row_count = 0 THEN 0 END,
            CASE WHEN :expected_row_count = 0 THEN 0 END,
            CASE WHEN :expected_row_count = 0 THEN clock_timestamp() END)
    """), rows)


async def list_for_run(conn: AsyncConnection, run_id: UUID) -> list[PartitionRow]:
    rows = (await conn.execute(
        text(f"SELECT {_COLUMNS} FROM nifi_ops.load_partition WHERE run_id = :run_id "
             "ORDER BY partition_id"), {"run_id": run_id})).mappings().all()
    return [PartitionRow(**dict(m)) for m in rows]


async def lock(conn: AsyncConnection, run_id: UUID, partition_id: str) -> PartitionRow | None:
    m = (await conn.execute(text(f"""
        SELECT {_COLUMNS} FROM nifi_ops.load_partition
         WHERE run_id = :run_id AND partition_id = :partition_id
           FOR UPDATE
    """), {"run_id": run_id, "partition_id": partition_id})).mappings().first()
    return PartitionRow(**dict(m)) if m else None


async def claim(conn: AsyncConnection, run_id: UUID, partition_id: str, *, claim_token: UUID,
                worker_node: str) -> int:
    """PENDING/RETRY → RUNNING. 새 attempt 번호를 돌려준다. 호출 전 상태 확인은 서비스 계층 책임."""
    m = (await conn.execute(text("""
        UPDATE nifi_ops.load_partition
           SET status = 'RUNNING',
               claim_token = :claim_token,
               worker_node = :worker_node,
               attempt_count = attempt_count + 1,
               started_at = COALESCE(started_at, clock_timestamp()),
               heartbeat_at = clock_timestamp(),
               error_code = NULL,
               error_message = NULL
         WHERE run_id = :run_id AND partition_id = :partition_id
           AND status IN ('PENDING', 'RETRY')
        RETURNING attempt_count
    """), {"run_id": run_id, "partition_id": partition_id, "claim_token": claim_token,
           "worker_node": worker_node})).first()
    if m is None:
        raise AssertionError("claim called on non-claimable partition")
    return int(m[0])


async def touch(conn: AsyncConnection, run_id: UUID, partition_id: str) -> None:
    await conn.execute(text("""
        UPDATE nifi_ops.load_partition SET heartbeat_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id
    """), {"run_id": run_id, "partition_id": partition_id})


async def mark_success(conn: AsyncConnection, run_id: UUID, partition_id: str, *, claim_token: UUID,
                       rows: int, files: int, bytes_: int) -> bool:
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_partition
           SET status = 'SUCCESS',
               actual_row_count = :rows,
               file_count = :files,
               fragment_count = :files,
               byte_count = :bytes,
               completed_at = clock_timestamp(),
               heartbeat_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id
           AND claim_token = :claim_token AND status = 'RUNNING'
        RETURNING partition_id
    """), {"run_id": run_id, "partition_id": partition_id, "claim_token": claim_token,
           "rows": rows, "files": files, "bytes": bytes_})
    return result.first() is not None


async def mark_failed(conn: AsyncConnection, run_id: UUID, partition_id: str, *, code: str,
                      message: str | None) -> bool:
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_partition
           SET status = 'FAILED',
               error_code = :code,
               error_message = :message,
               completed_at = clock_timestamp(),
               heartbeat_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id
           AND status IN ('PENDING', 'RUNNING', 'RETRY')
        RETURNING partition_id
    """), {"run_id": run_id, "partition_id": partition_id, "code": code,
           "message": (message or "")[:2000] or None})
    return result.first() is not None


async def count_by_status(conn: AsyncConnection, run_id: UUID) -> dict[str, int]:
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_partition WHERE run_id = :run_id GROUP BY status
    """), {"run_id": run_id})).all()
    return {str(r[0]): int(r[1]) for r in rows}
