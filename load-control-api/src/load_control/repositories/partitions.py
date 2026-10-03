"""load_partition(파티션 manifest) SQL.

파티션 상태: PENDING → RUNNING → SUCCESS | FAILED. sweeper 재발행 시 RUNNING → RETRY → RUNNING,
timeout 시 미완료 파티션은 TIMED_OUT. 상태를 바꾸는 UPDATE는 WHERE에 기대 상태(와 claim token)를 넣어
CAS로 실행한다. 행 잠금은 반드시 load_run 행을 먼저 잠근 뒤에 잡는다(잠금 순서 load_run → load_partition).
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


@dataclass(frozen=True, slots=True)
class PartitionRow:
    """load_partition 한 행."""

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
    """manifest 일괄 등록. 0건 파티션은 바로 SUCCESS(actual=0)로 넣는다.

    rows는 파티션마다 run_id, partition_id, lower_bound, upper_bound, upper_inclusive, is_null_partition,
    expected_row_count를 담은 dict 목록이며 executemany로 한 번에 넣는다. 0건 파티션은 Worker에 보내지
    않으므로 actual_row_count·file_count·fragment_count=0, completed_at=지금으로 완료 상태를 미리 채운다.
    (run_id, partition_id) 기본키 때문에 같은 manifest를 두 번 넣으면 IntegrityError가 난다.
    """
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
    """run의 파티션 전체(partition_id 순, 잠그지 않음)."""
    rows = (await conn.execute(
        text(f"SELECT {_COLUMNS} FROM nifi_ops.load_partition WHERE run_id = :run_id "
             "ORDER BY partition_id"), {"run_id": run_id})).mappings().all()
    return [PartitionRow(**dict(m)) for m in rows]


async def lock(conn: AsyncConnection, run_id: UUID, partition_id: str) -> PartitionRow | None:
    """파티션 행을 잠근다. 반드시 run 행을 먼저 잠근 뒤 호출한다(잠금 순서).

    순서를 지키지 않으면 run 판정(try_complete_extract)과 파티션 보고가 서로 다른 순서로 잠가 deadlock이
    날 수 있다. 행이 없으면 None을 돌려준다.
    """
    m = (await conn.execute(text(f"""
        SELECT {_COLUMNS} FROM nifi_ops.load_partition
         WHERE run_id = :run_id AND partition_id = :partition_id
           FOR UPDATE
    """), {"run_id": run_id, "partition_id": partition_id})).mappings().first()
    return PartitionRow(**dict(m)) if m else None


async def claim(conn: AsyncConnection, run_id: UUID, partition_id: str, *, claim_token: UUID,
                worker_node: str) -> int:
    """PENDING/RETRY → RUNNING. 새 attempt 번호를 돌려준다. 호출 전 상태 확인은 서비스 계층 책임.

    claim_token·worker_node를 새 소유자로 바꾸고 attempt_count를 1 올린다. started_at은 첫 claim 시각을
    유지하고(COALESCE), 이전 시도의 오류 정보는 지운다. 이후 이 파티션의 chunk·실패 보고는 같은
    claim_token일 때만 받아들인다.

    Raises:
        AssertionError: 파티션이 PENDING/RETRY가 아니어서 한 행도 바뀌지 않은 경우(서비스 계층 버그).
    """
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
    """heartbeat 갱신. sweeper의 stale 판정 기준이다.

    상태와 무관하게 갱신한다. claim 재요청과 chunk 보고 때 호출해 RUNNING 파티션이 살아 있음을 알린다.
    """
    await conn.execute(text("""
        UPDATE nifi_ops.load_partition SET heartbeat_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id
    """), {"run_id": run_id, "partition_id": partition_id})


async def mark_success(conn: AsyncConnection, run_id: UUID, partition_id: str, *, claim_token: UUID,
                       rows: int, files: int, bytes_: int) -> bool:
    """RUNNING → SUCCESS. claim token이 맞을 때만 바꾼다.

    WHERE claim_token = :claim_token AND status = 'RUNNING'이 CAS 조건이다. 재발행으로 소유자가 바뀌었거나
    이미 끝난 파티션이면 아무것도 바꾸지 않고 False를 돌려준다. 집계한 row·file·byte 수를 함께 저장한다
    (fragment_count는 file_count와 같은 값).
    """
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
    """미완료 파티션을 FAILED로 바꾼다.

    PENDING·RUNNING·RETRY일 때만 바꾸고 SUCCESS·FAILED·TIMED_OUT은 그대로 둔다(이미 끝난 파티션을 덮어쓰지
    않음). claim token은 보지 않으므로 소유권 확인은 호출자가 한다. message는 2000자로 자르고 빈 값은
    NULL로 저장한다. 바꿨으면 True.
    """
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
    """run의 상태별 파티션 수. 0건인 상태는 결과에 없다."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_partition WHERE run_id = :run_id GROUP BY status
    """), {"run_id": run_id})).all()
    return {str(r[0]): int(r[1]) for r in rows}
