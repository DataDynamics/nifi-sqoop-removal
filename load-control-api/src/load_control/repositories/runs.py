import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


@dataclass(frozen=True, slots=True)
class RunRow:
    run_id: UUID
    job_key: str
    business_key: str
    status: str
    snapshot_scn: Decimal | None
    source_count: int | None
    expected_partition_count: int | None
    success_partition_count: int
    failed_partition_count: int
    extracted_count: int
    hdfs_run_path: str | None
    stage_table_name: str | None
    parameters: dict[str, Any]
    started_at: datetime
    heartbeat_at: datetime
    extract_completed_at: datetime | None
    completed_at: datetime | None
    error_stage: str | None
    error_code: str | None
    error_message: str | None


_COLUMNS = """run_id, job_key, business_key, status, snapshot_scn, source_count,
       expected_partition_count, success_partition_count, failed_partition_count,
       extracted_count, hdfs_run_path, stage_table_name, parameters, started_at,
       heartbeat_at, extract_completed_at, completed_at, error_stage, error_code, error_message"""


def _row(m: Any) -> RunRow:
    data = dict(m)
    params = data["parameters"]
    data["parameters"] = json.loads(params) if isinstance(params, str) else (params or {})
    return RunRow(**data)


async def insert(conn: AsyncConnection, *, run_id: UUID, job_key: str, business_key: str,
                 hdfs_run_path: str, stage_table_name: str, parameters: dict[str, Any]) -> None:
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_run (
            run_id, job_key, business_key, status, hdfs_run_path, stage_table_name, parameters)
        VALUES (:run_id, :job_key, :business_key, 'CREATED', :hdfs_run_path, :stage_table_name,
                CAST(:parameters AS jsonb))
    """), {"run_id": run_id, "job_key": job_key, "business_key": business_key,
           "hdfs_run_path": hdfs_run_path, "stage_table_name": stage_table_name,
           "parameters": json.dumps(parameters, ensure_ascii=False)})


async def get(conn: AsyncConnection, run_id: UUID) -> RunRow | None:
    m = (await conn.execute(text(f"SELECT {_COLUMNS} FROM nifi_ops.load_run WHERE run_id = :run_id"),
                            {"run_id": run_id})).mappings().first()
    return _row(m) if m else None


async def lock(conn: AsyncConnection, run_id: UUID) -> RunRow | None:
    """run 행을 잠근다. 같은 run의 판정을 직렬화하는 핵심 잠금(API 설계 3.2)."""
    m = (await conn.execute(
        text(f"SELECT {_COLUMNS} FROM nifi_ops.load_run WHERE run_id = :run_id FOR UPDATE"),
        {"run_id": run_id})).mappings().first()
    return _row(m) if m else None


async def touch(conn: AsyncConnection, run_id: UUID) -> None:
    await conn.execute(text(
        "UPDATE nifi_ops.load_run SET heartbeat_at = clock_timestamp() WHERE run_id = :run_id"),
        {"run_id": run_id})


async def start_extracting(conn: AsyncConnection, run_id: UUID, *, snapshot_scn: Decimal | None,
                           source_count: int, source_null_split_count: int,
                           source_min_split: Decimal | None, source_max_split: Decimal | None,
                           expected_partition_count: int, empty_partition_count: int) -> bool:
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET status = 'EXTRACTING',
               snapshot_scn = :snapshot_scn,
               source_count = :source_count,
               source_null_split_count = :source_null_split_count,
               source_min_split = :source_min_split,
               source_max_split = :source_max_split,
               expected_partition_count = :expected_partition_count,
               success_partition_count = :empty_partition_count,
               heartbeat_at = clock_timestamp(),
               version_no = version_no + 1
         WHERE run_id = :run_id AND status = 'CREATED'
        RETURNING run_id
    """), {"run_id": run_id, "snapshot_scn": snapshot_scn, "source_count": source_count,
           "source_null_split_count": source_null_split_count,
           "source_min_split": source_min_split, "source_max_split": source_max_split,
           "expected_partition_count": expected_partition_count,
           "empty_partition_count": empty_partition_count})
    return result.first() is not None


async def try_complete_extract(conn: AsyncConnection, run_id: UUID) -> bool:
    """모든 파티션 SUCCESS이고 합계가 source count와 같으면 EXTRACTED_VALIDATED로 CAS(API 설계 3.3)."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run r
           SET status = 'EXTRACTED_VALIDATED',
               success_partition_count = s.success_cnt,
               extracted_count = s.row_sum,
               extract_completed_at = clock_timestamp(),
               heartbeat_at = clock_timestamp(),
               version_no = r.version_no + 1
          FROM (SELECT COUNT(*)                                   AS total_cnt,
                       COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_cnt,
                       COALESCE(SUM(actual_row_count), 0)         AS row_sum
                  FROM nifi_ops.load_partition
                 WHERE run_id = :run_id) s
         WHERE r.run_id = :run_id
           AND r.status = 'EXTRACTING'
           AND s.total_cnt = r.expected_partition_count
           AND s.success_cnt = r.expected_partition_count
           AND s.row_sum = r.source_count
        RETURNING r.run_id
    """), {"run_id": run_id})
    return result.first() is not None


async def increment_success(conn: AsyncConnection, run_id: UUID) -> None:
    await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET success_partition_count = success_partition_count + 1
         WHERE run_id = :run_id
    """), {"run_id": run_id})


async def increment_failed(conn: AsyncConnection, run_id: UUID) -> None:
    await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET failed_partition_count = failed_partition_count + 1
         WHERE run_id = :run_id
    """), {"run_id": run_id})


async def fail(conn: AsyncConnection, run_id: UUID, *, expected: str, to: str, stage: str,
               code: str, message: str | None) -> bool:
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET status = :to,
               error_stage = :stage,
               error_code = :code,
               error_message = :message,
               completed_at = clock_timestamp(),
               heartbeat_at = clock_timestamp(),
               version_no = version_no + 1
         WHERE run_id = :run_id AND status = :expected
        RETURNING run_id
    """), {"run_id": run_id, "expected": expected, "to": to, "stage": stage, "code": code,
           "message": (message or "")[:2000] or None})
    return result.first() is not None
