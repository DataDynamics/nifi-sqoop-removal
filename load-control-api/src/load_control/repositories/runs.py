"""load_run SQL. 모든 상태 전이는 WHERE status = 기대 상태 조건(CAS)으로 실행한다."""

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
    """load_run 한 행(서비스 계층에 필요한 컬럼만)."""

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
    """CREATED run을 만든다. 같은 업무키의 활성 run이 있으면 uq_load_run_active 위반."""
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_run (
            run_id, job_key, business_key, status, hdfs_run_path, stage_table_name, parameters)
        VALUES (:run_id, :job_key, :business_key, 'CREATED', :hdfs_run_path, :stage_table_name,
                CAST(:parameters AS jsonb))
    """), {"run_id": run_id, "job_key": job_key, "business_key": business_key,
           "hdfs_run_path": hdfs_run_path, "stage_table_name": stage_table_name,
           "parameters": json.dumps(parameters, ensure_ascii=False)})


async def get(conn: AsyncConnection, run_id: UUID) -> RunRow | None:
    """run 한 행(잠그지 않음)."""
    m = (await conn.execute(text(f"SELECT {_COLUMNS} FROM nifi_ops.load_run WHERE run_id = :run_id"),
                            {"run_id": run_id})).mappings().first()
    return _row(m) if m else None


async def lock(conn: AsyncConnection, run_id: UUID) -> RunRow | None:
    """run 행을 잠근다. 같은 run의 판정을 직렬화하는 핵심 잠금."""
    m = (await conn.execute(
        text(f"SELECT {_COLUMNS} FROM nifi_ops.load_run WHERE run_id = :run_id FOR UPDATE"),
        {"run_id": run_id})).mappings().first()
    return _row(m) if m else None


async def touch(conn: AsyncConnection, run_id: UUID) -> None:
    """run heartbeat 갱신."""
    await conn.execute(text(
        "UPDATE nifi_ops.load_run SET heartbeat_at = clock_timestamp() WHERE run_id = :run_id"),
        {"run_id": run_id})


async def start_extracting(conn: AsyncConnection, run_id: UUID, *, snapshot_scn: Decimal | None,
                           source_count: int, source_null_split_count: int,
                           source_min_split: Decimal | None, source_max_split: Decimal | None,
                           expected_partition_count: int, empty_partition_count: int) -> bool:
    """manifest 등록과 함께 CREATED → EXTRACTING. SCN과 source 지표를 저장한다."""
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
    """모든 파티션 SUCCESS이고 합계가 source count와 같으면 EXTRACTED_VALIDATED로 CAS."""
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
    """성공 파티션 수 +1(진행률 표시용, 최종 판정은 try_complete_extract가 다시 센다)."""
    await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET success_partition_count = success_partition_count + 1
         WHERE run_id = :run_id
    """), {"run_id": run_id})


async def increment_failed(conn: AsyncConnection, run_id: UUID) -> None:
    """실패 파티션 수 +1."""
    await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET failed_partition_count = failed_partition_count + 1
         WHERE run_id = :run_id
    """), {"run_id": run_id})


async def fail(conn: AsyncConnection, run_id: UUID, *, expected: str, to: str, stage: str,
               code: str, message: str | None) -> bool:
    """expected 상태일 때만 실패 상태로 바꾸고 오류 정보를 남긴다."""
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


# cas_status에서 함께 갱신할 수 있는 컬럼. 값 대신 SQL 표현식을 쓰려면 _NOW를 넘긴다.
_CAS_COLUMNS = frozenset({
    "staging_count", "target_count", "publish_token", "publish_started_at", "published_at",
    "completed_at", "error_stage", "error_code", "error_message"})
NOW = object()


async def cas_status(conn: AsyncConnection, run_id: UUID, *, expected: str, to: str,
                     **sets: Any) -> bool:
    """WHERE status = expected 조건의 상태 전이. 갱신됐으면 True."""
    unknown = set(sets) - _CAS_COLUMNS
    if unknown:
        raise ValueError(f"cas_status: unsupported columns {unknown}")
    clauses = ["status = :to", "heartbeat_at = clock_timestamp()", "version_no = version_no + 1"]
    params: dict[str, Any] = {"run_id": run_id, "expected": expected, "to": to}
    for col, value in sets.items():
        if value is NOW:
            clauses.append(f"{col} = clock_timestamp()")
        else:
            clauses.append(f"{col} = :{col}")
            params[col] = value
    result = await conn.execute(text(f"""
        UPDATE nifi_ops.load_run SET {", ".join(clauses)}
         WHERE run_id = :run_id AND status = :expected
        RETURNING run_id
    """), params)
    return result.first() is not None


async def get_publish_token(conn: AsyncConnection, run_id: UUID) -> UUID | None:
    """현재 publish token."""
    row = (await conn.execute(text("SELECT publish_token FROM nifi_ops.load_run WHERE run_id = :run_id"),
                              {"run_id": run_id})).first()
    return row[0] if row else None


async def list_runs(conn: AsyncConnection, *, job_key: str | None, business_key: str | None,
                    status: str | None, limit: int) -> list[RunRow]:
    """조건에 맞는 run 목록(최근 시작 순)."""
    rows = (await conn.execute(text(f"""
        SELECT {_COLUMNS} FROM nifi_ops.load_run
         WHERE (CAST(:job_key AS varchar) IS NULL OR job_key = :job_key)
           AND (CAST(:business_key AS varchar) IS NULL OR business_key = :business_key)
           AND (CAST(:status AS varchar) IS NULL OR status = :status)
         ORDER BY started_at DESC
         LIMIT :limit
    """), {"job_key": job_key, "business_key": business_key, "status": status,
           "limit": limit})).mappings().all()
    return [_row(m) for m in rows]
