"""정리 대상 조회와 정리 기록 SQL."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.domain import CLEANUP_FAILED_STATUSES, CLEANUP_SUCCESS_STATUSES


@dataclass(frozen=True, slots=True)
class CleanupRow:
    run_id: UUID
    job_key: str
    business_key: str
    status: str
    hdfs_run_path: str | None
    stage_table_name: str | None
    ended_at: datetime
    cleaned_at: datetime | None


_COLUMNS = """run_id, job_key, business_key, status, hdfs_run_path, stage_table_name,
       COALESCE(completed_at, heartbeat_at) AS ended_at, cleaned_at"""

# 끝난 시각은 completed_at. 오래된 실패 경로가 completed_at을 남기지 않았어도 heartbeat_at으로 대신한다.
_DUE = """(
        (status = ANY(:success_statuses)
         AND COALESCE(completed_at, heartbeat_at) < clock_timestamp() - CAST(:success_retention AS interval))
     OR (status = ANY(:failed_statuses)
         AND COALESCE(completed_at, heartbeat_at) < clock_timestamp() - CAST(:failed_retention AS interval))
    )"""


def _due_params(success_retention: timedelta, failed_retention: timedelta) -> dict[str, object]:
    return {"success_statuses": sorted(CLEANUP_SUCCESS_STATUSES),
            "failed_statuses": sorted(CLEANUP_FAILED_STATUSES),
            "success_retention": success_retention, "failed_retention": failed_retention}


async def due_runs(conn: AsyncConnection, *, job_key: str | None, success_retention: timedelta,
                   failed_retention: timedelta, limit: int) -> list[CleanupRow]:
    """보존 기간이 지났고 아직 정리하지 않은 끝난 run."""
    rows = (await conn.execute(text(f"""
        SELECT {_COLUMNS} FROM nifi_ops.load_run
         WHERE cleaned_at IS NULL
           AND (CAST(:job_key AS varchar) IS NULL OR job_key = :job_key)
           AND {_DUE}
         ORDER BY ended_at, run_id
         LIMIT :limit
    """), {"job_key": job_key, "limit": limit,
           **_due_params(success_retention, failed_retention)})).mappings().all()
    return [CleanupRow(**r) for r in rows]


async def lock(conn: AsyncConnection, run_id: UUID, *, success_retention: timedelta,
               failed_retention: timedelta) -> tuple[CleanupRow, bool] | None:
    """run 행을 잠그고 (행, 지금 정리 대상인지)를 돌려준다."""
    m = (await conn.execute(text(f"""
        SELECT {_COLUMNS}, {_DUE} AS due FROM nifi_ops.load_run
         WHERE run_id = :run_id FOR UPDATE
    """), {"run_id": run_id, **_due_params(success_retention, failed_retention)})).mappings().first()
    if m is None:
        return None
    data = dict(m)
    due = bool(data.pop("due"))
    return CleanupRow(**data), due


async def mark_cleaned(conn: AsyncConnection, run_id: UUID) -> bool:
    """정리 시각을 한 번만 기록한다."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run SET cleaned_at = clock_timestamp()
         WHERE run_id = :run_id AND cleaned_at IS NULL
        RETURNING run_id
    """), {"run_id": run_id})
    return result.first() is not None
