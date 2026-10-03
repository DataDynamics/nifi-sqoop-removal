"""정리 대상 조회와 정리 기록 SQL.

정리(staging table DROP, HDFS run 경로 삭제)는 NiFi PG-70이 실제로 하고, 이 모듈은 "어떤 run이 정리
대상인가"를 판정하는 조건(_DUE)과 정리 완료 시각(load_run.cleaned_at) 기록만 맡는다. 정리 대상은
보존 기간이 지난 끝난 run이다(SUCCESS는 success_retention, 실패·TIMED_OUT은 failed_retention).
PUBLISH_UNKNOWN처럼 아직 끝나지 않은 상태는 domain의 CLEANUP_*_STATUSES에 없으므로 대상이 되지 않는다.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.domain import CLEANUP_FAILED_STATUSES, CLEANUP_SUCCESS_STATUSES


@dataclass(frozen=True, slots=True)
class CleanupRow:
    """정리 판정에 필요한 load_run 컬럼. ended_at은 COALESCE(completed_at, heartbeat_at)이다."""

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
    """_DUE 조건에 넘길 바인드 파라미터를 만든다.

    상태 집합은 ANY(:...)에 배열로 넘기기 위해 list로 바꾼다(정렬은 SQL 로그를 안정적으로 보이게 할 뿐이다).
    retention은 timedelta를 그대로 넘기고 SQL에서 interval로 CAST한다.
    """
    return {"success_statuses": sorted(CLEANUP_SUCCESS_STATUSES),
            "failed_statuses": sorted(CLEANUP_FAILED_STATUSES),
            "success_retention": success_retention, "failed_retention": failed_retention}


async def due_runs(conn: AsyncConnection, *, job_key: str | None, success_retention: timedelta,
                   failed_retention: timedelta, limit: int) -> list[CleanupRow]:
    """보존 기간이 지났고 아직 정리하지 않은 끝난 run을 끝난 시각 순으로 돌려준다.

    잠그지 않는 읽기 전용 조회다. 후보 목록(GET /cleanup/candidates)과 모니터의 정리 대상 수에 쓴다.
    job_key가 None이면 전체 job을 본다. 실제 정리 기록 시점에는 lock()으로 다시 판정한다.
    """
    # CAST(:job_key AS varchar) IS NULL: 파라미터 타입을 고정해 "job_key 필터 없음"을 한 SQL로 표현한다.
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
    """run 행을 잠그고 (행, 지금 정리 대상인지)를 돌려준다.

    FOR UPDATE로 load_run 행을 잠근 채 _DUE를 같은 SELECT에서 평가하므로, 판정과 뒤이은
    mark_cleaned 사이에 다른 요청이 상태를 바꿀 수 없다. run이 없으면 None을 돌려준다.
    due는 cleaned_at 여부와 무관하게 상태·보존 기간만 본 값이다. 이미 정리됐는지는 호출자가
    cleaned_at으로 본다.
    """
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
    """정리 시각을 한 번만 기록한다.

    WHERE cleaned_at IS NULL 조건 때문에 두 번째 호출은 아무 행도 바꾸지 않는다. 처음 기록했으면 True,
    이미 기록돼 있었으면 False를 돌려준다.
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run SET cleaned_at = clock_timestamp()
         WHERE run_id = :run_id AND cleaned_at IS NULL
        RETURNING run_id
    """), {"run_id": run_id})
    return result.first() is not None
