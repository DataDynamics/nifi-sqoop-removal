"""staging table·run 경로 정리: 대상 판정과 기록. 실제 삭제는 NiFi PG-70이 한다."""

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.config import CleanupSettings
from load_control.domain import RunStatus
from load_control.errors import Conflict, NotFound
from load_control.repositories import cleanup, events, runs
from load_control.schemas.cleanup import (
    CleanupCandidate,
    CleanupCandidatesResponse,
    CleanupRequest,
    CleanupResponse,
)

log = structlog.get_logger(__name__)


async def candidates(conn: AsyncConnection, settings: CleanupSettings, *, job_key: str | None,
                     limit: int) -> CleanupCandidatesResponse:
    """보존 기간이 지난 끝난 run(SUCCESS: success_retention, 실패·TIMED_OUT: failed_retention)."""
    rows = await cleanup.due_runs(conn, job_key=job_key, success_retention=settings.success_retention,
                                  failed_retention=settings.failed_retention,
                                  limit=min(limit, settings.max_batch))
    return CleanupCandidatesResponse(runs=[CleanupCandidate(
        run_id=str(r.run_id), job_key=r.job_key, business_key=r.business_key, status=RunStatus(r.status),
        hdfs_run_path=r.hdfs_run_path, stage_table=r.stage_table_name, completed_at=r.ended_at)
        for r in rows])


async def mark_cleaned(conn: AsyncConnection, settings: CleanupSettings, run_id: UUID,
                       req: CleanupRequest) -> CleanupResponse:
    """NiFi가 지운 뒤 호출한다. 정리 대상이 아닌 run(진행 중, 보존 기간 안)은 409로 거부한다."""
    locked = await cleanup.lock(conn, run_id, success_retention=settings.success_retention,
                                failed_retention=settings.failed_retention)
    if locked is None:
        raise NotFound("RUN_NOT_FOUND")
    row, due = locked
    if row.cleaned_at is not None:
        return CleanupResponse(run_status=RunStatus(row.status), changed=False)
    if not due:
        log.warning("cleanup_rejected", runId=str(run_id), runStatus=row.status)
        raise Conflict("CLEANUP_NOT_DUE", runStatus=row.status)
    await cleanup.mark_cleaned(conn, run_id)
    run = await runs.get(conn, run_id)
    await events.record(conn, "RUN_CLEANED", run, details={
        "droppedTable": req.dropped_table, "deletedPath": req.deleted_path})
    log.info("run_cleaned", runId=str(run_id), jobKey=row.job_key, runStatus=row.status,
             droppedTable=req.dropped_table, deletedPath=req.deleted_path)
    return CleanupResponse(run_status=RunStatus(row.status), changed=True)
