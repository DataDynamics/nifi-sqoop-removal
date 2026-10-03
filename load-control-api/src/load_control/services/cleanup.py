"""staging table·run 경로 정리: 대상 판정과 기록. 실제 삭제는 NiFi PG-70이 한다.

흐름: PG-70이 candidates로 대상 run을 받아 staging table DROP과 HDFS run 경로 삭제를 한 뒤 mark_cleaned로
결과를 알린다. API는 run 상태를 바꾸지 않고 load_run.cleaned_at과 RUN_CLEANED 이벤트만 남긴다.
"""

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
    """보존 기간이 지난 끝난 run(SUCCESS: success_retention, 실패·TIMED_OUT: failed_retention).

    읽기 전용이며 잠그지 않는다. limit은 설정의 max_batch를 넘지 않게 줄인다. completed_at에는 끝난
    시각(completed_at이 없으면 heartbeat_at)을 담는다.
    """
    rows = await cleanup.due_runs(conn, job_key=job_key, success_retention=settings.success_retention,
                                  failed_retention=settings.failed_retention,
                                  limit=min(limit, settings.max_batch))
    return CleanupCandidatesResponse(runs=[CleanupCandidate(
        run_id=str(r.run_id), job_key=r.job_key, business_key=r.business_key, status=RunStatus(r.status),
        hdfs_run_path=r.hdfs_run_path, stage_table=r.stage_table_name, completed_at=r.ended_at)
        for r in rows])


async def mark_cleaned(conn: AsyncConnection, settings: CleanupSettings, run_id: UUID,
                       req: CleanupRequest) -> CleanupResponse:
    """NiFi가 지운 뒤 호출한다. 정리 대상이 아닌 run(진행 중, 보존 기간 안)은 409로 거부한다.

    load_run 행을 잠근 채 정리 대상 여부를 다시 판정한다(후보 조회 이후 상태가 바뀌었을 수 있다).
    - 이미 cleaned_at이 있으면 changed=False로 성공 응답한다(재요청 멱등).
    - 대상이면 cleaned_at을 기록하고 RUN_CLEANED 이벤트(삭제한 table·경로)를 남긴다. run 상태는 그대로다.

    Raises:
        NotFound: RUN_NOT_FOUND.
        Conflict: CLEANUP_NOT_DUE. 진행 중이거나 보존 기간이 아직 남은 run.
    """
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
    # 이벤트에 job_key·business_key를 채우려고 RunRow를 읽는다(행은 위에서 이미 잠갔다).
    run = await runs.get(conn, run_id)
    await events.record(conn, "RUN_CLEANED", run, details={
        "droppedTable": req.dropped_table, "deletedPath": req.deleted_path})
    log.info("run_cleaned", runId=str(run_id), jobKey=row.job_key, runStatus=row.status,
             droppedTable=req.dropped_table, deletedPath=req.deleted_path)
    return CleanupResponse(run_status=RunStatus(row.status), changed=True)
