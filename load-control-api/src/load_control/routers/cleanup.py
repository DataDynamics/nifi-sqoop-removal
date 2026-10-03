"""정리 엔드포인트(NiFi PG-70 Cleanup이 호출).

API는 대상 판정과 기록만 한다. staging table DROP과 HDFS run 경로 삭제는 NiFi가 한다.
"""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from load_control.routers.deps import run_tx
from load_control.schemas.cleanup import CleanupCandidatesResponse, CleanupRequest, CleanupResponse
from load_control.security import require_role
from load_control.services import cleanup

router = APIRouter(prefix="/v1", tags=["cleanup"])


# jobKey 필터. schemas.common.JobKey와 같은 형식
JobKeyQuery = Annotated[str | None, Query(alias="jobKey", pattern=r"^[A-Z0-9_]{1,200}$")]


@router.get("/cleanup/candidates", response_model=CleanupCandidatesResponse,
            dependencies=[Depends(require_role("nifi", "operator"))])
async def list_candidates(request: Request, job_key: JobKeyQuery = None,
                          limit: Annotated[int, Query(ge=1, le=500)] = 50) -> CleanupCandidatesResponse:
    """보존 기간이 지나 staging table과 run 경로를 지울 run 목록.

    cleaned_at이 비어 있고 끝난 시각이 보존 기간(SUCCESS: cleanup.success_retention, 실패·TIMED_OUT:
    cleanup.failed_retention)을 지난 run을 끝난 시각이 오래된 순으로 돌려준다. 진행 중인 run과
    PUBLISH_UNKNOWN은 제외한다. 개수는 limit과 cleanup.max_batch 중 작은 값까지. 상태를 바꾸지 않는다.
    """
    settings = request.app.state.settings.cleanup
    return await run_tx(request, lambda conn: cleanup.candidates(
        conn, settings, job_key=job_key, limit=limit))


@router.post("/runs/{run_id}/cleanup", response_model=CleanupResponse,
             dependencies=[Depends(require_role("nifi", "operator"))])
async def report_cleanup(run_id: UUID, body: CleanupRequest, request: Request) -> CleanupResponse:
    """정리 완료를 기록한다. 정리 대상이 아니면 409 CLEANUP_NOT_DUE.

    NiFi가 경로 검사로 거부한 run(예: HDFS.STAGE.ROOT 변경 전 run)은 운영자가 직접 지우고 기록한다.

    후보 목록과 같은 조건을 run 행을 잠근 채 다시 확인한 뒤 cleaned_at과 RUN_CLEANED 이벤트를 남긴다.
    본문(droppedTable, deletedPath)은 이벤트 기록용이다. 이미 기록된 run은 changed=false(멱등),
    run이 없으면 404 RUN_NOT_FOUND.
    """
    settings = request.app.state.settings.cleanup
    return await run_tx(request, lambda conn: cleanup.mark_cleaned(conn, settings, run_id, body))
