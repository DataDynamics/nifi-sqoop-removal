"""정리 엔드포인트(NiFi PG-70 Cleanup이 호출)."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from load_control.routers.deps import run_tx
from load_control.schemas.cleanup import CleanupCandidatesResponse, CleanupRequest, CleanupResponse
from load_control.security import require_role
from load_control.services import cleanup

router = APIRouter(prefix="/v1", tags=["cleanup"])


JobKeyQuery = Annotated[str | None, Query(alias="jobKey", pattern=r"^[A-Z0-9_]{1,200}$")]


@router.get("/cleanup/candidates", response_model=CleanupCandidatesResponse,
            dependencies=[Depends(require_role("nifi", "operator"))])
async def list_candidates(request: Request, job_key: JobKeyQuery = None,
                          limit: Annotated[int, Query(ge=1, le=500)] = 50) -> CleanupCandidatesResponse:
    """보존 기간이 지나 staging table과 run 경로를 지울 run 목록."""
    settings = request.app.state.settings.cleanup
    return await run_tx(request, lambda conn: cleanup.candidates(
        conn, settings, job_key=job_key, limit=limit))


@router.post("/runs/{run_id}/cleanup", response_model=CleanupResponse,
             dependencies=[Depends(require_role("nifi", "operator"))])
async def report_cleanup(run_id: UUID, body: CleanupRequest, request: Request) -> CleanupResponse:
    """정리 완료를 기록한다. 정리 대상이 아니면 409 CLEANUP_NOT_DUE.

    NiFi가 경로 검사로 거부한 run(예: HDFS.STAGE.ROOT 변경 전 run)은 운영자가 직접 지우고 기록한다.
    """
    settings = request.app.state.settings.cleanup
    return await run_tx(request, lambda conn: cleanup.mark_cleaned(conn, settings, run_id, body))
