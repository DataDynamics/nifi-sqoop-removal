"""run 엔드포인트(NiFi PG-10 Coordinator, 조회)."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from load_control.domain import RunStatus
from load_control.errors import Unprocessable
from load_control.routers.deps import run_tx
from load_control.schemas.runs import (
    ManifestRequest,
    ManifestResponse,
    RunCreateRequest,
    RunCreateResponse,
    RunDetail,
    RunFailRequest,
    RunFailResponse,
    RunListItem,
)
from load_control.security import require_role
from load_control.services import manifest, runs

router = APIRouter(prefix="/v1/runs", tags=["runs"])
nifi = [Depends(require_role("nifi"))]


@router.post("", response_model=RunCreateResponse, status_code=200, dependencies=nifi)
async def create_run(body: RunCreateRequest, request: Request) -> RunCreateResponse:
    """run을 만든다. 같은 업무키의 활성 run이 있으면 409 DUPLICATE_ACTIVE_RUN."""
    return await run_tx(request, lambda conn: runs.create_run(conn, body))


@router.post("/{run_id}/manifest", response_model=ManifestResponse, dependencies=nifi)
async def register_manifest(run_id: UUID, body: ManifestRequest, request: Request) -> ManifestResponse:
    """SCN·source 지표·파티션 manifest를 등록하고 EXTRACTING으로 바꾼다.

    불변식 위반이면 FAILED_MANIFEST를 기록하고 422.
    """
    outcome = await run_tx(request, lambda conn: manifest.register_manifest(conn, run_id, body))
    if outcome.response is None:
        # FAILED_MANIFEST는 이미 commit됐다.
        raise Unprocessable("MANIFEST_INVALID", "; ".join(outcome.violations),
                            violations=outcome.violations)
    return outcome.response


@router.post("/{run_id}/fail", response_model=RunFailResponse, dependencies=nifi)
async def fail_run(run_id: UUID, body: RunFailRequest, request: Request) -> RunFailResponse:
    """파티션 외 단계의 실패를 기록한다(기대 상태가 맞을 때만)."""
    return await run_tx(request, lambda conn: runs.fail_run(conn, run_id, body))


@router.get("", response_model=list[RunListItem],
            dependencies=[Depends(require_role("nifi", "operator"))])
async def list_runs(request: Request,
                    job_key: Annotated[str | None, Query(alias="jobKey", max_length=200)] = None,
                    business_key: Annotated[str | None, Query(alias="businessKey", max_length=200)] = None,
                    status: Annotated[RunStatus | None, Query()] = None,
                    limit: Annotated[int, Query(ge=1, le=500)] = 50) -> list[RunListItem]:
    """run 목록(최근 시작 순)."""
    return await run_tx(request, lambda conn: runs.list_runs(
        conn, job_key=job_key, business_key=business_key, status=status, limit=limit))


@router.get("/{run_id}", response_model=RunDetail,
            dependencies=[Depends(require_role("nifi", "operator"))])
async def get_run(run_id: UUID, request: Request) -> RunDetail:
    """run 상태, 파티션별 상태, dispatch 상태."""
    return await run_tx(request, lambda conn: runs.get_run_detail(conn, run_id))
