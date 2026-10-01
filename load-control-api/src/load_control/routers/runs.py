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
    return await run_tx(request, lambda conn: runs.create_run(conn, body))


@router.post("/{run_id}/manifest", response_model=ManifestResponse, dependencies=nifi)
async def register_manifest(run_id: UUID, body: ManifestRequest, request: Request) -> ManifestResponse:
    outcome = await run_tx(request, lambda conn: manifest.register_manifest(conn, run_id, body))
    if outcome.response is None:
        # FAILED_MANIFEST는 이미 commit됐다(API 설계 9.5).
        raise Unprocessable("MANIFEST_INVALID", "; ".join(outcome.violations),
                            violations=outcome.violations)
    return outcome.response


@router.post("/{run_id}/fail", response_model=RunFailResponse, dependencies=nifi)
async def fail_run(run_id: UUID, body: RunFailRequest, request: Request) -> RunFailResponse:
    return await run_tx(request, lambda conn: runs.fail_run(conn, run_id, body))


@router.get("", response_model=list[RunListItem],
            dependencies=[Depends(require_role("nifi", "operator"))])
async def list_runs(request: Request,
                    job_key: Annotated[str | None, Query(alias="jobKey", max_length=200)] = None,
                    business_key: Annotated[str | None, Query(alias="businessKey", max_length=200)] = None,
                    status: Annotated[RunStatus | None, Query()] = None,
                    limit: Annotated[int, Query(ge=1, le=500)] = 50) -> list[RunListItem]:
    return await run_tx(request, lambda conn: runs.list_runs(
        conn, job_key=job_key, business_key=business_key, status=status, limit=limit))


@router.get("/{run_id}", response_model=RunDetail,
            dependencies=[Depends(require_role("nifi", "operator"))])
async def get_run(run_id: UUID, request: Request) -> RunDetail:
    return await run_tx(request, lambda conn: runs.get_run_detail(conn, run_id))
