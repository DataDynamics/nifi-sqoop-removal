from uuid import UUID

from fastapi import APIRouter, Depends, Request

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


@router.get("/{run_id}", response_model=RunDetail,
            dependencies=[Depends(require_role("nifi", "operator"))])
async def get_run(run_id: UUID, request: Request) -> RunDetail:
    return await run_tx(request, lambda conn: runs.get_run_detail(conn, run_id))
