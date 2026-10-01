from uuid import UUID

from fastapi import APIRouter, Depends, Request

from load_control.routers.deps import run_tx
from load_control.schemas.publish import (
    PublishClaimRequest,
    PublishClaimResponse,
    PublishResultRequest,
    PublishResultResponse,
)
from load_control.schemas.validation import (
    StageValidatedResponse,
    SuccessRequest,
    SuccessResponse,
    ValidationsRequest,
    ValidationsResponse,
    ValidationStartRequest,
    ValidationStartResponse,
)
from load_control.security import require_role
from load_control.services import publish, validation

router = APIRouter(prefix="/v1/runs/{run_id}", tags=["validation"],
                   dependencies=[Depends(require_role("nifi"))])


@router.post("/validation/start", response_model=ValidationStartResponse)
async def start(run_id: UUID, body: ValidationStartRequest, request: Request) -> ValidationStartResponse:
    return await run_tx(request, lambda conn: validation.start(conn, run_id, body))


@router.post("/validations", response_model=ValidationsResponse)
async def record_validations(run_id: UUID, body: ValidationsRequest,
                             request: Request) -> ValidationsResponse:
    return await run_tx(request, lambda conn: validation.record(conn, run_id, body))


@router.post("/stage-validated", response_model=StageValidatedResponse)
async def stage_validated(run_id: UUID, request: Request) -> StageValidatedResponse:
    return await run_tx(request, lambda conn: validation.stage_validated(conn, run_id))


@router.post("/publish/claim", response_model=PublishClaimResponse)
async def publish_claim(run_id: UUID, body: PublishClaimRequest, request: Request) -> PublishClaimResponse:
    return await run_tx(request, lambda conn: publish.claim(conn, run_id, body))


@router.post("/publish/result", response_model=PublishResultResponse)
async def publish_result(run_id: UUID, body: PublishResultRequest,
                         request: Request) -> PublishResultResponse:
    return await run_tx(request, lambda conn: publish.result(conn, run_id, body))


@router.post("/success", response_model=SuccessResponse)
async def success(run_id: UUID, body: SuccessRequest, request: Request) -> SuccessResponse:
    return await run_tx(request, lambda conn: validation.succeed(conn, run_id, body))
