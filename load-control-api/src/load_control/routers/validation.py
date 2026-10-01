from uuid import UUID

from fastapi import APIRouter, Depends, Request

from load_control.routers.deps import run_tx
from load_control.schemas.validation import ValidationStartRequest, ValidationStartResponse
from load_control.security import require_role
from load_control.services import validation

router = APIRouter(prefix="/v1/runs/{run_id}", tags=["validation"],
                   dependencies=[Depends(require_role("nifi"))])


@router.post("/validation/start", response_model=ValidationStartResponse)
async def start(run_id: UUID, body: ValidationStartRequest, request: Request) -> ValidationStartResponse:
    return await run_tx(request, lambda conn: validation.start(conn, run_id, body))
