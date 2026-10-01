"""운영자 전용 엔드포인트(role operator)."""

from uuid import UUID

from fastapi import APIRouter, Depends, Request

from load_control.routers.deps import run_tx
from load_control.schemas.publish import (
    DispatchResendResponse,
    PublishResultResponse,
    PublishUnknownResolveRequest,
)
from load_control.security import require_role
from load_control.services import ops, publish

# 운영자 전용. NiFi 서비스 계정 토큰으로는 호출할 수 없다(API 설계 5.2).
router = APIRouter(prefix="/v1/runs/{run_id}", tags=["operator"],
                   dependencies=[Depends(require_role("operator"))])


@router.post("/dispatches/{dispatch_id}/resend", response_model=DispatchResendResponse)
async def resend_dispatch(run_id: UUID, dispatch_id: UUID, request: Request) -> DispatchResendResponse:
    """DEAD 또는 ACK 없는 SENT dispatch를 다시 보내게 한다. 시도 횟수는 0으로 초기화한다."""
    return await run_tx(request, lambda conn: ops.resend_dispatch(conn, run_id, dispatch_id))


@router.post("/publish-unknown/resolve", response_model=PublishResultResponse)
async def resolve_publish_unknown(run_id: UUID, body: PublishUnknownResolveRequest,
                                  request: Request) -> PublishResultResponse:
    """PUBLISH_UNKNOWN을 PUBLISHED 또는 FAILED_PUBLISH로 확정한다.

    Hive 이력·target 지표 확인 후 사유와 함께 호출한다.
    """
    return await run_tx(request, lambda conn: publish.resolve_unknown(
        conn, run_id, body, operator=getattr(request.state, "role", "operator")))
