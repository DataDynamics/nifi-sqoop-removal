"""헬스체크와 메트릭. 인증 없이 열려 있으므로 LB 내부에서만 접근하게 한다."""

import structlog
from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

router = APIRouter(tags=["health"])
log = structlog.get_logger(__name__)


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """프로세스 생존 확인. DB를 확인하지 않는다."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request, response: Response) -> dict[str, str]:
    """DB에 SELECT 1이 되면 200, 아니면 503. LB가 트래픽을 보낼지 판단한다.

    예외 종류와 관계없이 503 {"status": "unavailable"}을 돌려준다. 원인(예외 종류와 메시지)은 응답에
    넣지 않고 로그(readyz_failed)에 남긴다. 인증 없이 열린 경로라 내부 정보를 응답에 싣지 않는다.
    """
    try:
        async with request.app.state.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as e:
        log.warning("readyz_failed", error=f"{type(e).__name__}: {e}"[:500])
        response.status_code = 503
        return {"status": "unavailable"}
    return {"status": "ok"}


@router.get("/metrics")
async def prometheus_metrics() -> Response:
    """Prometheus 메트릭(이 프로세스 기준).

    server.workers가 1보다 크면 요청을 받은 프로세스의 값만 보인다(metrics.py 참고).
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
