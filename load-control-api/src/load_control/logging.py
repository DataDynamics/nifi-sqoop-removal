import logging
import re
import sys
import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from load_control import metrics

REQUEST_ID_HEADER = "X-Request-Id"
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._\-]{1,100}$")


def configure_logging(level: str = "INFO", json: bool = True) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper(), force=True)
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer())
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        cache_logger_on_first_use=True,
    )


class RequestContextMiddleware(BaseHTTPMiddleware):
    """X-Request-Id 전파, 구조화 access log, 요청 메트릭(API 설계 9.11)."""

    async def dispatch(self, request: Request,
                       call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        incoming = request.headers.get(REQUEST_ID_HEADER, "")
        request_id = incoming if _SAFE_REQUEST_ID.match(incoming) else str(uuid.uuid4())
        request.state.request_id = request_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(requestId=request_id)
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            elapsed = time.perf_counter() - started
            route = request.scope.get("route")
            endpoint = getattr(route, "path", "unmatched")
            params = request.scope.get("path_params", {})
            metrics.REQUESTS.labels(endpoint, str(status)).inc()
            metrics.REQUEST_SECONDS.labels(endpoint).observe(elapsed)
            if endpoint not in ("/healthz", "/readyz", "/metrics"):
                structlog.get_logger("access").info(
                    "request", method=request.method, endpoint=endpoint, httpStatus=status,
                    durationMs=round(elapsed * 1000, 1), runId=params.get("run_id"),
                    partitionId=params.get("partition_id"),
                    role=getattr(request.state, "role", None))
