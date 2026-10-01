"""로그 설정과 요청 컨텍스트(API 설계 9.11).

- structlog 이벤트와 stdlib 로그(uvicorn, SQLAlchemy, alembic, asyncpg)를 같은 handler와 형식으로 낸다.
- 출력: 표준출력(json 또는 console)과 선택적으로 회전 파일(항상 json).
- 요청마다 X-Request-Id를 contextvar에 묶어 그 요청 중에 남는 모든 로그에 requestId가 붙는다.
"""

import logging
import logging.handlers
import re
import sys
import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from load_control import metrics
from load_control.config import LoggingSettings

REQUEST_ID_HEADER = "X-Request-Id"
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._\-]{1,100}$")
# access 로그를 남기지 않는 경로(헬스체크, 메트릭 수집은 너무 잦다)
_QUIET_ENDPOINTS = frozenset({"/healthz", "/readyz", "/metrics"})

# 모든 로그(structlog, stdlib)에 공통으로 적용하는 전처리
_SHARED_PROCESSORS: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_log_level,
    structlog.stdlib.add_logger_name,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
]


def _formatter(fmt: str) -> structlog.stdlib.ProcessorFormatter:
    renderer: structlog.types.Processor
    if fmt == "json":
        tail: list[structlog.types.Processor] = [structlog.processors.dict_tracebacks]
        renderer = structlog.processors.JSONRenderer(ensure_ascii=False)
    else:
        tail = []
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())
    return structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_SHARED_PROCESSORS,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *tail, renderer],
    )


def configure_logging(cfg: LoggingSettings) -> None:
    """설정대로 root logger를 다시 구성한다. 여러 번 호출해도 handler가 중복되지 않는다."""
    handlers: list[logging.Handler] = []
    if cfg.stdout:
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(_formatter(cfg.format))
        handlers.append(stream)
    if cfg.file is not None:
        cfg.file.path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            cfg.file.path, maxBytes=cfg.file.max_bytes, backupCount=cfg.file.backup_count,
            encoding="utf-8")
        file_handler.setFormatter(_formatter("json"))  # 파일은 수집기가 읽으므로 항상 json
        handlers.append(file_handler)

    root = logging.getLogger()
    for old in root.handlers[:]:
        root.removeHandler(old)
        old.close()
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(cfg.level)

    # uvicorn은 자체 handler를 붙이므로 떼어내고 root로 보낸다. 요청 로그는 미들웨어가 남긴다.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    for name, level in cfg.loggers.items():
        logging.getLogger(name).setLevel(level)

    structlog.configure(
        processors=[structlog.stdlib.filter_by_level, *_SHARED_PROCESSORS,
                    structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # 테스트와 worker에서 설정을 다시 적용할 수 있도록 logger를 캐시하지 않는다.
        cache_logger_on_first_use=False,
    )


class RequestContextMiddleware(BaseHTTPMiddleware):
    """X-Request-Id 전파, 구조화 access 로그, 요청 메트릭.

    NiFi InvokeHTTP가 보낸 X-Request-Id(동적 속성 `${UUID()}`)를 그대로 쓰고, 없거나 형식이
    이상하면 새로 만든다. 응답 헤더에도 같은 값을 돌려줘 NiFi Provenance와 대조할 수 있게 한다.
    """

    def __init__(self, app: ASGIApp, access_log: bool = True) -> None:
        super().__init__(app)
        self.access_log = access_log
        self.log = structlog.get_logger("load_control.access")

    async def dispatch(self, request: Request,
                       call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """요청 하나를 처리하며 requestId를 묶고, 끝나면 메트릭과 access 로그를 남긴다."""
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
            endpoint = getattr(route, "path", "unmatched")  # 경로 템플릿(메트릭 라벨 폭증 방지)
            params = request.scope.get("path_params", {})
            metrics.REQUESTS.labels(endpoint, str(status)).inc()
            metrics.REQUEST_SECONDS.labels(endpoint).observe(elapsed)
            if self.access_log and endpoint not in _QUIET_ENDPOINTS:
                # 5xx는 ERROR, 4xx는 WARNING으로 올려 경보 규칙을 단순하게 한다.
                level = logging.ERROR if status >= 500 else logging.WARNING if status >= 400 else logging.INFO
                self.log.log(level, "request", method=request.method, endpoint=endpoint,
                             httpStatus=status, durationMs=round(elapsed * 1000, 1),
                             runId=params.get("run_id"), partitionId=params.get("partition_id"),
                             role=getattr(request.state, "role", None),
                             client=request.client.host if request.client else None)
            structlog.contextvars.clear_contextvars()
