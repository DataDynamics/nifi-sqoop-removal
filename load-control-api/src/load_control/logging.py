"""로그 설정과 요청 컨텍스트.

- structlog 이벤트와 stdlib 로그(uvicorn, SQLAlchemy, alembic, asyncpg)를 같은 handler와 형식으로 낸다.
- 시각은 서버 현지 시각 `YYYY-MM-DD HH:MM:SS.SSS`.
- 이벤트 코드(`run_created` 등)에 한글 메시지(`message`)를 붙인다(log_messages.MESSAGES).
- 형식: text(사람이 읽는 한 줄), json(수집기용), console(개발용 색상). 표준출력과 회전 파일에 각각 지정한다.
- 요청마다 X-Request-Id, runId, partitionId를 contextvar에 묶어 그 요청 중의 모든 로그에 붙인다.
  API 호출은 수신(api_request)과 응답(api_response) 두 줄을 남기고, 설정하면 요청·응답 본문도 넣는다.

text 형식 예:
  2026-10-03 03:12:45.123 INFO  [load_control.services.runs]
      run 생성: ORACLE_INSP_DTL_DAILY 업무일자 2026-09-28
      (run_created) requestId=... runId=... jobKey=... businessKey=...
"""

import json
import logging
import logging.handlers
import re
import sys
import time
import uuid
from collections.abc import Awaitable, Callable, MutableMapping
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from load_control import metrics
from load_control.config import LoggingSettings
from load_control.log_messages import MESSAGES

REQUEST_ID_HEADER = "X-Request-Id"
# 받은 X-Request-Id를 그대로 쓸 수 있는 형식. 로그 주입(개행·공백)과 과도한 길이를 막는다.
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._\-]{1,100}$")
# 수신·응답 로그를 남기지 않는 경로(헬스체크, 메트릭 수집은 너무 잦다)
_QUIET_ENDPOINTS = frozenset({"/healthz", "/readyz", "/metrics"})
# 경로에서 runId(UUID 36자)·partitionId(4자리 번호 또는 NULL)를 꺼내 로그 컨텍스트에 묶는다.
_RUN_ID_IN_PATH = re.compile(r"/runs/([0-9a-fA-F-]{36})")
_PARTITION_IN_PATH = re.compile(r"/partitions/([0-9A-Za-z]{1,10})")
# text 형식에서 앞쪽 고정 칸으로 쓰는 키
_HEAD_KEYS = ("timestamp", "level", "logger", "event", "message", "exception")
# text 형식에서 key=value 목록의 맨 끝에 원문 그대로 붙이는 본문 키
_BODY_KEYS = ("requestBody", "responseBody", "body")


def add_timestamp(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """서버 현지 시각을 밀리초까지 `YYYY-MM-DD HH:MM:SS.SSS`로 붙인다."""
    now = datetime.now()
    event_dict["timestamp"] = f"{now:%Y-%m-%d %H:%M:%S}.{now.microsecond // 1000:03d}"
    return event_dict


class _Fields(dict[str, Any]):
    """메시지 템플릿에 없는 필드는 `-`로 채운다."""

    def __missing__(self, key: str) -> str:
        """str.format_map이 없는 키를 찾을 때 KeyError 대신 `-`를 돌려준다."""
        return "-"


def add_message(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """이벤트 코드에 해당하는 한글 메시지를 `message`로 붙인다. 표에 없는 이벤트는 그대로 둔다."""
    event = event_dict.get("event")
    template = MESSAGES.get(event) if isinstance(event, str) else None
    if template and "message" not in event_dict:
        try:
            event_dict["message"] = template.format_map(_Fields(event_dict))
        except (ValueError, IndexError):  # 템플릿 오류로 로그를 잃지 않는다
            event_dict["message"] = template
    return event_dict


# 모든 로그(structlog, stdlib)에 공통으로 적용하는 전처리
_SHARED_PROCESSORS: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_log_level,
    structlog.stdlib.add_logger_name,
    add_timestamp,
    add_message,
    structlog.processors.StackInfoRenderer(),
]


def _text_value(value: Any) -> str:
    """text 형식의 key=value에서 value 표기.

    문자열은 빈 값이거나 공백·따옴표·`=`가 있으면 JSON 문자열(따옴표·이스케이프)로, 아니면 그대로 쓴다.
    그래야 한 줄 안에서 key=value 경계가 모호해지지 않는다. dict·list·tuple은 공백 없는 JSON으로 쓴다.
    """
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False) if (not value or re.search(r"[\s\"=]", value)) else value
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    return str(value)


def render_text(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> str:
    """한 줄 텍스트: `시각 수준 [logger] 메시지 (이벤트 코드) key=value ...`. 예외는 다음 줄부터."""
    level = str(event_dict.get("level", "info")).upper().replace("WARNING", "WARN")  # 5칸 정렬
    event = event_dict.get("event", "")
    message = event_dict.get("message")
    head = f"{event_dict.get('timestamp', '')} {level:<5} [{event_dict.get('logger', '-')}] "
    head += f"{message} ({event})" if message else str(event)
    # 요청·응답 본문(JSON 문자열)은 따옴표·이스케이프 없이 원문으로 둔다(줄 끝에 오도록 마지막에).
    bodies = {k: v for k, v in event_dict.items() if k in _BODY_KEYS and v is not None}
    fields = " ".join([*(f"{k}={_text_value(v)}" for k, v in event_dict.items()
                         if k not in _HEAD_KEYS and k not in _BODY_KEYS and v is not None),
                       *(f"{k}={v if isinstance(v, str) else _text_value(v)}" for k, v in bodies.items())])
    line = f"{head} {fields}" if fields else head
    if event_dict.get("exception"):
        line += "\n" + str(event_dict["exception"])
    return line


def _formatter(fmt: str) -> structlog.stdlib.ProcessorFormatter:
    """출력 형식(text, json, console)에 맞는 stdlib handler용 formatter를 만든다.

    foreign_pre_chain으로 stdlib 로그(uvicorn, SQLAlchemy 등)에도 structlog와 같은 전처리
    (컨텍스트, 시각, 한글 메시지)를 적용한다. 예외는 json이면 구조화된 traceback, text면 문자열로 붙인다.
    """
    tail: list[structlog.types.Processor]
    renderer: structlog.types.Processor
    if fmt == "json":
        tail = [structlog.processors.dict_tracebacks]
        renderer = structlog.processors.JSONRenderer(ensure_ascii=False, default=str)
    elif fmt == "text":
        tail = [structlog.processors.format_exc_info]
        renderer = render_text
    else:
        tail = []
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())
    return structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_SHARED_PROCESSORS,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *tail, renderer],
    )


def configure_logging(cfg: LoggingSettings, service: str = "server") -> None:
    """설정대로 root logger를 다시 구성한다. 여러 번 호출해도 handler가 중복되지 않는다.

    파일 경로의 `{service}`는 server 또는 worker로 바뀐다. 두 서비스가 같은 파일에 섞여 쓰지 않게 한다.
    """
    handlers: list[logging.Handler] = []
    if cfg.stdout:
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(_formatter(cfg.format))
        handlers.append(stream)
    if cfg.file is not None:
        path = Path(str(cfg.file.path).replace("{service}", service))
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=cfg.file.max_bytes, backupCount=cfg.file.backup_count,
            encoding="utf-8")
        file_handler.setFormatter(_formatter(cfg.file.format))
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


def _clip(raw: bytes, limit: int) -> str | None:
    """로그에 넣을 본문. 비었거나 limit이 0이면 None, limit 글자를 넘으면 자르고 남은 글자 수를 적는다."""
    if not raw or limit <= 0:
        return None
    text = raw.decode("utf-8", errors="replace")
    return text if len(text) <= limit else f"{text[:limit]}...(+{len(text) - limit}자)"


class RequestContextMiddleware(BaseHTTPMiddleware):
    """X-Request-Id 전파, 요청 수신·응답 로그, 요청 메트릭.

    NiFi InvokeHTTP가 보낸 X-Request-Id(동적 속성 `${UUID()}`)를 그대로 쓰고, 없거나 형식이
    이상하면 새로 만든다. 응답 헤더에도 같은 값을 돌려줘 NiFi Provenance와 대조할 수 있게 한다.
    경로의 runId·partitionId도 contextvar에 묶어 서비스 로그에서 같은 요청을 찾을 수 있게 한다.
    Authorization 헤더는 남기지 않는다.
    """

    def __init__(self, app: ASGIApp, access_log: bool = True, access_body: bool = True,
                 access_body_max: int = 2000) -> None:
        """설정(logging.access_log, access_body, access_body_max)을 받는다. main.create_app이 등록한다."""
        super().__init__(app)
        self.access_log = access_log
        self.access_body = access_body
        self.access_body_max = access_body_max
        self.log = structlog.get_logger("load_control.access")

    async def dispatch(self, request: Request,
                       call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """요청 하나를 처리하며 컨텍스트를 묶고, 수신·응답 로그와 메트릭을 남긴다.

        - 요청 본문은 POST·PUT·PATCH에서만 읽는다. Starlette가 읽은 본문을 캐시하므로
          라우터도 다시 읽을 수 있다.
        - 응답 본문을 로그에 넣을 때는 스트림을 모두 읽은 뒤 같은 내용으로 Response를 다시 만든다.
        - 메트릭과 응답 로그는 finally에서 남기므로 처리 중 예외가 나도 status 500으로 기록된다.
        - 요청이 끝나면 contextvar를 비워 다음 요청에 값이 새지 않게 한다.
        """
        incoming = request.headers.get(REQUEST_ID_HEADER, "")
        request_id = incoming if _SAFE_REQUEST_ID.match(incoming) else str(uuid.uuid4())
        request.state.request_id = request_id
        path = request.url.path
        run_match = _RUN_ID_IN_PATH.search(path)
        part_match = _PARTITION_IN_PATH.search(path)
        # 이전 요청이 남긴 값을 지우고 이 요청의 추적 키를 묶는다(이후 모든 로그에 붙는다).
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            requestId=request_id, runId=run_match.group(1) if run_match else None,
            partitionId=part_match.group(1) if part_match else None)
        loud = self.access_log and path not in _QUIET_ENDPOINTS
        with_body = loud and self.access_body
        if loud:
            request_body = None
            if with_body and request.method in ("POST", "PUT", "PATCH"):
                request_body = _clip(await request.body(), self.access_body_max)
            self.log.info("api_request", method=request.method, path=path,
                          query=str(request.url.query) or None,
                          client=request.client.host if request.client else None,
                          xRunId=request.headers.get("X-Run-Id"), requestBody=request_body)
        started = time.perf_counter()
        status = 500  # call_next가 예외로 끝나면 이 값으로 메트릭·로그를 남긴다
        response_body = None
        try:
            response = await call_next(request)
            status = response.status_code
            if with_body:
                # 응답 본문을 읽어 로그에 남기고 같은 내용으로 다시 만든다(JSON 응답은 작다).
                chunks = [chunk async for chunk in response.body_iterator]  # type: ignore[attr-defined]
                raw = b"".join(c if isinstance(c, bytes) else str(c).encode() for c in chunks)
                response_body = _clip(raw, self.access_body_max)
                response = Response(content=raw, status_code=status, headers=dict(response.headers),
                                    background=response.background)
            response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            elapsed = time.perf_counter() - started
            route = request.scope.get("route")
            endpoint = getattr(route, "path", "unmatched")  # 경로 템플릿(메트릭 라벨 폭증 방지)
            metrics.REQUESTS.labels(endpoint, str(status)).inc()
            metrics.REQUEST_SECONDS.labels(endpoint).observe(elapsed)
            if loud:
                # 5xx는 ERROR, 4xx는 WARNING으로 올려 경보 규칙을 단순하게 한다.
                level = logging.ERROR if status >= 500 else logging.WARNING if status >= 400 else logging.INFO
                self.log.log(level, "api_response", method=request.method, path=path, endpoint=endpoint,
                             httpStatus=status, durationMs=round(elapsed * 1000, 1),
                             role=getattr(request.state, "role", None), responseBody=response_body)
            structlog.contextvars.clear_contextvars()
