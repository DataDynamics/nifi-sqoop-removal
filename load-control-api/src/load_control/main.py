"""FastAPI 앱 factory와 공통 예외 처리.

실행은 `python -m load_control.server`(config.yaml의 server 섹션 사용)가 기본이다.
개발 중에는 `uvicorn --factory load_control.main:create_app --reload`도 쓸 수 있다.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError

from load_control import __version__
from load_control.config import Settings, get_settings
from load_control.db import FOREIGN_KEY_VIOLATION, UNIQUE_VIOLATION, constraint_name, make_engine, sqlstate
from load_control.errors import ApiError
from load_control.logging import RequestContextMiddleware, configure_logging
from load_control.routers import cleanup, health, monitor, ops, partitions, runs, validation

log = structlog.get_logger(__name__)


def _error(request: Request, status: int, code: str, message: str,
           details: dict[str, object] | None = None) -> JSONResponse:
    """모든 오류 응답의 공통 형식: {code, message, requestId, details?}."""
    body: dict[str, object] = {"code": code, "message": message,
                               "requestId": getattr(request.state, "request_id", None)}
    if details:
        body["details"] = details
    return JSONResponse(status_code=status, content=body)


def create_app(settings: Settings | None = None) -> FastAPI:
    """앱을 만든다. settings가 없으면 config.yaml을 읽는다(테스트는 직접 넘긴다)."""
    settings = settings or get_settings()
    configure_logging(settings.logging)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """DB 연결 pool을 프로세스 수명 동안 하나 만들고 종료 시 닫는다."""
        app.state.engine = make_engine(settings)
        db = make_url(settings.database.url)
        # 비밀번호가 로그에 남지 않도록 호스트·DB 이름만 기록한다.
        log.info("api_started", version=__version__, dbHost=db.host, dbPort=db.port,
                 dbName=db.database, poolSize=settings.database.pool_size,
                 roles=sorted(settings.auth.token_digests))
        if not settings.auth.token_digests:
            log.warning("auth_not_configured",
                        detail="auth.token_digests is empty; all API calls get 401/403")
        try:
            yield
        finally:
            await app.state.engine.dispose()
            log.info("api_stopped")

    app = FastAPI(title="Load Control API", version=__version__, lifespan=lifespan,
                  root_path=settings.server.root_path,
                  description="Sqoop 대체 적재의 상태 원장 기록과 완료 판정(load-control-api-design.md).")
    app.state.settings = settings
    app.add_middleware(RequestContextMiddleware, access_log=settings.logging.access_log,
                       access_body=settings.logging.access_body,
                       access_body_max=settings.logging.access_body_max)

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError) -> JSONResponse:
        """의도한 업무 오류(404/409/422). 정상 경합도 많으므로 INFO로만 남긴다."""
        log.info("api_error", status=exc.status, code=exc.code, details=exc.details or None)
        return _error(request, exc.status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> Response:
        """입력 검증 실패(422). NiFi 쪽 attribute 목록 실수를 찾기 쉽도록 필드 위치를 남긴다."""
        log.warning("request_invalid",
                    errors=[{"loc": e.get("loc"), "type": e.get("type")} for e in exc.errors()][:20])
        return await request_validation_exception_handler(request, exc)

    @app.exception_handler(IntegrityError)
    async def integrity_error(request: Request, exc: IntegrityError) -> JSONResponse:
        """DB 제약 위반. 서비스 계층에서 따로 처리하지 않은 경우만 여기로 온다."""
        state, constraint = sqlstate(exc), constraint_name(exc)
        if state == UNIQUE_VIOLATION:
            log.warning("unique_violation", constraint=constraint)
            return _error(request, 409, "UNIQUE_VIOLATION", "unique constraint violated",
                          {"constraint": constraint})
        if state == FOREIGN_KEY_VIOLATION:
            log.warning("foreign_key_violation", constraint=constraint)
            return _error(request, 422, "REFERENCE_NOT_FOUND", "referenced row not found",
                          {"constraint": constraint})
        log.error("integrity_error", sqlstate=state, constraint=constraint)
        return _error(request, 422, "CONSTRAINT_VIOLATION", "constraint violated", {"constraint": constraint})

    @app.exception_handler(DBAPIError)
    async def db_error(request: Request, exc: DBAPIError) -> JSONResponse:
        """연결 장애·일시 오류: 503으로 응답해 NiFi InvokeHTTP가 Retry로 보내게 한다."""
        log.error("db_error", sqlstate=sqlstate(exc), error=type(exc.orig).__name__,
                  connection=isinstance(exc, OperationalError | InterfaceError), exc_info=exc)
        return _error(request, 503, "DATABASE_UNAVAILABLE", "database error, retry later")

    app.include_router(health.router)
    app.include_router(runs.router)
    app.include_router(partitions.router)
    app.include_router(validation.router)
    app.include_router(ops.router)
    app.include_router(cleanup.router)
    app.include_router(monitor.router)
    return app
