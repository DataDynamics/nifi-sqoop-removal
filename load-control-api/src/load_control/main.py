from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError

from load_control import __version__
from load_control.config import Settings, get_settings
from load_control.db import FOREIGN_KEY_VIOLATION, UNIQUE_VIOLATION, constraint_name, make_engine, sqlstate
from load_control.errors import ApiError
from load_control.logging import RequestContextMiddleware, configure_logging
from load_control.routers import health, partitions, runs, validation

log = structlog.get_logger(__name__)


def _error(request: Request, status: int, code: str, message: str, **details: object) -> JSONResponse:
    body: dict[str, object] = {"code": code, "message": message,
                               "requestId": getattr(request.state, "request_id", None)}
    if details:
        body["details"] = details
    return JSONResponse(status_code=status, content=body)


def create_app(settings: Settings | None = None) -> FastAPI:
    """앱 factory. 실행: uvicorn --factory load_control.main:create_app
    또는 gunicorn 'load_control.main:create_app()' -k uvicorn.workers.UvicornWorker"""
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.engine = make_engine(settings)
        try:
            yield
        finally:
            await app.state.engine.dispose()

    app = FastAPI(title="Load Control API", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(RequestContextMiddleware)

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError) -> JSONResponse:
        return _error(request, exc.status, exc.code, exc.message, **exc.details)

    @app.exception_handler(IntegrityError)
    async def integrity_error(request: Request, exc: IntegrityError) -> JSONResponse:
        state, constraint = sqlstate(exc), constraint_name(exc)
        if state == UNIQUE_VIOLATION:
            return _error(request, 409, "UNIQUE_VIOLATION", "unique constraint violated",
                          constraint=constraint)
        if state == FOREIGN_KEY_VIOLATION:
            return _error(request, 422, "REFERENCE_NOT_FOUND", "referenced row not found",
                          constraint=constraint)
        log.error("integrity_error", sqlstate=state, constraint=constraint)
        return _error(request, 422, "CONSTRAINT_VIOLATION", "constraint violated", constraint=constraint)

    @app.exception_handler(DBAPIError)
    async def db_error(request: Request, exc: DBAPIError) -> JSONResponse:
        # 연결 장애, 일시 오류: 503으로 응답해 NiFi InvokeHTTP가 Retry로 보내게 한다(API 설계 5.4).
        log.error("db_error", sqlstate=sqlstate(exc), error=type(exc.orig).__name__,
                  connection=isinstance(exc, OperationalError | InterfaceError))
        return _error(request, 503, "DATABASE_UNAVAILABLE", "database error, retry later")

    app.include_router(health.router)
    app.include_router(runs.router)
    app.include_router(partitions.router)
    app.include_router(validation.router)
    return app

