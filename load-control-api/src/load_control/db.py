import asyncio
from collections.abc import Awaitable, Callable

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from load_control.config import Settings

RETRYABLE_SQLSTATE = frozenset({"40P01", "40001"})  # deadlock_detected, serialization_failure
UNIQUE_VIOLATION = "23505"
FOREIGN_KEY_VIOLATION = "23503"


def make_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database.url.get_secret_value(),
        pool_size=settings.database.pool_size,
        max_overflow=settings.database.max_overflow,
        pool_pre_ping=True,
    )


def sqlstate(e: BaseException) -> str | None:
    """SQLAlchemy가 감싼 asyncpg 예외에서 SQLSTATE를 꺼낸다."""
    orig = getattr(e, "orig", e)
    for candidate in (orig, getattr(orig, "__cause__", None)):
        if candidate is None:
            continue
        code = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if code:
            return str(code)
    return None


def constraint_name(e: BaseException) -> str | None:
    orig = getattr(e, "orig", e)
    for candidate in (orig, getattr(orig, "__cause__", None)):
        name = getattr(candidate, "constraint_name", None)
        if name:
            return str(name)
    return None


async def in_tx[T](
    engine: AsyncEngine,
    fn: Callable[[AsyncConnection], Awaitable[T]],
    attempts: int = 3,
) -> T:
    """fn을 한 트랜잭션으로 실행한다. deadlock·serialization 실패는 트랜잭션 전체를 재실행한다.

    모든 상태 변경 엔드포인트는 멱등이므로 재실행해도 결과가 같다(API 설계 9.5).
    잠금 순서: load_run → load_partition → load_file → load_dispatch.
    """
    for i in range(attempts):
        try:
            async with engine.begin() as conn:
                return await fn(conn)
        except DBAPIError as e:
            if sqlstate(e) in RETRYABLE_SQLSTATE and i < attempts - 1:
                await asyncio.sleep(0.05 * 2**i)
                continue
            raise
    raise AssertionError("unreachable")
