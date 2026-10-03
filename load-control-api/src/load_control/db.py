"""DB 엔진, 트랜잭션 헬퍼, SQLSTATE 헬퍼.

ORM 없이 SQLAlchemy Core(async, asyncpg 드라이버)를 쓴다. 서비스 함수는 AsyncConnection 하나를 받아
그 안에서 SQL을 실행하고, 트랜잭션 경계(commit·rollback·재시도)는 `in_tx`가 정한다.
"""

import asyncio
from collections.abc import Awaitable, Callable

import structlog
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from load_control.config import Settings

log = structlog.get_logger(__name__)

# 트랜잭션 전체를 다시 실행하면 성공할 수 있는 오류
RETRYABLE_SQLSTATE = frozenset({"40P01", "40001"})  # deadlock_detected, serialization_failure
UNIQUE_VIOLATION = "23505"  # unique_violation: 서비스가 업무 오류(409)로 바꾸거나 main.py가 409로 응답
FOREIGN_KEY_VIOLATION = "23503"  # foreign_key_violation: main.py가 422 REFERENCE_NOT_FOUND로 응답


def make_engine(settings: Settings) -> AsyncEngine:
    """프로세스당 하나 만드는 async 엔진. pool_pre_ping으로 끊긴 연결을 자동으로 버린다.

    pool 크기는 database.pool_size + max_overflow가 프로세스당 최대 연결 수다. 연결은 이 함수가
    아니라 첫 사용 때 열린다. 호출자(main.lifespan, worker)가 종료 시 dispose()로 닫는다.
    """
    return create_async_engine(
        settings.database.url,
        pool_size=settings.database.pool_size,
        max_overflow=settings.database.max_overflow,
        pool_pre_ping=True,
    )


def sqlstate(e: BaseException) -> str | None:
    """SQLAlchemy가 감싼 asyncpg 예외에서 SQLSTATE를 꺼낸다.

    SQLAlchemy 예외의 `orig`(드라이버 어댑터 예외)와 그 `__cause__`(asyncpg 원본 예외)를 차례로 보고
    `sqlstate`(asyncpg) 또는 `pgcode`(psycopg 계열) 속성을 찾는다. 없으면 None.
    """
    orig = getattr(e, "orig", e)
    for candidate in (orig, getattr(orig, "__cause__", None)):
        if candidate is None:
            continue
        code = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if code:
            return str(code)
    return None


def constraint_name(e: BaseException) -> str | None:
    """IntegrityError에서 위반된 제약 이름을 꺼낸다(asyncpg만 제공).

    `sqlstate`와 같은 순서로 `orig`와 그 `__cause__`를 본다. 찾지 못하면 None.
    """
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

    모든 상태 변경 엔드포인트는 멱등이므로 재실행해도 결과가 같다.
    잠금 순서: load_run → load_partition → load_file → load_dispatch.

    - fn이 정상 반환하면 commit하고 그 값을 돌려준다. 예외(ApiError 포함)가 나면 rollback된다.
    - 재실행은 attempts번까지(첫 시도 포함). 대기는 0.05초 × 2^(i) 지수 증가.
    - 재시도 대상이 아닌 DBAPIError와 마지막 시도의 오류는 그대로 올린다(main.py가 503 등으로 바꾼다).
    - fn은 재실행될 수 있으므로 트랜잭션 밖 부수 효과(HTTP 호출 등)를 두면 안 된다.
    """
    for i in range(attempts):
        try:
            async with engine.begin() as conn:
                return await fn(conn)
        except DBAPIError as e:
            if sqlstate(e) in RETRYABLE_SQLSTATE and i < attempts - 1:
                # 잠금 순서를 지키면 거의 생기지 않지만, 생기면 트랜잭션 전체를 짧게 쉬었다가 다시 실행한다.
                log.warning("tx_retry", sqlstate=sqlstate(e), attempt=i + 1, maxAttempts=attempts)
                await asyncio.sleep(0.05 * 2**i)
                continue
            raise
    raise AssertionError("unreachable")
