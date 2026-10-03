"""DB 엔진과 트랜잭션 및 SQLSTATE 보조 함수.

ORM 없이 SQLAlchemy Core와 asyncpg 드라이버를 사용한다. 서비스 함수는 `AsyncConnection`으로 SQL만
실행하고, commit·rollback·재시도 같은 트랜잭션 경계는 `in_tx`가 관리한다.
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
    """프로세스에서 공유할 비동기 DB 엔진을 만든다.

    `pool_pre_ping`으로 끊긴 연결을 사용 전에 걸러낸다. 프로세스당 최대 연결 수는
    `database.pool_size + database.max_overflow`이며, 실제 연결은 처음 사용할 때 열린다.
    호출자는 종료할 때 `dispose()`로 엔진을 닫아야 한다.
    """
    return create_async_engine(
        settings.database.url,
        pool_size=settings.database.pool_size,
        max_overflow=settings.database.max_overflow,
        pool_pre_ping=True,
    )


def sqlstate(e: BaseException) -> str | None:
    """SQLAlchemy가 감싼 DB 드라이버 예외에서 SQLSTATE를 찾는다.

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
    """`IntegrityError`에서 위반한 DB 제약 이름을 찾는다.

    `sqlstate`와 같은 순서로 `orig`와 그 `__cause__`를 확인한다. 현재 asyncpg만 제약 이름을
    제공하며, 찾지 못하면 `None`을 반환한다.
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
    """`fn`을 한 트랜잭션에서 실행하고 일시적인 동시성 오류를 재시도한다.

    모든 상태 변경 엔드포인트는 멱등성을 보장하므로 트랜잭션 전체를 다시 실행해도 결과가 같다.
    교착을 줄이기 위해 잠금 순서는 `load_run → load_partition → load_file → load_dispatch`로
    통일한다.

    - `fn`이 반환하면 commit하고 반환값을 그대로 돌려준다. 예외가 발생하면 rollback한다.
    - deadlock과 serialization 실패만 `attempts`회까지 지수 backoff로 재시도한다.
    - 재시도할 수 없는 오류와 마지막 시도의 오류는 호출자에게 그대로 전달한다.
    - `fn`은 반복 실행될 수 있으므로 HTTP 호출 같은 트랜잭션 외부 부수 효과를 포함하면 안 된다.
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
