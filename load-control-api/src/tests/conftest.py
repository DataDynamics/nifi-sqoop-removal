"""테스트 DB: LCA_TEST_DATABASE_URL이 있으면 그 DB를, 없으면 testcontainers로 PostgreSQL 16을 띄운다.

동시성 규칙(run 행 잠금, CAS, partial unique index)은 실제 PostgreSQL에서만 검증할 수 있다.
"""

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from load_control.config import Settings
from load_control.main import create_app
from load_control.security import token_digest

ROOT = Path(__file__).resolve().parents[2]  # src/tests/conftest.py → 프로젝트 루트
NIFI_TOKEN = "test-nifi-token"
OPERATOR_TOKEN = "test-operator-token"
TABLES = "load_event, load_dispatch, load_validation, load_file, load_partition, load_run"


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """세션 동안 쓸 테스트 DB URL을 돌려준다.

    LCA_TEST_DATABASE_URL이 있으면 그 DB를 쓰고, 없으면 PostgreSQL 16 컨테이너를 띄워
    세션이 끝날 때 함께 내린다.
    """
    url = os.environ.get("LCA_TEST_DATABASE_URL")
    if url:
        yield url
        return
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def migrated_url(database_url: str) -> str:
    """Alembic으로 nifi_ops 스키마를 head까지 올린 뒤 같은 URL을 돌려준다(세션당 한 번)."""
    cfg = Config(str(ROOT / "config" / "alembic.ini"))
    cfg.attributes["database_url"] = database_url
    cfg.attributes["configure_logger"] = False
    command.upgrade(cfg, "head")
    return database_url


@pytest.fixture
def settings(migrated_url: str) -> Settings:
    """테스트용 Settings를 만든다.

    동시성 테스트가 연결을 많이 쓰므로 pool을 넉넉히 잡고, nifi·operator 토큰의 digest를
    등록한다. 로그는 WARNING 이상만 콘솔로 낸다.
    """
    return Settings(
        database={"url": migrated_url, "pool_size": 20, "max_overflow": 20},  # type: ignore[arg-type]
        auth={"token_digests": {"nifi": [token_digest(NIFI_TOKEN)],  # type: ignore[arg-type]
                                "operator": [token_digest(OPERATOR_TOKEN)]}},
        logging={"level": "WARNING", "format": "console"},  # type: ignore[arg-type]
    )


def override(settings: Settings, **sections: dict[str, Any]) -> Settings:
    """섹션 일부 값을 바꾼 새 Settings. 검증을 다시 거친다."""
    data = settings.model_dump()
    for name, values in sections.items():
        data[name] = {**data[name], **values}
    return Settings(**data)


@pytest_asyncio.fixture
async def engine(migrated_url: str) -> AsyncIterator[AsyncEngine]:
    """테스트마다 새 AsyncEngine을 만들고 nifi_ops 테이블을 모두 비운 뒤 넘긴다.

    테스트 사이에 행이 남지 않으므로 각 테스트는 빈 원장에서 시작한다.
    """
    eng = create_async_engine(migrated_url)
    # 자식 테이블부터 한 문장으로 비워 FK 순서 문제 없이 테스트마다 빈 원장에서 시작한다.
    async with eng.begin() as conn:
        await conn.execute(text(f"TRUNCATE {', '.join('nifi_ops.' + t.strip() for t in TABLES.split(','))}"))
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def app(settings: Settings, engine: AsyncEngine) -> AsyncIterator[FastAPI]:
    """lifespan(엔진·로깅 초기화)까지 실행한 FastAPI 앱을 넘긴다."""
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """NiFi 토큰을 단 HTTP 클라이언트. ASGI로 앱을 직접 호출하므로 소켓을 열지 않는다."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                 headers={"Authorization": f"Bearer {NIFI_TOKEN}"}) as c:
        yield c


@pytest_asyncio.fixture
async def operator(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """운영자 토큰을 단 HTTP 클라이언트. 조회와 운영 작업(재전송, 확정 등)에 쓴다."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                 headers={"Authorization": f"Bearer {OPERATOR_TOKEN}"}) as c:
        yield c


class Db:
    """테스트에서 원장 상태를 직접 읽고 바꾸는 얇은 SQL 헬퍼.

    API로 만들기 어려운 상태(오래된 heartbeat, DEAD dispatch 등)를 SQL로 강제하거나,
    API 응답과 별개로 DB에 실제로 남은 값을 확인할 때 쓴다.
    """
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def all(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        """SELECT 결과를 dict 목록으로 돌려준다."""
        async with self.engine.connect() as conn:
            return [dict(m) for m in (await conn.execute(text(sql), params)).mappings().all()]

    async def one(self, sql: str, **params: Any) -> dict[str, Any]:
        """SELECT 결과가 정확히 한 행인지 확인하고 그 행을 돌려준다."""
        rows = await self.all(sql, **params)
        assert len(rows) == 1, rows
        return rows[0]

    async def execute(self, sql: str, **params: Any) -> int:
        """쓰기용: commit한다. 영향 행 수를 돌려준다."""
        async with self.engine.begin() as conn:
            return (await conn.execute(text(sql), params)).rowcount

    async def scalar(self, sql: str, **params: Any) -> Any:
        """SELECT 결과의 첫 행 첫 열 값을 돌려준다(행이 없으면 None)."""
        async with self.engine.connect() as conn:
            return (await conn.execute(text(sql), params)).scalar()


@pytest.fixture
def db(engine: AsyncEngine) -> Db:
    """테스트용 Db 헬퍼를 넘긴다(engine fixture가 테이블을 비운 상태)."""
    return Db(engine)
