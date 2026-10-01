"""테스트 DB: LCA_TEST_DATABASE_URL이 있으면 그 DB를, 없으면 testcontainers로 PostgreSQL 16을 띄운다.

동시성 규칙(run 행 잠금, CAS, partial unique index)은 실제 PostgreSQL에서만 검증할 수 있다(API 설계 11.1).
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

ROOT = Path(__file__).resolve().parents[1]
NIFI_TOKEN = "test-nifi-token"
OPERATOR_TOKEN = "test-operator-token"
TABLES = "load_event, load_dispatch, load_validation, load_file, load_partition, load_run"


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    url = os.environ.get("LCA_TEST_DATABASE_URL")
    if url:
        yield url
        return
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def migrated_url(database_url: str) -> str:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.attributes["database_url"] = database_url
    cfg.attributes["configure_logger"] = False
    command.upgrade(cfg, "head")
    return database_url


@pytest.fixture
def settings(migrated_url: str) -> Settings:
    return Settings(
        database_url=migrated_url,  # type: ignore[arg-type]
        token_digests={"nifi": [token_digest(NIFI_TOKEN)],
                       "operator": [token_digest(OPERATOR_TOKEN)]},
        db_pool_size=20,
        db_max_overflow=20,
        log_json=False,
        log_level="WARNING",
    )


@pytest_asyncio.fixture
async def engine(migrated_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_url)
    async with eng.begin() as conn:
        await conn.execute(text(f"TRUNCATE {', '.join('nifi_ops.' + t.strip() for t in TABLES.split(','))}"))
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def app(settings: Settings, engine: AsyncEngine) -> AsyncIterator[FastAPI]:
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                 headers={"Authorization": f"Bearer {NIFI_TOKEN}"}) as c:
        yield c


@pytest_asyncio.fixture
async def operator(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                 headers={"Authorization": f"Bearer {OPERATOR_TOKEN}"}) as c:
        yield c


class Db:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def all(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        async with self.engine.connect() as conn:
            return [dict(m) for m in (await conn.execute(text(sql), params)).mappings().all()]

    async def one(self, sql: str, **params: Any) -> dict[str, Any]:
        rows = await self.all(sql, **params)
        assert len(rows) == 1, rows
        return rows[0]

    async def execute(self, sql: str, **params: Any) -> int:
        """쓰기용: commit한다. 영향 행 수를 돌려준다."""
        async with self.engine.begin() as conn:
            return (await conn.execute(text(sql), params)).rowcount

    async def scalar(self, sql: str, **params: Any) -> Any:
        async with self.engine.connect() as conn:
            return (await conn.execute(text(sql), params)).scalar()


@pytest.fixture
def db(engine: AsyncEngine) -> Db:
    return Db(engine)
