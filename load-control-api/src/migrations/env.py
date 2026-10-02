"""Alembic async 환경. ORM 모델이 없으므로 autogenerate는 쓰지 않고 SQL을 직접 작성한다(API 설계 9.9)."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

config = context.config
if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

VERSION_TABLE_SCHEMA = "nifi_ops"


def database_url() -> str:
    """테스트가 넘긴 URL, 없으면 config.yaml의 database.migration_url(없으면 database.url)."""
    url = config.attributes.get("database_url")
    if url:
        return str(url)
    from load_control.config import Settings

    db = Settings.load().database
    return db.migration_url or db.url


def do_run_migrations(connection: Connection) -> None:
    # 버전 테이블도 nifi_ops에 둔다. 스키마가 없으면 먼저 만든다.
    connection.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {VERSION_TABLE_SCHEMA}")
    context.configure(connection=connection, target_metadata=None,
                      version_table_schema=VERSION_TABLE_SCHEMA, transaction_per_migration=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(database_url())
    async with engine.begin() as conn:
        await conn.run_sync(do_run_migrations)
    await engine.dispose()


def run_migrations_offline() -> None:
    context.configure(url=database_url(), target_metadata=None, literal_binds=True,
                      version_table_schema=VERSION_TABLE_SCHEMA)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
