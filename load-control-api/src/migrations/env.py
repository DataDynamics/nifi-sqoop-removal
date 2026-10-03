"""Alembic async 환경. ORM 모델이 없으므로 autogenerate는 쓰지 않고 SQL을 직접 작성한다.

bin/migrate.sh(alembic upgrade)와 테스트가 이 모듈을 실행한다. 접속 URL은 database_url()이 정하고,
alembic_version 테이블도 nifi_ops 스키마에 둔다. online 모드는 async 엔진(asyncpg)으로 접속해
run_sync로 동기 alembic API를 실행하며, revision마다 별도 트랜잭션으로 적용한다.
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

config = context.config
# 테스트는 configure_logger=False를 넘겨 alembic.ini의 로깅 설정이 테스트 로깅을 덮어쓰지 않게 한다.
if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

# alembic_version 테이블을 둘 스키마. 원장 테이블과 같은 곳에 두어 public을 건드리지 않는다.
VERSION_TABLE_SCHEMA = "nifi_ops"


def database_url() -> str:
    """테스트가 넘긴 URL, 없으면 config.yaml의 database.migration_url(없으면 database.url).

    migration_url은 DDL 권한이 있는 migration 전용 계정이다. API 런타임 계정에는 DDL 권한을 주지
    않으므로 운영에서는 migration_url을 설정한다. Settings는 테스트 경로에서 config.yaml 없이도
    동작하도록 필요할 때만 import한다.
    """
    url = config.attributes.get("database_url")
    if url:
        return str(url)
    from load_control.config import Settings

    db = Settings.load().database
    return db.migration_url or db.url


def do_run_migrations(connection: Connection) -> None:
    """동기 연결에서 버전 스키마를 준비하고 migration을 적용한다(run_sync로 호출된다).

    transaction_per_migration=True라서 revision 하나가 실패해도 앞서 끝난 revision은 커밋된 채 남는다.
    """
    # 버전 테이블도 nifi_ops에 둔다. 스키마가 없으면 먼저 만든다.
    connection.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {VERSION_TABLE_SCHEMA}")
    context.configure(connection=connection, target_metadata=None,
                      version_table_schema=VERSION_TABLE_SCHEMA, transaction_per_migration=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """async 엔진으로 접속해 do_run_migrations를 실행하고 엔진을 정리한다(online 모드)."""
    engine = create_async_engine(database_url())
    async with engine.begin() as conn:
        await conn.run_sync(do_run_migrations)
    await engine.dispose()


def run_migrations_offline() -> None:
    """DB에 접속하지 않고 SQL 스크립트만 출력한다(alembic upgrade --sql).

    값은 literal_binds로 SQL에 직접 넣는다. online 모드와 달리 nifi_ops 스키마를 미리 만들지 않으므로
    출력된 스크립트의 alembic_version 생성 전에 스키마가 있어야 한다.
    """
    context.configure(url=database_url(), target_metadata=None, literal_binds=True,
                      version_table_schema=VERSION_TABLE_SCHEMA)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
