"""라우터 공통 의존성."""

from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Path, Request
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.db import in_tx
from load_control.schemas.common import PARTITION_ID_PATTERN

PartitionIdPath = Annotated[str, Path(pattern=PARTITION_ID_PATTERN)]


async def run_tx[T](request: Request, fn: Callable[[AsyncConnection], Awaitable[T]]) -> T:
    """서비스 함수를 한 트랜잭션으로 실행한다(deadlock 시 재실행, 설정 database.tx_attempts)."""
    return await in_tx(request.app.state.engine, fn,
                       attempts=request.app.state.settings.database.tx_attempts)
