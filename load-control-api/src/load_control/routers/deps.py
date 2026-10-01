from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Path, Request
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.db import in_tx
from load_control.schemas.common import PARTITION_ID_PATTERN

PartitionIdPath = Annotated[str, Path(pattern=PARTITION_ID_PATTERN)]


async def run_tx[T](request: Request, fn: Callable[[AsyncConnection], Awaitable[T]]) -> T:
    return await in_tx(request.app.state.engine, fn,
                       attempts=request.app.state.settings.database.tx_attempts)
