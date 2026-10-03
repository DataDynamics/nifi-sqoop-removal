"""라우터 공통 의존성."""

from collections.abc import Awaitable, Callable
from typing import Annotated, TypeVar

from fastapi import Path, Request
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.db import in_tx
from load_control.schemas.common import PARTITION_ID_PATTERN

# 경로의 partition_id: 4자리 번호(0000~9999) 또는 NULL 파티션. 형식이 다르면 422
PartitionIdPath = Annotated[str, Path(pattern=PARTITION_ID_PATTERN)]

T = TypeVar("T")


async def run_tx(request: Request, fn: Callable[[AsyncConnection], Awaitable[T]]) -> T:
    """서비스 함수를 한 트랜잭션으로 실행한다(deadlock 시 재실행, 설정 database.tx_attempts).

    엔진은 main.lifespan이 app.state.engine에 만든 프로세스 공용 pool이다. fn이 ApiError를 내면
    rollback되고 예외는 그대로 올라가 main.py의 처리기가 응답을 만든다. fn은 재실행될 수 있다.
    """
    return await in_tx(request.app.state.engine, fn,
                       attempts=request.app.state.settings.database.tx_attempts)
