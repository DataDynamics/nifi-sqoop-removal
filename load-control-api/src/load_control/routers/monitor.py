"""모니터(TUI)용 읽기 전용 엔드포인트. role nifi, operator 모두 조회할 수 있다."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from load_control.routers.deps import run_tx
from load_control.schemas.monitor import EventList, MonitorSummary, ValidationList
from load_control.security import require_role
from load_control.services import monitor

router = APIRouter(prefix="/v1", tags=["monitor"], dependencies=[Depends(require_role("nifi", "operator"))])


# 요약에 담을 최대 경보 수(ERROR 먼저, 최근 순)
AlertLimit = Annotated[int, Query(alias="alertLimit", ge=1, le=500)]


@router.get("/monitor/summary", response_model=MonitorSummary)
async def get_summary(request: Request, alert_limit: AlertLimit = 100) -> MonitorSummary:
    """진행 중·최근 run 수, dispatch 현황, 정리 대상 수, 경보 목록.

    최근 window는 24시간이다. 경보 종류: PUBLISH_UNKNOWN, DISPATCH_DEAD, RUN_FAILED(최근 실패),
    RUN_STALE(오래 변화 없는 run), CLEANUP_FAILED. 상태를 바꾸지 않는다.
    """
    settings = request.app.state.settings
    return await run_tx(request, lambda conn: monitor.summary(conn, settings, alert_limit=alert_limit))


@router.get("/runs/{run_id}/validations", response_model=ValidationList)
async def get_validations(run_id: UUID, request: Request) -> ValidationList:
    """run의 SOURCE·STAGING·TARGET 지표.

    stage 순서(SOURCE, STAGING, TARGET) 다음 지표 이름 순. run이 없으면 404 RUN_NOT_FOUND.
    """
    return await run_tx(request, lambda conn: monitor.validations(conn, run_id))


@router.get("/runs/{run_id}/events", response_model=EventList)
async def get_events(run_id: UUID, request: Request,
                     limit: Annotated[int, Query(ge=1, le=1000)] = 200) -> EventList:
    """run의 이벤트 타임라인(오래된 순, 최근 limit개).

    API의 상태 변화 이벤트와 NiFi PG-90이 남긴 오류 이벤트를 함께 보여 준다. run이 없으면 404 RUN_NOT_FOUND.
    """
    return await run_tx(request, lambda conn: monitor.events(conn, run_id, limit=limit))
