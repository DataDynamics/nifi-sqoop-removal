"""모니터(TUI)용 조회 모델."""

from datetime import datetime

from load_control.schemas.common import ApiModel


class Alert(ApiModel):
    """운영자가 볼 일 하나."""

    kind: str          # PUBLISH_UNKNOWN, DISPATCH_DEAD, RUN_FAILED, RUN_STALE, CLEANUP_FAILED
    severity: str      # ERROR, WARN
    run_id: str | None
    job_key: str | None
    business_key: str | None
    status: str | None   # run 상태(CLEANUP_FAILED는 null)
    at: datetime | None  # 경보 기준 시각(종류별: 마지막 heartbeat, 종료 시각, 이벤트 시각 등)
    message: str | None  # 오류 메시지 등(최대 500자)
    dispatch_id: str | None = None   # DISPATCH_DEAD일 때 재전송 대상


class MonitorSummary(ApiModel):
    """대시보드 요약."""

    server_time: datetime
    active_runs: dict[str, int]          # 진행 중 상태별 run 수
    recent_runs: dict[str, int]          # 최근 window 안에 끝난 상태별 run 수
    recent_window_hours: int             # recent_runs의 window(시간)
    dispatches: dict[str, int]           # PENDING, SENT, DEAD
    cleanup_due: int                     # 지금 정리 대상인 run 수
    alerts: list[Alert]


class ValidationItem(ApiModel):
    """검증 지표 하나."""

    stage: str                   # SOURCE, STAGING, TARGET
    metric_name: str             # 예: SOURCE_COUNT, STAGE_COUNT, AMOUNT_SUM
    expected_value: str | None
    actual_value: str | None
    result: str                  # PASS, FAIL, WARN
    measured_at: datetime


class ValidationList(ApiModel):
    """run의 검증 지표 목록(stage 순, 같은 stage 안에서는 지표 이름 순)."""

    metrics: list[ValidationItem]


class EventItem(ApiModel):
    """이벤트 하나(API 상태 변화 또는 NiFi 오류)."""

    event_time: datetime
    level: str                   # INFO, WARN, ERROR
    name: str                    # 이벤트 이름(예: RUN_STARTED, PARTITION_FAILED, DISPATCH_DEAD)
    partition_id: str | None     # 파티션 이벤트일 때
    process_group: str | None    # 남긴 곳: API 이벤트는 LOAD_CONTROL_API, NiFi 이벤트는 PG 이름
    error_code: str | None
    message: str | None


class EventList(ApiModel):
    """run의 이벤트 목록(오래된 순)."""

    events: list[EventItem]
