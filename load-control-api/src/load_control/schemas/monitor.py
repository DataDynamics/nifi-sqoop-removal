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
    status: str | None
    at: datetime | None
    message: str | None
    dispatch_id: str | None = None   # DISPATCH_DEAD일 때 재전송 대상


class MonitorSummary(ApiModel):
    """대시보드 요약."""

    server_time: datetime
    active_runs: dict[str, int]          # 진행 중 상태별 run 수
    recent_runs: dict[str, int]          # 최근 window 안에 끝난 상태별 run 수
    recent_window_hours: int
    dispatches: dict[str, int]           # PENDING, SENT, DEAD
    cleanup_due: int                     # 지금 정리 대상인 run 수
    alerts: list[Alert]


class ValidationItem(ApiModel):
    """검증 지표 하나."""

    stage: str
    metric_name: str
    expected_value: str | None
    actual_value: str | None
    result: str
    measured_at: datetime


class ValidationList(ApiModel):
    metrics: list[ValidationItem]


class EventItem(ApiModel):
    """이벤트 하나(API 상태 변화 또는 NiFi 오류)."""

    event_time: datetime
    level: str
    name: str
    partition_id: str | None
    process_group: str | None
    error_code: str | None
    message: str | None


class EventList(ApiModel):
    events: list[EventItem]
