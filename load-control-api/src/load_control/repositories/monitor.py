"""모니터(TUI)용 읽기 전용 SQL. 상태를 바꾸지 않는다."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.domain import CLEANUP_FAILED_STATUSES

# 진행 중으로 보는 상태(같은 업무일자의 새 run을 막는 상태와 같다)
ACTIVE_STATUSES = ("CREATED", "EXTRACTING", "EXTRACTED_VALIDATED", "STAGE_VALIDATING",
                   "STAGING_VALIDATED", "PUBLISHING", "PUBLISHED", "PUBLISH_UNKNOWN")


@dataclass(frozen=True, slots=True)
class AlertRow:
    kind: str
    severity: str
    run_id: UUID | None
    job_key: str | None
    business_key: str | None
    status: str | None
    at: datetime | None
    message: str | None


async def active_counts(conn: AsyncConnection) -> dict[str, int]:
    """진행 중 상태별 run 수."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_run WHERE status = ANY(:statuses) GROUP BY status
    """), {"statuses": list(ACTIVE_STATUSES)})).all()
    return {str(r[0]): int(r[1]) for r in rows}


async def recent_finished_counts(conn: AsyncConnection, window: timedelta) -> dict[str, int]:
    """최근 window 안에 끝난 run의 상태별 수."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_run
         WHERE status <> ALL(:active)
           AND COALESCE(completed_at, heartbeat_at) >= clock_timestamp() - CAST(:window AS interval)
         GROUP BY status
    """), {"active": list(ACTIVE_STATUSES), "window": window})).all()
    return {str(r[0]): int(r[1]) for r in rows}


async def alerts(conn: AsyncConnection, *, window: timedelta, validation_stale: timedelta,
                 stale: timedelta, limit: int) -> list[AlertRow]:
    """운영자가 볼 일: 게시 결과 불명, DEAD dispatch, 최근 실패, 멈춘 run, 정리 실패."""
    rows = (await conn.execute(text("""
        SELECT * FROM (
            SELECT 'PUBLISH_UNKNOWN' AS kind, 'ERROR' AS severity, run_id, job_key, business_key, status,
                   heartbeat_at AS at, error_message AS message
              FROM nifi_ops.load_run WHERE status = 'PUBLISH_UNKNOWN'
            UNION ALL
            SELECT 'DISPATCH_DEAD', 'ERROR', r.run_id, r.job_key, r.business_key, r.status,
                   d.next_attempt_at, d.dispatch_type || ': ' || COALESCE(d.last_error, '')
              FROM nifi_ops.load_dispatch d JOIN nifi_ops.load_run r USING (run_id)
             WHERE d.status = 'DEAD'
            UNION ALL
            SELECT 'RUN_FAILED', 'ERROR', run_id, job_key, business_key, status,
                   COALESCE(completed_at, heartbeat_at),
                   COALESCE(error_code, '') || ' ' || COALESCE(error_message, '')
              FROM nifi_ops.load_run
             WHERE status = ANY(:failed)
               AND COALESCE(completed_at, heartbeat_at) >= clock_timestamp() - CAST(:window AS interval)
            UNION ALL
            SELECT 'RUN_STALE', 'WARN', run_id, job_key, business_key, status, heartbeat_at,
                   '마지막 변화 이후 오래 멈춤'
              FROM nifi_ops.load_run
             WHERE (status IN ('STAGE_VALIDATING', 'PUBLISHED', 'EXTRACTED_VALIDATED')
                    AND heartbeat_at < clock_timestamp() - CAST(:validation_stale AS interval))
                OR (status IN ('CREATED', 'EXTRACTING')
                    AND heartbeat_at < clock_timestamp() - CAST(:stale AS interval))
            UNION ALL
            SELECT DISTINCT ON (e.run_id) 'CLEANUP_FAILED', 'WARN', e.run_id, e.job_key, e.business_key,
                   NULL, e.event_time, e.message
              FROM nifi_ops.load_event e
             WHERE e.event_name = 'CLEANUP_FAILED'
               AND e.event_time >= clock_timestamp() - CAST(:window AS interval)
               AND NOT EXISTS (SELECT 1 FROM nifi_ops.load_run r
                                WHERE r.run_id = e.run_id AND r.cleaned_at IS NOT NULL)
        ) a
        ORDER BY CASE severity WHEN 'ERROR' THEN 0 ELSE 1 END, at DESC NULLS LAST
        LIMIT :limit
    """), {"failed": sorted(CLEANUP_FAILED_STATUSES), "window": window,
           "validation_stale": validation_stale, "stale": stale, "limit": limit})).mappings().all()
    return [AlertRow(**dict(r)) for r in rows]


async def validations(conn: AsyncConnection, run_id: UUID) -> list[dict[str, Any]]:
    """run의 모든 stage 지표."""
    rows = (await conn.execute(text("""
        SELECT stage, metric_name, expected_value, actual_value, result, measured_at
          FROM nifi_ops.load_validation WHERE run_id = :run_id
         ORDER BY CASE stage WHEN 'SOURCE' THEN 0 WHEN 'STAGING' THEN 1 WHEN 'TARGET' THEN 2 ELSE 3 END,
                  metric_name
    """), {"run_id": run_id})).mappings().all()
    return [dict(r) for r in rows]


async def events(conn: AsyncConnection, run_id: UUID, limit: int) -> list[dict[str, Any]]:
    """run의 이벤트(API 상태 변화 + NiFi 오류), 오래된 순."""
    rows = (await conn.execute(text("""
        SELECT * FROM (
            SELECT event_time, event_level, event_name, partition_id, process_group, error_code, message
              FROM nifi_ops.load_event WHERE run_id = :run_id
             ORDER BY event_time DESC LIMIT :limit) t
        ORDER BY event_time
    """), {"run_id": run_id, "limit": limit})).mappings().all()
    return [dict(r) for r in rows]
