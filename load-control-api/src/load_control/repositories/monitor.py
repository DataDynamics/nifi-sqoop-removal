"""모니터(TUI)용 읽기 전용 SQL. 상태를 바꾸지 않는다.

잠금을 잡지 않으므로 처리 중인 요청과 경합하지 않는다. 보이는 값은 조회 시점의 스냅샷이다.
"""

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
    """alerts()의 한 행. kind별로 쓰는 컬럼이 다르며 해당 없는 값은 None이다.

    kind: PUBLISH_UNKNOWN, DISPATCH_DEAD, RUN_FAILED, RUN_STALE, CLEANUP_FAILED.
    severity: ERROR 또는 WARN. dispatch_id는 DISPATCH_DEAD일 때만 채워진다(TUI의 재전송 대상).
    """

    kind: str
    severity: str
    run_id: UUID | None
    job_key: str | None
    business_key: str | None
    status: str | None
    at: datetime | None
    message: str | None
    dispatch_id: UUID | None


async def active_counts(conn: AsyncConnection) -> dict[str, int]:
    """진행 중 상태별 run 수. 0건인 상태는 결과에 없다."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_run WHERE status = ANY(:statuses) GROUP BY status
    """), {"statuses": list(ACTIVE_STATUSES)})).all()
    return {str(r[0]): int(r[1]) for r in rows}


async def recent_finished_counts(conn: AsyncConnection, window: timedelta) -> dict[str, int]:
    """최근 window 안에 끝난 run의 상태별 수.

    ACTIVE_STATUSES가 아닌 상태를 끝난 것으로 본다. 끝난 시각은 completed_at이고, 없으면 heartbeat_at으로
    대신한다(cleanup의 ended_at과 같은 규칙).
    """
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_run
         WHERE status <> ALL(:active)
           AND COALESCE(completed_at, heartbeat_at) >= clock_timestamp() - CAST(:window AS interval)
         GROUP BY status
    """), {"active": list(ACTIVE_STATUSES), "window": window})).all()
    return {str(r[0]): int(r[1]) for r in rows}


async def alerts(conn: AsyncConnection, *, window: timedelta, validation_stale: timedelta,
                 stale: timedelta, limit: int) -> list[AlertRow]:
    """운영자가 볼 일: 게시 결과 불명, DEAD dispatch, 최근 실패, 멈춘 run, 정리 실패.

    다섯 가지 조건을 UNION ALL로 모아 ERROR 먼저, 그 안에서 최근 시각 순으로 limit개를 돌려준다.
    - PUBLISH_UNKNOWN: 기간 제한 없이 모두(운영자가 확정할 때까지 남는다).
    - DISPATCH_DEAD: 기간 제한 없이 모두(resend하거나 ACK될 때까지 남는다).
    - RUN_FAILED: window 안에 끝난 실패·TIMED_OUT run.
    - RUN_STALE: 검증·게시 대기 상태는 validation_stale, CREATED·EXTRACTING은 stale 동안 heartbeat가 없는 run.
    - CLEANUP_FAILED: window 안에 PG-70이 남긴 CLEANUP_FAILED 이벤트 중 아직 정리되지 않은 run(run당 1건).
    """
    # 각 UNION ALL 분기는 같은 컬럼 순서(kind, severity, run_id, job_key, business_key, status, at,
    # message, dispatch_id)를 지킨다. CLEANUP_FAILED 분기는 DISTINCT ON (e.run_id)와 event_time DESC로
    # run당 가장 최근 정리 실패 이벤트 한 건만 보여 준다(ORDER BY를 쓰려고 하위 쿼리로 감쌌다).
    # 이후 run이 정리되면(cleaned_at 기록) NOT EXISTS 조건으로 경보가 사라진다.
    rows = (await conn.execute(text("""
        SELECT * FROM (
            SELECT 'PUBLISH_UNKNOWN' AS kind, 'ERROR' AS severity, run_id, job_key, business_key, status,
                   heartbeat_at AS at, error_message AS message, NULL::uuid AS dispatch_id
              FROM nifi_ops.load_run WHERE status = 'PUBLISH_UNKNOWN'
            UNION ALL
            SELECT 'DISPATCH_DEAD', 'ERROR', r.run_id, r.job_key, r.business_key, r.status,
                   d.next_attempt_at, d.dispatch_type || ': ' || COALESCE(d.last_error, ''), d.dispatch_id
              FROM nifi_ops.load_dispatch d JOIN nifi_ops.load_run r USING (run_id)
             WHERE d.status = 'DEAD'
            UNION ALL
            SELECT 'RUN_FAILED', 'ERROR', run_id, job_key, business_key, status,
                   COALESCE(completed_at, heartbeat_at),
                   COALESCE(error_code, '') || ' ' || COALESCE(error_message, ''), NULL
              FROM nifi_ops.load_run
             WHERE status = ANY(:failed)
               AND COALESCE(completed_at, heartbeat_at) >= clock_timestamp() - CAST(:window AS interval)
            UNION ALL
            SELECT 'RUN_STALE', 'WARN', run_id, job_key, business_key, status, heartbeat_at,
                   '마지막 변화 이후 오래 멈춤', NULL
              FROM nifi_ops.load_run
             WHERE (status IN ('STAGE_VALIDATING', 'PUBLISHED', 'EXTRACTED_VALIDATED')
                    AND heartbeat_at < clock_timestamp() - CAST(:validation_stale AS interval))
                OR (status IN ('CREATED', 'EXTRACTING')
                    AND heartbeat_at < clock_timestamp() - CAST(:stale AS interval))
            UNION ALL
            SELECT * FROM (
                SELECT DISTINCT ON (e.run_id) 'CLEANUP_FAILED', 'WARN', e.run_id, e.job_key, e.business_key,
                       NULL, e.event_time, e.message, NULL::uuid
                  FROM nifi_ops.load_event e
                 WHERE e.event_name = 'CLEANUP_FAILED'
                   AND e.event_time >= clock_timestamp() - CAST(:window AS interval)
                   AND NOT EXISTS (SELECT 1 FROM nifi_ops.load_run r
                                    WHERE r.run_id = e.run_id AND r.cleaned_at IS NOT NULL)
                 ORDER BY e.run_id, e.event_time DESC
            ) c
        ) a
        ORDER BY CASE severity WHEN 'ERROR' THEN 0 ELSE 1 END, at DESC NULLS LAST
        LIMIT :limit
    """), {"failed": sorted(CLEANUP_FAILED_STATUSES), "window": window,
           "validation_stale": validation_stale, "stale": stale, "limit": limit})).mappings().all()
    return [AlertRow(**dict(r)) for r in rows]


async def validations(conn: AsyncConnection, run_id: UUID) -> list[dict[str, Any]]:
    """run의 모든 stage 지표를 SOURCE → STAGING → TARGET, 지표 이름 순으로 돌려준다."""
    rows = (await conn.execute(text("""
        SELECT stage, metric_name, expected_value, actual_value, result, measured_at
          FROM nifi_ops.load_validation WHERE run_id = :run_id
         ORDER BY CASE stage WHEN 'SOURCE' THEN 0 WHEN 'STAGING' THEN 1 WHEN 'TARGET' THEN 2 ELSE 3 END,
                  metric_name
    """), {"run_id": run_id})).mappings().all()
    return [dict(r) for r in rows]


async def events(conn: AsyncConnection, run_id: UUID, limit: int) -> list[dict[str, Any]]:
    """run의 이벤트(API 상태 변화 + NiFi 오류), 오래된 순.

    최근 limit개를 먼저 고른 뒤(안쪽 DESC) 바깥에서 오래된 순으로 다시 정렬해 타임라인으로 보여 준다.
    """
    rows = (await conn.execute(text("""
        SELECT * FROM (
            SELECT event_time, event_level, event_name, partition_id, process_group, error_code, message
              FROM nifi_ops.load_event WHERE run_id = :run_id
             ORDER BY event_time DESC LIMIT :limit) t
        ORDER BY event_time
    """), {"run_id": run_id, "limit": limit})).mappings().all()
    return [dict(r) for r in rows]
