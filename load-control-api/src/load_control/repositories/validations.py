"""load_validation(stage별 검증 지표) SQL.

stage는 SOURCE(manifest 등록 시 API가 기록), STAGING·TARGET(검증 flow가 보고)이다. 한 지표는
(run_id, stage, metric_name, query_version)으로 식별한다(uq_load_validation_metric).
"""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def upsert_many(conn: AsyncConnection, run_id: UUID, stage: str, query_version: str,
                      metrics: list[dict[str, Any]]) -> None:
    """metrics: [{metric_name, expected_value, actual_value, tolerance, result, details}].

    같은 (run_id, stage, metric_name, query_version)이 이미 있으면 새 값으로 덮어쓰고 measured_at을
    갱신한다. NiFi 재시도로 같은 지표가 다시 와도 행이 늘지 않는다(멱등). query_version이 다르면
    별도 행으로 남는다. metric_name·result는 필수이고 나머지는 없으면 NULL(details는 {})이다.
    metrics가 비어 있으면 아무 SQL도 실행하지 않는다.
    """
    if not metrics:
        return
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_validation (
            run_id, stage, metric_name, expected_value, actual_value, tolerance, result,
            query_version, details)
        VALUES (:run_id, :stage, :metric_name, :expected_value, :actual_value, :tolerance, :result,
                :query_version, CAST(:details AS jsonb))
        ON CONFLICT (run_id, stage, metric_name, query_version)
        DO UPDATE SET
            expected_value = EXCLUDED.expected_value,
            actual_value   = EXCLUDED.actual_value,
            tolerance      = EXCLUDED.tolerance,
            result         = EXCLUDED.result,
            details        = EXCLUDED.details,
            measured_at    = clock_timestamp()
    """), [{"run_id": run_id, "stage": stage, "query_version": query_version,
            "metric_name": m["metric_name"], "expected_value": m.get("expected_value"),
            "actual_value": m.get("actual_value"), "tolerance": m.get("tolerance"),
            "result": m["result"], "details": json.dumps(m.get("details") or {}, ensure_ascii=False)}
           for m in metrics])


async def list_by_stage(conn: AsyncConnection, run_id: UUID, stage: str) -> list[dict[str, Any]]:
    """run의 stage 지표 목록(metric_name, query_version 순).

    여러 query_version의 같은 지표가 모두 나온다. 판정하는 쪽(validation 서비스)은 하나라도
    FAIL이면 통과시키지 않는다.
    """
    rows = (await conn.execute(text("""
        SELECT metric_name, expected_value, actual_value, result, query_version
          FROM nifi_ops.load_validation
         WHERE run_id = :run_id AND stage = :stage
         ORDER BY metric_name, query_version
    """), {"run_id": run_id, "stage": stage})).mappings().all()
    return [dict(m) for m in rows]
