import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def upsert_many(conn: AsyncConnection, run_id: UUID, stage: str, query_version: str,
                      metrics: list[dict[str, Any]]) -> None:
    """metrics: [{metric_name, expected_value, actual_value, tolerance, result, details}]."""
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
