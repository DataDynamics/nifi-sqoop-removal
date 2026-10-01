"""load_event 기록. NiFi PG-90도 같은 테이블에 쓰지만 이 모듈은 API의 상태 전이 이벤트만 남긴다."""

import json
import uuid
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.repositories.runs import RunRow


async def record(conn: AsyncConnection, name: str, run: RunRow | None = None, *,
                 run_id: UUID | None = None, level: str = "INFO", partition_id: str | None = None,
                 chunk_index: int | None = None, row_count: int | None = None,
                 error_class: str | None = None, error_code: str | None = None,
                 message: str | None = None, details: dict[str, Any] | None = None) -> None:
    """상태 전이 이벤트를 같은 트랜잭션에서 load_event에 기록한다(가이드 14.4)."""
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_event (
            event_id, event_level, event_name, run_id, job_key, business_key, partition_id,
            chunk_index, process_group, processor_name, row_count, error_class, error_code,
            message, details)
        VALUES (:event_id, :level, :name, :run_id, :job_key, :business_key, :partition_id,
                :chunk_index, 'LOAD_CONTROL_API', :processor_name, :row_count, :error_class,
                :error_code, :message, CAST(:details AS jsonb))
    """), {"event_id": uuid.uuid4(), "level": level, "name": name,
           "run_id": run.run_id if run else run_id,
           "job_key": run.job_key if run else None,
           "business_key": run.business_key if run else None,
           "partition_id": partition_id, "chunk_index": chunk_index, "processor_name": name.lower(),
           "row_count": row_count, "error_class": error_class, "error_code": error_code,
           "message": (message or "")[:2000] or None,
           "details": json.dumps(details or {}, ensure_ascii=False, default=str)})
