import uuid
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.db import UNIQUE_VIOLATION, constraint_name, sqlstate
from load_control.domain import ALLOWED_RUN_FAILURES, RunStatus
from load_control.errors import Conflict, NotFound, Unprocessable
from load_control.repositories import dispatch, events, partitions, runs
from load_control.schemas.runs import (
    DispatchSummary,
    PartitionSummary,
    RunCreateRequest,
    RunCreateResponse,
    RunDetail,
    RunFailRequest,
    RunFailResponse,
)


def build_run_paths(req: RunCreateRequest, run_id: UUID) -> tuple[str, str]:
    """run 전용 HDFS 경로와 stage table 이름(가이드 1장 경로 원칙)."""
    hdfs_run_path = f"{req.hdfs_root.rstrip('/')}/{req.job_key}/run_id={run_id}"
    stage_table = f"{req.stage_table_prefix}{run_id.hex}".lower()
    return hdfs_run_path, stage_table


async def create_run(conn: AsyncConnection, req: RunCreateRequest) -> RunCreateResponse:
    if ".." in req.hdfs_root.split("/"):
        raise Unprocessable("INVALID_HDFS_ROOT")
    run_id = uuid.uuid4()
    hdfs_run_path, stage_table = build_run_paths(req, run_id)
    try:
        await runs.insert(conn, run_id=run_id, job_key=req.job_key, business_key=req.business_key,
                          hdfs_run_path=hdfs_run_path, stage_table_name=stage_table,
                          parameters={**req.parameters, "allowEmptySource": req.allow_empty_source})
    except IntegrityError as e:
        if sqlstate(e) == UNIQUE_VIOLATION and constraint_name(e) in (None, "uq_load_run_active"):
            raise Conflict("DUPLICATE_ACTIVE_RUN", jobKey=req.job_key,
                           businessKey=req.business_key) from e
        raise
    run = await runs.get(conn, run_id)
    await events.record(conn, "RUN_STARTED", run)
    return RunCreateResponse(run_id=str(run_id), status=RunStatus.CREATED,
                             hdfs_run_path=hdfs_run_path, stage_table=stage_table)


async def fail_run(conn: AsyncConnection, run_id: UUID, req: RunFailRequest) -> RunFailResponse:
    if (req.expected_status, req.fail_status) not in ALLOWED_RUN_FAILURES:
        raise Unprocessable("FAIL_TRANSITION_NOT_ALLOWED",
                            expectedStatus=req.expected_status, failStatus=req.fail_status)
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    if run.status == req.fail_status:  # 멱등 재요청
        return RunFailResponse(run_id=str(run_id), run_status=RunStatus(run.status), changed=False)
    if run.status != req.expected_status:
        raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status)
    await runs.fail(conn, run_id, expected=req.expected_status, to=req.fail_status,
                    stage=req.error_stage, code=req.error_code, message=req.message)
    await events.record(conn, "RUN_FAILED", run, level="ERROR", error_code=req.error_code,
                        message=req.message, details={"stage": req.error_stage,
                                                      "from": run.status, "to": req.fail_status})
    return RunFailResponse(run_id=str(run_id), run_status=req.fail_status, changed=True)


async def get_run_detail(conn: AsyncConnection, run_id: UUID) -> RunDetail:
    run = await runs.get(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    parts = await partitions.list_for_run(conn, run_id)
    counts: dict[str, int] = {}
    for p in parts:
        counts[p.status] = counts.get(p.status, 0) + 1
    disp = await dispatch.list_for_run(conn, run_id)
    return RunDetail(
        run_id=str(run.run_id), job_key=run.job_key, business_key=run.business_key,
        status=RunStatus(run.status),
        snapshot_scn=str(run.snapshot_scn) if run.snapshot_scn is not None else None,
        source_count=run.source_count, expected_partition_count=run.expected_partition_count,
        success_partition_count=run.success_partition_count,
        failed_partition_count=run.failed_partition_count, extracted_count=run.extracted_count,
        hdfs_run_path=run.hdfs_run_path, stage_table=run.stage_table_name,
        started_at=run.started_at, heartbeat_at=run.heartbeat_at,
        extract_completed_at=run.extract_completed_at, completed_at=run.completed_at,
        error_stage=run.error_stage, error_code=run.error_code, error_message=run.error_message,
        partition_counts=counts,
        partitions=[PartitionSummary(
            partition_id=p.partition_id, status=p.status, expected_row_count=p.expected_row_count,
            actual_row_count=p.actual_row_count, file_count=p.file_count,
            attempt_count=p.attempt_count, worker_node=p.worker_node, error_code=p.error_code)
            for p in parts],
        dispatches=[DispatchSummary(
            dispatch_id=str(d.dispatch_id), dispatch_type=d.dispatch_type,
            partition_id=d.partition_id, status=d.status, attempt_count=d.attempt_count,
            sent_at=d.sent_at, acked_at=d.acked_at) for d in disp],
    )
