"""run 생성, 단계 실패 기록, 조회.

파티션 단위 처리(claim, chunk, 파티션 실패)는 completion, manifest는 manifest 모듈이 맡는다.
"""

import uuid
from uuid import UUID

import structlog
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
    RunListItem,
)

log = structlog.get_logger(__name__)


def build_run_paths(req: RunCreateRequest, run_id: UUID) -> tuple[str, str]:
    """run 전용 HDFS 경로와 stage table 이름.

    경로는 `{hdfsRoot}/{jobKey}/run_id={runId}`, table은 `{stageTablePrefix}{runId hex}`를
    소문자로 만든 것이다.
    run_id를 넣어 run마다 겹치지 않게 하므로, 정리(PG-70)는 이 경로·table만 지우면 된다.
    """
    hdfs_run_path = f"{req.hdfs_root.rstrip('/')}/{req.job_key}/run_id={run_id}"
    stage_table = f"{req.stage_table_prefix}{run_id.hex}".lower()
    return hdfs_run_path, stage_table


async def create_run(conn: AsyncConnection, req: RunCreateRequest) -> RunCreateResponse:
    """CREATED run을 만든다. 같은 업무키의 활성 run이 있으면 409 DUPLICATE_ACTIVE_RUN.

    활성 run 중복은 미리 조회하지 않고 uq_load_run_active 부분 유니크 인덱스로 막는다. 그래서 동시 요청이
    와도 한 건만 만들어진다. allowEmptySource는 이후 manifest 검증에서 쓰도록 parameters에 함께 저장한다.
    RUN_STARTED 이벤트를 남긴다.

    Raises:
        Unprocessable: INVALID_HDFS_ROOT. hdfsRoot에 '..' 경로 조각이 있음.
        Conflict: DUPLICATE_ACTIVE_RUN. 같은 (jobKey, businessKey)의 활성 run이 있음.
    """
    if ".." in req.hdfs_root.split("/"):
        raise Unprocessable("INVALID_HDFS_ROOT")
    run_id = uuid.uuid4()
    hdfs_run_path, stage_table = build_run_paths(req, run_id)
    try:
        await runs.insert(conn, run_id=run_id, job_key=req.job_key, business_key=req.business_key,
                          hdfs_run_path=hdfs_run_path, stage_table_name=stage_table,
                          parameters={**req.parameters, "allowEmptySource": req.allow_empty_source})
    except IntegrityError as e:
        # 드라이버가 제약 이름을 주지 않는 경우(None)도 활성 run 중복으로 본다.
        # 다른 유니크 위반은 그대로 올린다.
        # 트랜잭션은 이미 실패 상태이므로 예외를 올려 rollback되게 한다.
        if sqlstate(e) == UNIQUE_VIOLATION and constraint_name(e) in (None, "uq_load_run_active"):
            log.warning("run_duplicate_active", jobKey=req.job_key, businessKey=req.business_key)
            raise Conflict("DUPLICATE_ACTIVE_RUN", jobKey=req.job_key,
                           businessKey=req.business_key) from e
        raise
    run = await runs.get(conn, run_id)
    await events.record(conn, "RUN_STARTED", run)
    log.info("run_created", runId=str(run_id), jobKey=req.job_key, businessKey=req.business_key,
             hdfsRunPath=hdfs_run_path, stageTable=stage_table)
    return RunCreateResponse(run_id=str(run_id), status=RunStatus.CREATED,
                             hdfs_run_path=hdfs_run_path, stage_table=stage_table)


async def fail_run(conn: AsyncConnection, run_id: UUID, req: RunFailRequest) -> RunFailResponse:
    """파티션 외 단계(SCN, 검증, 게시 등)의 실패를 기록한다. 허용 전이는 domain.ALLOWED_RUN_FAILURES.

    run 행을 잠근 뒤 expectedStatus → failStatus CAS로 바꾸고 RUN_FAILED 이벤트를 남긴다. 이미 failStatus면
    changed=False로 성공 응답한다(멱등).

    Raises:
        Unprocessable: FAIL_TRANSITION_NOT_ALLOWED. (expected, fail) 조합이 허용 목록에 없음
            (잠그기 전에 거른다).
        NotFound: RUN_NOT_FOUND.
        Conflict: RUN_STATUS_MISMATCH. 현재 상태가 expectedStatus가 아님.
    """
    if (req.expected_status, req.fail_status) not in ALLOWED_RUN_FAILURES:
        raise Unprocessable("FAIL_TRANSITION_NOT_ALLOWED",
                            expectedStatus=req.expected_status, failStatus=req.fail_status)
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    if run.status == req.fail_status:  # 멱등 재요청
        log.debug("run_fail_replayed", runId=str(run_id), status=run.status)
        return RunFailResponse(run_id=str(run_id), run_status=RunStatus(run.status), changed=False)
    if run.status != req.expected_status:
        raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status)
    await runs.fail(conn, run_id, expected=req.expected_status, to=req.fail_status,
                    stage=req.error_stage, code=req.error_code, message=req.message)
    await events.record(conn, "RUN_FAILED", run, level="ERROR", error_code=req.error_code,
                        message=req.message, details={"stage": req.error_stage,
                                                      "from": run.status, "to": req.fail_status})
    log.error("run_failed", runId=str(run_id), fromStatus=run.status, toStatus=req.fail_status,
              stage=req.error_stage, errorCode=req.error_code, errorMessage=req.message[:300])
    return RunFailResponse(run_id=str(run_id), run_status=req.fail_status, changed=True)


async def get_run_detail(conn: AsyncConnection, run_id: UUID) -> RunDetail:
    """run 상태, 파티션별 상태, dispatch 상태(운영 조회·후속 Job 선행 조건 확인용).

    잠그지 않는 읽기 전용 조회다. run, 파티션, dispatch를 따로 읽으므로 세 값이 같은 시점이라는 보장은 없다.
    partition_counts는 읽은 파티션 목록에서 상태별로 센다.

    Raises:
        NotFound: RUN_NOT_FOUND.
    """
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
        staging_count=run.staging_count, target_count=run.target_count,
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


async def list_runs(conn: AsyncConnection, *, job_key: str | None, business_key: str | None,
                    status: str | None, limit: int) -> list[RunListItem]:
    """조건에 맞는 run 목록(최근 시작 순). None인 조건은 적용하지 않는다(잠그지 않음)."""
    rows = await runs.list_runs(conn, job_key=job_key, business_key=business_key, status=status,
                                limit=limit)
    return [RunListItem(run_id=str(r.run_id), job_key=r.job_key, business_key=r.business_key,
                        status=RunStatus(r.status), source_count=r.source_count,
                        extracted_count=r.extracted_count, staging_count=r.staging_count,
                        target_count=r.target_count, expected_partition_count=r.expected_partition_count,
                        success_partition_count=r.success_partition_count,
                        failed_partition_count=r.failed_partition_count, started_at=r.started_at,
                        heartbeat_at=r.heartbeat_at, completed_at=r.completed_at,
                        error_code=r.error_code) for r in rows]
