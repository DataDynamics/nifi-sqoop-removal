"""run 생성, 단계별 실패 기록 및 조회를 담당한다.

파티션 단위 처리(claim, chunk, 실패)는 `completion` 모듈이, manifest 등록은 `manifest` 모듈이 맡는다.
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
    """run 전용 HDFS 경로와 staging table 이름을 만든다.

    HDFS 경로는 `{hdfsRoot}/{jobKey}/run_id={runId}`, table 이름은 소문자로 변환한
    `{stageTablePrefix}{runId hex}`다. 두 값 모두 run ID를 포함하므로 Cleanup은 다른 run에 영향을
    주지 않고 해당 경로와 table만 삭제할 수 있다.
    """
    hdfs_run_path = f"{req.hdfs_root.rstrip('/')}/{req.job_key}/run_id={run_id}"
    stage_table = f"{req.stage_table_prefix}{run_id.hex}".lower()
    return hdfs_run_path, stage_table


async def create_run(conn: AsyncConnection, req: RunCreateRequest) -> RunCreateResponse:
    """`CREATED` run을 만들고 `RUN_STARTED` 이벤트를 기록한다.

    같은 업무 키의 활성 run은 사전 조회가 아니라 `uq_load_run_active` 부분 유니크 인덱스로 막는다.
    따라서 생성 요청이 동시에 들어와도 하나만 성공한다. `allowEmptySource`는 이후 manifest 검증에서
    사용하도록 `parameters`에 저장한다.

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
    """SCN, 검증, 게시 등 파티션 외 단계의 실패를 기록한다.

    허용 전이는 `domain.ALLOWED_RUN_FAILURES`에 정의되어 있다. run 행을 잠근 뒤
    `expectedStatus → failStatus`로 CAS 전이하고 `RUN_FAILED` 이벤트를 남긴다. 이미 요청한 실패 상태면
    멱등 재요청으로 보고 `changed=false`를 반환한다.

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
    """운영 조회와 후속 Job의 선행 조건 확인에 필요한 run 상세를 반환한다.

    run, 파티션 및 dispatch 상태를 잠금 없이 각각 조회하므로 모두 같은 시점의 스냅샷이라는 보장은
    없다. `partition_counts`는 조회한 파티션 목록을 상태별로 집계한다.

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
