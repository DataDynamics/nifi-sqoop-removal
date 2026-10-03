"""run 엔드포인트(NiFi PG-10 Coordinator, PG-90 실패 보고, 조회)."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from load_control.domain import RunStatus
from load_control.errors import Unprocessable
from load_control.routers.deps import run_tx
from load_control.schemas.runs import (
    ManifestRequest,
    ManifestResponse,
    RunCreateRequest,
    RunCreateResponse,
    RunDetail,
    RunFailRequest,
    RunFailResponse,
    RunListItem,
)
from load_control.security import require_role
from load_control.services import manifest, runs

router = APIRouter(prefix="/v1/runs", tags=["runs"])
# 상태를 바꾸는 엔드포인트는 NiFi 서비스 계정만 호출한다. 조회는 operator도 허용한다.
nifi = [Depends(require_role("nifi"))]


@router.post("", response_model=RunCreateResponse, status_code=200, dependencies=nifi)
async def create_run(body: RunCreateRequest, request: Request) -> RunCreateResponse:
    """run을 만든다. 같은 업무키의 활성 run이 있으면 409 DUPLICATE_ACTIVE_RUN.

    run 전용 HDFS 경로(`{hdfsRoot}/{jobKey}/run_id={runId}`)와 staging table 이름
    (`{stageTablePrefix}{runId hex}` 소문자)을 만들어 돌려준다. 활성 run 중복은 DB partial unique index가
    막으므로 동시 요청에도 하나만 만들어진다. 끝난 run(SUCCESS, FAILED_*, TIMED_OUT)이 있어도 새로 만들 수
    있다(재실행). hdfsRoot에 `..`가 있으면 422 INVALID_HDFS_ROOT.
    """
    return await run_tx(request, lambda conn: runs.create_run(conn, body))


@router.post("/{run_id}/manifest", response_model=ManifestResponse, dependencies=nifi)
async def register_manifest(run_id: UUID, body: ManifestRequest, request: Request) -> ManifestResponse:
    """SCN·source 지표·파티션 manifest를 등록하고 EXTRACTING으로 바꾼다.

    불변식 위반이면 FAILED_MANIFEST를 기록하고 422.

    불변식: 파티션 예상 건수 합계 = 원천 건수, 파티션 수 = 계획 수, 경계 연속(마지막만 상한 포함),
    NULL 파티션 규칙, 원천 0건은 allowEmptySource일 때만. 예상 0건 파티션은 바로 SUCCESS로 등록하고
    dispatchPartitions에서 뺀다. 모든 파티션이 0건이면 이 호출에서 EXTRACTED_VALIDATED까지 가고 검증 호출을
    예약한다. 이미 manifest가 등록된 run에 파티션 ID 목록과 원천 건수가 같은 요청이 다시 오면(응답 유실 후
    재요청) 등록된 파티션 목록을 다시 돌려주고, 다르면 409 RUN_STATUS_MISMATCH.
    """
    outcome = await run_tx(request, lambda conn: manifest.register_manifest(conn, run_id, body))
    if outcome.response is None:
        # FAILED_MANIFEST는 이미 commit됐다.
        raise Unprocessable("MANIFEST_INVALID", "; ".join(outcome.violations),
                            violations=outcome.violations)
    return outcome.response


@router.post("/{run_id}/fail", response_model=RunFailResponse, dependencies=nifi)
async def fail_run(run_id: UUID, body: RunFailRequest, request: Request) -> RunFailResponse:
    """파티션 외 단계의 실패를 기록한다(기대 상태가 맞을 때만).

    허용 조합(expectedStatus → failStatus)은 domain.ALLOWED_RUN_FAILURES이며 그 밖은 422
    FAIL_TRANSITION_NOT_ALLOWED. run이 이미 failStatus면 changed=false(멱등), 현재 상태가 expectedStatus가
    아니면 409 RUN_STATUS_MISMATCH. 성공하면 run을 끝내고 RUN_FAILED 이벤트를 남긴다.
    """
    return await run_tx(request, lambda conn: runs.fail_run(conn, run_id, body))


@router.get("", response_model=list[RunListItem],
            dependencies=[Depends(require_role("nifi", "operator"))])
async def list_runs(request: Request,
                    job_key: Annotated[str | None, Query(alias="jobKey", max_length=200)] = None,
                    business_key: Annotated[str | None, Query(alias="businessKey", max_length=200)] = None,
                    status: Annotated[RunStatus | None, Query()] = None,
                    limit: Annotated[int, Query(ge=1, le=500)] = 50) -> list[RunListItem]:
    """run 목록(최근 시작 순).

    jobKey, businessKey, status로 거를 수 있고 최대 limit개를 돌려준다. 상태를 바꾸지 않는다.
    """
    return await run_tx(request, lambda conn: runs.list_runs(
        conn, job_key=job_key, business_key=business_key, status=status, limit=limit))


@router.get("/{run_id}", response_model=RunDetail,
            dependencies=[Depends(require_role("nifi", "operator"))])
async def get_run(run_id: UUID, request: Request) -> RunDetail:
    """run 상태, 파티션별 상태, dispatch 상태.

    운영 조회와 후속 Job의 선행 조건 확인(status=SUCCESS)에 쓴다. run이 없으면 404 RUN_NOT_FOUND.
    """
    return await run_tx(request, lambda conn: runs.get_run_detail(conn, run_id))
