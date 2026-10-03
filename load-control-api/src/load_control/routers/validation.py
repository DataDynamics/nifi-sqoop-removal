"""검증·게시 엔드포인트(NiFi PG-40~60이 호출).

PG-40 staging 검증, PG-50 게시(INSERT OVERWRITE), PG-60 target 검증. API는 NiFi가 보낸 PASS/FAIL 판정을
그대로 믿지 않고, 저장된 지표에 FAIL이 하나라도 있으면 다음 상태로 넘기지 않는다.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Request

from load_control.routers.deps import run_tx
from load_control.schemas.publish import (
    PublishClaimRequest,
    PublishClaimResponse,
    PublishResultRequest,
    PublishResultResponse,
)
from load_control.schemas.validation import (
    StageValidatedResponse,
    SuccessRequest,
    SuccessResponse,
    ValidationsRequest,
    ValidationsResponse,
    ValidationStartRequest,
    ValidationStartResponse,
)
from load_control.security import require_role
from load_control.services import publish, validation

router = APIRouter(prefix="/v1/runs/{run_id}", tags=["validation"],
                   dependencies=[Depends(require_role("nifi"))])


@router.post("/validation/start", response_model=ValidationStartResponse)
async def start(run_id: UUID, body: ValidationStartRequest, request: Request) -> ValidationStartResponse:
    """검증 flow의 첫 단계. 성공한 한 호출만 started=true를 받는다(중복 dispatch 제거).

    dispatchId가 이 run의 VALIDATE_RUN dispatch가 아니면 409 DISPATCH_MISMATCH. 맞으면 dispatch를 ACKED로
    바꾸고(sweeper 재전송 중단), EXTRACTED_VALIDATED → STAGE_VALIDATING CAS에 성공한 호출에만
    검증에 필요한 기대값(원천 건수·지표, 추출 건수, run 경로, staging table)을 돌려준다.
    outbox 재전송이나 LB 재시도로 온 두 번째 호출은 started=false(200)로 받고 NiFi는 그 FlowFile을 끝낸다.
    """
    return await run_tx(request, lambda conn: validation.start(conn, run_id, body))


@router.post("/validations", response_model=ValidationsResponse)
async def record_validations(run_id: UUID, body: ValidationsRequest,
                             request: Request) -> ValidationsResponse:
    """STAGING 또는 TARGET 지표를 저장한다.

    STAGING은 run이 STAGE_VALIDATING, TARGET은 PUBLISHED일 때만 받는다(아니면 409 RUN_STATUS_MISMATCH).
    같은 (stage, metricName, queryVersion)은 덮어쓰므로 NiFi 재시도에 멱등이다. 상태는 바꾸지 않으며
    FAIL이 있으면 `{stage}_METRIC_FAILED` 이벤트를 남긴다. 판정은 stage-validated·success가 한다.
    """
    return await run_tx(request, lambda conn: validation.record(conn, run_id, body))


@router.post("/stage-validated", response_model=StageValidatedResponse)
async def stage_validated(run_id: UUID, request: Request) -> StageValidatedResponse:
    """저장된 STAGING 지표가 모두 PASS면 STAGING_VALIDATED로 바꾼다.

    지표가 없거나 FAIL이 있으면 stageValidated=false와 reasons를 200으로 돌려준다(상태 유지). run을 실패로
    끝내려면 /runs/{id}/fail(STAGE_VALIDATING → FAILED_STAGE_VALIDATION)을 따로 호출한다.
    STAGE_COUNT 지표 값을 staging 건수로 기록한다.
    이미 STAGING_VALIDATED면 true(멱등).
    """
    return await run_tx(request, lambda conn: validation.stage_validated(conn, run_id))


@router.post("/publish/claim", response_model=PublishClaimResponse)
async def publish_claim(run_id: UUID, body: PublishClaimRequest, request: Request) -> PublishClaimResponse:
    """게시 소유권 요청. claimed=true인 FlowFile만 INSERT OVERWRITE를 실행한다.

    STAGING_VALIDATED → PUBLISHING CAS와 함께 publishToken을 run에 저장한다. 동시 요청 중 하나만 성공한다.
    이미 PUBLISHING이면 같은 token만 claimed=true(응답 유실 재시도), 다른 token이나 다른 상태는
    claimed=false(200).
    """
    return await run_tx(request, lambda conn: publish.claim(conn, run_id, body))


@router.post("/publish/result", response_model=PublishResultResponse)
async def publish_result(run_id: UUID, body: PublishResultRequest,
                         request: Request) -> PublishResultResponse:
    """게시 결과 보고: PUBLISHED, FAILED_PUBLISH, PUBLISH_UNKNOWN.

    claim 때 저장한 publishToken과 같을 때만 받는다(다르면 409 PUBLISH_TOKEN_MISMATCH). run이 이미 그
    결과 상태면 changed=false(멱등), PUBLISHING이 아니면 409 RUN_STATUS_MISMATCH. PUBLISH_UNKNOWN은
    INSERT OVERWRITE가 반영됐는지 모를 때 보고하며, 이후 운영자가 확정할 때까지 자동 전이가 없다.
    """
    return await run_tx(request, lambda conn: publish.result(conn, run_id, body))


@router.post("/success", response_model=SuccessResponse)
async def success(run_id: UUID, body: SuccessRequest, request: Request) -> SuccessResponse:
    """저장된 TARGET 지표가 모두 PASS면 SUCCESS로 바꾼다.

    run이 PUBLISHED일 때만 판정한다. 지표가 없거나 FAIL이 있으면 success=false와 reasons(200, 상태 유지).
    target 건수는 본문 targetCount, 없으면 TARGET_COUNT 지표 값으로 기록한다. 이미 SUCCESS면 true(멱등).
    """
    return await run_tx(request, lambda conn: validation.succeed(conn, run_id, body))
