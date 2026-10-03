"""실행과 파티션의 상태 값 및 허용 전이.

각 값은 DB의 CHECK 제약(`ck_load_run_status` 등)과 일치해야 한다. 서비스 계층은 상태를 바꿀 때
항상 CAS(현재 상태가 기대값과 같을 때만 갱신하는 조건부 UPDATE)를 사용한다. 전체 상태 흐름은
`load-control-api-design.md` 3장을 참고한다.
"""

from enum import StrEnum


class RunStatus(StrEnum):
    """`load_run.status`에 저장하는 값.

    정상 경로는 다음과 같다.

    `CREATED → EXTRACTING → EXTRACTED_VALIDATED → STAGE_VALIDATING →
    STAGING_VALIDATED → PUBLISHING → PUBLISHED → SUCCESS`

    `FAILED_*`와 `TIMED_OUT`은 종료 상태이므로 재실행할 때 새 run을 만든다. `PUBLISH_UNKNOWN`은
    운영자가 `PUBLISHED` 또는 `FAILED_PUBLISH`로 확정할 때까지 활성 상태로 남는다.
    """

    CREATED = "CREATED"  # `POST /runs`로 생성되어 manifest를 기다리는 상태
    EXTRACTING = "EXTRACTING"  # manifest 등록을 마치고 파티션을 추출하는 상태
    EXTRACTED_VALIDATED = "EXTRACTED_VALIDATED"  # 추출·건수 검증을 마치고 검증 dispatch를 예약한 상태
    STAGE_VALIDATING = "STAGE_VALIDATING"  # staging 데이터를 검증하는 상태
    STAGING_VALIDATED = "STAGING_VALIDATED"  # staging 지표가 모두 통과하여 게시를 기다리는 상태
    PUBLISHING = "PUBLISHING"  # publish token 소유자가 `INSERT OVERWRITE`를 실행하는 상태
    PUBLISHED = "PUBLISHED"  # 게시를 마치고 target 데이터를 검증하는 상태
    SUCCESS = "SUCCESS"  # target 지표가 모두 통과한 정상 종료 상태
    FAILED_MANIFEST = "FAILED_MANIFEST"  # manifest 불변식 위반 또는 등록 실패
    FAILED_EXTRACT = "FAILED_EXTRACT"  # 파티션 추출 실패 또는 건수 불일치
    FAILED_STAGE_VALIDATION = "FAILED_STAGE_VALIDATION"  # staging 검증 실패
    FAILED_PUBLISH = "FAILED_PUBLISH"  # 게시 실패 또는 `PUBLISH_UNKNOWN`을 실패로 확정
    PUBLISH_UNKNOWN = "PUBLISH_UNKNOWN"  # 게시 결과를 알 수 없어 운영자 확인이 필요한 상태
    FAILED_TARGET_VALIDATION = "FAILED_TARGET_VALIDATION"  # target 검증 실패
    FAILED_SNAPSHOT_EXPIRED = "FAILED_SNAPSHOT_EXPIRED"  # ORA-01555 등으로 같은 SCN을 읽을 수 없는 상태
    TIMED_OUT = "TIMED_OUT"  # 실행 시간 초과 또는 파티션 정체로 sweeper가 종료한 상태


class PartitionStatus(StrEnum):
    """`load_partition.status`에 저장하는 값.

    기본 흐름은 `PENDING → (claim) RUNNING → SUCCESS | FAILED`이다. `REISSUE` 모드에서는
    정체된 파티션을 `RETRY`로 되돌려 다시 claim한다. sweeper가 run을 종료하면 남은 파티션은
    `TIMED_OUT`으로 바뀐다.
    """

    PENDING = "PENDING"  # manifest에 등록되어 claim을 기다리는 상태
    RUNNING = "RUNNING"  # worker가 claim token을 소유하고 chunk를 보고하는 상태
    RETRY = "RETRY"  # 재발행을 위해 기존 claim을 지우고 새 claim을 기다리는 상태
    SUCCESS = "SUCCESS"  # chunk 합계가 예상 건수와 일치한 상태(예상 0건이면 등록 즉시 성공)
    FAILED = "FAILED"  # worker가 최종 실패를 보고했거나 건수가 일치하지 않는 상태
    TIMED_OUT = "TIMED_OUT"  # sweeper가 run과 함께 종료한 상태


# `POST /runs/{id}/fail`에서 허용하는 (현재 상태, 실패 상태) 조합이다. 파티션 실패는 별도
# 엔드포인트에서 처리한다. 목록에 없는 전이는 422 `FAIL_TRANSITION_NOT_ALLOWED`로 거부한다.
ALLOWED_RUN_FAILURES: frozenset[tuple[RunStatus, RunStatus]] = frozenset({
    (RunStatus.CREATED, RunStatus.FAILED_MANIFEST),
    (RunStatus.EXTRACTING, RunStatus.FAILED_EXTRACT),
    (RunStatus.STAGE_VALIDATING, RunStatus.FAILED_STAGE_VALIDATION),
    (RunStatus.STAGING_VALIDATED, RunStatus.FAILED_PUBLISH),
    (RunStatus.PUBLISHED, RunStatus.FAILED_TARGET_VALIDATION),
})

# 아래 Oracle 오류는 같은 SCN으로 재시도할 수 없으므로 run 전체를 `FAILED_SNAPSHOT_EXPIRED`로 끝낸다.
SNAPSHOT_ERROR_CODES = frozenset({"ORA-01555", "ORA-08180"})

# NiFi PG-70 Cleanup의 정리 대상이 되는 종료 상태다. `PUBLISH_UNKNOWN`은 운영자가 결과를 확정하기
# 전까지 활성 상태로 취급한다.
CLEANUP_SUCCESS_STATUSES = frozenset({RunStatus.SUCCESS})
CLEANUP_FAILED_STATUSES = frozenset({
    RunStatus.FAILED_MANIFEST, RunStatus.FAILED_EXTRACT, RunStatus.FAILED_STAGE_VALIDATION,
    RunStatus.FAILED_PUBLISH, RunStatus.FAILED_TARGET_VALIDATION, RunStatus.FAILED_SNAPSHOT_EXPIRED,
    RunStatus.TIMED_OUT})
