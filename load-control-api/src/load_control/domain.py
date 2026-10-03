"""상태 값과 허용 전이.

값은 DB CHECK 제약(ck_load_run_status 등)과 같아야 한다. 상태는 서비스 계층이 CAS(기대 상태 조건부
UPDATE)로만 바꾼다. 전체 흐름은 load-control-api-design.md 3장.
"""

from enum import StrEnum


class RunStatus(StrEnum):
    """load_run.status. ck_load_run_status 제약과 같은 값.

    정상 경로: CREATED → EXTRACTING → EXTRACTED_VALIDATED → STAGE_VALIDATING → STAGING_VALIDATED
    → PUBLISHING → PUBLISHED → SUCCESS. FAILED_*와 TIMED_OUT은 끝난 상태이며 재실행은 새 run으로 한다.
    PUBLISH_UNKNOWN은 운영자가 PUBLISHED 또는 FAILED_PUBLISH로 확정할 때까지 활성 run으로 남는다.
    """

    CREATED = "CREATED"  # POST /runs로 생성, manifest 대기
    EXTRACTING = "EXTRACTING"  # manifest 등록 후 파티션 추출 중
    EXTRACTED_VALIDATED = "EXTRACTED_VALIDATED"  # 모든 파티션 SUCCESS·건수 일치, 검증 dispatch 예약됨
    STAGE_VALIDATING = "STAGE_VALIDATING"  # /validation/start 성공, staging 검증 중
    STAGING_VALIDATED = "STAGING_VALIDATED"  # STAGING 지표 모두 PASS, 게시 대기
    PUBLISHING = "PUBLISHING"  # publish token 소유자가 INSERT OVERWRITE 실행 중
    PUBLISHED = "PUBLISHED"  # 게시 완료, target 검증 중
    SUCCESS = "SUCCESS"  # TARGET 지표 모두 PASS, 정상 종료
    FAILED_MANIFEST = "FAILED_MANIFEST"  # manifest 불변식 위반 또는 manifest 단계 실패
    FAILED_EXTRACT = "FAILED_EXTRACT"  # 파티션 실패·건수 불일치
    FAILED_STAGE_VALIDATION = "FAILED_STAGE_VALIDATION"  # staging 검증 단계 실패
    FAILED_PUBLISH = "FAILED_PUBLISH"  # 게시 실패(또는 PUBLISH_UNKNOWN을 실패로 확정)
    PUBLISH_UNKNOWN = "PUBLISH_UNKNOWN"  # 게시 결과 불명. 자동 전이 없음, 운영자가 확정
    FAILED_TARGET_VALIDATION = "FAILED_TARGET_VALIDATION"  # target 검증 단계 실패
    FAILED_SNAPSHOT_EXPIRED = "FAILED_SNAPSHOT_EXPIRED"  # ORA-01555 등으로 같은 SCN을 다시 읽을 수 없음
    TIMED_OUT = "TIMED_OUT"  # sweeper가 run_timeout 초과·멈춘 파티션으로 끝냄


class PartitionStatus(StrEnum):
    """load_partition.status.

    PENDING → (claim) RUNNING → SUCCESS / FAILED. REISSUE 모드에서 멈춘 파티션은 RETRY로 돌아가 다시
    claim된다. sweeper가 run을 끝내면 미완료 파티션은 TIMED_OUT.
    """

    PENDING = "PENDING"  # manifest로 등록됨, claim 대기
    RUNNING = "RUNNING"  # Worker가 claim token으로 소유, chunk 보고 중
    RETRY = "RETRY"  # sweeper가 재발행을 위해 claim을 지움, 다시 claim 대기
    SUCCESS = "SUCCESS"  # chunk가 다 모였고 건수 합계 = 예상 건수(예상 0건 파티션은 등록 즉시)
    FAILED = "FAILED"  # Worker 최종 실패 보고 또는 건수 불일치
    TIMED_OUT = "TIMED_OUT"  # sweeper가 run과 함께 끝냄


# POST /runs/{id}/fail 로 허용하는 (기대 상태, 실패 상태). 파티션 실패는 별도 엔드포인트.
# 목록에 없는 조합은 422 FAIL_TRANSITION_NOT_ALLOWED. 예: 진행 중 상태(PUBLISHING)를 이 경로로 끝낼 수 없다.
ALLOWED_RUN_FAILURES: frozenset[tuple[RunStatus, RunStatus]] = frozenset({
    (RunStatus.CREATED, RunStatus.FAILED_MANIFEST),
    (RunStatus.EXTRACTING, RunStatus.FAILED_EXTRACT),
    (RunStatus.STAGE_VALIDATING, RunStatus.FAILED_STAGE_VALIDATION),
    (RunStatus.STAGING_VALIDATED, RunStatus.FAILED_PUBLISH),
    (RunStatus.PUBLISHED, RunStatus.FAILED_TARGET_VALIDATION),
})

# 이 오류로 파티션이 실패하면 run을 FAILED_SNAPSHOT_EXPIRED로 바꾼다(같은 SCN으로 다시 읽을 수 없음).
SNAPSHOT_ERROR_CODES = frozenset({"ORA-01555", "ORA-08180"})

# 정리(PG-70) 대상이 되는 끝난 상태. PUBLISH_UNKNOWN은 운영자가 확정하기 전까지 끝난 상태가 아니다.
CLEANUP_SUCCESS_STATUSES = frozenset({RunStatus.SUCCESS})
CLEANUP_FAILED_STATUSES = frozenset({
    RunStatus.FAILED_MANIFEST, RunStatus.FAILED_EXTRACT, RunStatus.FAILED_STAGE_VALIDATION,
    RunStatus.FAILED_PUBLISH, RunStatus.FAILED_TARGET_VALIDATION, RunStatus.FAILED_SNAPSHOT_EXPIRED,
    RunStatus.TIMED_OUT})
