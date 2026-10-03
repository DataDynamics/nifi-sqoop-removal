"""nifi_ops baseline: 상태 원장 테이블, 인덱스, 권한

테이블 정의의 원본은 migration이다. 바꿀 때는 새 revision을 추가한다.
asyncpg는 prepared statement 하나에 여러 문장을 허용하지 않으므로 문장을 하나씩 실행한다.

Revision ID: 0001_nifi_ops_baseline
Revises:
Create Date: 2026-10-01
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0001_nifi_ops_baseline"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# upgrade()가 순서대로 하나씩 실행할 DDL. 테이블은 FK 참조 순서(load_run → load_partition → load_file,
# load_validation, load_dispatch, load_event)로 만들고, 각 테이블의 인덱스는 테이블 바로 뒤에 둔다.
STATEMENTS: list[str] = [
    # 원장 스키마. env.py도 alembic_version을 두려고 먼저 만들 수 있으므로 IF NOT EXISTS로 만든다.
    """
CREATE SCHEMA IF NOT EXISTS nifi_ops
""",
    # load_run: 적재 1회(run)의 상태 원장. 원천 건수·SCN, 파티션 성공·실패 수,
    # 단계별 건수(추출·staging·target), HDFS run 경로, staging 테이블, publish token, 오류 정보를 담는다.
    # - 상태는 CAS(UPDATE ... WHERE status = 기대 상태)로만 바꾸며 version_no는 전이마다 1씩 오른다.
    # - heartbeat_at: 마지막 진행 시각. sweeper가 검증·게시 정체(validation_stale) 판단에 쓴다.
    # - retry_of_run_id: 실패한 run을 새 run으로 재실행할 때 원래 run을 가리킨다(자기 참조 FK).
    # - ck_load_run_status: 허용 상태 목록. 상태를 추가하려면 새 revision에서 이 제약을 바꿔야 한다.
    # - ck_load_run_counts: 건수 컬럼은 음수가 될 수 없다(NULL은 아직 모름).
    # - 시각 기본값은 clock_timestamp()다. now()와 달리 트랜잭션 시작 시각이 아니라 실제 실행 시각이다.
    """
CREATE TABLE nifi_ops.load_run (
    run_id                       uuid PRIMARY KEY,
    job_key                      varchar(200) NOT NULL,
    business_key                 varchar(200) NOT NULL,
    status                       varchar(40) NOT NULL,
    snapshot_scn                 numeric(38, 0),
    source_count                 bigint,
    source_null_split_count      bigint,
    source_min_split             numeric(38, 0),
    source_max_split             numeric(38, 0),
    expected_partition_count     integer,
    success_partition_count      integer NOT NULL DEFAULT 0,
    failed_partition_count       integer NOT NULL DEFAULT 0,
    extracted_count              bigint NOT NULL DEFAULT 0,
    staging_count                bigint,
    target_count                 bigint,
    hdfs_run_path                varchar(1000),
    stage_table_name             varchar(255),
    publish_token                uuid,
    retry_of_run_id              uuid REFERENCES nifi_ops.load_run(run_id),
    version_no                   integer NOT NULL DEFAULT 0,
    parameters                   jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at                   timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at                 timestamptz NOT NULL DEFAULT clock_timestamp(),
    extract_completed_at         timestamptz,
    publish_started_at           timestamptz,
    published_at                 timestamptz,
    completed_at                 timestamptz,
    error_stage                  varchar(80),
    error_code                   varchar(100),
    error_message                varchar(2000),
    CONSTRAINT ck_load_run_status CHECK (status IN (
        'CREATED', 'EXTRACTING',
        'EXTRACTED_VALIDATED', 'STAGE_VALIDATING', 'STAGING_VALIDATED',
        'PUBLISHING', 'PUBLISHED', 'SUCCESS',
        'FAILED_MANIFEST', 'FAILED_EXTRACT',
        'FAILED_STAGE_VALIDATION', 'FAILED_PUBLISH',
        'PUBLISH_UNKNOWN', 'FAILED_TARGET_VALIDATION',
        'FAILED_SNAPSHOT_EXPIRED', 'TIMED_OUT'
    )),
    CONSTRAINT ck_load_run_counts CHECK (
        COALESCE(source_count, 0) >= 0
        AND COALESCE(expected_partition_count, 0) >= 0
        AND success_partition_count >= 0
        AND failed_partition_count >= 0
        AND extracted_count >= 0
    )
)
""",
    # 같은 job_key + business_key에 진행 중인 run은 하나만 있을 수 있다(partial unique index).
    # 끝나지 않은 상태와 PUBLISH_UNKNOWN(게시 결과 불명, 운영자 확정 전)이 '진행 중'이다.
    # 두 번째 POST /runs는 이 인덱스 위반으로 409 DUPLICATE_ACTIVE_RUN이 된다. 끝난 run(SUCCESS, FAILED_*,
    # TIMED_OUT)은 인덱스에서 빠지므로 같은 키로 새 run을 만들 수 있다.
    """
CREATE UNIQUE INDEX uq_load_run_active
    ON nifi_ops.load_run (job_key, business_key)
    WHERE status IN (
        'CREATED', 'EXTRACTING',
        'EXTRACTED_VALIDATED', 'STAGE_VALIDATING', 'STAGING_VALIDATED',
        'PUBLISHING', 'PUBLISHED', 'PUBLISH_UNKNOWN'
    )
""",
    # 상태별 조회와 sweeper의 정체 run 탐색(status + heartbeat_at 경과)용.
    """
CREATE INDEX ix_load_run_status_heartbeat
    ON nifi_ops.load_run (status, heartbeat_at)
""",
    # job별 최근 run 목록(조회 API·모니터)용.
    """
CREATE INDEX ix_load_run_job_started
    ON nifi_ops.load_run (job_key, started_at DESC)
""",
    # load_partition: run의 추출 파티션. manifest 등록 때 만들어지고 NiFi PG-20 Worker가 claim해 추출한다.
    # - 경계: [lower_bound, upper_bound) 또는 upper_inclusive면 닫힌 구간. is_null_partition은 split 컬럼이
    #   NULL인 행 전용 파티션(경계 없음).
    # - claim_token: claim할 때마다 새로 발급된다. 재발행 뒤 이전 시도의 늦은 보고는 token이 달라
    #   409 CLAIM_MISMATCH로 거부된다. attempt_count는 claim마다 1 오르며 REISSUE 최대 시도 판정에 쓴다.
    # - heartbeat_at: claim과 chunk 보고 때만 갱신된다. sweeper의 stale 파티션 판정 기준이다.
    # - ON DELETE RESTRICT: run을 지우려면 파티션부터 지워야 한다(원장 기록이 실수로 사라지지 않게).
    # - ck_load_partition_bounds: NULL 파티션이 아니면 경계가 둘 다 있고 lower <= upper여야 한다.
    """
CREATE TABLE nifi_ops.load_partition (
    run_id                  uuid NOT NULL
                            REFERENCES nifi_ops.load_run(run_id) ON DELETE RESTRICT,
    partition_id            varchar(40) NOT NULL,
    lower_bound             numeric(38, 0),
    upper_bound             numeric(38, 0),
    upper_inclusive         boolean NOT NULL DEFAULT false,
    is_null_partition       boolean NOT NULL DEFAULT false,
    status                  varchar(20) NOT NULL DEFAULT 'PENDING',
    expected_row_count      bigint NOT NULL,
    actual_row_count        bigint,
    fragment_count          integer,
    file_count              integer,
    byte_count              bigint,
    attempt_count           integer NOT NULL DEFAULT 0,
    claim_token             uuid,
    worker_node             varchar(200),
    started_at              timestamptz,
    heartbeat_at            timestamptz,
    completed_at            timestamptz,
    error_code              varchar(100),
    error_message           varchar(2000),
    PRIMARY KEY (run_id, partition_id),
    CONSTRAINT ck_load_partition_status CHECK (status IN (
        'PENDING', 'RUNNING', 'RETRY', 'SUCCESS', 'FAILED', 'TIMED_OUT'
    )),
    CONSTRAINT ck_load_partition_counts CHECK (
        expected_row_count >= 0
        AND COALESCE(actual_row_count, 0) >= 0
        AND COALESCE(fragment_count, 0) >= 0
        AND COALESCE(file_count, 0) >= 0
        AND COALESCE(byte_count, 0) >= 0
        AND attempt_count >= 0
    ),
    CONSTRAINT ck_load_partition_bounds CHECK (
        is_null_partition
        OR (lower_bound IS NOT NULL AND upper_bound IS NOT NULL
            AND lower_bound <= upper_bound)
    )
)
""",
    # run별 상태 집계(완료 판정에서 성공·실패 파티션 수 확인)용.
    """
CREATE INDEX ix_load_partition_status
    ON nifi_ops.load_partition (run_id, status)
""",
    # sweeper의 stale 파티션 탐색용. 진행 중(RUNNING, RETRY)인 행만 담아 인덱스를 작게 유지한다.
    """
CREATE INDEX ix_load_partition_recovery
    ON nifi_ops.load_partition (status, heartbeat_at)
    WHERE status IN ('RUNNING', 'RETRY')
""",
    # load_file: Worker가 HDFS에 쓴 chunk(파일) 보고 기록. (run, partition, chunk_index) 기준 UPSERT라서
    # 같은 chunk를 다시 보고해도 행이 늘지 않는다(멱등).
    # - status: WRITTEN(집계 대상), FAILED(재발행 전에 이전 시도의 chunk를 무효화한 것; 같은 chunk를 다시
    #   보고하면 WRITTEN으로 돌아온다), VERIFIED(예약 값, 현재 코드에서는 쓰지 않는다).
    # - FK (run_id, partition_id): 등록된 파티션의 chunk만 받는다.
    # - uq_load_file_path: 한 HDFS 파일은 한 chunk 행에만 기록된다(같은 파일이 다른 chunk·파티션·run으로
    #   보고되어 건수가 이중 집계되는 것을 막는다).
    # - ck_load_file_counts: chunk_index는 0부터, fragment_count는 1 이상, 건수는 음수 불가.
    """
CREATE TABLE nifi_ops.load_file (
    run_id                  uuid NOT NULL,
    partition_id            varchar(40) NOT NULL,
    chunk_index             integer NOT NULL,
    fragment_identifier     varchar(100),
    fragment_count          integer NOT NULL,
    hdfs_path               varchar(1500) NOT NULL,
    record_count            bigint NOT NULL,
    byte_count              bigint,
    checksum                varchar(128),
    status                  varchar(20) NOT NULL DEFAULT 'WRITTEN',
    written_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, partition_id, chunk_index),
    FOREIGN KEY (run_id, partition_id)
        REFERENCES nifi_ops.load_partition(run_id, partition_id)
        ON DELETE RESTRICT,
    CONSTRAINT uq_load_file_path UNIQUE (hdfs_path),
    CONSTRAINT ck_load_file_status CHECK (status IN ('WRITTEN', 'VERIFIED', 'FAILED')),
    CONSTRAINT ck_load_file_counts CHECK (
        chunk_index >= 0 AND fragment_count > 0
        AND record_count >= 0 AND COALESCE(byte_count, 0) >= 0
    )
)
""",
    # 파티션별 상태별 chunk 집계(추출 건수 합계, 무효화 대상 조회)용.
    """
CREATE INDEX ix_load_file_partition_status
    ON nifi_ops.load_file (run_id, partition_id, status)
""",
    # load_validation: 단계(stage)별 검증 지표. SOURCE, PARTITION, HDFS, STAGING, TARGET 단계마다
    # metric_name의 기대값·실제값·허용 오차와 판정(PASS/FAIL/WARN)을 남긴다.
    # query_version은 지표를 계산한 쿼리 버전이다(쿼리를 바꾸면 이전 결과와 구분된다).
    """
CREATE TABLE nifi_ops.load_validation (
    validation_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 uuid NOT NULL
                           REFERENCES nifi_ops.load_run(run_id) ON DELETE RESTRICT,
    stage                  varchar(30) NOT NULL,
    metric_name            varchar(150) NOT NULL,
    expected_value         text,
    actual_value           text,
    tolerance              text,
    result                 varchar(10) NOT NULL,
    query_version          varchar(50) NOT NULL,
    details                jsonb NOT NULL DEFAULT '{}'::jsonb,
    measured_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_load_validation_stage CHECK (stage IN (
        'SOURCE', 'PARTITION', 'HDFS', 'STAGING', 'TARGET'
    )),
    CONSTRAINT ck_load_validation_result CHECK (result IN ('PASS', 'FAIL', 'WARN'))
)
""",
    # run·stage별 판정 집계(모두 PASS인지 확인해 STAGING_VALIDATED·SUCCESS로 넘길 때)용.
    """
CREATE INDEX ix_load_validation_run_stage
    ON nifi_ops.load_validation (run_id, stage, result)
""",
    # 같은 run·stage·지표·쿼리 버전의 결과는 한 행이다. 검증 결과를 다시 보고해도 행이 늘지 않는다
    # (UPSERT의 충돌 기준).
    """
CREATE UNIQUE INDEX uq_load_validation_metric
    ON nifi_ops.load_validation (run_id, stage, metric_name, query_version)
""",
    # load_dispatch: API → NiFi PG-05 호출 요청 outbox. 완료 판정(또는 sweeper 재발행)과 같은 트랜잭션에서
    # 행만 만들고, 커밋 뒤 worker의 dispatcher가 보낸다.
    # - 상태: PENDING(전송 대기·재시도 대기) → SENT(NiFi가 2xx로 받음) → ACKED(flow가 실제 시작),
    #   최대 시도 초과·4xx면 DEAD(운영자가 resend로 PENDING으로 되돌린다).
    # - next_attempt_at: 다음 전송 가능 시각. lease(선점 시 now+lease)와 backoff 재시도 예약에 함께 쓴다.
    # - attempt_count: lease할 때마다 1 오른다. dispatch.max_attempts에 도달하면 DEAD.
    # - ck_load_dispatch_partition: VALIDATE_RUN은 run 단위라 partition_id가 없고, REISSUE_PARTITION은
    #   반드시 대상 파티션이 있어야 한다.
    """
CREATE TABLE nifi_ops.load_dispatch (
    dispatch_id         uuid PRIMARY KEY,
    run_id              uuid NOT NULL
                        REFERENCES nifi_ops.load_run(run_id) ON DELETE RESTRICT,
    dispatch_type       varchar(30) NOT NULL,
    partition_id        varchar(40),
    status              varchar(20) NOT NULL DEFAULT 'PENDING',
    attempt_count       integer NOT NULL DEFAULT 0,
    next_attempt_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_http_status    integer,
    last_error          varchar(2000),
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at             timestamptz,
    acked_at            timestamptz,
    CONSTRAINT ck_load_dispatch_type CHECK (dispatch_type IN (
        'VALIDATE_RUN', 'REISSUE_PARTITION'
    )),
    CONSTRAINT ck_load_dispatch_status CHECK (status IN (
        'PENDING', 'SENT', 'ACKED', 'DEAD'
    )),
    CONSTRAINT ck_load_dispatch_partition CHECK (
        (dispatch_type = 'VALIDATE_RUN' AND partition_id IS NULL)
        OR (dispatch_type = 'REISSUE_PARTITION' AND partition_id IS NOT NULL)
    )
)
""",
    # run당 검증 호출(VALIDATE_RUN)은 하나만 있을 수 있다(partial unique index). 완료 판정이 동시에
    # 두 번 실행돼도 검증이 두 번 예약되지 않게 하는 마지막 방어선이며, enqueue_validation의
    # ON CONFLICT (run_id) WHERE dispatch_type = 'VALIDATE_RUN' DO NOTHING이 이 인덱스를 쓴다.
    # 재발행(REISSUE_PARTITION)은 같은 파티션에 여러 번 생길 수 있으므로 제외한다.
    """
CREATE UNIQUE INDEX uq_load_dispatch_validate
    ON nifi_ops.load_dispatch (run_id)
    WHERE dispatch_type = 'VALIDATE_RUN'
""",
    # dispatcher의 전송 대상 선점(PENDING이고 next_attempt_at이 지난 행)과 sweeper의 SENT 조회용.
    # 끝난 ACKED·DEAD 행은 빼서 인덱스를 작게 유지한다.
    """
CREATE INDEX ix_load_dispatch_due
    ON nifi_ops.load_dispatch (status, next_attempt_at)
    WHERE status IN ('PENDING', 'SENT')
""",
    # load_event: 이벤트 로그. API의 상태 전이 이벤트(같은 트랜잭션에서 기록)와 NiFi PG-90의 오류 이벤트를
    # 함께 담는다. 조회 API·모니터의 경보 화면이 읽는다.
    # - run_id에 FK를 두지 않는다. 이벤트 기록이 run 행 잠금·제약에 걸려 상태 변경을 막지 않게 하고,
    #   run이 없는 NiFi 오류도 남길 수 있게 하기 위해서다. job_key·business_key는 비정규화해 둔다.
    # - 보존 삭제(예: 90일)는 DBA 작업이다. API 계정에는 UPDATE·DELETE 권한이 없다(추가 전용).
    """
CREATE TABLE nifi_ops.load_event (
    event_id               uuid PRIMARY KEY,
    event_time             timestamptz NOT NULL DEFAULT clock_timestamp(),
    event_level            varchar(10) NOT NULL,
    event_name             varchar(80) NOT NULL,
    run_id                 uuid,
    job_key                varchar(200),
    business_key           varchar(200),
    partition_id           varchar(40),
    chunk_index            integer,
    process_group          varchar(100),
    processor_name         varchar(150),
    processor_id           varchar(100),
    node_id                varchar(200),
    attempt_no             integer,
    row_count              bigint,
    byte_count             bigint,
    duration_ms            bigint,
    error_class            varchar(40),
    error_code             varchar(100),
    message                varchar(2000),
    details                jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT ck_load_event_level CHECK (event_level IN (
        'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'
    )),
    CONSTRAINT ck_load_event_values CHECK (
        COALESCE(attempt_no, 0) >= 0
        AND COALESCE(row_count, 0) >= 0
        AND COALESCE(byte_count, 0) >= 0
        AND COALESCE(duration_ms, 0) >= 0
    )
)
""",
    # run 상세 화면의 이벤트 타임라인(최신순)용.
    """
CREATE INDEX ix_load_event_run_time
    ON nifi_ops.load_event (run_id, event_time DESC)
""",
    # job별 최근 이벤트 조회용.
    """
CREATE INDEX ix_load_event_job_time
    ON nifi_ops.load_event (job_key, event_time DESC)
""",
    # 경보 화면(최근 WARN·ERROR)용. 대부분인 INFO 이하 이벤트는 빼서 인덱스를 작게 유지한다.
    """
CREATE INDEX ix_load_event_error
    ON nifi_ops.load_event (event_time DESC, event_name)
    WHERE event_level IN ('WARN', 'ERROR')
""",
    # 시각 범위 조회·보존 삭제용 BRIN. 이벤트는 시간순으로 추가만 되므로 아주 작은 인덱스로 충분하다.
    """
CREATE INDEX ix_load_event_time_brin
    ON nifi_ops.load_event USING brin (event_time)
""",
]

# 역할은 DBA가 사전에 만든다. 역할이 없는 환경(테스트 등)에서는 건너뛴다.
# - load_control_api(server·worker 런타임 계정): 원장 테이블 SELECT·INSERT·UPDATE만. DELETE와 DDL은 없다.
#   load_event는 추가·조회만 한다. 스키마의 시퀀스(load_validation IDENTITY 등)에는 USAGE·SELECT를 준다.
# - nifi_runtime(NiFi PG-90): load_event INSERT만. 상태 원장은 API를 거쳐서만 바꾼다.
# 이 GRANT는 이 revision이 만든 테이블에만 적용된다. 나중에 테이블을 추가하면 그 revision에서 따로 준다.
GRANTS = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'load_control_api') THEN
        GRANT USAGE ON SCHEMA nifi_ops TO load_control_api;
        GRANT SELECT, INSERT, UPDATE ON
            nifi_ops.load_run, nifi_ops.load_partition, nifi_ops.load_file,
            nifi_ops.load_validation, nifi_ops.load_dispatch
            TO load_control_api;
        GRANT INSERT, SELECT ON nifi_ops.load_event TO load_control_api;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA nifi_ops TO load_control_api;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nifi_runtime') THEN
        GRANT USAGE ON SCHEMA nifi_ops TO nifi_runtime;
        GRANT INSERT ON nifi_ops.load_event TO nifi_runtime;
    END IF;
END
$$
"""

# downgrade에서 지울 순서. FK가 ON DELETE RESTRICT이므로 참조하는 쪽(자식)부터 지운다.
# 스키마(nifi_ops)는 alembic_version이 있으므로 지우지 않는다.
DROP_TABLES = ["load_event", "load_dispatch", "load_validation", "load_file", "load_partition", "load_run"]


def upgrade() -> None:
    """nifi_ops 스키마와 원장 테이블·인덱스를 만들고 런타임 역할에 권한을 준다.

    asyncpg는 한 번에 한 문장만 실행하므로 STATEMENTS를 하나씩 실행한다. env.py가
    transaction_per_migration=True이므로 중간에 실패하면 이 revision 전체가 롤백된다.
    """
    for statement in STATEMENTS:
        op.execute(statement)
    op.execute(GRANTS)


def downgrade() -> None:
    """이 revision의 테이블을 모두 지운다(데이터도 사라진다). 인덱스·제약은 테이블과 함께 지워진다."""
    for table in DROP_TABLES:
        op.execute(f"DROP TABLE IF EXISTS nifi_ops.{table}")
