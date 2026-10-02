"""nifi_ops baseline: 가이드 4.1 DDL

가이드(nifi-sqoop-removal-guide.md) 4.1의 DDL이 원본이다. DDL을 바꿀 때는 가이드와 새 revision을 함께 고친다.
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

STATEMENTS: list[str] = [
    """
CREATE SCHEMA IF NOT EXISTS nifi_ops
""",
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
    """
CREATE UNIQUE INDEX uq_load_run_active
    ON nifi_ops.load_run (job_key, business_key)
    WHERE status IN (
        'CREATED', 'EXTRACTING',
        'EXTRACTED_VALIDATED', 'STAGE_VALIDATING', 'STAGING_VALIDATED',
        'PUBLISHING', 'PUBLISHED', 'PUBLISH_UNKNOWN'
    )
""",
    """
CREATE INDEX ix_load_run_status_heartbeat
    ON nifi_ops.load_run (status, heartbeat_at)
""",
    """
CREATE INDEX ix_load_run_job_started
    ON nifi_ops.load_run (job_key, started_at DESC)
""",
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
    """
CREATE INDEX ix_load_partition_status
    ON nifi_ops.load_partition (run_id, status)
""",
    """
CREATE INDEX ix_load_partition_recovery
    ON nifi_ops.load_partition (status, heartbeat_at)
    WHERE status IN ('RUNNING', 'RETRY')
""",
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
    """
CREATE INDEX ix_load_file_partition_status
    ON nifi_ops.load_file (run_id, partition_id, status)
""",
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
    """
CREATE INDEX ix_load_validation_run_stage
    ON nifi_ops.load_validation (run_id, stage, result)
""",
    """
CREATE UNIQUE INDEX uq_load_validation_metric
    ON nifi_ops.load_validation (run_id, stage, metric_name, query_version)
""",
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
    """
CREATE UNIQUE INDEX uq_load_dispatch_validate
    ON nifi_ops.load_dispatch (run_id)
    WHERE dispatch_type = 'VALIDATE_RUN'
""",
    """
CREATE INDEX ix_load_dispatch_due
    ON nifi_ops.load_dispatch (status, next_attempt_at)
    WHERE status IN ('PENDING', 'SENT')
""",
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
    """
CREATE INDEX ix_load_event_run_time
    ON nifi_ops.load_event (run_id, event_time DESC)
""",
    """
CREATE INDEX ix_load_event_job_time
    ON nifi_ops.load_event (job_key, event_time DESC)
""",
    """
CREATE INDEX ix_load_event_error
    ON nifi_ops.load_event (event_time DESC, event_name)
    WHERE event_level IN ('WARN', 'ERROR')
""",
    """
CREATE INDEX ix_load_event_time_brin
    ON nifi_ops.load_event USING brin (event_time)
""",
]

# 역할은 DBA가 사전에 만든다(가이드 4.1 "권한 예시"). 역할이 없는 환경(테스트 등)에서는 건너뛴다.
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

DROP_TABLES = ["load_event", "load_dispatch", "load_validation", "load_file", "load_partition", "load_run"]


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)
    op.execute(GRANTS)


def downgrade() -> None:
    for table in DROP_TABLES:
        op.execute(f"DROP TABLE IF EXISTS nifi_ops.{table}")
