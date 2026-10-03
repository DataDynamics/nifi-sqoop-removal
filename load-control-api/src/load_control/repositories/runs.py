"""load_run SQL. 모든 상태 전이는 WHERE status = 기대 상태 조건(CAS)으로 실행한다.

상태 전이 함수는 바꾼 행이 있으면 True를 돌려준다. 서비스 계층은 보통 lock()으로 run 행을 먼저 잠그고
현재 상태를 확인한 뒤 전이하지만, CAS 조건이 있어 잠금 없이 경합해도 한 요청만 성공한다.
상태가 바뀔 때마다 heartbeat_at을 지금으로, version_no를 1 올린다.
시각은 now()(트랜잭션 시작 시각) 대신 clock_timestamp()(실제 현재 시각)를 쓴다.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


@dataclass(frozen=True, slots=True)
class RunRow:
    """load_run 한 행(서비스 계층에 필요한 컬럼만)."""

    run_id: UUID
    job_key: str
    business_key: str
    status: str
    snapshot_scn: Decimal | None
    source_count: int | None
    expected_partition_count: int | None
    success_partition_count: int
    failed_partition_count: int
    extracted_count: int
    staging_count: int | None
    target_count: int | None
    hdfs_run_path: str | None
    stage_table_name: str | None
    parameters: dict[str, Any]
    started_at: datetime
    heartbeat_at: datetime
    extract_completed_at: datetime | None
    completed_at: datetime | None
    error_stage: str | None
    error_code: str | None
    error_message: str | None


_COLUMNS = """run_id, job_key, business_key, status, snapshot_scn, source_count,
       expected_partition_count, success_partition_count, failed_partition_count,
       extracted_count, staging_count, target_count, hdfs_run_path, stage_table_name, parameters,
       started_at, heartbeat_at, extract_completed_at, completed_at, error_stage, error_code, error_message"""


def _row(m: Any) -> RunRow:
    """SELECT 결과 한 행을 RunRow로 바꾼다. parameters(jsonb)가 문자열로 오면 dict로 파싱한다."""
    data = dict(m)
    params = data["parameters"]
    data["parameters"] = json.loads(params) if isinstance(params, str) else (params or {})
    return RunRow(**data)


async def insert(conn: AsyncConnection, *, run_id: UUID, job_key: str, business_key: str,
                 hdfs_run_path: str, stage_table_name: str, parameters: dict[str, Any]) -> None:
    """CREATED run을 만든다. 같은 업무키의 활성 run이 있으면 uq_load_run_active 위반.

    uq_load_run_active는 (job_key, business_key)에 대해 활성 상태(CREATED ~ PUBLISH_UNKNOWN)인 행이 하나만
    있도록 하는 부분 유니크 인덱스다. 위반 시 IntegrityError가 그대로 올라가고, 서비스 계층이
    409 DUPLICATE_ACTIVE_RUN으로 바꾼다. parameters는 jsonb로 저장한다.
    """
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_run (
            run_id, job_key, business_key, status, hdfs_run_path, stage_table_name, parameters)
        VALUES (:run_id, :job_key, :business_key, 'CREATED', :hdfs_run_path, :stage_table_name,
                CAST(:parameters AS jsonb))
    """), {"run_id": run_id, "job_key": job_key, "business_key": business_key,
           "hdfs_run_path": hdfs_run_path, "stage_table_name": stage_table_name,
           "parameters": json.dumps(parameters, ensure_ascii=False)})


async def get(conn: AsyncConnection, run_id: UUID) -> RunRow | None:
    """run 한 행(잠그지 않음). 없으면 None."""
    m = (await conn.execute(text(f"SELECT {_COLUMNS} FROM nifi_ops.load_run WHERE run_id = :run_id"),
                            {"run_id": run_id})).mappings().first()
    return _row(m) if m else None


async def lock(conn: AsyncConnection, run_id: UUID) -> RunRow | None:
    """run 행을 잠근다. 같은 run의 판정을 직렬화하는 핵심 잠금.

    SELECT ... FOR UPDATE로 트랜잭션이 끝날 때까지 같은 run에 대한 다른 상태 변경 요청을 기다리게 한다.
    파티션·dispatch 행을 잠가야 하면 반드시 이 잠금을 먼저 잡는다(load_run → load_partition 순서).
    없으면 None.
    """
    m = (await conn.execute(
        text(f"SELECT {_COLUMNS} FROM nifi_ops.load_run WHERE run_id = :run_id FOR UPDATE"),
        {"run_id": run_id})).mappings().first()
    return _row(m) if m else None


async def touch(conn: AsyncConnection, run_id: UUID) -> None:
    """run heartbeat 갱신. 상태와 version_no는 바꾸지 않는다(진행 중임을 보여 주는 용도)."""
    await conn.execute(text(
        "UPDATE nifi_ops.load_run SET heartbeat_at = clock_timestamp() WHERE run_id = :run_id"),
        {"run_id": run_id})


async def start_extracting(conn: AsyncConnection, run_id: UUID, *, snapshot_scn: Decimal | None,
                           source_count: int, source_null_split_count: int,
                           source_min_split: Decimal | None, source_max_split: Decimal | None,
                           expected_partition_count: int, empty_partition_count: int) -> bool:
    """manifest 등록과 함께 CREATED → EXTRACTING. SCN과 source 지표를 저장한다.

    WHERE status = 'CREATED'가 CAS 조건이다. success_partition_count는 0건 파티션 수로 시작한다(0건
    파티션은 insert_many가 이미 SUCCESS로 넣었다). 바꿨으면 True.
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET status = 'EXTRACTING',
               snapshot_scn = :snapshot_scn,
               source_count = :source_count,
               source_null_split_count = :source_null_split_count,
               source_min_split = :source_min_split,
               source_max_split = :source_max_split,
               expected_partition_count = :expected_partition_count,
               success_partition_count = :empty_partition_count,
               heartbeat_at = clock_timestamp(),
               version_no = version_no + 1
         WHERE run_id = :run_id AND status = 'CREATED'
        RETURNING run_id
    """), {"run_id": run_id, "snapshot_scn": snapshot_scn, "source_count": source_count,
           "source_null_split_count": source_null_split_count,
           "source_min_split": source_min_split, "source_max_split": source_max_split,
           "expected_partition_count": expected_partition_count,
           "empty_partition_count": empty_partition_count})
    return result.first() is not None


async def try_complete_extract(conn: AsyncConnection, run_id: UUID) -> bool:
    """모든 파티션 SUCCESS이고 합계가 source count와 같으면 EXTRACTED_VALIDATED로 CAS.

    run 완료 판정의 최종 관문이다. 증분 카운터(success_partition_count)를 믿지 않고 load_partition을
    다시 세어 판정하며, 성공 시 success_partition_count·extracted_count를 실제 집계값으로 덮어쓴다.
    호출자는 run 행을 잠근 상태이므로 마지막 파티션들이 동시에 끝나도 한 요청만 True를 받는다.
    True를 받은 호출자만 검증 dispatch를 예약한다.
    """
    # 하위 쿼리 s가 파티션 총수·SUCCESS 수·row 합계를 한 번에 센다. WHERE의 세 조건은
    # (1) manifest의 파티션이 모두 등록돼 있고 (2) 모두 SUCCESS이며 (3) 추출 건수 합이
    # source_count와 같을 때만 전이하게 한다. r.status = 'EXTRACTING'은 CAS 조건이라
    # 이미 전이·실패한 run은 바뀌지 않는다.
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run r
           SET status = 'EXTRACTED_VALIDATED',
               success_partition_count = s.success_cnt,
               extracted_count = s.row_sum,
               extract_completed_at = clock_timestamp(),
               heartbeat_at = clock_timestamp(),
               version_no = r.version_no + 1
          FROM (SELECT COUNT(*)                                   AS total_cnt,
                       COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_cnt,
                       COALESCE(SUM(actual_row_count), 0)         AS row_sum
                  FROM nifi_ops.load_partition
                 WHERE run_id = :run_id) s
         WHERE r.run_id = :run_id
           AND r.status = 'EXTRACTING'
           AND s.total_cnt = r.expected_partition_count
           AND s.success_cnt = r.expected_partition_count
           AND s.row_sum = r.source_count
        RETURNING r.run_id
    """), {"run_id": run_id})
    return result.first() is not None


async def increment_success(conn: AsyncConnection, run_id: UUID) -> None:
    """성공 파티션 수 +1(진행률 표시용, 최종 판정은 try_complete_extract가 다시 센다).

    상태 조건 없이 더한다. 호출자가 run 행을 잠근 상태에서 부른다.
    """
    await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET success_partition_count = success_partition_count + 1
         WHERE run_id = :run_id
    """), {"run_id": run_id})


async def increment_failed(conn: AsyncConnection, run_id: UUID) -> None:
    """실패 파티션 수 +1(진행률·조회용). 상태 조건 없이 더하며, 호출자가 run 행을 잠근 상태에서 부른다."""
    await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET failed_partition_count = failed_partition_count + 1
         WHERE run_id = :run_id
    """), {"run_id": run_id})


async def fail(conn: AsyncConnection, run_id: UUID, *, expected: str, to: str, stage: str,
               code: str, message: str | None) -> bool:
    """expected 상태일 때만 실패 상태로 바꾸고 오류 정보를 남긴다.

    expected → to(FAILED_* 또는 TIMED_OUT) CAS다. error_stage·error_code·error_message(2000자로 자름)와
    completed_at을 기록한다. completed_at은 정리(cleanup) 보존 기간 계산의 기준이 된다.
    이미 다른 상태로 바뀐 run이면 아무것도 바꾸지 않고 False를 돌려준다.
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_run
           SET status = :to,
               error_stage = :stage,
               error_code = :code,
               error_message = :message,
               completed_at = clock_timestamp(),
               heartbeat_at = clock_timestamp(),
               version_no = version_no + 1
         WHERE run_id = :run_id AND status = :expected
        RETURNING run_id
    """), {"run_id": run_id, "expected": expected, "to": to, "stage": stage, "code": code,
           "message": (message or "")[:2000] or None})
    return result.first() is not None


# cas_status에서 함께 갱신할 수 있는 컬럼. 값 대신 SQL 표현식(clock_timestamp())을 쓰려면 NOW를 넘긴다.
# 컬럼 이름은 SQL 문자열에 그대로 들어가므로 이 화이트리스트에 있는 이름만 허용한다.
_CAS_COLUMNS = frozenset({
    "staging_count", "target_count", "publish_token", "publish_started_at", "published_at",
    "completed_at", "error_stage", "error_code", "error_message"})
# cas_status에 값 대신 넘기면 해당 컬럼을 clock_timestamp()로 채운다(DB 시각 기준으로 기록하기 위함).
NOW = object()


async def cas_status(conn: AsyncConnection, run_id: UUID, *, expected: str, to: str,
                     **sets: Any) -> bool:
    """WHERE status = expected 조건의 상태 전이. 갱신됐으면 True.

    sets로 _CAS_COLUMNS의 컬럼을 함께 갱신할 수 있다. 값이 NOW면 clock_timestamp()로, 그 밖에는 바인드
    파라미터로 넣는다. 상태 전이와 함께 heartbeat_at·version_no도 항상 갱신한다.

    Raises:
        ValueError: _CAS_COLUMNS에 없는 컬럼을 넘긴 경우(프로그래밍 오류).
    """
    unknown = set(sets) - _CAS_COLUMNS
    if unknown:
        raise ValueError(f"cas_status: unsupported columns {unknown}")
    clauses = ["status = :to", "heartbeat_at = clock_timestamp()", "version_no = version_no + 1"]
    params: dict[str, Any] = {"run_id": run_id, "expected": expected, "to": to}
    for col, value in sets.items():
        if value is NOW:
            clauses.append(f"{col} = clock_timestamp()")
        else:
            clauses.append(f"{col} = :{col}")
            params[col] = value
    result = await conn.execute(text(f"""
        UPDATE nifi_ops.load_run SET {", ".join(clauses)}
         WHERE run_id = :run_id AND status = :expected
        RETURNING run_id
    """), params)
    return result.first() is not None


async def get_publish_token(conn: AsyncConnection, run_id: UUID) -> UUID | None:
    """현재 publish token(게시 소유자 식별값). run이 없거나 게시 claim 전이면 None.

    RunRow에 넣지 않은 컬럼이라 따로 읽는다. 호출자는 run 행을 잠근 상태에서 부른다.
    """
    row = (await conn.execute(text("SELECT publish_token FROM nifi_ops.load_run WHERE run_id = :run_id"),
                              {"run_id": run_id})).first()
    return row[0] if row else None


async def list_runs(conn: AsyncConnection, *, job_key: str | None, business_key: str | None,
                    status: str | None, limit: int) -> list[RunRow]:
    """조건에 맞는 run 목록(최근 시작 순, 잠그지 않음).

    job_key·business_key·status는 None이면 그 조건을 적용하지 않는다.
    """
    rows = (await conn.execute(text(f"""
        SELECT {_COLUMNS} FROM nifi_ops.load_run
         WHERE (CAST(:job_key AS varchar) IS NULL OR job_key = :job_key)
           AND (CAST(:business_key AS varchar) IS NULL OR business_key = :business_key)
           AND (CAST(:status AS varchar) IS NULL OR status = :status)
         ORDER BY started_at DESC
         LIMIT :limit
    """), {"job_key": job_key, "business_key": business_key, "status": status,
           "limit": limit})).mappings().all()
    return [_row(m) for m in rows]
