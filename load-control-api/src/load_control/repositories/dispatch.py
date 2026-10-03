"""outbox(`load_dispatch`) SQL. 상태: `PENDING → SENT → ACKED`, 실패 누적 시 `DEAD`.

API→NiFi 호출은 상태 전이와 같은 트랜잭션에서 이 테이블에 예약한다. 실제 HTTP 전송은 commit 후
worker의 dispatcher가 맡는다. 따라서 rollback된 상태 변경의 외부 호출이 실행되지 않으며, commit 후
전송에 실패해도 예약이 남아 재시도하거나 `DEAD` 상태로 운영자에게 드러난다.

- PENDING: 보낼 차례를 기다린다. dispatcher는 next_attempt_at을 lease로 써서 행을 선점한다.
- SENT: NiFi가 2xx로 받았다. NiFi flow가 실제로 시작했다는 확인(ACK)을 기다린다.
- ACKED: VALIDATE_RUN은 /validation/start, REISSUE_PARTITION은 재발행 파티션의 claim이 ACK다.
- DEAD: 재시도 한도 초과 또는 4xx. 운영자가 resend로 PENDING에 되돌릴 수 있다.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

# dispatcher가 구독하는 `LISTEN/NOTIFY` 채널. 새 예약이 commit되면 즉시 dispatcher를 깨운다.
CHANNEL = "load_dispatch"


@dataclass(frozen=True, slots=True)
class DispatchRow:
    """조회 API에 보여 줄 dispatch 요약."""

    dispatch_id: UUID
    dispatch_type: str
    partition_id: str | None
    status: str
    attempt_count: int
    sent_at: datetime | None
    acked_at: datetime | None


@dataclass(frozen=True, slots=True)
class LeasedDispatch:
    """lease로 선점한, 지금 보낼 dispatch."""

    dispatch_id: UUID
    run_id: UUID
    dispatch_type: str
    partition_id: str | None
    attempt_count: int
    job_key: str


async def _notify(conn: AsyncConnection, run_id: UUID) -> None:
    """dispatcher를 깨우는 pg_notify를 같은 트랜잭션에서 보낸다(payload는 run_id).

    NOTIFY는 폴링 지연을 줄이는 힌트일 뿐이다. 알림을 놓쳐도 dispatcher의 주기적 폴링이 행을 찾는다.
    """
    # NOTIFY는 commit될 때만 전달된다. rollback된 예약은 dispatcher를 깨우지 않는다.
    await conn.execute(text("SELECT pg_notify(:channel, :payload)"),
                       {"channel": CHANNEL, "payload": str(run_id)})


async def enqueue_validation(conn: AsyncConnection, run_id: UUID) -> bool:
    """검증 호출을 outbox에 예약하고 dispatcher를 깨운다.

    uq_load_dispatch_validate가 run당 1행을 보장하는 마지막 방어선이다.

    호출자는 run 행을 잠근 채 EXTRACTING → EXTRACTED_VALIDATED CAS에 성공한 직후에만 부른다. 그래도
    중복 INSERT가 오면 ON CONFLICT DO NOTHING으로 조용히 무시한다. 새로 넣었으면 True, 이미 있었으면 False.
    NOTIFY는 삽입 여부와 관계없이 보낸다(중복 알림은 무해하다).
    """
    # ON CONFLICT 대상은 부분 유니크 인덱스 uq_load_dispatch_validate
    # (run_id) WHERE dispatch_type = 'VALIDATE_RUN'이다. 인덱스 조건과 같은 WHERE를 써야
    # PostgreSQL이 그 인덱스를 conflict 판정에 쓴다.
    inserted = (await conn.execute(text("""
        INSERT INTO nifi_ops.load_dispatch (dispatch_id, run_id, dispatch_type)
        VALUES (:dispatch_id, :run_id, 'VALIDATE_RUN')
        ON CONFLICT (run_id) WHERE dispatch_type = 'VALIDATE_RUN' DO NOTHING
        RETURNING dispatch_id
    """), {"dispatch_id": uuid.uuid4(), "run_id": run_id})).first()
    await _notify(conn, run_id)
    return inserted is not None


async def enqueue_reissue(conn: AsyncConnection, run_id: UUID, partition_id: str) -> UUID:
    """stale 파티션 재발행 요청을 outbox에 넣는다(sweeper REISSUE 모드).

    VALIDATE_RUN과 달리 유니크 제약이 없다. 같은 파티션이 여러 번 stale이 되면 시도마다 새 행이 생긴다.
    호출 전 sweeper가 파티션을 RUNNING → RETRY로 되돌려 둔다. 새 dispatch_id를 돌려준다.
    """
    dispatch_id = uuid.uuid4()
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_dispatch (dispatch_id, run_id, dispatch_type, partition_id)
        VALUES (:dispatch_id, :run_id, 'REISSUE_PARTITION', :partition_id)
    """), {"dispatch_id": dispatch_id, "run_id": run_id, "partition_id": partition_id})
    await _notify(conn, run_id)
    return dispatch_id


async def list_for_run(conn: AsyncConnection, run_id: UUID) -> list[DispatchRow]:
    """run의 dispatch 목록을 생성 순으로 돌려준다(조회 API용, 잠그지 않음)."""
    rows = (await conn.execute(text("""
        SELECT dispatch_id, dispatch_type, partition_id, status, attempt_count, sent_at, acked_at
          FROM nifi_ops.load_dispatch WHERE run_id = :run_id ORDER BY created_at
    """), {"run_id": run_id})).mappings().all()
    return [DispatchRow(**dict(m)) for m in rows]


async def get_for_update(conn: AsyncConnection, dispatch_id: UUID) -> tuple[UUID, str, str] | None:
    """dispatch 행을 잠그고 (run_id, type, status)를 돌려준다.

    호출자는 run 행을 먼저 잠근 뒤 부른다(잠금 순서 load_run → load_dispatch). 행이 없으면 None.
    호출자는 run_id가 경로의 run과 같은지 확인해 다른 run의 dispatch를 건드리지 못하게 한다.
    """
    row = (await conn.execute(text("""
        SELECT run_id, dispatch_type, status FROM nifi_ops.load_dispatch
         WHERE dispatch_id = :dispatch_id FOR UPDATE
    """), {"dispatch_id": dispatch_id})).first()
    return (row[0], str(row[1]), str(row[2])) if row else None


async def lease_due(conn: AsyncConnection, *, batch: int, lease: timedelta) -> list[LeasedDispatch]:
    """전송할 행을 lease로 선점한다. 전송 동안 트랜잭션을 열어 두지 않기 위해서다.

    PENDING이고 next_attempt_at이 지난 행을 최대 batch개 골라 attempt_count를 1 올리고
    next_attempt_at을 "지금 + lease"로 미룬다. 상태는 PENDING 그대로이므로, 이 트랜잭션이 commit된 뒤
    전송 중에 worker가 죽어도 lease가 끝나면 다른 dispatcher가 다시 가져간다(최소 한 번 전송).
    전송 결과는 mark_sent / schedule_retry / mark_dead가 별도 트랜잭션에서 기록한다.
    본문의 job_key는 전송 URL을 정하기 위해 load_run에서 함께 읽는다.
    """
    # 하위 SELECT의 FOR UPDATE SKIP LOCKED: 다른 dispatcher가 지금 선점 중인 행은 기다리지 않고 건너뛴다.
    # 그래서 worker를 여러 개 띄워도 같은 행을 동시에 선점하지 않는다. 선점이 commit된 뒤에는
    # next_attempt_at이 미래가 되어 다른 dispatcher의 "next_attempt_at <= clock_timestamp()" 조건에서 빠진다.
    # clock_timestamp()는 트랜잭션 시작 시각(now())이 아니라 실제 현재 시각이라 lease 계산이 정확하다.
    rows = (await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch d
           SET attempt_count   = d.attempt_count + 1,
               next_attempt_at = clock_timestamp() + CAST(:lease AS interval)
          FROM nifi_ops.load_run r
         WHERE r.run_id = d.run_id
           AND d.dispatch_id IN (
                SELECT dispatch_id
                  FROM nifi_ops.load_dispatch
                 WHERE status = 'PENDING' AND next_attempt_at <= clock_timestamp()
                 ORDER BY next_attempt_at
                 LIMIT :batch
                 FOR UPDATE SKIP LOCKED)
        RETURNING d.dispatch_id, d.run_id, d.dispatch_type, d.partition_id, d.attempt_count, r.job_key
    """), {"batch": batch, "lease": lease})).mappings().all()
    return [LeasedDispatch(**dict(m)) for m in rows]


async def mark_sent(conn: AsyncConnection, dispatch_id: UUID, http_status: int) -> bool:
    """NiFi가 2xx로 받았음을 기록한다(PENDING → SENT). PENDING일 때만 바꾼다.

    바꿨으면 True. 이미 ACKED(또는 운영자 조치로 다른 상태)면 아무것도 바꾸지 않고 False를 돌려준다.
    """
    # PENDING일 때만 바꾼다. NiFi가 202 직후 /validation/start를 먼저 호출해 이미 ACKED일 수 있다.
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'SENT', sent_at = clock_timestamp(), last_http_status = :http_status,
               last_error = NULL
         WHERE dispatch_id = :dispatch_id AND status = 'PENDING'
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id, "http_status": http_status})
    return result.first() is not None


async def schedule_retry(conn: AsyncConnection, dispatch_id: UUID, *, http_status: int | None,
                         error: str, delay: timedelta) -> None:
    """전송 실패: delay 뒤에 다시 보내도록 next_attempt_at을 미룬다.

    상태는 PENDING으로 두고 마지막 HTTP 상태와 오류(최대 2000자)만 남긴다. 그사이 ACKED 등으로 바뀐
    행은 WHERE status = 'PENDING' 조건 때문에 바뀌지 않는다. 재시도 한도 판정은 dispatcher가 한다.
    """
    await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET next_attempt_at = clock_timestamp() + CAST(:delay AS interval),
               last_http_status = :http_status, last_error = :error
         WHERE dispatch_id = :dispatch_id AND status = 'PENDING'
    """), {"dispatch_id": dispatch_id, "delay": delay, "http_status": http_status,
           "error": error[:2000]})


async def mark_dead(conn: AsyncConnection, dispatch_id: UUID, *, http_status: int | None,
                    error: str) -> bool:
    """PENDING → DEAD. 더 보내지 않는다. 운영자가 resend로 되살릴 수 있다.

    재시도 한도 초과, 4xx 응답, 본문·URL 구성 실패 때 dispatcher가 부른다. 바꿨으면 True이며,
    호출자는 True일 때만 DISPATCH_DEAD 이벤트를 남긴다(중복 이벤트 방지).
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'DEAD', last_http_status = :http_status, last_error = :error
         WHERE dispatch_id = :dispatch_id AND status = 'PENDING'
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id, "http_status": http_status, "error": error[:2000]})
    return result.first() is not None


async def ack(conn: AsyncConnection, dispatch_id: UUID) -> bool:
    """검증 flow가 /validation/start로 실제 시작을 알렸다(→ ACKED).

    SENT뿐 아니라 PENDING·DEAD에서도 ACKED로 바꾼다. NiFi가 2xx 응답 기록(mark_sent)보다 먼저
    /validation/start를 부르거나, 응답이 실패로 보여 DEAD가 된 뒤에도 실제로는 시작될 수 있기 때문이다.
    이미 ACKED면 False를 돌려준다.
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'ACKED', acked_at = clock_timestamp()
         WHERE dispatch_id = :dispatch_id AND status IN ('PENDING', 'SENT', 'DEAD')
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id})
    return result.first() is not None


async def ack_reissue(conn: AsyncConnection, run_id: UUID, partition_id: str) -> None:
    """재발행된 파티션이 claim되면 해당 REISSUE dispatch를 ACK로 본다.

    파티션의 REISSUE_PARTITION 행 중 아직 ACKED가 아닌 것을 모두 ACKED로 바꾼다(여러 번 재발행된
    경우 이전 행도 함께 닫힌다). 호출자는 run·파티션 행을 이미 잠근 상태다.
    """
    await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'ACKED', acked_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id
           AND dispatch_type = 'REISSUE_PARTITION' AND status IN ('PENDING', 'SENT', 'DEAD')
    """), {"run_id": run_id, "partition_id": partition_id})


async def resend(conn: AsyncConnection, run_id: UUID, dispatch_id: UUID) -> bool:
    """운영자 재전송: DEAD 또는 SENT를 PENDING으로 되돌리고 시도 횟수를 초기화한다.

    next_attempt_at을 지금으로 두어 바로 전송 대상이 되게 하고, 바꿨을 때만 dispatcher를 깨운다.
    ACKED·PENDING이거나 run_id가 다르면 False(호출자가 409로 바꾼다).
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_dispatch
           SET status = 'PENDING', attempt_count = 0, next_attempt_at = clock_timestamp(),
               last_error = NULL
         WHERE dispatch_id = :dispatch_id AND run_id = :run_id AND status IN ('DEAD', 'SENT')
        RETURNING dispatch_id
    """), {"dispatch_id": dispatch_id, "run_id": run_id})
    if result.first() is None:
        return False
    await _notify(conn, run_id)
    return True


async def build_body(conn: AsyncConnection, d: LeasedDispatch) -> dict[str, Any]:
    """API→NiFi 호출 본문. 재발행은 Worker 실행에 필요한 값을 모두 담는다.

    VALIDATE_RUN은 runId·dispatchId만 보낸다(나머지는 검증 flow가 /validation/start 응답으로 받는다).
    REISSUE_PARTITION은 Worker가 manifest 없이 Oracle 범위 조회를 다시 할 수 있도록 SCN, HDFS 경로,
    파티션 경계, 예상 건수를 load_run·load_partition에서 읽어 함께 담는다. 파티션 행이 없으면
    one()이 예외를 내고, dispatcher는 이를 재시도해도 같은 오류로 보고 DEAD로 처리한다.
    """
    body: dict[str, Any] = {"runId": str(d.run_id), "dispatchId": str(d.dispatch_id)}
    if d.dispatch_type != "REISSUE_PARTITION":
        return body
    m = (await conn.execute(text("""
        SELECT r.business_key, r.snapshot_scn, r.hdfs_run_path, p.lower_bound, p.upper_bound,
               p.upper_inclusive, p.is_null_partition, p.expected_row_count
          FROM nifi_ops.load_partition p JOIN nifi_ops.load_run r ON r.run_id = p.run_id
         WHERE p.run_id = :run_id AND p.partition_id = :partition_id
    """), {"run_id": d.run_id, "partition_id": d.partition_id})).mappings().one()

    def num(v: object) -> str | None:
        """NUMBER(Decimal) 값을 정밀도 손실 없이 문자열로 보낸다(JSON float 변환 방지)."""
        return None if v is None else str(v)

    body.update({
        "partitionId": d.partition_id, "businessKey": m["business_key"],
        "snapshotScn": num(m["snapshot_scn"]), "hdfsRunPath": m["hdfs_run_path"],
        "lowerBound": num(m["lower_bound"]), "upperBound": num(m["upper_bound"]),
        "upperInclusive": m["upper_inclusive"], "isNullPartition": m["is_null_partition"],
        "expectedRowCount": m["expected_row_count"]})
    return body


async def backlog(conn: AsyncConnection) -> dict[str, int]:
    """상태별 미완료 dispatch 수(메트릭·모니터용). 건수가 0인 상태도 0으로 채워 돌려준다."""
    rows = (await conn.execute(text("""
        SELECT status, COUNT(*) FROM nifi_ops.load_dispatch
         WHERE status IN ('PENDING', 'SENT', 'DEAD') GROUP BY status
    """))).all()
    counts = {"PENDING": 0, "SENT": 0, "DEAD": 0}
    counts.update({str(r[0]): int(r[1]) for r in rows})
    return counts
