"""outbox(load_dispatch)를 NiFi PG-05로 전달한다.

server는 run 완료 판정(또는 sweeper는 재발행 결정)과 같은 트랜잭션에서 load_dispatch 행만 만들고,
실제 HTTP 호출은 커밋 뒤에 이 dispatcher가 한다. 그래서 "호출은 됐는데 커밋 실패"나
"커밋은 됐는데 호출 실패"가 생기지 않는다.

전달 방식:
- 깨우기: pg_notify(LISTEN)로 즉시 깨어나고, 알림을 놓쳐도 dispatch.poll_interval마다 확인한다.
- 선점(lease): 짧은 트랜잭션에서 FOR UPDATE SKIP LOCKED로 행을 골라 attempt_count를 올리고
  next_attempt_at을 lease만큼 미룬 뒤 바로 커밋한다. 상태는 PENDING 그대로라서 전송 중 worker가
  죽으면 lease가 끝난 뒤 다른 worker(또는 재시작한 자신)가 다시 가져간다. 여러 worker가 동시에
  돌아도 같은 행을 동시에 보내지 않는다.
- 결과: 2xx → SENT, 연결 실패·5xx 등 → backoff 뒤 재시도(PENDING 유지), 4xx·본문 생성 실패·
  최대 시도 초과 → DEAD(DISPATCH_DEAD 이벤트). DEAD는 운영자가 resend로 되살린다.
- 전달은 "최소 1회"다. 중복 수신은 NiFi 쪽 /validation/start의 CAS(첫 요청만 started=true)와
  재발행 claim token으로 걸러진다.
"""

import asyncio
import contextlib
import time
from datetime import timedelta

import asyncpg
import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from load_control import metrics
from load_control.config import Settings
from load_control.db import in_tx
from load_control.repositories import dispatch, events
from load_control.repositories.dispatch import LeasedDispatch

log = structlog.get_logger(__name__)

# 다시 보내면 성공할 수 있는 4xx: 408 Request Timeout, 425 Too Early, 429 Too Many Requests.
# PG-05 앞에 proxy·LB를 두면 생길 수 있다. 나머지 4xx(경로·헤더 오류 등)는 재시도해도 같으므로 바로 DEAD다.
RETRYABLE_4XX = frozenset({408, 425, 429})


def backoff(settings: Settings, attempt: int) -> timedelta:
    """attempt(1부터)에 따른 지수 backoff. min × 2^(attempt-1), 상한 max.

    attempt는 lease 때 이미 1 올린 load_dispatch.attempt_count이므로 첫 실패 뒤에는 backoff_min을
    기다린다. 기본값(5초, 상한 5분)이면 5s, 10s, 20s, ... 5분으로 늘어난다.

    Args:
        settings: dispatch.backoff_min/backoff_max를 읽을 설정.
        attempt: 방금 실패한 시도 번호. 0 이하도 1로 취급한다.

    Returns:
        다음 시도까지 기다릴 시간.
    """
    # 시프트 지수를 30으로 묶어 attempt가 아주 커도 거대한 정수·timedelta overflow가 나지 않게 한다.
    delay: timedelta = settings.dispatch.backoff_min * (1 << min(max(attempt - 1, 0), 30))
    return min(delay, settings.dispatch.backoff_max)


def make_client(settings: Settings) -> httpx.AsyncClient:
    """NiFi 호출용 HTTP 클라이언트. 설정이 있으면 mTLS를 쓴다.

    nifi.client_cert와 nifi.client_key가 둘 다 있을 때만 클라이언트 인증서를 붙인다.
    nifi.ca_bundle이 있으면 그 CA로 서버 인증서를 검증하고, 없으면 시스템 기본 CA를 쓴다.
    timeout은 연결·읽기 등 모든 단계에 nifi.timeout_seconds를 적용한다.
    worker 프로세스 전체에서 하나를 공유하며, 종료할 때 run_worker가 aclose()한다.
    """
    cert = ((settings.nifi.client_cert, settings.nifi.client_key)
            if settings.nifi.client_cert and settings.nifi.client_key else None)
    verify: bool | str = settings.nifi.ca_bundle or True
    return httpx.AsyncClient(cert=cert, verify=verify, timeout=settings.nifi.timeout_seconds)


def target_url(settings: Settings, d: LeasedDispatch) -> str:
    """dispatch 종류에 따른 PG-05 수신 URL: /validate/{jobKey} 또는 /reissue/{jobKey}.

    VALIDATE_RUN은 /validate, 그 밖(REISSUE_PARTITION)은 /reissue로 보낸다. receiver_url 끝의 '/'는
    떼고 붙인다.

    Raises:
        RuntimeError: nifi.receiver_url이 설정되지 않은 경우. send_one이 잡아 DEAD로 처리한다.
    """
    if settings.nifi.receiver_url is None:
        raise RuntimeError("nifi.receiver_url이 설정되지 않았습니다(config.yaml 또는 LCA_NIFI__RECEIVER_URL)")
    action = "validate" if d.dispatch_type == "VALIDATE_RUN" else "reissue"
    return f"{str(settings.nifi.receiver_url).rstrip('/')}/{action}/{d.job_key}"


async def wait_first(*events: asyncio.Event) -> None:
    """events 중 하나가 set될 때까지 기다린다.

    이벤트마다 wait() 태스크를 만들고, 하나가 끝나면(또는 이 코루틴이 취소·timeout되면) 남은 태스크를
    모두 취소해 태스크가 새지 않게 한다.
    """
    waiters = [asyncio.create_task(e.wait()) for e in events]
    try:
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()


class Dispatcher:
    """outbox 전달 루프. 프로세스당 하나 만든다.

    run()이 메인 루프이고, LISTEN 연결을 관리하는 _listen()을 보조 태스크로 띄운다. 알림이 오면
    wake 이벤트를 set해 메인 루프의 대기를 즉시 끝낸다. 여러 worker 프로세스가 각각 Dispatcher를
    돌려도 lease(SKIP LOCKED) 덕분에 같은 dispatch를 동시에 보내지 않는다.
    """

    def __init__(self, settings: Settings, engine: AsyncEngine, client: httpx.AsyncClient) -> None:
        """설정, DB 엔진, 공유 HTTP 클라이언트를 받는다. 연결·태스크는 run()에서 만든다."""
        self.settings = settings
        self.engine = engine
        self.client = client
        # LISTEN 알림(또는 재연결 직후)에 set되어 메인 루프의 poll 대기를 끊는다.
        self.wake = asyncio.Event()

    async def dispatch_once(self) -> int:
        """기한이 된 dispatch를 모두 보낸다. 보낸 건수를 돌려준다.

        dispatch.batch개씩 lease로 선점(별도 짧은 트랜잭션, 커밋 후 전송)하고 한 batch를 동시에
        보낸다. 선점할 행이 없을 때까지 반복한다. 반환값은 전송을 시도한 건수이며 성공 건수가 아니다.
        send_one 안에서 DB 오류 같은 예외가 나면 그대로 올라가 run()이 로그를 남긴다. 그 batch의
        나머지 행은 lease가 끝나면 다시 선점된다.
        """
        sent = 0
        while True:
            # lease_due는 attempt_count를 올리고 next_attempt_at을 now+lease로 미룬다(상태는 PENDING 유지).
            batch = await in_tx(self.engine, lambda conn: dispatch.lease_due(
                conn, batch=self.settings.dispatch.batch, lease=self.settings.dispatch.lease))
            if not batch:
                return sent
            await asyncio.gather(*(self.send_one(d) for d in batch))
            sent += len(batch)

    async def send_one(self, d: LeasedDispatch) -> None:
        """dispatch 하나를 보내고 결과(SENT, 재시도, DEAD)를 기록한다.

        처리 규칙:
        - 본문 생성·URL 계산 실패: 설정·데이터 문제라 재시도해도 같으므로 바로 DEAD.
        - httpx 오류(연결 실패, timeout 등): _retry(backoff 또는 최대 시도 초과 시 DEAD).
        - 2xx: SENT로 기록한다. 이후 검증 flow의 /validation/start(또는 재발행 claim)가 ACKED로 바꾸고,
          ack_timeout 안에 ACK가 없으면 sweeper가 PENDING으로 되돌려 다시 보낸다.
        - 4xx: NiFi 설정 오류(경로·인증 등)로 보고 바로 DEAD. 단 RETRYABLE_4XX(408, 425, 429)는
          일시적 거절(앞단 proxy·LB의 timeout, 요청 제한)이므로 _retry.
        - 그 밖(5xx, 3xx 등): _retry.

        HTTP 호출 동안에는 트랜잭션을 열지 않는다. 각 결과 기록은 별도 짧은 트랜잭션이다.
        """
        s = self.settings
        try:
            body = await in_tx(self.engine, lambda conn: dispatch.build_body(conn, d))
            url = target_url(s, d)
        except Exception as e:  # 설정·데이터 오류: 재시도해도 같으므로 DEAD
            await self._dead(d, None, f"build failed: {e!r}")
            return
        # PG-05는 이 헤더로 run·dispatch를 식별해 검증·재발행 flow에 넘긴다.
        headers = {"X-Run-Id": str(d.run_id), "X-Dispatch-Id": str(d.dispatch_id)}
        log.info("dispatch_sending", dispatchId=str(d.dispatch_id), runId=str(d.run_id), type=d.dispatch_type,
                 partitionId=d.partition_id, url=url, attempt=d.attempt_count, body=body)
        started = time.perf_counter()
        try:
            r = await self.client.post(url, json=body, headers=headers)
        except httpx.HTTPError as e:
            await self._retry(d, None, repr(e))
            return
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        if r.is_success:
            # PENDING일 때만 SENT로 바꾼다. NiFi가 응답 전에 /validation/start를 먼저 불러 이미 ACKED면
            # 아무것도 바꾸지 않는다.
            await in_tx(self.engine, lambda conn: dispatch.mark_sent(conn, d.dispatch_id, r.status_code))
            metrics.DISPATCH.labels(d.dispatch_type, "sent").inc()
            log.info("dispatch_sent", dispatchId=str(d.dispatch_id), runId=str(d.run_id),
                     type=d.dispatch_type, httpStatus=r.status_code, attempt=d.attempt_count,
                     durationMs=elapsed_ms, url=url)
        elif 400 <= r.status_code < 500 and r.status_code not in RETRYABLE_4XX:
            await self._dead(d, r.status_code, r.text)
        else:
            await self._retry(d, r.status_code, r.text)

    async def _retry(self, d: LeasedDispatch, status: int | None, error: str) -> None:
        """일시적 실패를 기록하고 backoff 뒤에 다시 보내도록 예약한다.

        attempt_count(lease 때 이미 올린 값)가 dispatch.max_attempts에 도달했으면 더 보내지 않고 DEAD로
        넘긴다. 아니면 상태는 PENDING으로 두고 next_attempt_at만 now+backoff로 미룬다(lease로 미뤘던
        시각을 덮어쓴다). schedule_retry는 PENDING일 때만 바꾸므로 그 사이 ACKED·DEAD가 됐으면 무시된다.

        Args:
            d: 실패한 dispatch.
            status: HTTP 상태 코드. 연결 실패 등 응답이 없으면 None.
            error: 오류 내용(응답 본문 또는 예외 repr). DB에는 2000자까지 저장된다.
        """
        if d.attempt_count >= self.settings.dispatch.max_attempts:
            await self._dead(d, status, f"max attempts reached: {error}")
            return
        delay = backoff(self.settings, d.attempt_count)
        await in_tx(self.engine, lambda conn: dispatch.schedule_retry(
            conn, d.dispatch_id, http_status=status, error=error, delay=delay))
        metrics.DISPATCH.labels(d.dispatch_type, "retry").inc()
        log.warning("dispatch_retry", dispatchId=str(d.dispatch_id), runId=str(d.run_id),
                    type=d.dispatch_type, httpStatus=status, attempt=d.attempt_count,
                    delaySeconds=delay.total_seconds(),
                    error=error[:300])

    async def _dead(self, d: LeasedDispatch, status: int | None, error: str) -> None:
        """dispatch를 DEAD로 바꾸고 DISPATCH_DEAD(ERROR) 이벤트를 남긴다.

        상태 변경과 이벤트 기록은 한 트랜잭션이다. mark_dead는 PENDING일 때만 바꾸므로, 그 사이
        ACKED 등으로 바뀌었으면 이벤트도 남기지 않는다(메트릭·로그는 남는다). DEAD는 자동으로
        다시 보내지 않으며, 운영자가 resend(TUI 또는 API)로 PENDING으로 되돌린다.

        Args:
            d: 포기할 dispatch.
            status: 마지막 HTTP 상태 코드. 응답이 없었으면 None.
            error: 실패 이유. 이벤트 message에는 2000자, 로그에는 300자까지 남긴다.
        """
        async def fn(conn: AsyncConnection) -> None:
            """같은 트랜잭션에서 DEAD 전이에 성공했을 때만 이벤트를 기록한다."""
            if await dispatch.mark_dead(conn, d.dispatch_id, http_status=status, error=error):
                await events.record(conn, "DISPATCH_DEAD", run_id=d.run_id, level="ERROR",
                                    partition_id=d.partition_id, message=error[:2000],
                                    details={"dispatchId": str(d.dispatch_id), "type": d.dispatch_type,
                                             "httpStatus": status, "attempt": d.attempt_count})

        await in_tx(self.engine, fn)
        metrics.DISPATCH.labels(d.dispatch_type, "dead").inc()
        log.error("dispatch_dead", dispatchId=str(d.dispatch_id), runId=str(d.run_id),
                  type=d.dispatch_type, httpStatus=status, attempt=d.attempt_count, error=error[:300])

    async def run(self, stop: asyncio.Event) -> None:
        """LISTEN으로 즉시 깨어나고, 알림을 놓쳐도 poll 주기마다 확인한다.

        한 바퀴: dispatch_once로 기한이 된 행을 모두 보낸 뒤, wake(알림) 또는 stop이 set되거나
        dispatch.poll_interval이 지날 때까지 기다린다. backoff로 미뤄진 행은 알림 없이 poll 때
        집어 간다. dispatch_once의 예외는 로그만 남기고 다음 바퀴로 넘어간다(DB 일시 장애 등).
        stop이 set되면 진행 중인 batch를 마친 뒤 루프를 빠져나오고 LISTEN 태스크를 취소한다.
        """
        listener_task = asyncio.create_task(self._listen(stop))
        poll = self.settings.dispatch.poll_interval.total_seconds()
        try:
            while not stop.is_set():
                try:
                    await self.dispatch_once()
                except Exception:
                    log.exception("dispatch_loop_error")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wait_first(self.wake, stop), poll)
                # dispatch_once 도중 온 알림도 여기서 지워지지만, 다음 바퀴의 dispatch_once가 바로
                # 기한이 된 행을 모두 다시 조회하므로 놓치지 않는다.
                self.wake.clear()
        finally:
            listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task

    async def _listen(self, stop: asyncio.Event) -> None:
        """load_dispatch 채널을 LISTEN하고 알림이 오면 wake를 set한다. 끊기면 다시 연결한다.

        SQLAlchemy 풀의 연결은 트랜잭션이 끝나면 반납되어 LISTEN을 유지할 수 없으므로
        database.listen_dsn(postgresql://... 형식)으로 asyncpg 전용 연결을 따로 열어 계속 붙잡고 있는다.
        listen_dsn이 없으면 LISTEN 없이 poll만 한다.

        연결이 끊기면(termination listener) 바로 다시 연결하고, 연결·LISTEN 자체가 실패하면 5초 쉬고
        다시 시도한다. 알림 payload는 쓰지 않는다. 깨어난 뒤 기한이 된 행을 모두 조회하기 때문이다.
        stop이 set되거나 run()이 태스크를 취소하면 연결을 닫고 끝난다.
        """
        if self.settings.database.listen_dsn is None:
            log.warning("listen_disabled",
                        reason="database.listen_dsn not set (config.yaml or LCA_DATABASE__LISTEN_DSN); "
                               "polling only")
            return
        dsn = self.settings.database.listen_dsn
        while not stop.is_set():
            conn: asyncpg.Connection | None = None
            try:
                closed = asyncio.Event()
                conn = await asyncpg.connect(dsn)
                # ev=closed 기본 인자로 이번 연결의 이벤트를 묶어 둔다(재연결 때 새 이벤트를 쓴다).
                conn.add_termination_listener(lambda _c, ev=closed: ev.set())
                await conn.add_listener(dispatch.CHANNEL, lambda *_: self.wake.set())
                self.wake.set()  # 재연결 직후 놓친 알림이 있을 수 있으므로 한 번 확인
                await wait_first(stop, closed)
                if not stop.is_set():
                    log.warning("listen_connection_lost")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("listen_error")
                await asyncio.sleep(5)
            finally:
                if conn is not None and not conn.is_closed():
                    await conn.close()
