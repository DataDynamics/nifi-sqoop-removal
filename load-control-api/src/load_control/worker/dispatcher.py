"""outbox(load_dispatch)를 NiFi PG-05로 전달한다(API 설계 4장, 9.8)."""

import asyncio
import contextlib
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


def backoff(settings: Settings, attempt: int) -> timedelta:
    """attempt(1부터)에 따른 지수 backoff. min × 2^(attempt-1), 상한 max."""
    delay: timedelta = settings.dispatch.backoff_min * (1 << min(max(attempt - 1, 0), 30))
    return min(delay, settings.dispatch.backoff_max)


def make_client(settings: Settings) -> httpx.AsyncClient:
    cert = ((settings.nifi.client_cert, settings.nifi.client_key)
            if settings.nifi.client_cert and settings.nifi.client_key else None)
    verify: bool | str = settings.nifi.ca_bundle or True
    return httpx.AsyncClient(cert=cert, verify=verify, timeout=settings.nifi.timeout_seconds)


def target_url(settings: Settings, d: LeasedDispatch) -> str:
    if settings.nifi.receiver_url is None:
        raise RuntimeError("LCA_NIFI_RECEIVER_URL이 설정되지 않았습니다")
    action = "validate" if d.dispatch_type == "VALIDATE_RUN" else "reissue"
    return f"{str(settings.nifi.receiver_url).rstrip('/')}/{action}/{d.job_key}"


async def wait_first(*events: asyncio.Event) -> None:
    """events 중 하나가 set될 때까지 기다린다."""
    waiters = [asyncio.create_task(e.wait()) for e in events]
    try:
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()


class Dispatcher:
    def __init__(self, settings: Settings, engine: AsyncEngine, client: httpx.AsyncClient) -> None:
        self.settings = settings
        self.engine = engine
        self.client = client
        self.wake = asyncio.Event()

    async def dispatch_once(self) -> int:
        """기한이 된 dispatch를 모두 보낸다. 보낸 건수를 돌려준다."""
        sent = 0
        while True:
            batch = await in_tx(self.engine, lambda conn: dispatch.lease_due(
                conn, batch=self.settings.dispatch.batch, lease=self.settings.dispatch.lease))
            if not batch:
                return sent
            await asyncio.gather(*(self.send_one(d) for d in batch))
            sent += len(batch)

    async def send_one(self, d: LeasedDispatch) -> None:
        s = self.settings
        try:
            body = await in_tx(self.engine, lambda conn: dispatch.build_body(conn, d))
            url = target_url(s, d)
        except Exception as e:  # 설정·데이터 오류: 재시도해도 같으므로 DEAD
            await self._dead(d, None, f"build failed: {e!r}")
            return
        headers = {"X-Run-Id": str(d.run_id), "X-Dispatch-Id": str(d.dispatch_id)}
        try:
            r = await self.client.post(url, json=body, headers=headers)
        except httpx.HTTPError as e:
            await self._retry(d, None, repr(e))
            return
        if r.is_success:
            await in_tx(self.engine, lambda conn: dispatch.mark_sent(conn, d.dispatch_id, r.status_code))
            metrics.DISPATCH.labels(d.dispatch_type, "sent").inc()
            log.info("dispatch_sent", dispatchId=str(d.dispatch_id), runId=str(d.run_id),
                     type=d.dispatch_type, httpStatus=r.status_code, attempt=d.attempt_count)
        elif 400 <= r.status_code < 500:
            await self._dead(d, r.status_code, r.text)
        else:
            await self._retry(d, r.status_code, r.text)

    async def _retry(self, d: LeasedDispatch, status: int | None, error: str) -> None:
        if d.attempt_count >= self.settings.dispatch.max_attempts:
            await self._dead(d, status, f"max attempts reached: {error}")
            return
        delay = backoff(self.settings, d.attempt_count)
        await in_tx(self.engine, lambda conn: dispatch.schedule_retry(
            conn, d.dispatch_id, http_status=status, error=error, delay=delay))
        metrics.DISPATCH.labels(d.dispatch_type, "retry").inc()
        log.warning("dispatch_retry", dispatchId=str(d.dispatch_id), runId=str(d.run_id),
                    httpStatus=status, attempt=d.attempt_count, delaySeconds=delay.total_seconds(),
                    error=error[:300])

    async def _dead(self, d: LeasedDispatch, status: int | None, error: str) -> None:
        async def fn(conn: AsyncConnection) -> None:
            if await dispatch.mark_dead(conn, d.dispatch_id, http_status=status, error=error):
                await events.record(conn, "DISPATCH_DEAD", run_id=d.run_id, level="ERROR",
                                    partition_id=d.partition_id, message=error[:2000],
                                    details={"dispatchId": str(d.dispatch_id), "type": d.dispatch_type,
                                             "httpStatus": status, "attempt": d.attempt_count})

        await in_tx(self.engine, fn)
        metrics.DISPATCH.labels(d.dispatch_type, "dead").inc()
        log.error("dispatch_dead", dispatchId=str(d.dispatch_id), runId=str(d.run_id),
                  httpStatus=status, attempt=d.attempt_count, error=error[:300])

    async def run(self, stop: asyncio.Event) -> None:
        """LISTEN으로 즉시 깨어나고, 알림을 놓쳐도 poll 주기마다 확인한다."""
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
                self.wake.clear()
        finally:
            listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task

    async def _listen(self, stop: asyncio.Event) -> None:
        if self.settings.database.listen_dsn is None:
            log.warning("listen_disabled", reason="LCA_LISTEN_DSN not set; polling only")
            return
        dsn = self.settings.database.listen_dsn.get_secret_value()
        while not stop.is_set():
            conn: asyncpg.Connection | None = None
            try:
                closed = asyncio.Event()
                conn = await asyncpg.connect(dsn)
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
