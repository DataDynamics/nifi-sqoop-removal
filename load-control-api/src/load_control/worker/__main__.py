"""worker 진입점: python -m load_control.worker [--config PATH]

dispatcher(검증·재발행 호출 전달)와 sweeper(stale·timeout 정리)를 한 프로세스에서 실행한다.
여러 인스턴스를 띄워도 lease(SKIP LOCKED)와 advisory lock이 중복 처리를 막는다(API 설계 9.2).
"""

import argparse
import asyncio
import contextlib
import os
import signal

import structlog
from prometheus_client import start_http_server

from load_control import __version__
from load_control.config import CONFIG_ENV, Settings
from load_control.db import make_engine
from load_control.logging import configure_logging
from load_control.worker.dispatcher import Dispatcher, make_client
from load_control.worker.sweeper import run_sweeper

log = structlog.get_logger("load_control.worker")


async def run_worker(settings: Settings, stop: asyncio.Event) -> None:
    """dispatcher와 sweeper를 stop이 set될 때까지 실행하고, 끝나면 연결을 정리한다."""
    engine = make_engine(settings)
    client = make_client(settings)
    try:
        tasks = [
            asyncio.create_task(Dispatcher(settings, engine, client).run(stop), name="dispatcher"),
            asyncio.create_task(run_sweeper(engine, settings, stop), name="sweeper"),
        ]
        log.info("worker_started", version=__version__, tasks=[t.get_name() for t in tasks],
                 receiverUrl=str(settings.nifi.receiver_url), recoveryMode=settings.recovery.mode,
                 listen=settings.database.listen_dsn is not None)
        await asyncio.gather(*tasks)
    finally:
        await client.aclose()
        await engine.dispose()
        log.info("worker_stopped")


def main(argv: list[str] | None = None) -> None:
    """설정을 읽고 worker를 실행한다. SIGTERM/SIGINT로 정상 종료한다."""
    parser = argparse.ArgumentParser(description="Load Control worker (dispatcher + sweeper)")
    parser.add_argument("--config", help=f"config.yaml 경로(기본: ${CONFIG_ENV} 또는 ./config/config.yaml)")
    args = parser.parse_args(argv)
    if args.config:
        os.environ[CONFIG_ENV] = args.config

    settings = Settings.load()
    configure_logging(settings.logging, service="worker")
    if settings.nifi.receiver_url is None:
        raise SystemExit("nifi.receiver_url must be set in config.yaml for the worker")
    if settings.worker.metrics_port:
        start_http_server(settings.worker.metrics_port, addr=settings.worker.metrics_host)
        log.info("worker_metrics_listening", host=settings.worker.metrics_host,
                 port=settings.worker.metrics_port)

    async def runner() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def request_stop(sig: signal.Signals) -> None:
            log.info("worker_stopping", signal=sig.name)
            stop.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):  # Windows에는 add_signal_handler가 없다
                loop.add_signal_handler(sig, request_stop, sig)
        await run_worker(settings, stop)

    asyncio.run(runner())


if __name__ == "__main__":
    main()
