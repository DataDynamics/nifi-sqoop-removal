"""worker 진입점: python -m load_control.worker

dispatcher(검증·재발행 호출 전달)와 sweeper(stale·timeout 정리)를 한 프로세스에서 실행한다.
여러 인스턴스를 띄워도 lease(SKIP LOCKED)와 advisory lock이 중복 처리를 막는다(API 설계 9.2).
"""

import asyncio
import contextlib
import signal

import structlog
from prometheus_client import start_http_server

from load_control.config import Settings, get_settings
from load_control.db import make_engine
from load_control.logging import configure_logging
from load_control.worker.dispatcher import Dispatcher, make_client
from load_control.worker.sweeper import run_sweeper

log = structlog.get_logger("load_control.worker")


async def run_worker(settings: Settings, stop: asyncio.Event) -> None:
    engine = make_engine(settings)
    client = make_client(settings)
    try:
        tasks = [
            asyncio.create_task(Dispatcher(settings, engine, client).run(stop), name="dispatcher"),
            asyncio.create_task(run_sweeper(engine, settings, stop), name="sweeper"),
        ]
        log.info("worker_started", tasks=[t.get_name() for t in tasks])
        await asyncio.gather(*tasks)
    finally:
        await client.aclose()
        await engine.dispose()
        log.info("worker_stopped")


def main() -> None:
    settings = get_settings()
    configure_logging(settings.logging.level, settings.logging.format == "json")
    if settings.nifi.receiver_url is None:
        raise SystemExit("LCA_NIFI_RECEIVER_URL을 설정하세요")
    if settings.worker.metrics_port:
        start_http_server(settings.worker.metrics_port)

    async def runner() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)
        await run_worker(settings, stop)

    asyncio.run(runner())


if __name__ == "__main__":
    main()
