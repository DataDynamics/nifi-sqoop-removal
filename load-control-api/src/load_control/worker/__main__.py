"""worker 진입점: python -m load_control.worker [--config PATH]

dispatcher(검증·재발행 호출 전달)와 sweeper(stale·timeout 정리)를 한 프로세스에서 실행한다.
여러 인스턴스를 띄워도 lease(SKIP LOCKED)와 advisory lock이 중복 처리를 막는다.
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
    """dispatcher와 sweeper를 stop이 set될 때까지 실행하고, 끝나면 연결을 정리한다.

    DB 엔진과 NiFi 호출용 HTTP 클라이언트는 두 작업이 함께 쓴다. 두 작업 모두 내부에서 예외를
    잡아 로그만 남기고 루프를 이어가므로 정상적으로는 stop이 set될 때만 끝난다. 어느 쪽이든
    빠져나오면(정상 종료 또는 예상하지 못한 예외) finally에서 HTTP 클라이언트와 연결 풀을 닫는다.

    Args:
        settings: config.yaml에서 읽은 전체 설정.
        stop: 종료 요청 이벤트. 시그널 핸들러가 set한다.
    """
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
    """설정을 읽고 worker를 실행한다. SIGTERM/SIGINT로 정상 종료한다.

    순서: 인자 해석 → 설정 로드 → 로깅 설정 → 필수 설정 확인 → (설정 시) 메트릭 HTTP 서버 시작
    → 이벤트 루프에서 run_worker 실행. dispatcher가 호출할 곳이 없으면 의미가 없으므로
    nifi.receiver_url이 비어 있으면 SystemExit로 바로 끝낸다.

    Args:
        argv: 명령행 인자. None이면 sys.argv를 쓴다(테스트에서 직접 넘길 수 있다).
    """
    parser = argparse.ArgumentParser(description="Load Control worker (dispatcher + sweeper)")
    parser.add_argument("--config", help=f"config.yaml 경로(기본: ${CONFIG_ENV} 또는 ./config/config.yaml)")
    args = parser.parse_args(argv)
    if args.config:
        # Settings.load()는 이 환경 변수로 설정 파일 경로를 찾는다.
        os.environ[CONFIG_ENV] = args.config

    settings = Settings.load()
    configure_logging(settings.logging, service="worker")
    if settings.nifi.receiver_url is None:
        raise SystemExit("nifi.receiver_url must be set in config.yaml for the worker")
    if settings.worker.metrics_port:
        # prometheus_client가 별도 스레드로 /metrics를 연다. 포트가 비어 있거나 0이면 띄우지 않는다.
        start_http_server(settings.worker.metrics_port, addr=settings.worker.metrics_host)
        log.info("worker_metrics_listening", host=settings.worker.metrics_host,
                 port=settings.worker.metrics_port)

    async def runner() -> None:
        """이벤트 루프 안에서 stop 이벤트와 시그널 핸들러를 만들고 worker를 실행한다.

        asyncio.Event는 실행 중인 루프에서 만들어야 하므로 asyncio.run 안에서 만든다.
        """
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def request_stop(sig: signal.Signals) -> None:
            """종료 시그널을 받으면 stop을 set한다. 진행 중인 전송·정리는 끝까지 마친 뒤 루프가 멈춘다."""
            log.info("worker_stopping", signal=sig.name)
            stop.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):  # Windows에는 add_signal_handler가 없다
                loop.add_signal_handler(sig, request_stop, sig)
        await run_worker(settings, stop)

    asyncio.run(runner())


if __name__ == "__main__":
    main()
