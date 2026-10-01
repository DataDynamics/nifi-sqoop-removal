"""API 서버 진입점: python -m load_control.server [--config PATH]

config.yaml의 `server` 섹션(bind address, port, workers, TLS 등)과 `logging` 섹션으로 uvicorn을 실행한다.
workers가 1보다 크면 uvicorn이 자식 프로세스를 띄우고,
각 프로세스가 `create_app()`으로 같은 설정을 다시 읽는다.
"""

import argparse
import os
import ssl

import structlog
import uvicorn

from load_control import __version__
from load_control.config import CONFIG_ENV, Settings
from load_control.logging import configure_logging

log = structlog.get_logger("load_control.server")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """명령행 인자."""
    parser = argparse.ArgumentParser(description="Load Control API server")
    parser.add_argument("--config", help=f"config.yaml 경로(기본: ${CONFIG_ENV} 또는 ./config.yaml)")
    return parser.parse_args(argv)


def uvicorn_options(settings: Settings) -> dict[str, object]:
    """config.yaml의 server 섹션을 uvicorn.run 인자로 바꾼다."""
    s = settings.server
    options: dict[str, object] = {
        "host": s.host,
        "port": s.port,
        "workers": s.workers,
        "root_path": s.root_path,
        "proxy_headers": s.proxy_headers,
        "forwarded_allow_ips": s.forwarded_allow_ips,
        "timeout_keep_alive": s.timeout_keep_alive,
        "timeout_graceful_shutdown": s.timeout_graceful_shutdown,
        "limit_concurrency": s.limit_concurrency,
        # 로그는 load_control.logging이 구성한다. uvicorn 기본 설정과 access 로그는 끈다.
        "log_config": None,
        "access_log": False,
        "server_header": False,
    }
    if s.tls.enabled:
        options.update(ssl_certfile=s.tls.certfile, ssl_keyfile=s.tls.keyfile, ssl_ca_certs=s.tls.ca_certs,
                       ssl_cert_reqs=ssl.CERT_REQUIRED if s.tls.client_cert_required else ssl.CERT_NONE)
    return options


def main(argv: list[str] | None = None) -> None:
    """설정을 읽고 uvicorn을 실행한다."""
    args = parse_args(argv)
    if args.config:
        # workers > 1이면 자식 프로세스가 환경변수로 같은 파일을 찾는다.
        os.environ[CONFIG_ENV] = args.config
    settings = Settings.load()
    configure_logging(settings.logging)
    s = settings.server
    log.info("server_starting", version=__version__, host=s.host, port=s.port, workers=s.workers,
             tls=s.tls.enabled, mtls=s.tls.client_cert_required, rootPath=s.root_path or None)
    uvicorn.run("load_control.main:create_app", factory=True, **uvicorn_options(settings))  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
