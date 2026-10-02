"""TUI 모니터 진입점: python -m load_control.monitor [--config PATH] [--url URL] [--token TOKEN]
                                                   [--operator-token TOKEN]

API 주소와 토큰은 인자 > 환경변수(LCA_MONITOR__API_URL, LCA_MONITOR__TOKEN)
> config.yaml의 monitor 섹션 순서로 정한다.
운영 작업은 operator 토큰(monitor.operator_token, 없으면 token)으로 부른다.
서비스 관리는 $LCA_HOME/bin의 start.sh·stop.sh·restart.sh를 실행한다.
API 주소가 없으면 http://127.0.0.1:<server.port>를 쓴다.
"""

import argparse
import os
from pathlib import Path

from load_control.config import CONFIG_ENV, Settings
from load_control.monitor.app import MonitorApp
from load_control.monitor.client import MonitorClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Control TUI 모니터")
    parser.add_argument("--config", help=f"config.yaml 경로(기본: ${CONFIG_ENV} 또는 ./config/config.yaml)")
    parser.add_argument("--url", help="API 주소(예: http://api-host:8080)")
    parser.add_argument("--token", help="nifi 또는 operator 토큰")
    parser.add_argument("--operator-token", help="운영 작업(재전송, PUBLISH_UNKNOWN 확정)용 operator 토큰")
    parser.add_argument("--refresh", type=float, help="새로고침 주기(초)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = Settings.load(args.config)
    mon = settings.monitor
    url = args.url or (str(mon.api_url) if mon.api_url else f"http://127.0.0.1:{settings.server.port}")
    token = args.token or mon.token
    if not token:
        raise SystemExit("토큰이 없습니다: config.yaml monitor.token, LCA_MONITOR__TOKEN 또는 --token")
    client = MonitorClient(url, token, operator_token=args.operator_token or mon.operator_token)
    bin_dir = Path(os.environ.get("LCA_HOME", ".")) / "bin"
    MonitorApp(client, refresh_seconds=args.refresh or mon.refresh_seconds, log_dir=Path(mon.log_dir),
               bin_dir=bin_dir).run()


if __name__ == "__main__":
    main()
