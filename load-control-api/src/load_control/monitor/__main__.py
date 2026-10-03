"""TUI 모니터 진입점: python -m load_control.monitor [--config PATH] [--url URL] [--token TOKEN]
                                                   [--operator-token TOKEN] [--mouse]

API 주소와 토큰은 인자 > 환경변수(LCA_MONITOR__API_URL, LCA_MONITOR__TOKEN)
> config.yaml의 monitor 섹션 순서로 정한다.
운영 작업은 operator 토큰(monitor.operator_token, 없으면 token)으로 부른다.
서비스 관리는 $LCA_HOME/bin의 start.sh·stop.sh·restart.sh를 실행한다.
API 주소가 없으면 http://127.0.0.1:<server.port>를 쓴다.
마우스 입력은 기본적으로 비활성화하며, 호환성이 확인된 터미널에서만
--mouse로 명시적으로 활성화한다.
"""

import argparse
import os
from pathlib import Path

from load_control.config import CONFIG_ENV, Settings
from load_control.monitor.app import MonitorApp
from load_control.monitor.client import MonitorClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """명령행 인자를 읽는다.

    모든 인자는 선택이며, 주지 않은 값은 None으로 남겨 main에서 config.yaml(monitor 섹션) 값으로 채운다.
    argv가 None이면 sys.argv를 쓴다(테스트에서는 목록을 직접 넘긴다).
    """
    parser = argparse.ArgumentParser(description="Load Control TUI 모니터")
    parser.add_argument("--config", help=f"config.yaml 경로(기본: ${CONFIG_ENV} 또는 ./config/config.yaml)")
    parser.add_argument("--url", help="API 주소(예: http://api-host:8080)")
    parser.add_argument("--token", help="nifi 또는 operator 토큰")
    parser.add_argument("--operator-token", help="운영 작업(재전송, PUBLISH_UNKNOWN 확정)용 operator 토큰")
    parser.add_argument("--refresh", type=float, help="새로고침 주기(초)")
    parser.add_argument("--mouse", action="store_true", help="마우스 입력 활성화(기본: 비활성)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """설정을 읽어 MonitorClient와 MonitorApp을 만들고 TUI를 실행한다(종료할 때까지 막힌다).

    조회 토큰이 없으면 화면을 띄우지 않고 SystemExit로 끝낸다. 모든 API 호출이 인증을 요구하므로
    토큰 없이 띄우면 화면 전체가 인증 오류로만 채워지기 때문이다.
    operator 토큰은 없어도 되며, 그때는 조회 토큰으로 운영 작업을 부르고 API가 role로 허용 여부를 정한다.
    """
    args = parse_args(argv)
    settings = Settings.load(args.config)  # 환경변수(LCA_*) 덮어쓰기는 Settings.load가 처리한다
    mon = settings.monitor
    # api_url이 없으면 같은 호스트의 API(server.port)에 붙는다고 본다
    url = args.url or (str(mon.api_url) if mon.api_url else f"http://127.0.0.1:{settings.server.port}")
    token = args.token or mon.token
    if not token:
        raise SystemExit("토큰이 없습니다: config.yaml monitor.token, LCA_MONITOR__TOKEN 또는 --token")
    client = MonitorClient(url, token, operator_token=args.operator_token or mon.operator_token)
    # bin/monitor.sh가 LCA_HOME을 설치 디렉터리로 잡는다. 직접 실행하면 현재 디렉터리의 bin을 쓴다.
    bin_dir = Path(os.environ.get("LCA_HOME", ".")) / "bin"
    # log_dir(기본 logs)는 상대 경로면 현재 디렉터리 기준이다. bin/env.sh가 LCA_HOME으로 cd한 뒤 실행한다.
    MonitorApp(client, refresh_seconds=args.refresh or mon.refresh_seconds, log_dir=Path(mon.log_dir),
               bin_dir=bin_dir).run(mouse=args.mouse)


if __name__ == "__main__":
    main()
