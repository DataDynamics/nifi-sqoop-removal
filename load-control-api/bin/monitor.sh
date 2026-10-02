#!/usr/bin/env bash
# TUI 모니터(조회 전용): bin/monitor.sh [--url URL] [--token TOKEN] [--refresh 초]
#   API 주소·토큰은 config/config.yaml의 monitor 섹션(없으면 http://127.0.0.1:<server.port>).
#   서비스 PID와 로그(logs/)는 이 디렉터리 것을 읽으므로 API 서버 호스트에서 실행하는 것이 기본이다.
# 키: Enter run 상세, l 로그, a 진행 중만, r 새로고침, Esc 뒤로, q 종료
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
lca_check_python || exit 1
exec "$LCA_PYTHON" -m load_control.monitor --config "$LCA_CONFIG" "$@"
