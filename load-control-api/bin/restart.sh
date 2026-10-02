#!/usr/bin/env bash
# Load Control API 재시작: bin/restart.sh [server|worker|all]   (기본 all)
set -u
dir="$(dirname "${BASH_SOURCE[0]}")"
"$dir/stop.sh" "${1:-all}" || exit $?
"$dir/start.sh" "${1:-all}"
