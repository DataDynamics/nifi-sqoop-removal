#!/usr/bin/env bash
# Load Control API 중지: bin/stop.sh [server|worker|all]   (기본 all)
# SIGTERM을 보내고 graceful shutdown을 기다린다. LCA_STOP_TIMEOUT초(기본 45) 안에 끝나지 않으면 SIGKILL.
# server의 기본 대기(server.timeout_graceful_shutdown)는 30초다.
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
targets="$(lca_targets "${1:-all}")" || exit 2
timeout="${LCA_STOP_TIMEOUT:-45}"

for svc in $targets; do
    if ! pid="$(lca_running_pid "$svc")"; then
        echo "$svc not running"
        rm -f "$(lca_pid_file "$svc")"
        continue
    fi
    kill -TERM "$pid"
    for ((i = 0; i < timeout; i++)); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "$svc did not stop in ${timeout}s; killing pid $pid" >&2
        # uvicorn workers>1이면 자식 프로세스도 같은 세션(setsid)이므로 세션 전체를 끝낸다.
        pkill -KILL -s "$pid" 2>/dev/null || kill -KILL "$pid"
    fi
    rm -f "$(lca_pid_file "$svc")"
    echo "$svc stopped"
done
