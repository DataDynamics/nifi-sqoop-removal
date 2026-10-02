#!/usr/bin/env bash
# Load Control API 시작: bin/start.sh [server|worker|all]   (기본 all)
#   server  API(uvicorn). config의 server.host/port로 연다
#   worker  dispatcher + sweeper
# 표준출력·오류는 logs/<서비스>.out, PID는 logs/<서비스>.pid. 구조화 로그는 config의 logging.file.path.
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
targets="$(lca_targets "${1:-all}")" || exit 2
lca_check_python || exit 1
if [[ ! -f "$LCA_CONFIG" ]]; then
    echo "config not found: $LCA_CONFIG (copy config/config.example.yaml)" >&2
    exit 1
fi

rc=0
for svc in $targets; do
    if pid="$(lca_running_pid "$svc")"; then
        echo "$svc already running (pid $pid)"
        continue
    fi
    nohup setsid "$LCA_PYTHON" -m "${LCA_MODULES[$svc]}" --config "$LCA_CONFIG" \
        >> "$LCA_LOG_DIR/$svc.out" 2>&1 < /dev/null &
    echo $! > "$(lca_pid_file "$svc")"
    sleep 2
    if pid="$(lca_running_pid "$svc")"; then
        echo "$svc started (pid $pid)"
    else
        echo "$svc failed to start; see $LCA_LOG_DIR/$svc.out" >&2
        tail -n 20 "$LCA_LOG_DIR/$svc.out" >&2
        rm -f "$(lca_pid_file "$svc")"
        rc=1
    fi
done

# API는 /readyz(DB 연결 포함)가 ok가 될 때까지 기다린다.
if [[ " $targets " == *" server "* ]] && lca_running_pid server > /dev/null; then
    "$LCA_HOME/bin/status.sh" server --wait 30 > /dev/null || rc=1
fi
exit $rc
