#!/usr/bin/env bash
# Load Control API 상태: bin/status.sh [server|worker|all] [--wait 초]
# 프로세스 실행 여부와, server는 /readyz(DB 연결 포함) 응답을 본다.
# 종료 코드: 0 모두 정상, 3 하나라도 중지 또는 준비 안 됨
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
target="all"; wait_s=0
while (($#)); do
    case "$1" in
        --wait) wait_s="$2"; shift 2 ;;
        *) target="$1"; shift ;;
    esac
done
targets="$(lca_targets "$target")" || exit 2

rc=0
for svc in $targets; do
    if ! pid="$(lca_running_pid "$svc")"; then
        echo "$svc: stopped"
        rc=3
        continue
    fi
    if [[ "$svc" == server ]]; then
        host="$(lca_config_value server.host)"; port="$(lca_config_value server.port)"
        [[ "$host" == 0.0.0.0 || "$host" == "::" ]] && host=127.0.0.1
        url="http://$host:$port/readyz"
        ready=""
        for ((i = 0; i <= wait_s; i++)); do
            ready="$(curl -s -m 3 "$url" || true)"
            [[ "$ready" == *'"ok"'* ]] && break
            ((i < wait_s)) && sleep 1
        done
        if [[ "$ready" == *'"ok"'* ]]; then
            echo "server: running (pid $pid), $url ok"
        else
            echo "server: running (pid $pid), $url not ready: ${ready:-no response}"
            rc=3
        fi
    else
        echo "$svc: running (pid $pid)"
    fi
done
exit $rc
