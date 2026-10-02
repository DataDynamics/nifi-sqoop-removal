#!/usr/bin/env bash
# systemd 서비스 설치: sudo bin/systemd/install.sh [실행 사용자]   (기본: 설치 디렉터리 소유자)
#   이 디렉터리의 *.service에서 @LCA_HOME@, @LCA_USER@, @LCA_GROUP@을 채워 /etc/systemd/system에 넣고
#   daemon-reload, enable 한다. 시작은 하지 않는다: systemctl start load-control-api load-control-worker
#   bin/start.sh로 띄운 프로세스가 있으면 먼저 bin/stop.sh로 멈춘다.
# 제거: sudo bin/systemd/install.sh --uninstall
set -eu
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
home="$(cd "$here/../.." && pwd)"
unit_dir="${SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
units=(load-control-api.service load-control-worker.service)

if [[ $EUID -ne 0 ]]; then
    echo "run as root (sudo)" >&2
    exit 1
fi
if [[ "${1:-}" == --uninstall ]]; then
    systemctl disable --now "${units[@]}" 2>/dev/null || true
    for u in "${units[@]}"; do rm -f "$unit_dir/$u"; done
    systemctl daemon-reload
    echo "removed: ${units[*]}"
    exit 0
fi

user="${1:-$(stat -c %U "$home")}"
group="$(id -gn "$user")"
[[ -x "$home/.venv/bin/python" ]] || { echo "run bin/install.sh first ($home/.venv missing)" >&2; exit 1; }
[[ -f "$home/config/config.yaml" ]] || { echo "create $home/config/config.yaml first" >&2; exit 1; }
for f in "$home"/logs/*.pid; do
    [[ -e "$f" ]] && kill -0 "$(cat "$f")" 2>/dev/null \
        && { echo "processes started by bin/start.sh are running; run bin/stop.sh first" >&2; exit 1; }
done
chown -R "$user:$group" "$home/logs"
for u in "${units[@]}"; do
    sed -e "s#@LCA_HOME@#$home#g" -e "s#@LCA_USER@#$user#g" -e "s#@LCA_GROUP@#$group#g" \
        "$here/$u" > "$unit_dir/$u"
    chmod 644 "$unit_dir/$u"
done
systemctl daemon-reload
systemctl enable "${units[@]}"
echo "installed for user $user: ${units[*]}"
echo "start:  systemctl start load-control-api load-control-worker"
echo "status: systemctl status load-control-api load-control-worker; logs: $home/logs/{server,worker}.log"
