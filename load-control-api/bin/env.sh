# bin 스크립트 공통 환경. 직접 실행하지 않고 다른 스크립트가 source 한다.
#
#   LCA_HOME    설치 디렉터리(이 파일의 상위). 모든 스크립트는 이 디렉터리에서 실행한다
#   LCA_CONFIG  설정 파일. 기본 $LCA_HOME/config/config.yaml
#   LCA_PYTHON  실행할 python. 기본 $LCA_HOME/.venv/bin/python(bin/install.sh가 만든다)
#
# 프로젝트를 설치하지 않고 src를 PYTHONPATH로 실행하므로 .venv에는 의존 패키지만 있으면 된다.

LCA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export LCA_HOME
export LCA_CONFIG="${LCA_CONFIG:-$LCA_HOME/config/config.yaml}"
LCA_PYTHON="${LCA_PYTHON:-$LCA_HOME/.venv/bin/python}"
LCA_LOG_DIR="$LCA_HOME/logs"
export PYTHONPATH="$LCA_HOME/src${PYTHONPATH:+:$PYTHONPATH}"

# 서비스 이름 → python 모듈
declare -A LCA_MODULES=([server]=load_control.server [worker]=load_control.worker)
LCA_SERVICES=(server worker)

cd "$LCA_HOME" || exit 1
mkdir -p "$LCA_LOG_DIR"

lca_pid_file() { echo "$LCA_LOG_DIR/$1.pid"; }

# 실행 중이면 PID를 출력하고 0을 돌려준다. PID 파일이 남아 있어도 다른 프로세스면 실행 중이 아니다.
lca_running_pid() {
    local pid_file pid
    pid_file="$(lca_pid_file "$1")"
    [[ -f "$pid_file" ]] || return 1
    pid="$(cat "$pid_file")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null \
        && tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q -- "-m ${LCA_MODULES[$1]}"; then
        echo "$pid"
        return 0
    fi
    return 1
}

# 인자 없음 또는 all이면 모든 서비스. 그 밖에는 server, worker만 받는다.
lca_targets() {
    local arg="${1:-all}"
    if [[ "$arg" == all ]]; then
        echo "${LCA_SERVICES[@]}"
    elif [[ -n "${LCA_MODULES[$arg]:-}" ]]; then
        echo "$arg"
    else
        echo "unknown service: $arg (server|worker|all)" >&2
        return 2
    fi
}

lca_check_python() {
    if [[ ! -x "$LCA_PYTHON" ]]; then
        echo "python not found: $LCA_PYTHON (run bin/install.sh first)" >&2
        return 1
    fi
}

# config.yaml의 값을 읽는다. 예: lca_config_value server.port
lca_config_value() {
    "$LCA_PYTHON" - "$1" <<'PY'
import sys
from load_control.config import Settings
value = Settings.load()
for part in sys.argv[1].split("."):
    value = getattr(value, part)
print(value if value is not None else "")
PY
}
