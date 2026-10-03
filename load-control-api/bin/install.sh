#!/usr/bin/env bash
# 실행 환경(.venv) 설치: bin/install.sh [--online]
#   기본은 airgap 설치다. packages/의 wheel만 쓴다(pip --no-index).
#   --online이면 PyPI에서 받는다.
# Python 3.11 이상이 필요하다. LCA_INSTALL_PYTHON으로 지정한다(기본 python3.11). RHEL 9는 python3.11 패키지.
set -eu
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
py="${LCA_INSTALL_PYTHON:-python3.11}"
command -v "$py" > /dev/null || { echo "$py not found (set LCA_INSTALL_PYTHON)" >&2; exit 1; }
"$py" -c 'import sys; sys.exit(sys.version_info < (3, 11))' \
    || { echo "Python 3.11+ required: $("$py" --version)" >&2; exit 1; }

if [[ ! -x "$LCA_HOME/.venv/bin/python" ]]; then
    "$py" -m venv "$LCA_HOME/.venv"
fi
pip=("$LCA_HOME/.venv/bin/python" -m pip install --disable-pip-version-check)
if [[ "${1:-}" == --online ]]; then
    "${pip[@]}" -r "$LCA_HOME/packages/requirements.txt"
else
    ls "$LCA_HOME"/packages/*.whl > /dev/null 2>&1 \
        || { echo "no wheels in packages/ (run bin/download-packages.sh on a machine with internet)" >&2; exit 1; }
    "${pip[@]}" --no-index --find-links "$LCA_HOME/packages" -r "$LCA_HOME/packages/requirements.txt"
fi
"$LCA_HOME/.venv/bin/python" -c 'import fastapi, uvicorn, asyncpg, sqlalchemy, alembic, structlog; print("ok")'
echo "installed: $LCA_HOME/.venv. next: cp config/config.example.yaml config/config.yaml, bin/migrate.sh, bin/start.sh"
