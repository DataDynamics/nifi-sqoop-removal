#!/usr/bin/env bash
# airgap 설치용 wheel 받기(인터넷이 되는 장비에서 실행): bin/download-packages.sh [--lock]
#   --lock  pyproject.toml의 의존성으로 packages/requirements.txt를 다시 만든다(uv 필요)
#   그다음 requirements.txt의 wheel을 대상 환경용으로 packages/에 받는다.
# 대상 환경: LCA_PKG_PYTHON(기본 3.12), LCA_PKG_PLATFORMS(기본 manylinux x86_64). 받은 packages/를 그대로 옮긴다.
set -eu
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
pyver="${LCA_PKG_PYTHON:-3.12}"
platforms="${LCA_PKG_PLATFORMS:-manylinux_2_28_x86_64 manylinux_2_17_x86_64 manylinux2014_x86_64}"
req="$LCA_HOME/packages/requirements.txt"

if [[ "${1:-}" == --lock || ! -f "$req" ]]; then
    command -v uv > /dev/null || { echo "uv not found (needed to write requirements.txt)" >&2; exit 1; }
    uv pip compile "$LCA_HOME/pyproject.toml" --python-version "$pyver" \
        --python-platform x86_64-manylinux_2_28 --no-header --annotation-style line -o "$req"
fi

py="${LCA_DOWNLOAD_PYTHON:-python3}"
args=(-m pip download --disable-pip-version-check --only-binary=:all: --implementation cp
      --python-version "$pyver" -d "$LCA_HOME/packages" -r "$req")
for p in $platforms; do args+=(--platform "$p"); done
"$py" "${args[@]}"
# 대상 환경에 pip가 오래됐을 때를 위해 pip wheel도 함께 둔다
"$py" -m pip download --disable-pip-version-check --only-binary=:all: --python-version "$pyver" \
    -d "$LCA_HOME/packages" pip
echo "wheels: $(ls "$LCA_HOME"/packages/*.whl | wc -l) in $LCA_HOME/packages"
