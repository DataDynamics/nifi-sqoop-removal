#!/usr/bin/env bash
# airgap 설치용 wheel 받기(인터넷이 되는 장비에서 실행): bin/download-packages.sh [--lock]
#   --lock  pyproject.toml의 의존성으로 packages/requirements.txt를 다시 만든다(uv 필요)
#   그다음 requirements.txt의 wheel을 대상 환경용으로 packages/에 받는다.
# 대상 환경: LCA_PKG_PYTHON(기본 3.11), LCA_PKG_PLATFORMS(기본 manylinux x86_64). 받은 packages/를 그대로 옮긴다.
# wheel이 없고 소스(sdist)만 있는 순수 python 패키지(LCA_PKG_BUILD, 기본 pure-sasl)는 이 장비에서 wheel로 만든다.
set -eu
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
pyver="${LCA_PKG_PYTHON:-3.11}"
platforms="${LCA_PKG_PLATFORMS:-manylinux_2_28_x86_64 manylinux_2_17_x86_64 manylinux2014_x86_64}"
req="$LCA_HOME/packages/requirements.txt"

if [[ "${1:-}" == --lock || ! -f "$req" ]]; then
    command -v uv > /dev/null || { echo "uv not found (needed to write requirements.txt)" >&2; exit 1; }
    uv pip compile "$LCA_HOME/pyproject.toml" --python-version "$pyver" \
        --python-platform x86_64-manylinux_2_28 --no-header --annotation-style line -o "$req"
fi

py="${LCA_DOWNLOAD_PYTHON:-python3}"
build="${LCA_PKG_BUILD:-pure-sasl}"
# 직접 빌드할 패키지는 binary 다운로드 목록에서 빼고(--no-deps라 의존성은 requirements.txt가 모두 가진다)
binreq="$(mktemp)"
trap 'rm -f "$binreq"' EXIT
build_re="^($(echo "$build" | tr ' ' '|'))=="
grep -Ev "$build_re" "$req" > "$binreq"
args=(-m pip download --disable-pip-version-check --only-binary=:all: --implementation cp --no-deps
      --python-version "$pyver" -d "$LCA_HOME/packages" -r "$binreq")
for p in $platforms; do args+=(--platform "$p"); done
"$py" "${args[@]}"
# 순수 python sdist → py3-none-any wheel(대상 플랫폼과 관계없다)
# --use-pep517: setup.py만 있는 패키지도 격리 환경(setuptools·wheel을 index에서 받음)에서 빌드한다.
# 오래된 pip(RHEL 9 python3.11-pip 22.3)는 이 옵션이 없으면 wheel 패키지가 없어 bdist_wheel에서 실패한다.
grep -E "$build_re" "$req" | sed 's/ *#.*//' | while read -r spec; do
    "$py" -m pip wheel --disable-pip-version-check --no-deps --use-pep517 --no-binary="${spec%%==*}" \
        -w "$LCA_HOME/packages" "$spec"
done
# 대상 환경에 pip가 오래됐을 때를 위해 pip wheel도 함께 둔다
"$py" -m pip download --disable-pip-version-check --only-binary=:all: --python-version "$pyver" \
    -d "$LCA_HOME/packages" pip
echo "wheels: $(ls "$LCA_HOME"/packages/*.whl | wc -l) in $LCA_HOME/packages"
