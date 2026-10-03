# packages

airgap 설치용 wheel을 두는 곳이다. 대상은 RHEL 9(x86_64), Python 3.11(cp311)이다. `.gitignore`가 wheel을 제외하므로
wheel을 바꾸면 `git add -f packages/*.whl`로 저장소에 넣는다. `requirements.txt`는 고정 버전 목록이다.

- 받기(인터넷이 되는 장비): `bin/download-packages.sh` — Python 3.11, manylinux x86_64 wheel과 pip wheel
- 설치(airgap 장비): `bin/install.sh` — 이 디렉터리만 써서 `.venv`를 만든다(`pip --no-index`)
- 의존성 변경 후: `bin/download-packages.sh --lock`으로 `requirements.txt`를 다시 만든다
