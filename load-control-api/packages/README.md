# packages

airgap 설치용 wheel을 두는 곳이다. wheel은 저장소에 넣지 않고(`.gitignore`) `requirements.txt`(고정 버전 목록)만 둔다.

- 받기(인터넷이 되는 장비): `bin/download-packages.sh` — Python 3.12, manylinux x86_64 wheel과 pip wheel
- 설치(airgap 장비): `bin/install.sh` — 이 디렉터리만 써서 `.venv`를 만든다(`pip --no-index`)
- 의존성 변경 후: `bin/download-packages.sh --lock`으로 `requirements.txt`를 다시 만든다
