"""API 서버 호스트의 로컬 정보: bin 스크립트가 남긴 PID 파일과 로그 파일.

API를 거치지 않고 이 호스트의 파일을 직접 읽으므로, 모니터를 API 서버와 다른 호스트에서 실행하면
PID·로그가 비어 보인다. 파일을 읽기만 하고 쓰거나 지우지 않는다.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

# 서비스 이름 → python -m 모듈. bin/env.sh의 LCA_MODULES와 같아야 PID 확인 결과가 bin/status.sh와 일치한다.
MODULES = {"server": "load_control.server", "worker": "load_control.worker"}


def service_pid(log_dir: Path, service: str) -> int | None:
    """bin/start.sh가 띄운 서비스가 살아 있으면 PID. PID 파일이 없거나 다른 프로세스면 None.

    log_dir/<service>.pid의 PID로 /proc/<pid>/cmdline을 읽어 "-m load_control.<service>"가 들어 있을 때만
    살아 있다고 본다. 프로세스가 죽은 뒤 PID가 다른 프로세스에 재사용된 경우를 걸러내기 위해서다
    (bin/env.sh lca_running_pid와 같은 판단). systemd로 띄운 서비스는 PID 파일이 없으므로 None이다.
    /proc를 쓰므로 Linux 전용이다. service는 MODULES의 키(server, worker)여야 한다.
    """
    try:
        pid = int((log_dir / f"{service}.pid").read_text().strip())
        # cmdline은 인자 사이가 NUL이다. 공백으로 바꿔 "-m 모듈" 문자열로 찾는다.
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except (OSError, ValueError):
        # PID 파일 없음, 숫자가 아님, 프로세스 없음(/proc 항목 없음), 권한 없음
        return None
    return pid if f"-m {MODULES[service]}" in cmdline else None


@dataclass
class LogTail:
    """파일 끝에 새로 붙은 줄을 읽는다. 회전(파일이 바뀌거나 작아짐)되면 처음부터 다시 읽는다.

    tail -F처럼 inode와 읽은 위치(_offset)를 기억해 두고, 호출할 때마다 그 뒤에 붙은 바이트만 읽는다.
    RotatingFileHandler가 파일을 rename하고 새로 만들면 inode가 바뀌고, truncate되면 크기가 줄어드므로
    두 경우 모두 새 파일로 보고 처음부터 읽는다(회전 직전 옛 파일에 마지막으로 쓰인 줄은 놓칠 수 있다).
    바이트 단위로 읽으므로 UTF-8 문자 중간에서 끊기면 그 문자는 깨져(U+FFFD) 보일 수 있다.
    """

    path: Path
    _inode: int | None = None  # 마지막으로 읽은 파일의 inode. None이면 아직 연 적이 없다.
    _offset: int = 0           # 다음에 읽을 바이트 위치
    _partial: str = field(default="")  # 줄바꿈이 아직 오지 않은 마지막 줄 조각(다음 읽기 앞에 붙인다)

    def last_lines(self, count: int) -> list[str]:
        """처음 열 때: 마지막 count줄을 돌려주고 이후 read_new는 그 뒤부터 읽는다.

        큰 로그 전체를 읽지 않도록 끝에서 최대 512KiB만 읽는다.
        그 안에 count줄이 없으면 있는 만큼만 돌려주며, 잘라 읽은 첫 줄은 앞부분이 빠진 조각일 수 있다.
        파일을 읽을 수 없으면 빈 목록을 돌려주고 상태를 바꾸지 않는다.
        """
        try:
            st = self.path.stat()
            with self.path.open("rb") as f:
                size = st.st_size
                f.seek(max(0, size - 512 * 1024))
                data = f.read()
        except OSError:
            return []
        # stat 시점의 크기를 기준으로 삼는다. stat 뒤 read 전에 붙은 바이트는 위에서 이미 읽었어도
        # read_new가 다시 읽으므로 한 줄이 두 번 보일 수는 있지만 빠지지는 않는다.
        self._inode, self._offset, self._partial = st.st_ino, size, ""
        return data.decode("utf-8", errors="replace").splitlines()[-count:]

    def read_new(self) -> list[str]:
        """마지막으로 읽은 위치 뒤에 새로 붙은 완성된 줄들을 돌려준다.

        한 번에 최대 4MiB만 읽어 화면이 멈추지 않게 하고, 남은 부분은 다음 호출에서 읽는다.
        마지막 줄이 줄바꿈으로 끝나지 않았으면 _partial에 남겨 두었다가 다음 읽기와 이어 붙인다.
        파일이 없거나 읽을 수 없으면 빈 목록을 돌려준다(다음 호출에서 다시 시도한다).
        """
        try:
            st = self.path.stat()
        except OSError:
            return []
        # 회전 감지: 다른 파일(inode 변경)이거나 크기가 읽은 위치보다 작아졌으면(truncate) 처음부터
        if st.st_ino != self._inode or st.st_size < self._offset:
            self._inode, self._offset, self._partial = st.st_ino, 0, ""
        if st.st_size == self._offset:
            return []
        with self.path.open("rb") as f:
            f.seek(self._offset)
            data = f.read(4 * 1024 * 1024)
        self._offset += len(data)
        text = self._partial + data.decode("utf-8", errors="replace")
        lines = text.split("\n")
        self._partial = lines.pop()  # 아직 줄바꿈이 오지 않은 마지막 조각
        return lines


def log_files(log_dir: Path) -> dict[str, Path]:
    """서비스별 구조화 로그 파일 경로(logging.file.path 기본값 logs/{service}.log에 맞춘다)."""
    return {name: log_dir / f"{name}.log" for name in ("server", "worker")}


def is_readable(path: Path) -> bool:
    """파일이 있고 이 프로세스가 읽을 수 있으면 True(다른 계정 소유 로그를 미리 걸러 낸다)."""
    return path.exists() and os.access(path, os.R_OK)
