"""API 서버 호스트의 로컬 정보: bin 스크립트가 남긴 PID 파일과 로그 파일."""

import os
from dataclasses import dataclass, field
from pathlib import Path

MODULES = {"server": "load_control.server", "worker": "load_control.worker"}


def service_pid(log_dir: Path, service: str) -> int | None:
    """bin/start.sh가 띄운 서비스가 살아 있으면 PID. PID 파일이 없거나 다른 프로세스면 None."""
    try:
        pid = int((log_dir / f"{service}.pid").read_text().strip())
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except (OSError, ValueError):
        return None
    return pid if f"-m {MODULES[service]}" in cmdline else None


@dataclass
class LogTail:
    """파일 끝에 새로 붙은 줄을 읽는다. 회전(파일이 바뀌거나 작아짐)되면 처음부터 다시 읽는다."""

    path: Path
    _inode: int | None = None
    _offset: int = 0
    _partial: str = field(default="")

    def last_lines(self, count: int) -> list[str]:
        """처음 열 때: 마지막 count줄을 돌려주고 이후 read_new는 그 뒤부터 읽는다."""
        try:
            st = self.path.stat()
            with self.path.open("rb") as f:
                size = st.st_size
                f.seek(max(0, size - 512 * 1024))
                data = f.read()
        except OSError:
            return []
        self._inode, self._offset, self._partial = st.st_ino, size, ""
        return data.decode("utf-8", errors="replace").splitlines()[-count:]

    def read_new(self) -> list[str]:
        try:
            st = self.path.stat()
        except OSError:
            return []
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
    return {name: log_dir / f"{name}.log" for name in ("server", "worker")}


def is_readable(path: Path) -> bool:
    return path.exists() and os.access(path, os.R_OK)
