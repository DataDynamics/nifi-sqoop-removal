"""대화형 입력 공통: readline 편집·이력, 프롬프트 입력. SQL 셸과 HDFS 셸이 함께 쓴다.

이력은 도구별로 ~/.lca_<도구>_history에 남긴다(최대 1000줄). readline이 없는 python 빌드에서도
동작하도록 import 실패는 무시한다(줄 편집·이력만 빠진다).
"""

import atexit
import os
from pathlib import Path

HISTORY_LINES = 1000


def setup_history(tool: str) -> None:
    """readline 이력을 읽고, 종료할 때 저장하도록 등록한다. 이력 파일 오류는 무시한다."""
    try:
        import readline
    except ImportError:  # pragma: no cover - readline 없는 빌드
        return
    path = Path(os.environ.get("LCA_HISTORY_DIR", Path.home())) / f".lca_{tool}_history"
    try:
        readline.read_history_file(path)
    except OSError:
        pass
    readline.set_history_length(HISTORY_LINES)

    def save() -> None:
        try:
            readline.write_history_file(path)
            path.chmod(0o600)  # 이력에 SQL 조건값이 남으므로 본인만 읽게 한다
        except OSError:
            pass

    atexit.register(save)


def read_line(prompt: str) -> str | None:
    """프롬프트를 보여 주고 한 줄을 읽는다. EOF(Ctrl-D)면 None. Ctrl-C는 호출자가 처리한다."""
    try:
        return input(prompt)
    except EOFError:
        print()
        return None
