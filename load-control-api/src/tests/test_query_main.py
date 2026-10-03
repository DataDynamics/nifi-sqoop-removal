"""운영 조회 도구 진입점(python -m load_control.query)의 설정 처리를 검증한다."""

from pathlib import Path

import pytest

from load_control.query.__main__ import build_parser, main

CONFIG = """
database:
  url: postgresql+asyncpg://u@h:5432/db
logging:
  stdout: true
  file: null
"""


def test_missing_client_section_exits_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """clients.oracle이 없고 명령행으로도 채우지 않으면 접속하지 않고 종료 코드 2."""
    cfg = tmp_path / "config.yaml"
    cfg.write_text(CONFIG, encoding="utf-8")
    with pytest.raises(SystemExit) as exc:
        main(["oracle", "--config", str(cfg), "-c", "select 1 from dual"])
    assert exc.value.code == 2
    assert "clients.oracle" in capsys.readouterr().err


def test_missing_config_file_exits_2(tmp_path: Path) -> None:
    assert main(["hdfs", "--config", str(tmp_path / "none.yaml"), "pwd"]) == 2


def test_hdfs_options_after_command_go_to_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """bin/hdfs.sh pwd 처럼 위치 인자로 명령 하나를 실행한다. 명령 뒤의 -c는 명령 인자다(head -c)."""
    args = build_parser().parse_args(["hdfs", "head", "-c", "16", "/a"])
    assert args.command == ["head", "-c", "16", "/a"] and args.script is None
    cfg = tmp_path / "config.yaml"
    cfg.write_text(CONFIG + "clients:\n  hdfs:\n    namenode_urls: [http://127.0.0.1:9]\n    home: /x\n",
                   encoding="utf-8")
    assert main(["hdfs", "--config", str(cfg), "pwd"]) == 0
    assert capsys.readouterr().out.strip() == "/x"
