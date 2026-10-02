from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from load_control.config import Settings

EXAMPLE = Path(__file__).resolve().parents[2] / "config" / "config.example.yaml"

MINIMAL = """
database:
  url: postgresql+asyncpg://u@h:5432/db
recovery:
  stale: PT2H
  mode: REISSUE
dispatch:
  ack_timeout: 300
logging:
  format: console
"""


def write(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content, encoding="utf-8")
    return p


def test_example_config_is_valid() -> None:
    s = Settings.load(EXAMPLE)
    assert s.database.pool_size == 10
    assert s.recovery.stale == timedelta(minutes=90)
    assert str(s.nifi.receiver_url) == "http://nifi-lb.internal:9443/"
    assert s.cleanup.failed_retention == timedelta(days=14)
    assert set(s.auth.token_digests) == {"nifi", "operator"}


def test_load_minimal_with_defaults(tmp_path: Path) -> None:
    s = Settings.load(write(tmp_path, MINIMAL))
    assert s.database.url == "postgresql+asyncpg://u@h:5432/db"
    assert s.recovery.stale == timedelta(hours=2)          # ISO 8601
    assert s.dispatch.ack_timeout == timedelta(seconds=300)  # 초
    assert s.recovery.mode == "REISSUE"
    assert s.recovery.run_timeout == timedelta(hours=6)    # 기본값
    assert s.logging.format == "console"
    assert s.nifi.receiver_url is None


def test_env_overrides_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LCA_DATABASE__URL", "postgresql+asyncpg://secret@h/db")
    monkeypatch.setenv("LCA_DISPATCH__BATCH", "7")
    s = Settings.load(write(tmp_path, MINIMAL))
    assert s.database.url == "postgresql+asyncpg://secret@h/db"
    assert s.dispatch.batch == 7
    assert s.recovery.mode == "REISSUE"  # 나머지 YAML 값은 유지


def test_lca_config_env_selects_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LCA_CONFIG", str(write(tmp_path, MINIMAL)))
    assert Settings.load().recovery.mode == "REISSUE"


def test_missing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LCA_CONFIG", str(tmp_path / "nope.yaml"))
    with pytest.raises(FileNotFoundError, match=r"nope\.yaml"):
        Settings.load()


@pytest.mark.parametrize("content", [
    MINIMAL + "unknown_section: {}\n",
    MINIMAL.replace("  mode: REISSUE", "  mode: REISSUE\n  typo_key: 1"),
    MINIMAL.replace("mode: REISSUE", "mode: SOMETIMES"),
    MINIMAL.replace("stale: PT2H", "stale: PT30M"),  # extract_query_timeout(60분)보다 작음
    "recovery:\n  mode: FAIL\n",  # database.url 없음
])
def test_invalid_configs_rejected(tmp_path: Path, content: str) -> None:
    with pytest.raises(ValidationError):
        Settings.load(write(tmp_path, content))


def test_secret_only_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """YAML에 database.url을 두지 않고 환경변수로만 주입할 수 있다."""
    monkeypatch.setenv("LCA_DATABASE__URL", "postgresql+asyncpg://from-env@h/db")
    s = Settings.load(write(tmp_path, "database:\n  pool_size: 4\n"))
    assert s.database.url == "postgresql+asyncpg://from-env@h/db"
    assert s.database.pool_size == 4
