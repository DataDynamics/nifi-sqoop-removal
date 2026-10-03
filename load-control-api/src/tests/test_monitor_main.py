"""monitor CLI 실행 옵션 테스트."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import load_control.monitor.__main__ as monitor_main


@pytest.mark.parametrize(
    ("argv", "expected_mouse"),
    [
        ([], False),
        (["--mouse"], True),
    ],
)
def test_main_passes_explicit_mouse_mode(monkeypatch, argv, expected_mouse):
    """기본은 no-mouse이고 --mouse를 지정한 경우에만 활성화한다."""
    settings = SimpleNamespace(
        server=SimpleNamespace(port=8080),
        monitor=SimpleNamespace(
            api_url=None,
            token="viewer-token",
            operator_token=None,
            refresh_seconds=5,
            log_dir="logs",
        ),
    )

    class FakeSettings:
        @staticmethod
        def load(_path=None):
            return settings

    class FakeMonitorApp:
        run_kwargs = None

        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, **kwargs):
            type(self).run_kwargs = kwargs

    monkeypatch.setattr(monitor_main, "Settings", FakeSettings)
    monkeypatch.setattr(monitor_main, "MonitorClient", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(monitor_main, "MonitorApp", FakeMonitorApp)

    monitor_main.main(argv)

    assert FakeMonitorApp.run_kwargs == {"mouse": expected_mouse}
