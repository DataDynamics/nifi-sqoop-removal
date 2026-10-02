import uuid
from pathlib import Path

import httpx
from fastapi import FastAPI
from textual.widgets import DataTable, RichLog, Static

from load_control.monitor.app import Dashboard, LogScreen, MonitorApp, RunDetail
from load_control.monitor.client import MonitorClient
from load_control.monitor.local import LogTail
from tests.conftest import NIFI_TOKEN, Db
from tests.helpers import complete_run, create_run


def make_app(app: FastAPI, log_dir: Path) -> MonitorApp:
    client = MonitorClient("http://test", NIFI_TOKEN, transport=httpx.ASGITransport(app=app))
    return MonitorApp(client, refresh_seconds=60, log_dir=log_dir)


async def wait_rows(pilot, table: DataTable, count: int) -> None:  # type: ignore[no-untyped-def]
    for _ in range(50):
        if table.row_count >= count:
            return
        await pilot.pause(0.05)
    raise AssertionError(f"{table.id}: rows {table.row_count} < {count}")


async def test_dashboard_detail_and_logs(app: FastAPI, client: httpx.AsyncClient, db: Db,
                                         tmp_path: Path) -> None:
    run, _ = await complete_run(client, [3, 4])
    failed = await create_run(client)
    await db.execute("UPDATE nifi_ops.load_run SET status = 'FAILED_EXTRACT', error_code = 'ORA-00942', "
                     "completed_at = clock_timestamp() WHERE run_id = :id", id=uuid.UUID(failed.run_id))
    (tmp_path / "server.log").write_text(
        f"2026-10-03 04:00:00.000 INFO  [x] run 생성 (run_created) runId={run.run_id}\n"
        f"2026-10-03 04:00:01.000 ERROR [x] 오류 (api_error) runId={failed.run_id}\n", encoding="utf-8")

    tui = make_app(app, tmp_path)
    async with tui.run_test(size=(160, 50)) as pilot:
        dash = tui.screen
        assert isinstance(dash, Dashboard)
        runs = dash.query_one("#runs", DataTable)
        await wait_rows(pilot, runs, 2)
        alerts = dash.query_one("#alerts", DataTable)
        await wait_rows(pilot, alerts, 1)
        assert "FAILED_EXTRACT 1" in str(dash.query_one("#counts", Static).content)
        assert "EXTRACTED_VALIDATED 1" in str(dash.query_one("#counts", Static).content)

        runs.focus()
        runs.move_cursor(row=runs.get_row_index(run.run_id))
        await pilot.press("enter")
        await pilot.pause(0.2)
        detail = tui.screen
        assert isinstance(detail, RunDetail) and detail.run_id == run.run_id
        await wait_rows(pilot, detail.query_one("#parts", DataTable), 2)
        await wait_rows(pilot, detail.query_one("#events", DataTable), 3)
        assert detail.query_one("#metrics", DataTable).row_count >= 1  # SOURCE 지표

        await pilot.press("l")
        await pilot.pause(0.2)
        logs = tui.screen
        assert isinstance(logs, LogScreen) and logs.filter_text == run.run_id
        lines = logs.query_one("#log", RichLog).lines
        assert len(lines) == 1 and run.run_id in "".join(seg.text for seg in lines[0])

        await pilot.press("escape", "escape")
        await pilot.pause(0.1)
        assert isinstance(tui.screen, Dashboard)


async def test_dashboard_shows_api_error(app: FastAPI, tmp_path: Path) -> None:
    client = MonitorClient("http://test", "wrong-token", transport=httpx.ASGITransport(app=app))
    tui = MonitorApp(client, refresh_seconds=60, log_dir=tmp_path)
    async with tui.run_test(size=(160, 40)) as pilot:
        await pilot.pause(0.3)
        assert "인증 실패" in str(tui.screen.query_one("#services", Static).content)


def test_log_tail_follows_rotation(tmp_path: Path) -> None:
    path = tmp_path / "server.log"
    path.write_text("a\nb\n", encoding="utf-8")
    tail = LogTail(path)
    assert tail.last_lines(10) == ["a", "b"]
    with path.open("a", encoding="utf-8") as f:
        f.write("c\nd")                      # d는 아직 줄바꿈 전
    assert tail.read_new() == ["c"]
    with path.open("a", encoding="utf-8") as f:
        f.write("\n")
    assert tail.read_new() == ["d"]
    path.rename(tmp_path / "server.log.1")   # 회전
    path.write_text("e\n", encoding="utf-8")
    assert tail.read_new() == ["e"]
