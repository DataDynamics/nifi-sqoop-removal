import uuid
from pathlib import Path

import httpx
from fastapi import FastAPI
from textual.widgets import Button, DataTable, Footer, Input, RadioSet, RichLog, Static

from load_control.monitor.app import (
    ConfirmScreen,
    Dashboard,
    LogScreen,
    MonitorApp,
    ResolveScreen,
    RunDetail,
    ServiceScreen,
)
from load_control.monitor.client import MonitorClient
from load_control.monitor.local import LogTail
from tests.conftest import NIFI_TOKEN, OPERATOR_TOKEN, Db
from tests.helpers import complete_run, create_run


def make_app(app: FastAPI, log_dir: Path, *, operator_token: str | None = None,
             bin_dir: Path | None = None) -> MonitorApp:
    client = MonitorClient("http://test", NIFI_TOKEN, operator_token=operator_token,
                           transport=httpx.ASGITransport(app=app))
    return MonitorApp(client, refresh_seconds=60, log_dir=log_dir, bin_dir=bin_dir)


async def wait_for(pilot, check) -> None:  # type: ignore[no-untyped-def]
    for _ in range(60):
        if await check():
            return
        await pilot.pause(0.05)
    raise AssertionError("조건을 기다리다 시간 초과")


async def wait_screen(pilot, tui: MonitorApp, kind: type) -> None:  # type: ignore[no-untyped-def]
    """화면이 바뀌고 위젯이 모두 붙을 때까지 기다린다."""
    async def check() -> bool:
        return isinstance(tui.screen, kind) and bool(tui.screen.query(Footer) or tui.screen.query(Button))
    await wait_for(pilot, check)


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


async def make_dead(client: httpx.AsyncClient, db: Db) -> tuple[str, str]:
    run, _ = await complete_run(client)
    await db.execute("UPDATE nifi_ops.load_dispatch SET status = 'DEAD', last_error = 'HTTP 404' "
                     "WHERE run_id = :id", id=uuid.UUID(run.run_id))
    dispatch_id = await db.scalar("SELECT dispatch_id FROM nifi_ops.load_dispatch WHERE run_id = :id",
                                  id=uuid.UUID(run.run_id))
    return run.run_id, str(dispatch_id)


async def test_resend_dead_dispatch_from_alert(app: FastAPI, client: httpx.AsyncClient, db: Db,
                                               tmp_path: Path) -> None:
    run_id, dispatch_id = await make_dead(client, db)
    tui = make_app(app, tmp_path, operator_token=OPERATOR_TOKEN)
    async with tui.run_test(size=(160, 50)) as pilot:
        alerts = tui.screen.query_one("#alerts", DataTable)
        await wait_rows(pilot, alerts, 1)
        alerts.focus()
        await pilot.press("x")
        await wait_screen(pilot, tui, ConfirmScreen)
        assert tui.screen.focused is tui.screen.query_one("#no", Button)  # 기본은 취소
        await pilot.click("#yes")

        async def resent() -> bool:
            status = await db.scalar("SELECT status FROM nifi_ops.load_dispatch WHERE dispatch_id = :id",
                                     id=uuid.UUID(dispatch_id))
            return bool(status == "PENDING")
        await wait_for(pilot, resent)
        assert isinstance(tui.screen, Dashboard)
    assert run_id


async def test_resend_from_detail_and_cancel(app: FastAPI, client: httpx.AsyncClient, db: Db,
                                             tmp_path: Path) -> None:
    run_id, dispatch_id = await make_dead(client, db)
    tui = make_app(app, tmp_path, operator_token=OPERATOR_TOKEN)
    async with tui.run_test(size=(160, 50)) as pilot:
        await wait_rows(pilot, tui.screen.query_one("#runs", DataTable), 1)
        tui.push_screen(RunDetail(run_id))
        await wait_screen(pilot, tui, RunDetail)
        await wait_rows(pilot, tui.screen.query_one("#disp", DataTable), 1)
        await pilot.press("s")
        await wait_screen(pilot, tui, ConfirmScreen)
        await pilot.press("escape")                       # 취소하면 바뀌지 않는다
        await wait_screen(pilot, tui, RunDetail)
        assert await db.scalar("SELECT status FROM nifi_ops.load_dispatch WHERE dispatch_id = :id",
                               id=uuid.UUID(dispatch_id)) == "DEAD"
        await pilot.press("s")
        await wait_screen(pilot, tui, ConfirmScreen)
        await pilot.click("#yes")

        async def resent() -> bool:
            status = await db.scalar("SELECT status FROM nifi_ops.load_dispatch WHERE dispatch_id = :id",
                                     id=uuid.UUID(dispatch_id))
            return bool(status == "PENDING")
        await wait_for(pilot, resent)


async def test_resend_without_operator_token_reports_error(app: FastAPI, client: httpx.AsyncClient,
                                                           db: Db, tmp_path: Path) -> None:
    _, dispatch_id = await make_dead(client, db)
    tui = make_app(app, tmp_path)                         # nifi 토큰뿐
    async with tui.run_test(size=(160, 50)) as pilot:
        alerts = tui.screen.query_one("#alerts", DataTable)
        await wait_rows(pilot, alerts, 1)
        alerts.focus()
        await pilot.press("x")
        await wait_screen(pilot, tui, ConfirmScreen)
        await pilot.click("#yes")

        async def notified() -> bool:
            return any("권한 없음" in str(n.message) for n in tui._notifications)
        await wait_for(pilot, notified)
    assert await db.scalar("SELECT status FROM nifi_ops.load_dispatch WHERE dispatch_id = :id",
                           id=uuid.UUID(dispatch_id)) == "DEAD"


async def test_resolve_publish_unknown(app: FastAPI, client: httpx.AsyncClient, db: Db,
                                       tmp_path: Path) -> None:
    run = await create_run(client)
    await db.execute("UPDATE nifi_ops.load_run SET status = 'PUBLISH_UNKNOWN' WHERE run_id = :id",
                     id=uuid.UUID(run.run_id))
    tui = make_app(app, tmp_path, operator_token=OPERATOR_TOKEN)
    async with tui.run_test(size=(160, 50)) as pilot:
        await wait_rows(pilot, tui.screen.query_one("#runs", DataTable), 1)
        tui.push_screen(RunDetail(run.run_id))
        await wait_screen(pilot, tui, RunDetail)

        async def loaded() -> bool:
            return isinstance(tui.screen, RunDetail) and tui.screen.run is not None
        await wait_for(pilot, loaded)
        await pilot.press("p")
        await wait_screen(pilot, tui, ResolveScreen)
        assert tui.screen.query_one("#resolution", RadioSet).pressed_button.id == "FAILED_PUBLISH"  # type: ignore[union-attr]
        await pilot.press("a", "b")
        await pilot.click("#ok")                          # 근거가 짧으면 남는다
        assert isinstance(tui.screen, ResolveScreen)
        assert "5자" in str(tui.screen.query_one("#resolve-error", Static).content)
        tui.screen.query_one("#reason", Input).value = "Hive history 확인 결과 미반영"
        await pilot.pause(0.1)                            # 오류 문구로 바뀐 배치가 반영된 뒤 누른다
        await pilot.click("#ok")
        await wait_screen(pilot, tui, ConfirmScreen)
        await pilot.click("#yes")

        async def resolved() -> bool:
            status = await db.scalar("SELECT status FROM nifi_ops.load_run WHERE run_id = :id",
                                     id=uuid.UUID(run.run_id))
            return bool(status == "FAILED_PUBLISH")
        await wait_for(pilot, resolved)


async def test_service_screen_runs_bin_script(app: FastAPI, tmp_path: Path) -> None:
    bin_dir = tmp_path / "home" / "bin"
    bin_dir.mkdir(parents=True)
    for action in ("start", "stop", "restart"):
        script = bin_dir / f"{action}.sh"
        script.write_text(f'#!/bin/sh\necho "{action} $1 in $(basename "$PWD")"\n', encoding="utf-8")
        script.chmod(0o755)
    tui = make_app(app, tmp_path, bin_dir=bin_dir)
    async with tui.run_test(size=(160, 50)) as pilot:
        await pilot.press("s")
        await wait_screen(pilot, tui, ServiceScreen)
        service = tui.screen
        await pilot.click("#restart")
        await wait_screen(pilot, tui, ConfirmScreen)
        await pilot.click("#yes")
        await wait_screen(pilot, tui, ServiceScreen)
        out = service.query_one("#service-output", RichLog)

        async def done() -> bool:
            return any("종료 코드 0" in "".join(seg.text for seg in line) for line in out.lines)
        await wait_for(pilot, done)
        text = "\n".join("".join(seg.text for seg in line) for line in out.lines)
        assert "restart all in home" in text
