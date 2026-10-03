"""Textual TUI 모니터(bin/monitor.sh)를 Pilot으로 조작해 화면 전환과 운영 작업을 검증한다.

MonitorClient는 ASGI transport로 테스트 앱을 직접 호출한다.
"""

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
    """테스트 앱에 붙는 MonitorApp을 만든다.

    자동 새로 고침은 60초로 길게 잡아 테스트 도중 끼어들지 않게 한다.
    """
    client = MonitorClient("http://test", NIFI_TOKEN, operator_token=operator_token,
                           transport=httpx.ASGITransport(app=app))
    return MonitorApp(client, refresh_seconds=60, log_dir=log_dir, bin_dir=bin_dir)


async def wait_for(pilot, check) -> None:  # type: ignore[no-untyped-def]
    """check()가 참이 될 때까지 최대 약 3초 동안 화면을 진행시키며 기다린다."""
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
    """DataTable에 행이 count개 이상 채워질 때까지 기다린다(비동기 조회 완료 대기)."""
    for _ in range(50):
        if table.row_count >= count:
            return
        await pilot.pause(0.05)
    raise AssertionError(f"{table.id}: rows {table.row_count} < {count}")


async def test_dashboard_detail_and_logs(app: FastAPI, client: httpx.AsyncClient, db: Db,
                                         tmp_path: Path) -> None:
    """대시보드 → run 상세 → 로그 화면으로 이동하고 esc로 돌아온다.

    대시보드에 run 목록·경보·상태별 수가 나오고, 상세에는 파티션·이벤트·지표가,
    로그 화면에는 그 run ID가 들어간 줄만 걸러져 나오는지 확인한다.
    """
    run, _ = await complete_run(client, [3, 4])
    failed = await create_run(client)
    # 경보 표에 한 줄이 나오도록 실패 run을 SQL로 만든다.
    await db.execute("UPDATE nifi_ops.load_run SET status = 'FAILED_EXTRACT', error_code = 'ORA-00942', "
                     "completed_at = clock_timestamp() WHERE run_id = :id", id=uuid.UUID(failed.run_id))
    # 로그 화면의 run ID 필터를 확인하려고 서로 다른 run의 로그 두 줄을 둔다.
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
    """API 인증이 실패하면 대시보드 서비스 영역에 "인증 실패"를 보여 준다."""
    client = MonitorClient("http://test", "wrong-token", transport=httpx.ASGITransport(app=app))
    tui = MonitorApp(client, refresh_seconds=60, log_dir=tmp_path)
    async with tui.run_test(size=(160, 40)) as pilot:
        await pilot.pause(0.3)
        assert "인증 실패" in str(tui.screen.query_one("#services", Static).content)


def test_log_tail_follows_rotation(tmp_path: Path) -> None:
    """LogTail은 완성된 줄만 읽고, 로그 파일이 회전되면 새 파일을 처음부터 따라간다."""
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
    """검증 대기 run을 만들고 그 dispatch를 DEAD로 바꿔 (run_id, dispatch_id)를 돌려준다."""
    run, _ = await complete_run(client)
    # dispatcher가 재시도를 다 쓴 상태를 SQL로 바로 만든다.
    await db.execute("UPDATE nifi_ops.load_dispatch SET status = 'DEAD', last_error = 'HTTP 404' "
                     "WHERE run_id = :id", id=uuid.UUID(run.run_id))
    dispatch_id = await db.scalar("SELECT dispatch_id FROM nifi_ops.load_dispatch WHERE run_id = :id",
                                  id=uuid.UUID(run.run_id))
    return run.run_id, str(dispatch_id)


async def test_resend_dead_dispatch_from_alert(app: FastAPI, client: httpx.AsyncClient, db: Db,
                                               tmp_path: Path) -> None:
    """경보 표에서 x로 DEAD dispatch를 재전송하면 확인 후 PENDING이 된다.

    확인 창의 기본 포커스는 취소 버튼이어야 한다.
    """
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
    """run 상세에서 s로 재전송한다. 확인 창을 취소하면 DEAD 그대로, 확인하면 PENDING이 된다."""
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
    """운영자 토큰 없이 재전송하면 "권한 없음" 알림을 띄우고 DB는 바뀌지 않는다."""
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
    """run 상세에서 p로 PUBLISH_UNKNOWN을 확정한다.

    기본 선택은 FAILED_PUBLISH이고, 근거가 5자 미만이면 창이 닫히지 않고 오류를 보여 준다.
    근거를 채우고 확인하면 run이 FAILED_PUBLISH로 바뀐다.
    """
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
    """서비스 화면의 재시작 버튼은 확인 후 bin/restart.sh all을 실행하고 출력을 보여 준다.

    가짜 스크립트가 인자와 작업 디렉터리 이름을 출력하므로, 설치 홈(bin의 부모)에서
    all 인자로 실행됐고 종료 코드 0까지 표시되는지 확인한다.
    """
    bin_dir = tmp_path / "home" / "bin"
    bin_dir.mkdir(parents=True)
    for action in ("start", "stop", "restart"):
        script = bin_dir / f"{action}.sh"
        # 실제 서비스를 건드리지 않도록 인자와 작업 디렉터리만 출력하는 가짜 스크립트를 둔다.
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
