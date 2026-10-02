"""TUI 모니터 화면: 대시보드, run 상세, 로그.

조회 API(/v1/monitor/summary, /v1/runs, /v1/runs/{id}/...)만 쓰고 상태를 바꾸지 않는다.
서비스 PID와 로그 파일은 API 서버 호스트의 logs/ 디렉터리에서 읽는다(다른 호스트에서 실행하면 비어 보인다).
"""

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Input, RichLog, Static, TabbedContent, TabPane

from load_control.monitor.client import ApiError, MonitorClient
from load_control.monitor.local import LogTail, is_readable, log_files, service_pid

ACTIVE = ("CREATED", "EXTRACTING", "EXTRACTED_VALIDATED", "STAGE_VALIDATING", "STAGING_VALIDATED",
          "PUBLISHING", "PUBLISHED", "PUBLISH_UNKNOWN")


def status_text(status: str | None) -> Text:
    """상태 값에 색을 입힌다: 성공 초록, 실패 빨강, 결과 불명 자홍, 진행 중 노랑."""
    if not status:
        return Text("-")
    if status == "SUCCESS":
        style = "green"
    elif status == "PUBLISH_UNKNOWN":
        style = "bold magenta"
    elif status.startswith("FAILED") or status in ("TIMED_OUT", "FAILED", "DEAD", "FAIL"):
        style = "bold red"
    elif status in ("PASS", "ACKED"):
        style = "green"
    else:
        style = "yellow"
    return Text(status, style=style)


def local_time(value: str | None, with_date: bool = True) -> str:
    """API의 ISO 시각을 서버 현지 시각으로."""
    if not value:
        return "-"
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone()
    return dt.strftime("%m-%d %H:%M:%S" if with_date else "%H:%M:%S")


def elapsed(start: str | None, end: str | None) -> str:
    if not start:
        return "-"
    t0 = datetime.fromisoformat(start.replace("Z", "+00:00"))
    t1 = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else datetime.now(UTC)
    sec = max(0, int((t1 - t0).total_seconds()))
    return f"{sec // 3600}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


def num(value: Any) -> str:
    return "-" if value is None else f"{value:,}" if isinstance(value, int) else str(value)


def short_id(run_id: str | None) -> str:
    return run_id[:8] if run_id else "-"


def fill(table: DataTable[Any], rows: Iterable[tuple[str, list[Any]]]) -> None:
    """표를 다시 채우고 커서를 같은 행(key)에 둔다."""
    key = None
    if table.row_count and table.cursor_row >= 0:
        try:
            key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
        except Exception:
            key = None
    table.clear()
    for row_key, cells in rows:
        table.add_row(*cells, key=row_key)
    if key is not None:
        try:
            table.move_cursor(row=table.get_row_index(key))
        except Exception:
            pass


class Dashboard(Screen[None]):
    """서비스 상태, run 수, 경보, run 목록."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("enter", "open_run", "run 상세", show=True),
        Binding("l", "logs", "로그"),
        Binding("a", "toggle_active", "진행 중만"),
        Binding("r", "refresh", "새로고침"),
        Binding("q", "app.quit", "종료"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.active_only = False
        self.alert_runs: dict[str, str] = {}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="top"):
            yield Static(id="services")
            yield Static(id="counts")
        yield Static("경보", classes="title", id="alerts-title")
        yield DataTable(id="alerts", cursor_type="row", zebra_stripes=True)
        yield Static("run", classes="title", id="runs-title")
        yield DataTable(id="runs", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self.app.sub_title = self.monitor_app.client.base_url
        alerts = self.query_one("#alerts", DataTable)
        alerts.add_columns("수준", "종류", "Job", "업무일자", "run", "상태", "시각", "내용")
        runs = self.query_one("#runs", DataTable)
        runs.add_columns("run", "Job", "업무일자", "상태", "파티션(성공/실패/전체)", "원천", "추출",
                         "staging", "target", "시작", "소요", "오류")
        runs.focus()
        self.action_refresh()
        self.set_interval(self.monitor_app.refresh_seconds, self.action_refresh)

    @property
    def monitor_app(self) -> "MonitorApp":
        assert isinstance(self.app, MonitorApp)
        return self.app

    def action_refresh(self) -> None:
        self.load()

    def action_toggle_active(self) -> None:
        self.active_only = not self.active_only
        self.action_refresh()

    def action_logs(self) -> None:
        self.app.push_screen(LogScreen())

    def action_open_run(self) -> None:
        focused = self.focused
        if isinstance(focused, DataTable) and focused.row_count:
            self._open(focused, focused.coordinate_to_cell_key(focused.cursor_coordinate).row_key.value)

    @on(DataTable.RowSelected)
    def row_selected(self, event: DataTable.RowSelected) -> None:
        self._open(event.data_table, event.row_key.value)

    def _open(self, table: DataTable[Any], key: str | None) -> None:
        run_id = self.alert_runs.get(key or "") if table.id == "alerts" else key
        if run_id:
            self.app.push_screen(RunDetail(run_id))

    @work(exclusive=True)
    async def load(self) -> None:
        app = self.monitor_app
        client = app.client
        ready = await client.ready()
        log_dir = app.log_dir
        pids = {s: service_pid(log_dir, s) for s in ("server", "worker")}
        services = Text()
        services.append("API  ")
        services.append("준비됨" if ready else "응답 없음", style="green" if ready else "bold red")
        for name, pid in pids.items():
            services.append(f"\n{name:<7}")
            services.append(f"실행 중(pid {pid})" if pid else "PID 파일 없음(systemd 또는 다른 호스트)",
                            style="green" if pid else "dim")
        try:
            summary = await client.summary()
            runs = await client.runs(limit=200)
        except ApiError as e:
            services.append(f"\n{e}", style="bold red")
            services.append(f"\n마지막 시도 {datetime.now():%H:%M:%S}", style="dim")
            self.query_one("#services", Static).update(services)
            return
        services.append(f"\n갱신 {datetime.now():%H:%M:%S}, {app.refresh_seconds:g}초마다", style="dim")
        self.query_one("#services", Static).update(services)
        self.query_one("#counts", Static).update(self._counts(summary))

        self.alert_runs = {}
        alert_rows = []
        for i, a in enumerate(summary["alerts"]):
            key = str(i)
            if a.get("runId"):
                self.alert_runs[key] = a["runId"]
            sev = Text(a["severity"], style="bold red" if a["severity"] == "ERROR" else "yellow")
            alert_rows.append((key, [sev, a["kind"], a.get("jobKey") or "-", a.get("businessKey") or "-",
                                     short_id(a.get("runId")), status_text(a.get("status")),
                                     local_time(a.get("at")), (a.get("message") or "")[:80]]))
        self.query_one("#alerts-title", Static).update(f"경보 {len(alert_rows)}건")
        fill(self.query_one("#alerts", DataTable), alert_rows)

        shown = [r for r in runs if not self.active_only or r["status"] in ACTIVE]
        self.query_one("#runs-title", Static).update(
            f"run {len(shown)}건"
            + (" (진행 중만, a: 전체)" if self.active_only else " (최근 200, a: 진행 중만)"))
        fill(self.query_one("#runs", DataTable), [(r["runId"], [
            short_id(r["runId"]), r["jobKey"], r["businessKey"], status_text(r["status"]),
            f"{r['successPartitionCount']}/{r['failedPartitionCount']}/{num(r.get('expectedPartitionCount'))}",
            num(r.get("sourceCount")), num(r.get("extractedCount")), num(r.get("stagingCount")),
            num(r.get("targetCount")), local_time(r["startedAt"]),
            elapsed(r["startedAt"], r.get("completedAt")), r.get("errorCode") or ""]) for r in shown])

    @staticmethod
    def _counts(summary: dict[str, Any]) -> Text:
        t = Text()
        active = summary["activeRuns"]
        t.append("진행 중  ")
        t.append(", ".join(f"{k} {v}" for k, v in sorted(active.items())) or "없음",
                 style="yellow" if active else "dim")
        recent = summary["recentRuns"]
        t.append(f"\n최근 {summary['recentWindowHours']}시간  ")
        if not recent:
            t.append("없음", style="dim")
        for i, (k, v) in enumerate(sorted(recent.items())):
            t.append(", " if i else "")
            t.append(f"{k} {v}", style="green" if k == "SUCCESS" else "red")
        d = summary["dispatches"]
        t.append(f"\ndispatch  PENDING {d.get('PENDING', 0)}, SENT {d.get('SENT', 0)}, ")
        t.append(f"DEAD {d.get('DEAD', 0)}", style="bold red" if d.get("DEAD") else "")
        t.append(f"\n정리 대상  {summary['cleanupDue']}건")
        return t


class RunDetail(Screen[None]):
    """run 하나의 파티션, 검증 지표, dispatch, 이벤트."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("escape", "app.pop_screen", "뒤로"),
        Binding("l", "logs", "이 run의 로그"),
        Binding("r", "refresh", "새로고침"),
        Binding("q", "app.quit", "종료"),
    ]

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="run-info")
        with TabbedContent():
            with TabPane("파티션", id="tab-parts"):
                yield DataTable(id="parts", cursor_type="row", zebra_stripes=True)
            with TabPane("검증 지표", id="tab-metrics"):
                yield DataTable(id="metrics", cursor_type="row", zebra_stripes=True)
            with TabPane("dispatch", id="tab-disp"):
                yield DataTable(id="disp", cursor_type="row", zebra_stripes=True)
            with TabPane("이벤트", id="tab-events"):
                yield DataTable(id="events", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self.app.sub_title = f"run {self.run_id}"
        self.query_one("#parts", DataTable).add_columns(
            "파티션", "상태", "예상", "실제", "파일", "시도", "노드", "오류")
        self.query_one("#metrics", DataTable).add_columns("단계", "지표", "기대값", "실제값", "결과", "측정")
        self.query_one("#disp", DataTable).add_columns(
            "종류", "파티션", "상태", "시도", "전송", "ACK", "dispatch")
        self.query_one("#events", DataTable).add_columns(
            "시각", "수준", "이벤트", "파티션", "주체", "코드", "내용")
        self.action_refresh()
        self.set_interval(self.monitor_app.refresh_seconds, self.action_refresh)

    @property
    def monitor_app(self) -> "MonitorApp":
        assert isinstance(self.app, MonitorApp)
        return self.app

    def action_refresh(self) -> None:
        self.load()

    def action_logs(self) -> None:
        self.app.push_screen(LogScreen(initial_filter=self.run_id))

    @work(exclusive=True)
    async def load(self) -> None:
        client = self.monitor_app.client
        try:
            run = await client.run(self.run_id)
            metrics = await client.validations(self.run_id)
            events = await client.events(self.run_id)
        except ApiError as e:
            self.query_one("#run-info", Static).update(Text(str(e), style="bold red"))
            return
        info = Text()
        info.append(f"{run['jobKey']}  업무일자 {run['businessKey']}  ")
        info.append_text(status_text(run["status"]))
        info.append(f"\nrun {run['runId']}  SCN {run.get('snapshotScn') or '-'}")
        info.append(f"\n건수  원천 {num(run.get('sourceCount'))}  추출 {num(run.get('extractedCount'))}  "
                    f"staging {num(run.get('stagingCount'))}  target {num(run.get('targetCount'))}")
        start, end = run["startedAt"], run.get("completedAt")
        info.append(f"\n시각  시작 {local_time(start)}  마지막 변화 {local_time(run['heartbeatAt'])}"
                    f"  종료 {local_time(end)}  소요 {elapsed(start, end)}")
        info.append(f"\n경로  {run.get('hdfsRunPath') or '-'}  staging {run.get('stageTable') or '-'}")
        if run.get("errorCode"):
            stage, message = run.get("errorStage") or "-", run.get("errorMessage") or ""
            info.append(f"\n오류  {stage} {run['errorCode']} {message}",
                        style="bold red")
        self.query_one("#run-info", Static).update(info)
        fill(self.query_one("#parts", DataTable), [(p["partitionId"], [
            p["partitionId"], status_text(p["status"]), num(p["expectedRowCount"]),
            num(p.get("actualRowCount")),
            num(p.get("fileCount")), str(p["attemptCount"]), p.get("workerNode") or "-",
            p.get("errorCode") or ""]) for p in run["partitions"]])
        fill(self.query_one("#metrics", DataTable), [(f"{m['stage']}:{m['metricName']}", [
            m["stage"], m["metricName"], m.get("expectedValue") or "", m.get("actualValue") or "",
            status_text(m["result"]), local_time(m["measuredAt"])]) for m in metrics])
        fill(self.query_one("#disp", DataTable), [(d["dispatchId"], [
            d["dispatchType"], d.get("partitionId") or "-", status_text(d["status"]), str(d["attemptCount"]),
            local_time(d.get("sentAt")), local_time(d.get("ackedAt")), short_id(d["dispatchId"])])
            for d in run["dispatches"]])
        fill(self.query_one("#events", DataTable), [(str(i), [
            local_time(e["eventTime"]), Text(e["level"], style="bold red" if e["level"] == "ERROR"
                                             else "yellow" if e["level"] == "WARN" else ""),
            e["name"], e.get("partitionId") or "", e.get("processGroup") or "", e.get("errorCode") or "",
            (e.get("message") or "")[:100]]) for i, e in enumerate(events)])


class LogScreen(Screen[None]):
    """logs/server.log, worker.log 실시간 보기. 입력란의 글자(run ID, requestId 등)가 들어간 줄만 보인다."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("escape", "app.pop_screen", "뒤로"),
        Binding("f2", "toggle_errors", "WARN·ERROR만"),
        Binding("f3", "cycle_source", "server/worker"),
        Binding("ctrl+q", "app.quit", "종료"),
    ]
    SOURCES: ClassVar[list[tuple[str, ...]]] = [("server", "worker"), ("server",), ("worker",)]
    INITIAL_LINES = 300

    def __init__(self, initial_filter: str = "") -> None:
        super().__init__()
        self.filter_text = initial_filter
        self.errors_only = False
        self.source_index = 0
        self.tails: dict[str, LogTail] = {}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Input(value=self.filter_text, id="filter",
                    placeholder="필터: run ID, requestId, 단어(대소문자 무시)")
        yield Static(id="log-status", classes="title")
        yield RichLog(id="log", wrap=False, markup=False, highlight=False, max_lines=5000)
        yield Footer()

    @property
    def monitor_app(self) -> "MonitorApp":
        assert isinstance(self.app, MonitorApp)
        return self.app

    def on_mount(self) -> None:
        self.app.sub_title = "로그"
        self.reload()
        self.set_interval(1.0, self.poll)

    def action_toggle_errors(self) -> None:
        self.errors_only = not self.errors_only
        self.reload()

    def action_cycle_source(self) -> None:
        self.source_index = (self.source_index + 1) % len(self.SOURCES)
        self.reload()

    @on(Input.Changed, "#filter")
    def filter_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value.strip()
        self.reload()

    def _matches(self, line: str) -> bool:
        if self.errors_only and " ERROR " not in line and " WARN " not in line:
            return False
        return not self.filter_text or self.filter_text.lower() in line.lower()

    def _write(self, source: str, line: str) -> None:
        style = "bold red" if " ERROR " in line else "yellow" if " WARN " in line else ""
        text = Text(f"{source[0].upper()}| ", style="dim")
        text.append(line, style=style)
        self.query_one("#log", RichLog).write(text)

    def reload(self) -> None:
        log = self.query_one("#log", RichLog)
        log.clear()
        files = log_files(self.monitor_app.log_dir)
        sources = self.SOURCES[self.source_index]
        self.tails = {s: LogTail(files[s]) for s in sources if is_readable(files[s])}
        missing = [str(files[s]) for s in sources if s not in self.tails]
        status = f"보기: {'+'.join(sources)}  WARN·ERROR만: {'예' if self.errors_only else '아니오'}"
        if missing:
            status += f"  (읽을 수 없음: {', '.join(missing)})"
        self.query_one("#log-status", Static).update(status)
        merged: list[tuple[str, str, str]] = []
        for source, tail in self.tails.items():
            lines = [x for x in tail.last_lines(self.INITIAL_LINES * 20) if self._matches(x)]
            merged.extend((x[:23], source, x) for x in lines[-self.INITIAL_LINES:])
        for _, source, line in sorted(merged)[-self.INITIAL_LINES:]:
            self._write(source, line)

    def poll(self) -> None:
        for source, tail in self.tails.items():
            for line in tail.read_new():
                if line and self._matches(line):
                    self._write(source, line)


class MonitorApp(App[None]):
    """Load Control 모니터."""

    TITLE = "Load Control 모니터"
    CSS = """
    #top { height: auto; max-height: 8; }
    #services, #counts { width: 1fr; height: auto; border: round $primary; padding: 0 1; }
    .title { background: $boost; padding: 0 1; text-style: bold; }
    #alerts { height: auto; max-height: 10; }
    #runs { height: 1fr; }
    #run-info { height: auto; border: round $primary; padding: 0 1; }
    #log { height: 1fr; }
    """

    def __init__(self, client: MonitorClient, *, refresh_seconds: float, log_dir: Path) -> None:
        super().__init__()
        self.client = client
        self.refresh_seconds = refresh_seconds
        self.log_dir = log_dir

    def on_mount(self) -> None:
        self.push_screen(Dashboard())

    async def on_unmount(self) -> None:
        await self.client.close()
