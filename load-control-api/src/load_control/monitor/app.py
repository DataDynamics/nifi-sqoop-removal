"""TUI 모니터 화면: 대시보드, run 상세, 로그와 운영 작업.

- 조회: /v1/monitor/summary, /v1/runs, /v1/runs/{id}/...
- 운영 작업(확인 창을 거친다): DEAD dispatch 재전송, PUBLISH_UNKNOWN 확정(operator 토큰),
  서비스 시작·중지·재시작(이 호스트의 bin/start.sh, stop.sh, restart.sh).
서비스 PID와 로그 파일은 API 서버 호스트의 logs/ 디렉터리에서 읽는다(다른 호스트에서 실행하면 비어 보인다).

화면 구성: MonitorApp이 Dashboard를 띄우고, Enter로 RunDetail, l로 LogScreen, s로 ServiceScreen을 연다.
API 조회는 textual worker(@work(exclusive=True))에서 돌려 화면이 멈추지 않게 하고, 같은 화면의 이전 조회가
아직 끝나지 않았으면 취소하고 새 조회만 남긴다. 운영 작업은 API의 CAS·잠금에 맡기므로 여러 모니터가
동시에 같은 작업을 눌러도 한 번만 적용되고 나머지는 409 오류 메시지로 보인다.
"""

import asyncio
import shutil
import subprocess
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    RadioButton,
    RadioSet,
    RichLog,
    Static,
    TabbedContent,
    TabPane,
)

from load_control.monitor.client import ApiError, MonitorClient
from load_control.monitor.local import LogTail, is_readable, log_files, service_pid

# 진행 중으로 보는 run 상태(종료 상태 SUCCESS·FAILED_*·TIMED_OUT 등이 아닌 것).
# repositories/monitor.py ACTIVE_STATUSES와 같아야 "진행 중만" 목록과 요약 수가 맞는다.
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
    """API의 ISO 시각을 서버 현지 시각으로.

    "Z" 접미사를 +00:00으로 바꿔 fromisoformat이 읽게 하고, 모니터를 실행한 호스트의 시간대로 바꾼다.
    값이 없으면 "-". with_date=False면 시각만(HH:MM:SS) 돌려준다.
    """
    if not value:
        return "-"
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone()
    return dt.strftime("%m-%d %H:%M:%S" if with_date else "%H:%M:%S")


def elapsed(start: str | None, end: str | None) -> str:
    """start부터 end(없으면 지금)까지 걸린 시간을 H:MM:SS로 돌려준다.

    진행 중인 run은 end가 없으므로 지금까지의 소요 시간이 새로고침마다 늘어난다.
    호스트 간 시계 차이로 음수가 나오면 0으로 본다.
    """
    if not start:
        return "-"
    t0 = datetime.fromisoformat(start.replace("Z", "+00:00"))
    t1 = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else datetime.now(UTC)
    sec = max(0, int((t1 - t0).total_seconds()))
    return f"{sec // 3600}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


def num(value: Any) -> str:
    """건수 표시: None은 "-", 정수는 천 단위 쉼표, 그 밖(문자열로 온 큰 수 등)은 그대로."""
    return "-" if value is None else f"{value:,}" if isinstance(value, int) else str(value)


def short_id(run_id: str | None) -> str:
    """UUID 앞 8자리(표 폭을 줄이기 위한 표시용). 없으면 "-"."""
    return run_id[:8] if run_id else "-"


def fill(table: DataTable[Any], rows: Iterable[tuple[str, list[Any]]]) -> None:
    """표를 다시 채우고 커서를 같은 행(key)에 둔다.

    주기 새로고침마다 표를 통째로 다시 그리므로, 사용자가 보던 행을 잃지 않게 이전 커서 행의 key를
    기억했다가 같은 key의 행으로 커서를 옮긴다. 그 행이 사라졌으면 커서는 textual 기본 위치에 둔다.
    rows는 (row key, 셀 목록) 쌍이며, key는 표 안에서 유일해야 한다.
    """
    key = None
    if table.row_count and table.cursor_row >= 0:
        try:
            key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
        except Exception:  # 커서가 범위를 벗어난 경우 등: 위치 복원만 포기한다
            key = None
    table.clear()
    for row_key, cells in rows:
        table.add_row(*cells, key=row_key)
    if key is not None:
        try:
            table.move_cursor(row=table.get_row_index(key))
        except Exception:  # 같은 key의 행이 새 목록에 없다
            pass


class ConfirmScreen(ModalScreen[bool]):
    """실행 전 확인. 기본 포커스는 취소다.

    운영 작업과 서비스 조작 앞에 띄운다. Enter를 무심코 눌러 실행되지 않도록 처음 포커스를 취소 버튼에
    둔다. 실행을 고르면 True, 취소·Esc면 False로 닫히며 결과는 push_screen의 콜백으로 전달된다.
    """

    BINDINGS: ClassVar[list[BindingType]] = [Binding("escape", "cancel", "취소")]

    def __init__(self, title: str, body: str, action_label: str = "실행") -> None:
        """제목, 본문(무엇을 하는지), 실행 버튼 글자를 받는다."""
        super().__init__()
        # Screen.title과 겹치지 않도록 title_text로 둔다
        self.title_text, self.body, self.action_label = title, body, action_label

    def compose(self) -> ComposeResult:
        """제목, 본문, 실행·취소 버튼."""
        with Vertical(classes="dialog"):
            yield Static(self.title_text, classes="dialog-title")
            yield Static(self.body)
            with Horizontal(classes="buttons"):
                yield Button(self.action_label, variant="error", id="yes")
                yield Button("취소", id="no")

    def on_mount(self) -> None:
        """취소 버튼에 포커스를 둔다(실수로 실행하지 않게)."""
        self.query_one("#no", Button).focus()

    @on(Button.Pressed)
    def pressed(self, event: Button.Pressed) -> None:
        """실행 버튼이면 True, 그 밖의 버튼이면 False로 닫는다."""
        self.dismiss(event.button.id == "yes")

    def action_cancel(self) -> None:
        """Esc: 실행하지 않고 닫는다."""
        self.dismiss(False)


class ResolveScreen(ModalScreen[tuple[str, str] | None]):
    """PUBLISH_UNKNOWN 확정: 결과와 확인 근거를 받는다.

    PUBLISH_UNKNOWN은 Hive 게시 결과를 NiFi가 확인하지 못한 상태라, 사람이 Hive
    이력과 target 데이터를 보고 결과를 정해야 한다. 기본 선택은 FAILED_PUBLISH다(잘못 고르더라도
    target 검증 없이 성공 처리되는 쪽보다 안전하다). (resolution, reason)을 돌려주고, 취소면 None이다.
    실제 API 호출은 MonitorApp.ask_resolve가 한 번 더 확인한 뒤 한다.
    """

    BINDINGS: ClassVar[list[BindingType]] = [Binding("escape", "cancel", "취소")]

    def __init__(self, run_id: str) -> None:
        """확정할 run ID를 받는다(제목에만 쓴다)."""
        super().__init__()
        self.run_id = run_id

    def compose(self) -> ComposeResult:
        """안내문, 결과 선택(RadioSet), 근거 입력란, 오류 표시줄, 확정·취소 버튼."""
        with Vertical(classes="dialog"):
            yield Static(f"PUBLISH_UNKNOWN 확정  run {self.run_id}", classes="dialog-title")
            yield Static("Hive query history와 target 데이터를 확인한 뒤 고른다.\n"
                         "PUBLISHED를 고르면 PG-60 target 검증이 실행되지 않으므로 target 검증을 직접 한다.")
            with RadioSet(id="resolution"):
                yield RadioButton("FAILED_PUBLISH: target이 바뀌지 않았다", value=True, id="FAILED_PUBLISH")
                yield RadioButton("PUBLISHED: target이 바뀌었고 데이터가 맞다", id="PUBLISHED")
            yield Input(placeholder="확인 근거(5자 이상, 이벤트에 남는다)", id="reason")
            yield Static("", id="resolve-error", classes="error")
            with Horizontal(classes="buttons"):
                yield Button("확정", variant="error", id="ok")
                yield Button("취소", id="cancel")

    def on_mount(self) -> None:
        """근거 입력란에 포커스를 둔다."""
        self.query_one("#reason", Input).focus()

    @on(Button.Pressed, "#ok")
    def ok(self) -> None:
        """근거가 5자 이상이면 (선택한 결과, 근거)로 닫는다. 짧으면 오류만 보이고 창을 유지한다.

        5자 제한은 API(PublishUnknownResolveRequest.reason min_length=5)와 같아, 보내기 전에 걸러 낸다.
        """
        reason = self.query_one("#reason", Input).value.strip()
        if len(reason) < 5:
            self.query_one("#resolve-error", Static).update("확인 근거를 5자 이상 적는다")
            return
        pressed = self.query_one("#resolution", RadioSet).pressed_button
        # RadioButton id가 곧 resolution 값이다. 선택이 없으면 안전한 쪽(FAILED_PUBLISH)으로 본다.
        self.dismiss((pressed.id if pressed and pressed.id else "FAILED_PUBLISH", reason))

    @on(Button.Pressed, "#cancel")
    def action_cancel(self) -> None:
        """취소 버튼·Esc: 아무것도 하지 않고 None으로 닫는다."""
        self.dismiss(None)


def systemd_active() -> bool:
    """서비스가 systemd로 관리되고 있으면 bin 스크립트로 조작하지 않는다.

    load-control-api·load-control-worker 유닛 중 하나라도 active이면 True다. 이때 bin/start.sh로 또 띄우면
    같은 서비스가 두 벌 돌거나 systemd가 곧바로 다시 살려 조작이 어긋나므로 버튼을 막는 데 쓴다.
    systemctl이 없으면(컨테이너, 비 systemd 호스트) False다. 동기 호출이라 화면을 열 때 한 번만 부른다.
    """
    if not shutil.which("systemctl"):
        return False
    units = ["load-control-api.service", "load-control-worker.service"]
    result = subprocess.run(["systemctl", "is-active", *units], capture_output=True, text=True, check=False)
    # 유닛마다 한 줄(active, inactive, failed, unknown ...)이 나온다. 종료 코드는 보지 않는다.
    return "active" in result.stdout.split()


class ServiceScreen(ModalScreen[None]):
    """bin/start.sh, stop.sh, restart.sh로 server·worker를 조작한다.

    대상(server + worker, server, worker)과 동작을 고르면 확인 창을 거쳐 스크립트를 실행하고,
    표준출력·오류를 창 아래 로그에 흘려 보여 준다. systemd로 관리 중이거나 bin 스크립트가 없으면
    버튼을 막는다. 스크립트가 도는 동안에는 창을 닫지 못하게 한다(결과를 놓치지 않게).
    """

    BINDINGS: ClassVar[list[BindingType]] = [Binding("escape", "close", "닫기")]
    ACTIONS: ClassVar[dict[str, str]] = {"start": "시작", "stop": "중지", "restart": "재시작"}

    def __init__(self, bin_dir: Path) -> None:
        """bin_dir은 start.sh 등이 있는 디렉터리($LCA_HOME/bin)다."""
        super().__init__()
        self.bin_dir = bin_dir
        self.running = False  # 스크립트 실행 중이면 True(닫기를 막는다)

    def compose(self) -> ComposeResult:
        """안내문, 대상 선택, 동작 버튼, 실행 출력 로그."""
        with Vertical(classes="dialog wide"):
            yield Static("서비스 관리(이 호스트의 bin 스크립트)", classes="dialog-title")
            yield Static("server를 중지·재시작하면 모니터도 잠시 API에 연결하지 못한다.")
            with RadioSet(id="service"):
                yield RadioButton("server + worker", value=True, id="all")
                yield RadioButton("server(API)", id="server")
                yield RadioButton("worker(dispatcher, sweeper)", id="worker")
            with Horizontal(classes="buttons"):
                for action, label in self.ACTIONS.items():
                    yield Button(label, id=action, variant="warning" if action != "start" else "primary")
                yield Button("닫기", id="close")
            yield RichLog(id="service-output", markup=False, highlight=False, max_lines=500)

    def on_mount(self) -> None:
        """systemd 관리 중이거나 bin/start.sh가 없으면 이유를 보이고 동작 버튼을 막는다."""
        out = self.query_one("#service-output", RichLog)
        if systemd_active():
            out.write(Text("systemd로 관리 중이다. systemctl로 조작한다: "
                           "sudo systemctl restart load-control-api load-control-worker", style="yellow"))
            for action in self.ACTIONS:
                self.query_one(f"#{action}", Button).disabled = True
        elif not (self.bin_dir / "start.sh").exists():
            out.write(Text(f"bin 스크립트가 없다: {self.bin_dir}", style="bold red"))
            for action in self.ACTIONS:
                self.query_one(f"#{action}", Button).disabled = True

    @on(Button.Pressed, "#close")
    def action_close(self) -> None:
        """닫기 버튼·Esc: 스크립트가 끝난 뒤에만 닫는다(실행 중이면 무시한다)."""
        if not self.running:
            self.dismiss(None)

    @on(Button.Pressed, "#start, #stop, #restart")
    def ask(self, event: Button.Pressed) -> None:
        """누른 동작과 선택한 대상을 확인 창으로 묻고, 확인하면 run_script를 시작한다."""
        action = event.button.id or ""  # 버튼 id가 곧 스크립트 이름(start, stop, restart)이다
        pressed = self.query_one("#service", RadioSet).pressed_button
        target = pressed.id if pressed and pressed.id else "all"
        label = {"all": "server + worker", "server": "server", "worker": "worker"}[target]

        def confirmed(yes: bool | None) -> None:
            """확인 창 결과 콜백: 실행을 골랐을 때만 스크립트를 시작한다."""
            if yes:
                self.run_script(action, target)

        self.app.push_screen(ConfirmScreen(f"{label} {self.ACTIONS[action]}",
                                           f"bin/{action}.sh {target} 을 실행한다."), confirmed)

    @work(exclusive=True)
    async def run_script(self, action: str, target: str) -> None:
        """bin/<action>.sh <target>을 실행하고 출력을 줄 단위로 보여 준 뒤 종료 코드를 알린다.

        exclusive=True라 이 화면에서 동시에 두 스크립트가 돌지 않는다(새로 시작하면 이전 worker는 취소된다).
        cwd를 설치 디렉터리(bin의 상위)로 둬 스크립트의 상대 경로가 맞게 한다. stderr는 stdout에 합친다.
        실행 파일을 찾지 못하거나 실행 권한이 없으면(OSError) 종료 코드 -1로 보인다.
        """
        out = self.query_one("#service-output", RichLog)
        self.running = True
        out.write(Text(f"$ bin/{action}.sh {target}", style="bold"))
        try:
            proc = await asyncio.create_subprocess_exec(
                str(self.bin_dir / f"{action}.sh"), target, cwd=str(self.bin_dir.parent),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            assert proc.stdout is not None
            async for raw in proc.stdout:
                out.write(raw.decode(errors="replace").rstrip())
            code = await proc.wait()
        except OSError as e:
            out.write(Text(f"실행 실패: {e}", style="bold red"))
            code = -1
        finally:
            self.running = False
        out.write(Text(f"종료 코드 {code}", style="green" if code == 0 else "bold red"))
        self.app.notify(f"bin/{action}.sh {target}: 종료 코드 {code}",
                        severity="information" if code == 0 else "error")


class Dashboard(Screen[None]):
    """서비스 상태, run 수, 경보, run 목록.

    refresh_seconds마다 /readyz, /v1/monitor/summary, /v1/runs(최근 200건)를 불러 다시 그린다.
    경보 표에서 x를 누르면 경보 종류에 맞는 운영 작업(재전송, 확정)을 연다.
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("enter", "open_run", "run 상세", show=True),
        Binding("l", "logs", "로그"),
        Binding("a", "toggle_active", "진행 중만"),
        Binding("x", "alert_action", "경보 조치"),
        Binding("s", "services", "서비스 관리"),
        Binding("r", "refresh", "새로고침"),
        Binding("q", "app.quit", "종료"),
    ]

    def __init__(self) -> None:
        """표시 상태를 초기화한다."""
        super().__init__()
        self.active_only = False  # True면 run 표에 ACTIVE 상태만 보인다
        # 경보 표의 row key(목록 순번 문자열) → run ID / 경보 원본. 새로고침마다 다시 만든다.
        self.alert_runs: dict[str, str] = {}
        self.alert_data: dict[str, dict[str, Any]] = {}

    def compose(self) -> ComposeResult:
        """위쪽 서비스·건수 패널, 경보 표, run 표."""
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
        """표 열을 만들고 첫 조회를 한 뒤 주기 새로고침을 건다."""
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
        """self.app을 MonitorApp 타입으로 돌려준다(client·설정 접근용, mypy 타입 좁히기)."""
        assert isinstance(self.app, MonitorApp)
        return self.app

    def action_refresh(self) -> None:
        """r 또는 주기 타이머: 조회 worker를 (다시) 시작한다."""
        self.load()

    def action_toggle_active(self) -> None:
        """a: run 표를 진행 중만 / 최근 전체로 바꾼다."""
        self.active_only = not self.active_only
        self.action_refresh()

    def action_logs(self) -> None:
        """l: 필터 없이 로그 화면을 연다."""
        self.app.push_screen(LogScreen())

    def action_services(self) -> None:
        """s: 서비스 관리 창을 열고, 닫히면 바로 새로고침해 바뀐 상태를 보인다."""
        self.app.push_screen(ServiceScreen(self.monitor_app.bin_dir), lambda _: self.action_refresh())

    def action_alert_action(self) -> None:
        """선택한 경보에 맞는 조치: DISPATCH_DEAD는 재전송, PUBLISH_UNKNOWN은 확정.

        RUN_FAILED, RUN_STALE, CLEANUP_FAILED 등은 TUI에서 할 조치가 없어 안내만 한다.
        """
        table = self.query_one("#alerts", DataTable)
        if not table.row_count:
            self.app.notify("경보가 없다")
            return
        key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value or ""
        alert = self.alert_data.get(key)
        if not alert or not alert.get("runId"):
            return
        if alert["kind"] == "DISPATCH_DEAD" and alert.get("dispatchId"):
            self.monitor_app.confirm_resend(alert["runId"], alert["dispatchId"], self.action_refresh)
        elif alert["kind"] == "PUBLISH_UNKNOWN":
            self.monitor_app.ask_resolve(alert["runId"], self.action_refresh)
        else:
            self.app.notify(f"{alert['kind']}는 TUI 조치가 없다. Enter로 상세를 본다", severity="warning")

    def action_open_run(self) -> None:
        """Enter: 포커스된 표(경보 또는 run)의 커서 행에 해당하는 run 상세를 연다."""
        focused = self.focused
        if isinstance(focused, DataTable) and focused.row_count:
            self._open(focused, focused.coordinate_to_cell_key(focused.cursor_coordinate).row_key.value)

    @on(DataTable.RowSelected)
    def row_selected(self, event: DataTable.RowSelected) -> None:
        """표 행 선택(클릭·Enter)으로 run 상세를 연다."""
        self._open(event.data_table, event.row_key.value)

    def _open(self, table: DataTable[Any], key: str | None) -> None:
        """row key를 run ID로 바꿔 RunDetail을 연다.

        run 표는 row key가 곧 run ID이고, 경보 표는 순번이라 alert_runs로 바꾼다(run 없는 경보는 무시).
        """
        run_id = self.alert_runs.get(key or "") if table.id == "alerts" else key
        if run_id:
            self.app.push_screen(RunDetail(run_id))

    @work(exclusive=True)
    async def load(self) -> None:
        """API와 로컬 PID를 조회해 대시보드 전체를 다시 그린다(읽기만 한다).

        /readyz와 PID는 API 오류와 무관하게 먼저 보이고, summary·runs 조회가 실패하면 오류와 마지막 시도
        시각만 보이고 표는 이전 내용을 그대로 둔다(일시 장애에도 직전 상태를 볼 수 있게).
        exclusive=True라 새로고침이 겹치면 이전 조회는 취소된다.
        """
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
        self.alert_data = {}
        alert_rows = []
        # 경보에는 고유 ID가 없어(같은 run이 여러 경보를 가질 수 있다) API가 준 순서의 순번을 row key로 쓴다.
        # 그래서 새로고침 뒤 커서는 "같은 경보"가 아니라 "같은 순번"에 남는다. 조치 전 확인 창에 run·dispatch
        # ID가 보이므로 운영자가 대상을 다시 확인할 수 있다.
        for i, a in enumerate(summary["alerts"]):
            key = str(i)
            self.alert_data[key] = a
            if a.get("runId"):
                self.alert_runs[key] = a["runId"]
            sev = Text(a["severity"], style="bold red" if a["severity"] == "ERROR" else "yellow")
            alert_rows.append((key, [sev, a["kind"], a.get("jobKey") or "-", a.get("businessKey") or "-",
                                     short_id(a.get("runId")), status_text(a.get("status")),
                                     local_time(a.get("at")), (a.get("message") or "")[:80]]))
        self.query_one("#alerts-title", Static).update(
            f"경보 {len(alert_rows)}건 (x: DISPATCH_DEAD 재전송, PUBLISH_UNKNOWN 확정)")
        fill(self.query_one("#alerts", DataTable), alert_rows)

        # "진행 중만"은 서버에 다시 묻지 않고 받은 최근 200건 안에서 거른다
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
        """요약 패널 글자: 상태별 진행 중 run 수, 최근 N시간 종료 run 수, dispatch 적체, 정리 대상 수.

        최근 종료 수는 SUCCESS만 초록, 나머지(실패·타임아웃)는 빨강으로 보인다.
        DEAD dispatch가 있으면 강조한다.
        """
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
    """run 하나의 파티션, 검증 지표, dispatch, 이벤트.

    refresh_seconds마다 run 상세·검증 지표·이벤트를 다시 불러 탭별 표를 채운다.
    이 화면에서 dispatch 재전송(s)과 PUBLISH_UNKNOWN 확정(p)을 할 수 있다.
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("escape", "app.pop_screen", "뒤로"),
        Binding("l", "logs", "이 run의 로그"),
        Binding("s", "resend", "dispatch 재전송"),
        Binding("p", "resolve", "PUBLISH_UNKNOWN 확정"),
        Binding("r", "refresh", "새로고침"),
        Binding("q", "app.quit", "종료"),
    ]

    def __init__(self, run_id: str) -> None:
        """볼 run ID를 받는다. 상세는 load가 채운다."""
        super().__init__()
        self.run_id = run_id
        # 마지막으로 성공한 조회 결과. 재전송·확정 버튼이 상태를 판단하는 데 쓴다(조회 전이면 None).
        self.run: dict[str, Any] | None = None

    def compose(self) -> ComposeResult:
        """run 요약 패널과 파티션·검증 지표·dispatch·이벤트 탭."""
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
        """탭별 표 열을 만들고 첫 조회를 한 뒤 주기 새로고침을 건다."""
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
        """self.app을 MonitorApp 타입으로 돌려준다(client·설정 접근용, mypy 타입 좁히기)."""
        assert isinstance(self.app, MonitorApp)
        return self.app

    def action_refresh(self) -> None:
        """r 또는 주기 타이머: 조회 worker를 (다시) 시작한다."""
        self.load()

    def action_logs(self) -> None:
        """l: run ID로 거른 로그 화면을 연다(run ID가 들어간 줄만 보인다)."""
        self.app.push_screen(LogScreen(initial_filter=self.run_id))

    def action_resend(self) -> None:
        """dispatch 탭에서 고른 행(없으면 첫 DEAD·SENT dispatch)을 재전송한다.

        상태 판단은 마지막 조회 결과(self.run) 기준이다. 그 사이 상태가 바뀌었으면 API가 409로 거부한다.
        """
        if not self.run:
            return
        resendable = {d["dispatchId"]: d for d in self.run["dispatches"] if d["status"] in ("DEAD", "SENT")}
        table = self.query_one("#disp", DataTable)
        chosen = None
        if self.query_one(TabbedContent).active == "tab-disp" and table.row_count:
            chosen = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
        if chosen not in resendable:
            # dispatch 탭이 아니거나 고른 행이 재전송 대상이 아니면 첫 대상으로 대신한다
            # (확인 창에 dispatch ID가 보이므로 운영자가 확인할 수 있다)
            chosen = next(iter(resendable), None)
        if not chosen:
            self.app.notify("재전송할 dispatch가 없다(DEAD 또는 SENT만 가능)", severity="warning")
            return
        self.monitor_app.confirm_resend(self.run_id, chosen, self.action_refresh)

    def action_resolve(self) -> None:
        """p: run이 PUBLISH_UNKNOWN일 때만 확정 창을 연다(마지막 조회 결과 기준)."""
        if not self.run or self.run["status"] != "PUBLISH_UNKNOWN":
            self.app.notify("PUBLISH_UNKNOWN 상태의 run만 확정할 수 있다", severity="warning")
            return
        self.monitor_app.ask_resolve(self.run_id, self.action_refresh)

    @work(exclusive=True)
    async def load(self) -> None:
        """run 상세, 검증 지표, 이벤트(최근 300개)를 불러 요약 패널과 네 표를 다시 그린다(읽기만 한다).

        셋 중 하나라도 실패하면 요약 패널에 오류만 보이고 표와 self.run은 이전 값을 유지한다.
        """
        client = self.monitor_app.client
        try:
            run = await client.run(self.run_id)
            metrics = await client.validations(self.run_id)
            events = await client.events(self.run_id)
        except ApiError as e:
            self.query_one("#run-info", Static).update(Text(str(e), style="bold red"))
            return
        self.run = run
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
        # 이벤트에는 표시용 ID가 없어 순번을 row key로 쓴다(오래된 순이라 새 이벤트는 아래에 붙는다)
        fill(self.query_one("#events", DataTable), [(str(i), [
            local_time(e["eventTime"]), Text(e["level"], style="bold red" if e["level"] == "ERROR"
                                             else "yellow" if e["level"] == "WARN" else ""),
            e["name"], e.get("partitionId") or "", e.get("processGroup") or "", e.get("errorCode") or "",
            (e.get("message") or "")[:100]]) for i, e in enumerate(events)])


class LogScreen(Screen[None]):
    """logs/server.log, worker.log 실시간 보기. 입력란의 글자(run ID, requestId 등)가 들어간 줄만 보인다.

    API가 아니라 이 호스트의 로그 파일을 직접 읽는다(local.LogTail). 처음에는 두 파일의 마지막 줄들을
    시각 순으로 합쳐 보이고, 이후 1초마다 새로 붙은 줄을 이어 붙인다. 필터·보기 대상이 바뀌면 처음부터
    다시 읽는다. 각 줄 앞에 S|(server) 또는 W|(worker)를 붙여 출처를 구분한다.
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("escape", "app.pop_screen", "뒤로"),
        Binding("f2", "toggle_errors", "WARN·ERROR만"),
        Binding("f3", "cycle_source", "server/worker"),
        Binding("ctrl+q", "app.quit", "종료"),
    ]
    # F3로 순환하는 보기 대상: 둘 다 → server만 → worker만
    SOURCES: ClassVar[list[tuple[str, ...]]] = [("server", "worker"), ("server",), ("worker",)]
    INITIAL_LINES = 300  # 화면을 열거나 다시 읽을 때 보여 줄 최대 줄 수

    def __init__(self, initial_filter: str = "") -> None:
        """initial_filter는 필터 입력란의 초기값이다(run 상세에서 열면 run ID)."""
        super().__init__()
        self.filter_text = initial_filter
        self.errors_only = False
        self.source_index = 0  # SOURCES의 위치
        self.tails: dict[str, LogTail] = {}  # 보기 대상 중 읽을 수 있는 파일만

    def compose(self) -> ComposeResult:
        """필터 입력란, 상태줄, 로그 영역."""
        yield Header(show_clock=True)
        yield Input(value=self.filter_text, id="filter",
                    placeholder="필터: run ID, requestId, 단어(대소문자 무시)")
        yield Static(id="log-status", classes="title")
        yield RichLog(id="log", wrap=False, markup=False, highlight=False, max_lines=5000)
        yield Footer()

    @property
    def monitor_app(self) -> "MonitorApp":
        """self.app을 MonitorApp 타입으로 돌려준다(client·설정 접근용, mypy 타입 좁히기)."""
        assert isinstance(self.app, MonitorApp)
        return self.app

    def on_mount(self) -> None:
        """처음 읽기를 하고 1초 주기로 새 줄을 읽게 한다."""
        self.app.sub_title = "로그"
        self.reload()
        self.set_interval(1.0, self.poll)

    def action_toggle_errors(self) -> None:
        """F2: WARN·ERROR 줄만 보기를 켜고 끈다."""
        self.errors_only = not self.errors_only
        self.reload()

    def action_cycle_source(self) -> None:
        """F3: 보기 대상(server+worker, server, worker)을 바꾼다."""
        self.source_index = (self.source_index + 1) % len(self.SOURCES)
        self.reload()

    @on(Input.Changed, "#filter")
    def filter_changed(self, event: Input.Changed) -> None:
        """필터 글자가 바뀔 때마다 그 조건으로 다시 읽는다."""
        self.filter_text = event.value.strip()
        self.reload()

    def _matches(self, line: str) -> bool:
        """줄이 현재 조건(WARN·ERROR만, 필터 글자 포함·대소문자 무시)에 맞으면 True.

        수준은 텍스트 로그 형식("시각 수준 [logger] ...")의 " ERROR "·" WARN " 토막으로 판단한다.
        logging.format이 json이면 이 판단이 맞지 않는다.
        """
        if self.errors_only and " ERROR " not in line and " WARN " not in line:
            return False
        return not self.filter_text or self.filter_text.lower() in line.lower()

    def _write(self, source: str, line: str) -> None:
        """출처 표시(S| 또는 W|)를 붙이고 수준에 따라 색을 입혀 로그 영역에 한 줄 쓴다."""
        style = "bold red" if " ERROR " in line else "yellow" if " WARN " in line else ""
        text = Text(f"{source[0].upper()}| ", style="dim")
        text.append(line, style=style)
        self.query_one("#log", RichLog).write(text)

    def reload(self) -> None:
        """로그 영역을 비우고 현재 조건으로 처음부터 다시 읽는다.

        보기 대상마다 LogTail을 새로 만들어 이후 poll이 그 끝부터 이어 읽게 한다.
        필터에 맞는 줄이 드물어도 INITIAL_LINES만큼 보이도록 20배를 읽어 거른 뒤,
        두 파일을 시각 순으로 합쳐 마지막 INITIAL_LINES줄만 쓴다.
        읽을 수 없는 파일은 상태줄에 경로를 보인다.
        """
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
            # 정렬 키: 줄 앞 23자 = 타임스탬프 "YYYY-MM-DD HH:MM:SS.mmm"(logging.add_timestamp).
            # 타임스탬프가 없는 줄(예외 traceback의 이어지는 줄)은 원래 위치에서 벗어나 정렬될 수 있다.
            merged.extend((x[:23], source, x) for x in lines[-self.INITIAL_LINES:])
        for _, source, line in sorted(merged)[-self.INITIAL_LINES:]:
            self._write(source, line)

    def poll(self) -> None:
        """1초마다: 각 파일에 새로 붙은 줄 중 조건에 맞는 것만 이어 쓴다.

        파일별로 차례대로 쓰므로 같은 1초 안의 server·worker 줄은 시각 순으로 섞이지 않는다.
        """
        for source, tail in self.tails.items():
            for line in tail.read_new():
                if line and self._matches(line):
                    self._write(source, line)


class MonitorApp(App[None]):
    """Load Control 모니터.

    화면들이 함께 쓰는 MonitorClient·설정(refresh_seconds, log_dir, bin_dir)을 들고 있고,
    여러 화면에서 부르는 운영 작업 흐름(확인 창 → API 호출 → 알림 → 새로고침)을 제공한다.
    """

    TITLE = "Load Control 모니터"
    CSS = """
    #top { height: auto; max-height: 8; }
    #services, #counts { width: 1fr; height: auto; border: round $primary; padding: 0 1; }
    .title { background: $boost; padding: 0 1; text-style: bold; }
    #alerts { height: auto; max-height: 10; }
    #runs { height: 1fr; }
    #run-info { height: auto; border: round $primary; padding: 0 1; }
    #log { height: 1fr; }
    ModalScreen { align: center middle; }
    .dialog { width: 80; height: auto; border: thick $error; background: $surface; padding: 1 2; }
    .dialog.wide { width: 110; }
    .dialog-title { text-style: bold; margin-bottom: 1; }
    .buttons { height: auto; margin-top: 1; }
    .buttons Button { margin-right: 2; }
    .error { color: $error; }
    #service-output { height: 12; border: round $primary; }
    """

    def __init__(self, client: MonitorClient, *, refresh_seconds: float, log_dir: Path,
                 bin_dir: Path | None = None) -> None:
        """client와 새로고침 주기(초), 로그·PID 디렉터리, bin 스크립트 디렉터리(기본 ./bin)를 받는다."""
        super().__init__()
        self.client = client
        self.refresh_seconds = refresh_seconds
        self.log_dir = log_dir
        self.bin_dir = bin_dir or Path("bin")

    def confirm_resend(self, run_id: str, dispatch_id: str, done: Callable[[], None]) -> None:
        """DEAD·SENT dispatch 재전송. 확인 후 operator API를 부른다.

        확인하면 resend_dispatch를 별도 worker로 부르고(exclusive=False: 화면 조회 worker를 취소하지 않게),
        결과나 오류를 알림으로 보인 뒤 성공·실패와 관계없이 done()으로 호출한 화면을 새로고침한다.
        API는 DEAD·SENT일 때만 PENDING으로 되돌리므로, 두 번 눌러도 두 번째는 409로 거부된다.
        """
        async def call() -> None:
            """재전송 API를 부르고 결과·오류를 알린 뒤 화면을 새로고침한다(예외를 밖으로 내지 않는다)."""
            try:
                r = await self.client.resend_dispatch(run_id, dispatch_id)
                self.notify(f"재전송 예약: dispatch {short_id(dispatch_id)} → {r['status']}")
            except ApiError as e:
                self.notify(str(e), severity="error", timeout=10)
            done()

        def confirmed(yes: bool | None) -> None:
            """확인 창 결과 콜백: 재전송을 골랐을 때만 API 호출 worker를 시작한다."""
            if yes:
                self.run_worker(call(), exclusive=False)

        self.push_screen(ConfirmScreen(
            "dispatch 재전송", f"run {run_id}\ndispatch {dispatch_id}\n\n"
            "시도 횟수를 0으로 되돌리고 NiFi(PG-05)로 다시 보낸다.\n"
            "먼저 PG-05 수신(포트, 등록된 Job)을 확인한다.",
            "재전송"), confirmed)

    def ask_resolve(self, run_id: str, done: Callable[[], None]) -> None:
        """PUBLISH_UNKNOWN 확정. 결과·근거 입력 후 한 번 더 확인한다.

        ResolveScreen(결과·근거) → ConfirmScreen(최종 확인) → resolve_publish_unknown 순서다.
        되돌릴 수 없는 상태 전이라 두 번 묻는다(PUBLISHED면 PG-60 target 검증 없이 진행하고,
        FAILED_PUBLISH면 run이 실패로 끝난다).
        API 호출 뒤에는 결과와 관계없이 done()으로 호출한 화면을 새로고침한다.
        """
        async def call(resolution: str, reason: str) -> None:
            """확정 API를 부르고 결과·오류를 알린 뒤 화면을 새로고침한다(예외를 밖으로 내지 않는다)."""
            try:
                r = await self.client.resolve_publish_unknown(run_id, resolution, reason)
                self.notify(f"확정: run {short_id(run_id)} → {r['runStatus']}")
            except ApiError as e:
                self.notify(str(e), severity="error", timeout=10)
            done()

        def entered(result: tuple[str, str] | None) -> None:
            """ResolveScreen 결과 콜백: 입력이 있으면 최종 확인 창을 띄운다(취소면 끝낸다)."""
            if not result:
                return
            resolution, reason = result

            def confirmed(yes: bool | None) -> None:
                """최종 확인 콜백: 확정을 골랐을 때만 API 호출 worker를 시작한다."""
                if yes:
                    self.run_worker(call(resolution, reason), exclusive=False)

            self.push_screen(ConfirmScreen(f"{resolution}(으)로 확정", f"run {run_id}\n근거: {reason}",
                                           "확정"), confirmed)

        self.push_screen(ResolveScreen(run_id), entered)

    def on_mount(self) -> None:
        """시작 화면으로 대시보드를 띄운다."""
        self.push_screen(Dashboard())

    async def on_unmount(self) -> None:
        """종료할 때 API 연결 풀을 닫는다."""
        await self.client.close()
