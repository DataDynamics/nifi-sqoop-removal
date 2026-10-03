"""TUI 모니터: python -m load_control.monitor (bin/monitor.sh).

Load Control API의 조회 엔드포인트(/v1/monitor/summary, /v1/runs ...)를 주기적으로 불러 run·dispatch·경보를
보여 주고, operator 토큰으로 운영 작업(DEAD dispatch 재전송, PUBLISH_UNKNOWN 확정)을 부른다.
DB에 직접 붙지 않고 모든 상태 변경은 API를 거치므로, API의 CAS·이벤트 기록 규칙이 그대로 적용된다.

- app: textual 화면(대시보드, run 상세, 로그, 확인 창, 서비스 관리)
- client: httpx 기반 API 클라이언트
- local: API 서버 호스트의 PID 파일·로그 파일 읽기
"""
