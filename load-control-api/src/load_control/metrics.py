"""Prometheus 메트릭.

server.workers가 1보다 크면 PROMETHEUS_MULTIPROC_DIR를 설정해야 프로세스 합계가 맞는다.
"""

from prometheus_client import Counter, Histogram

REQUESTS = Counter("lca_requests_total", "HTTP 요청 수", ["endpoint", "status"])
REQUEST_SECONDS = Histogram("lca_request_seconds", "HTTP 요청 처리 시간", ["endpoint"])
CHUNK_REPORTS = Counter("lca_chunk_reports_total", "chunk 보고 판정 결과", ["result"])
RUN_LOCK_WAIT = Histogram("lca_run_lock_wait_seconds", "load_run 행 잠금 대기 시간",
                          buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))

from prometheus_client import Gauge  # noqa: E402

DISPATCH = Counter("lca_dispatch_total", "NiFi 호출 전달 결과", ["type", "result"])
DISPATCH_BACKLOG = Gauge("lca_dispatch_backlog", "상태별 dispatch 수", ["status"])
ACTIVE_RUNS = Gauge("lca_active_runs", "상태별 활성 run 수", ["status"])
SWEEPER_ACTIONS = Counter("lca_sweeper_actions_total", "sweeper 처리 건수", ["rule"])
