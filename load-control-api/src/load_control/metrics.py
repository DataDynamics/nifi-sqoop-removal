"""Prometheus 메트릭(API 설계 9.11). Gunicorn 멀티 프로세스에서는 PROMETHEUS_MULTIPROC_DIR를 설정한다."""

from prometheus_client import Counter, Histogram

REQUESTS = Counter("lca_requests_total", "HTTP 요청 수", ["endpoint", "status"])
REQUEST_SECONDS = Histogram("lca_request_seconds", "HTTP 요청 처리 시간", ["endpoint"])
CHUNK_REPORTS = Counter("lca_chunk_reports_total", "chunk 보고 판정 결과", ["result"])
RUN_LOCK_WAIT = Histogram("lca_run_lock_wait_seconds", "load_run 행 잠금 대기 시간",
                          buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
