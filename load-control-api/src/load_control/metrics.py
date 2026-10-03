"""Prometheus 메트릭.

server.workers가 1보다 크면 PROMETHEUS_MULTIPROC_DIR를 설정해야 프로세스 합계가 맞는다.
API server와 worker는 같은 정의를 쓰지만 프로세스가 다르므로 값은 각자 따로 모인다.
요청·chunk 메트릭은 API의 /metrics, dispatch·run·sweeper 메트릭은 worker의 /metrics
(worker.metrics_port)에서 본다.
"""

from prometheus_client import Counter, Histogram

# API: 요청 수. endpoint는 경로 템플릿(예: /v1/runs/{run_id}), 매칭 실패는 "unmatched".
# status는 HTTP 상태 코드
REQUESTS = Counter("lca_requests_total", "HTTP 요청 수", ["endpoint", "status"])
# API: 요청 처리 시간(초). 미들웨어가 응답 본문 재구성까지 포함해 잰다
REQUEST_SECONDS = Histogram("lca_request_seconds", "HTTP 요청 처리 시간", ["endpoint"])
# API: chunk 보고 판정 결과. result: progress(아직 모이는 중), ignored(run이 끝난 뒤 도착),
# partition_success, run_complete(run까지 완료), failed(건수 불일치)
CHUNK_REPORTS = Counter("lca_chunk_reports_total", "chunk 보고 판정 결과", ["result"])
# API: 파티션 판정 때 load_run 행 FOR UPDATE 대기 시간(초). 같은 run의 보고가 몰리면 늘어난다
RUN_LOCK_WAIT = Histogram("lca_run_lock_wait_seconds", "load_run 행 잠금 대기 시간",
                          buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))

from prometheus_client import Gauge  # noqa: E402

# worker: NiFi 호출 결과. type: VALIDATE_RUN, REISSUE_PARTITION. result: sent, retry, dead
DISPATCH = Counter("lca_dispatch_total", "NiFi 호출 전달 결과", ["type", "result"])
# worker: 미완료 dispatch 수(PENDING, SENT, DEAD). sweeper 주기마다 갱신
DISPATCH_BACKLOG = Gauge("lca_dispatch_backlog", "상태별 dispatch 수", ["status"])
# worker: 상태별 활성 run 수. sweeper 주기마다 갱신
ACTIVE_RUNS = Gauge("lca_active_runs", "상태별 활성 run 수", ["status"])
# worker: sweeper 규칙별 처리 건수(rule: timeout_stale_partition, reissue_partition, requeue_dispatch 등)
SWEEPER_ACTIONS = Counter("lca_sweeper_actions_total", "sweeper 처리 건수", ["rule"])
