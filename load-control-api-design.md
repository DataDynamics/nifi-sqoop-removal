# Load Control API 기반 완료 판정 설계

> 기준: [통합 설계 및 NiFi Flow 구현 명세](./nifi-sqoop-removal-guide.md)(이하 "가이드")의 PostgreSQL 원장과 상태 모델을 유지한다. 이 문서는 가이드 초안의 완료 판정 방식(PG-30 Wait/Notify)을 Load Control API로 대체한 설계를 정의한다. 가이드는 이 설계를 반영해 개정되었다.
>
> 상태: 채택 (2026-10-01). 구현은 Python FastAPI로 한다(9장). 1~8장은 구현과 무관한 계약(API, DB, 트랜잭션 규칙, NiFi 연동)이고 9장이 FastAPI 구현 설계다. NiFi Processor 단위 설정은 가이드에 있다.

## 1. 결정 요약

### 1.1 결정

NiFi는 데이터 처리(조회, 변환, HDFS 기록)만 담당하고, 상태 기록과 완료 판정, 다음 단계 호출은 Load Control API(이하 "API")가 담당한다.

```text
NiFi (Data plane)   : 파티션 조회 → Parquet → PutHDFS → API에 chunk 보고
API  (Control plane): 보고 기록 → 파티션 완료 판정 → run 완료 판정 → 검증 flow 호출(run당 1회)
NiFi (검증 flow)     : API 호출을 받아 Hive staging 검증 → 게시 → Target 검증, 결과를 API에 보고
```

API는 매 chunk 보고마다 해당 `run_id`의 모든 파티션이 완료됐는지 판정한다. 완료를 확정한 단 하나의 호출만 검증 flow 호출을 예약한다.

API는 Python FastAPI로 구현하고, HTTP 프로세스와 백그라운드 worker(dispatcher, sweeper) 프로세스로 나눠 배포한다(9.2).

이 문서의 `run_id`는 대화와 운영 용어의 "load id"와 같다.

### 1.2 배경

가이드 초안의 PG-30은 Processor 15개 정도, Wait 2개, Notify 3종, `MapCacheServer`/`MapCacheClientService`, 폴링 루프, 실패 시 Wait 해제 경로로 구성된다. 가이드 스스로 Wait/Notify는 wake-up 수단이고 최종 판정은 PostgreSQL 원장으로 한다고 정했으므로, 신호 전달용 장치를 한 벌 더 운영하는 셈이다. NiFi 2.4.0 PoC에서 발견된 결함도 대부분 이 영역에서 나왔다(PutSQL fragment livelock, Wait에 남는 control FlowFile, PutSQL CAS 결과 미확인, EL 안의 Parameter 미치환 — [poc/REVIEW.md](./poc/REVIEW.md) 2장).

채택 조건은 다음 두 가지이며 모두 충족한다.

- API를 소유하고 운영할 팀이 있다.
- 대상 Job들이 같은 구조라서 API를 공통 기반으로 재사용할 수 있다.

### 1.3 장점과 대가

| 구분 | 내용 |
|---|---|
| 장점 | PG-30 전체와 DMC Controller Service 제거, PG-20 끝을 `InvokeHTTP` 하나로 단순화 |
| 장점 | 완료 판정, CAS, 실패 처리가 한 트랜잭션 안의 코드가 되어 단위·동시성 테스트 가능 |
| 장점 | 원장에 쓰는 주체가 API 하나로 모여 상태 전이 규칙이 한곳에 있음 |
| 장점 | 실행기(NiFi 외 Spark 등)와 무관하게 재사용 가능, 운영 화면·수동 재처리 기반 |
| 대가 | API 서비스의 배포, 이중화, 모니터링, 인증 운영. API 장애 시 완료 판정 중단 |
| 대가 | NiFi→API, API→NiFi 두 통신 경로의 실패를 "최소 1회 전달 + 멱등"으로 설계해야 함 |
| 대가 | 하나의 run을 추적하려면 NiFi와 API 로그를 `run_id`로 함께 봐야 함 |

## 2. 구조

### 2.1 구성

```mermaid
flowchart LR
    T[PG-00 Trigger] --> C[PG-10 Run Coordinator]
    C -->|partition FlowFiles| W[PG-20 Extract Workers]
    W -->|PutHDFS| H[(HDFS run 경로)]

    C -- "POST /runs, /manifest" --> API[Load Control API]
    W -- "claim, chunks, fail" --> API
    API --- DB[(PostgreSQL nifi_ops<br/>원장 + outbox)]
    API -- "POST /validate/{jobKey} (outbox dispatch)" --> RC[PG-05 Control Receiver]
    RC --> V[PG-40 Staging Validation]
    V --> P[PG-50 Publish] --> TV[PG-60 Target Validation]
    V -- "start, results" --> API
    P -- "publish claim, result" --> API
    TV -- "results, success" --> API
    API -. "POST /reissue/{jobKey} (선택)" .-> RC
    RC -.-> W
```

### 2.2 책임 분리

| 영역 | 담당 | 하지 않는 일 |
|---|---|---|
| PG-10 Coordinator | Oracle SCN·source metric·manifest 계산, API에 등록 | `nifi_ops` 직접 쓰기 |
| PG-20 Worker | claim 요청, Oracle 조회, Parquet, PutHDFS, chunk·실패 보고 | 완료 판정, 다른 파티션 대기 |
| API | 원장 쓰기, 불변식 검증, 파티션·run 완료 판정, CAS, outbox, sweeper | 원천 조회, HDFS·Hive 접근 |
| 검증 flow(PG-40~60) | Hive DDL·DQ 조회, `INSERT OVERWRITE`, 결과 보고 | 상태 직접 갱신 |
| PG-90 | 관측 이벤트(`load_event`) 기록과 알림 | 업무 상태 변경 |

`nifi_ops`의 업무 테이블(`load_run`, `load_partition`, `load_file`, `load_validation`, `load_dispatch`)에 쓰는 주체는 API 하나다. NiFi 계정은 `load_event` INSERT 권한만 유지한다(6장).

### 2.3 처리 순서

```mermaid
sequenceDiagram
    participant C as NiFi PG-10
    participant W as NiFi PG-20 (N개 병렬)
    participant A as Load Control API
    participant D as PostgreSQL
    participant V as NiFi 검증 flow

    C->>A: POST /runs (jobKey, businessKey)
    A->>D: INSERT load_run (active unique lock)
    A-->>C: runId
    C->>A: POST /runs/{id}/manifest (SCN, metrics, partitions)
    A->>D: 불변식 검증, partition 일괄 INSERT, EXTRACTING
    A-->>C: dispatch 대상 partition 목록
    loop 파티션마다
        W->>A: POST .../claim
        W->>W: Oracle 조회 → Parquet → PutHDFS
        W->>A: POST .../chunks (chunk마다)
        A->>D: run FOR UPDATE, file UPSERT, 파티션 판정, run 판정
        Note over A,D: run 완료를 확정한 호출만<br/>같은 트랜잭션에서 outbox INSERT
    end
    A->>V: POST /validate/{jobKey} (dispatcher, 커밋 후, PG-05 경유)
    V-->>A: 202 Accepted
    V->>A: POST /runs/{id}/validation/start
    A->>D: EXTRACTED_VALIDATED → STAGE_VALIDATING CAS, dispatch ACKED
    V->>V: Hive staging 검증 → 게시 → Target 검증
    V->>A: 결과 보고와 상태 전이
```

## 3. 완료 판정 규칙

### 3.1 보고 단위는 chunk

한 파티션은 `Max Rows Per Flow File` 때문에 여러 파일로 나뉜다. 파티션 단위로 한 번만 보고하려면 NiFi에서 chunk를 다시 모아야 하고, 그러면 Wait/Notify가 다시 필요하다. 따라서 NiFi는 PutHDFS가 성공한 파일마다 보고하고, 파티션 완료는 API가 판정한다.

추출 `ExecuteSQLRecord`의 `Output Batch Size=0`(가이드 8.4)을 유지하면 모든 chunk FlowFile에 `fragment.count`가 붙어 있으므로, 각 보고에 `chunkCount`를 함께 보낼 수 있다. 이 때문에 가이드 초안에 있던 첫 fragment 복제(`DuplicateFlowFile`)와 partition-control FlowFile도 필요 없다.

### 3.2 판정 트랜잭션

chunk 보고 한 건은 하나의 트랜잭션으로 처리한다.

```text
BEGIN
 1. SELECT ... FROM load_run WHERE run_id = :runId FOR UPDATE
    - run이 없으면 404
    - status <> EXTRACTING 이면 file만 기록(정리용)하고 현재 상태를 반환, 판정하지 않음
 2. 파티션 소유권 확인
    - partition.claim_token = :claimToken 이어야 함 (아니면 409 CLAIM_MISMATCH)
    - partition.status = RUNNING, 또는 SUCCESS이면서 같은 chunk의 동일 재보고(멱등)
 3. load_file UPSERT (run_id, partition_id, chunk_index)
    - 이미 있고 hdfsPath/recordCount가 다르면서 파티션이 SUCCESS이면 409 CHUNK_CONFLICT
    - partition.heartbeat_at, run.heartbeat_at 갱신
 4. 파티션 판정 (status = RUNNING일 때만)
    files = COUNT(*), lo = MIN(chunk_index), hi = MAX(chunk_index),
    rows = SUM(record_count), counts = COUNT(DISTINCT fragment_count)
    - files = chunkCount AND lo = 0 AND hi = chunkCount-1 AND counts = 1
      AND rows = expected_row_count            → partition SUCCESS (actual/file/byte 저장)
    - files = chunkCount AND (rows <> expected OR counts <> 1)
                                               → partition FAILED(ROW_COUNT_MISMATCH),
                                                 run FAILED_EXTRACT
    - 그 외                                      → 진행 중
 5. run 판정 (4에서 partition이 SUCCESS가 된 경우만)
    UPDATE load_run SET status = 'EXTRACTED_VALIDATED', ...
     WHERE run_id = :runId AND status = 'EXTRACTING'
       AND 파티션 수 = expected_partition_count
       AND SUCCESS 파티션 수 = expected_partition_count
       AND SUM(actual_row_count) = source_count
    - 갱신 1건이면 같은 트랜잭션에서 load_dispatch(VALIDATE_RUN, PENDING) INSERT와
      pg_notify('load_dispatch', run_id) 실행
COMMIT
 6. NOTIFY는 commit될 때만 전달되므로 worker 프로세스의 dispatcher가 커밋 직후 깨어난다.
    알림을 놓쳐도 dispatcher 폴링이 처리한다(9.8).
```

run 행 잠금이 필수다. 잠그지 않으면 마지막 두 파티션이 동시에 끝났을 때 READ COMMITTED에서 서로의 커밋을 보지 못해, 둘 다 "아직 남았다"고 판단하고 아무도 run을 완료하지 못한다. 잠금은 트랜잭션 하나(수 ms) 동안만 유지되므로 파티션 수십 개, chunk 수천 개 수준에서는 문제가 없다.

### 3.3 판정 SQL 예시

```sql
-- 4. 파티션 집계
SELECT COUNT(*)                        AS files,
       MIN(chunk_index)                AS lo,
       MAX(chunk_index)                AS hi,
       COALESCE(SUM(record_count), 0)  AS rows,
       COALESCE(SUM(byte_count), 0)    AS bytes,
       COUNT(DISTINCT fragment_count)  AS counts
  FROM nifi_ops.load_file
 WHERE run_id = CAST(:run_id AS uuid) AND partition_id = :partition_id;

-- 5. run 완료 CAS
UPDATE nifi_ops.load_run r
   SET status = 'EXTRACTED_VALIDATED',
       success_partition_count = s.success_cnt,
       extracted_count = s.row_sum,
       extract_completed_at = clock_timestamp(),
       heartbeat_at = clock_timestamp(),
       version_no = r.version_no + 1
  FROM (SELECT COUNT(*)                                  AS total_cnt,
               COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_cnt,
               COALESCE(SUM(actual_row_count), 0)        AS row_sum
          FROM nifi_ops.load_partition
         WHERE run_id = CAST(:run_id AS uuid)) s
 WHERE r.run_id = CAST(:run_id AS uuid)
   AND r.status = 'EXTRACTING'
   AND s.total_cnt = r.expected_partition_count
   AND s.success_cnt = r.expected_partition_count
   AND s.row_sum = r.source_count
RETURNING r.run_id;
```

### 3.4 0건 파티션과 manifest

`POST /manifest`는 한 트랜잭션에서 다음을 처리한다.

1. 불변식 검증: `SUM(expectedRowCount) = sourceCount`, 파티션 수 = `plannedPartitionCount`, 경계 연속성(`lower(i+1) = upper(i)`, 마지막만 `upperInclusive=true`), NULL 파티션 정책.
2. 불일치면 run을 `FAILED_MANIFEST`로 바꾸고 422를 반환한다.
3. 모든 파티션을 일괄 INSERT하고 `expectedRowCount=0`인 파티션은 바로 `SUCCESS`(actual=0)로 만든다.
4. run을 `EXTRACTING`으로 바꾼 뒤 3.2의 5단계 run 판정을 한 번 실행한다. `ALLOW.EMPTY.SOURCE=true`이고 모든 파티션이 0건이면 이 시점에 바로 완료되고 dispatch가 예약된다.
5. 응답으로 Worker에 보낼 파티션(`expectedRowCount>0`) 목록을 반환한다.

manifest 일괄 등록은 가이드 초안의 파티션 행별 `PutSQL`(Fragmented=true)과 그로 인한 livelock 위험(REVIEW 2장 #2)을 구조적으로 없앤다.

### 3.5 실패 전파

- Worker는 일시 오류를 NiFi `RetryFlowFile`로 제한 재시도한 뒤, 최종 실패일 때만 `POST .../fail`을 호출한다.
- API는 같은 트랜잭션에서 partition `FAILED`, run `FAILED_EXTRACT`(스냅샷 오류는 `FAILED_SNAPSHOT_EXPIRED`)로 바꾼다. outbox에는 아무것도 넣지 않는다.
- 이후 다른 파티션의 chunk 보고는 file만 기록하고 `runStatus=FAILED_EXTRACT`를 반환한다. NiFi는 이 응답에 따라 분기하지 않고 data FlowFile을 끝낸다.
- 아직 실행 중인 다른 Worker의 Oracle 쿼리는 중단하지 않는다. 결과 파일은 실패 run의 격리 경로에만 남는다(가이드 1장 실패 원칙). claim 요청도 run이 `EXTRACTING`이 아니면 `claimed=false`를 반환하므로 대기 중인 파티션은 조회를 시작하지 않는다.

## 4. 검증 flow 호출 (outbox)

### 4.1 트랜잭션 안에서 NiFi를 호출하지 않는다

트랜잭션 안에서 NiFi를 호출하면 "호출 성공 + 커밋 실패"(존재하지 않는 완료 상태로 검증 실행)나 "커밋 성공 + 호출 실패"(run 정지)가 생긴다. 따라서 run 완료 CAS와 같은 트랜잭션에서 `load_dispatch` 행만 만들고, 커밋 후 별도 dispatcher가 전달한다.

### 4.2 dispatch 상태

```mermaid
stateDiagram-v2
    [*] --> PENDING: run 완료 CAS와 같은 트랜잭션
    PENDING --> SENT: NiFi가 2xx 응답
    PENDING --> PENDING: 실패, backoff 후 재시도
    SENT --> ACKED: 검증 flow가 /validation/start 호출
    SENT --> PENDING: ACK 대기 시간 초과(재전송)
    PENDING --> DEAD: 최대 시도 초과, 운영 알림
```

| 상태 | 의미 |
|---|---|
| `PENDING` | 전송 대기. `next_attempt_at`이 지나면 dispatcher가 가져간다 |
| `SENT` | NiFi `HandleHttpRequest`가 2xx로 응답함. FlowFile은 NiFi에 들어갔지만 처리 시작은 미확인 |
| `ACKED` | 검증 flow가 `/validation/start`로 실제 시작을 알림. 전달 완료 |
| `DEAD` | `dispatch.max_attempts` 초과. ERROR 알림 후 운영자가 재전송 API로 처리 |

`SENT`와 `ACKED`를 나누는 이유: NiFi가 202를 응답한 직후 노드가 죽으면 FlowFile이 유실될 수 있다. `dispatch.ack_timeout` 안에 `ACKED`가 되지 않으면 다시 `PENDING`으로 돌려 재전송한다.

### 4.3 dispatcher 규칙

dispatcher는 HTTP 전송 동안 DB 트랜잭션을 열어 두지 않도록 lease 방식으로 행을 가져간다. 짧은 트랜잭션에서 `next_attempt_at`을 lease 만료 시각으로 미루며 행을 선점하고, commit한 뒤 전송한다. 전송 중 worker가 죽으면 lease가 끝난 뒤 다른 worker가 다시 가져간다.

```sql
-- 여러 worker가 같은 행을 동시에 가져가지 않도록 SKIP LOCKED로 선점한다.
UPDATE nifi_ops.load_dispatch d
   SET attempt_count   = d.attempt_count + 1,
       next_attempt_at = clock_timestamp() + CAST(:lease AS interval)
  FROM nifi_ops.load_run r
 WHERE r.run_id = d.run_id
   AND d.dispatch_id IN (
        SELECT dispatch_id
          FROM nifi_ops.load_dispatch
         WHERE status = 'PENDING' AND next_attempt_at <= clock_timestamp()
         ORDER BY next_attempt_at
         LIMIT :batch
         FOR UPDATE SKIP LOCKED)
RETURNING d.dispatch_id, d.run_id, d.dispatch_type, d.partition_id, d.attempt_count, r.job_key;
```

- 전송 성공(2xx): `WHERE status = 'PENDING'` 조건으로 `SENT`, `sent_at`을 기록한다. 이미 `ACKED`이면 바꾸지 않는다(9.8).
- 실패(연결 오류, 5xx, timeout): `next_attempt_at = now + min(base × 2^attempt, max)`. `attempt_count`가 `dispatch.max_attempts`에 도달하면 `DEAD`.
- 4xx: 설정 오류로 보고 바로 `DEAD` 처리하고 알림.
- 전송 요청 헤더에 `X-Run-Id`, `X-Dispatch-Id`를 넣어 NiFi 로그와 상관 분석이 가능하게 한다.

### 4.4 수신 측 중복 제거

outbox는 최소 1회 전달만 보장하므로, 같은 run에 대한 검증 요청이 두 번 이상 도착할 수 있다. 검증 flow의 첫 단계는 반드시 `POST /runs/{id}/validation/start`이며, API는 `EXTRACTED_VALIDATED → STAGE_VALIDATING` CAS에 성공한 호출에만 `started=true`를 반환한다. `started=false`인 FlowFile은 DEBUG 로그만 남기고 종료한다.

## 5. API 명세

### 5.1 공통 규칙

- Base path: `/v1`. 요청·응답은 JSON, UTF-8.
- 인증: mTLS와 서비스 토큰(`Authorization: Bearer`). NiFi→API, API→NiFi 양방향에 적용한다(9.7, 10.2).
- 모든 요청과 응답에 `X-Request-Id`를 남기고, API 로그에는 `runId`, `partitionId`, `chunkIndex`를 구조화 필드로 기록한다.
- 식별자(`runId`, `claimToken` 등)는 UUID 형식, `partitionId`는 `^[0-9]{4}$|^NULL$`로 검증한다. 테이블명·컬럼명은 요청에서 받지 않는다.
- 모든 상태 변경 호출은 멱등이다. 같은 요청을 다시 보내면 같은 결과를 돌려준다.

### 5.2 엔드포인트

| Method | Path | 호출자 | 상태 전이 / 동작 |
|---|---|---|---|
| POST | `/v1/runs` | PG-10 | run 생성 `CREATED`. 활성 run이 있으면 409 `DUPLICATE_ACTIVE_RUN` |
| POST | `/v1/runs/{runId}/manifest` | PG-10 | SCN·metric 저장, 불변식 검증, 파티션 일괄 등록, `EXTRACTING` (3.4) |
| POST | `/v1/runs/{runId}/fail` | PG-10, 검증 flow | 비파티션 단계 실패 기록. 기대 상태(`expectedStatus`)와 실패 상태를 함께 받음 |
| POST | `/v1/runs/{runId}/partitions/{pid}/claim` | PG-20 | `PENDING/RETRY → RUNNING`. 같은 token 재요청은 `claimed=true` |
| POST | `/v1/runs/{runId}/partitions/{pid}/chunks` | PG-20 | file 기록, 파티션·run 판정 (3.2) |
| POST | `/v1/runs/{runId}/partitions/{pid}/fail` | PG-20 | partition `FAILED`, run `FAILED_EXTRACT`/`FAILED_SNAPSHOT_EXPIRED` |
| POST | `/v1/runs/{runId}/validation/start` | 검증 flow | `EXTRACTED_VALIDATED → STAGE_VALIDATING`, dispatch `ACKED` |
| POST | `/v1/runs/{runId}/validations` | 검증 flow | `load_validation` UPSERT(stage별 지표 묶음) |
| POST | `/v1/runs/{runId}/stage-validated` | 검증 flow | 저장된 STAGING 지표가 모두 PASS일 때만 `STAGE_VALIDATING → STAGING_VALIDATED` |
| POST | `/v1/runs/{runId}/publish/claim` | PG-50 | `STAGING_VALIDATED → PUBLISHING`, publish token 저장. 같은 token 재요청은 `claimed=true` |
| POST | `/v1/runs/{runId}/publish/result` | PG-50 | token 조건으로 `PUBLISHED`, `FAILED_PUBLISH`, `PUBLISH_UNKNOWN` 중 하나 |
| POST | `/v1/runs/{runId}/success` | PG-60 | TARGET 지표가 모두 PASS일 때만 `PUBLISHED → SUCCESS` |
| GET | `/v1/runs/{runId}` | 운영, 후속 Job | run 상태, 파티션 요약, 검증 결과, dispatch 상태 |
| GET | `/v1/runs?jobKey=&businessKey=&status=` | 운영 | 목록 조회 |
| POST | `/v1/runs/{runId}/dispatches/{dispatchId}/resend` | 운영자 | `DEAD`/`SENT` dispatch를 `PENDING`으로 되돌림 |
| POST | `/v1/runs/{runId}/publish-unknown/resolve` | 운영자 | `PUBLISH_UNKNOWN`을 확인 후 `PUBLISHED` 또는 `FAILED_PUBLISH`로 확정. 사유 필수 |

운영자 엔드포인트는 NiFi 서비스 계정과 다른 권한으로 분리한다.

### 5.3 주요 요청과 응답

`POST /v1/runs`

```json
{ "jobKey": "ORACLE_INSP_DTL_DAILY", "businessKey": "2026-09-28",
  "hdfsRoot": "/data/nifi/stage", "stageTablePrefix": "TMP_INSP_DTL_",
  "allowEmptySource": false, "parameters": { "trigger": "SCHEDULE" } }
```

```json
{ "runId": "0199d100-2222-7000-8000-000000000002", "status": "CREATED",
  "hdfsRunPath": "/data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=0199d100-...",
  "stageTable": "tmp_insp_dtl_0199d1002222700080000000000000002" }
```

`allowEmptySource`는 Job Parameter `ALLOW.EMPTY.SOURCE` 값이며 `load_run.parameters`에 저장해 manifest 검증에 쓴다. `runId`, HDFS 경로, stage table 이름은 API가 생성해 반환한다. stage table 이름은 prefix + 하이픈을 제거한 `runId`이며, prefix는 `^[A-Za-z_][A-Za-z0-9_]{0,60}$`로 검증한다. NiFi는 이 값을 attribute로 받아 쓴다(가이드 5장의 `load.run.id`, `load.hdfs.path`, `load.stage.table`).

`POST /v1/runs/{runId}/manifest`

```json
{
  "snapshotScn": "1234567890",
  "sourceCount": 105000, "sourceNullSplitCount": 0,
  "sourceMinSplit": "1", "sourceMaxSplit": "120000",
  "plannedPartitionCount": 8,
  "sourceMetrics": { "AMOUNT_SUM": "987654321.00",
                     "MIN_TS": "2026-09-28 00:00:01", "MAX_TS": "2026-09-28 23:59:58" },
  "partitions": [
    { "partitionId": "0000", "lowerBound": "1", "upperBound": "15001",
      "upperInclusive": false, "isNullPartition": false, "expectedRowCount": 15000 }
  ]
}
```

`partitions`는 가이드 7.4 manifest SQL의 `ExecuteSQLRecord` JSON 배열 출력을 가이드 7.2의 Jolt spec으로 옮긴 형태다. 경계값과 SCN은 정밀도 손실을 막기 위해 문자열로 보낸다. `snapshotScn`은 Oracle 원천일 때 필수이고, 불변 마감 조건을 쓰는 PostgreSQL 원천에서는 `null`이다(가이드 1장).

`sourceMetrics`는 count 외 source DQ 지표(가이드 7.3, `DQ.SOURCE.SQL`)다. API는 각 항목을 `load_validation`(stage=`SOURCE`, result=`PASS`)에 저장하고 `/validation/start` 응답으로 돌려준다. 검증 flow는 API 호출로 새로 시작되어 PG-10의 attribute를 갖고 있지 않기 때문이다.

```json
{ "runId": "...", "status": "EXTRACTING",
  "dispatchPartitions": [ { "partitionId": "0000", "lowerBound": "1", "upperBound": "15001",
                            "upperInclusive": false, "isNullPartition": false,
                            "expectedRowCount": 15000 } ],
  "emptyPartitionCount": 1 }
```

`POST /v1/runs/{runId}/partitions/{pid}/claim`

```json
{ "claimToken": "7b3c...", "workerNode": "nifi-02.example.com" }
```

```json
{ "claimed": true, "runStatus": "EXTRACTING", "partitionStatus": "RUNNING", "attempt": 1 }
```

`POST /v1/runs/{runId}/partitions/{pid}/chunks`

```json
{ "claimToken": "7b3c...", "chunkIndex": 2, "chunkCount": 3,
  "fragmentIdentifier": "c1f0...", "hdfsPath": "/data/nifi/stage/.../part-0000-000002.parquet",
  "recordCount": 5000, "byteCount": 183552 }
```

```json
{ "recorded": true, "partitionStatus": "SUCCESS", "runStatus": "EXTRACTED_VALIDATED",
  "receivedChunks": 3, "chunkCount": 3, "validationScheduled": true }
```

`POST /v1/runs/{runId}/partitions/{pid}/fail`

```json
{ "claimToken": "7b3c...", "errorStage": "ORACLE_EXTRACT", "errorClass": "NON_RETRYABLE",
  "errorCode": "ORA-01555", "message": "snapshot too old", "attempt": 1 }
```

`errorCode`가 `ORA-01555`이거나 `errorClass=SNAPSHOT`이면 run을 `FAILED_SNAPSHOT_EXPIRED`로 바꾼다.

`POST /v1/runs/{runId}/validation/start`

```json
{ "dispatchId": "5d2e...", "node": "nifi-01.example.com" }
```

```json
{ "started": true, "runStatus": "STAGE_VALIDATING",
  "jobKey": "ORACLE_INSP_DTL_DAILY", "businessKey": "2026-09-28", "snapshotScn": "1234567890",
  "hdfsRunPath": "...", "stageTable": "...", "sourceCount": 105000, "extractedCount": 105000,
  "sourceMetrics": { "AMOUNT_SUM": "987654321.00", "MIN_TS": "2026-09-28 00:00:01",
                     "MAX_TS": "2026-09-28 23:59:58" } }
```

검증 flow는 이 응답에서 검증에 필요한 값을 받아 attribute로 쓴다(가이드 10.2의 40U). 이미 시작된 run이면 `started=false`와 현재 `runStatus`만 돌려준다.

API→NiFi 호출(dispatch) 본문은 다음과 같다. URL은 `{nifi.receiver_url}/validate/{jobKey}` 또는 `/reissue/{jobKey}`이며, NiFi PG-05가 `jobKey`로 Job Process Group에 전달한다(가이드 9.5).

```json
// VALIDATE_RUN
{ "runId": "...", "dispatchId": "..." }

// REISSUE_PARTITION: Worker 실행에 필요한 값을 모두 담는다(가이드 13.2)
{ "runId": "...", "dispatchId": "...", "partitionId": "0003",
  "businessKey": "2026-09-28", "snapshotScn": "1234567890", "hdfsRunPath": "...",
  "lowerBound": "45001", "upperBound": "60001", "upperInclusive": false,
  "isNullPartition": false, "expectedRowCount": 15000 }
```

### 5.4 HTTP 상태 코드와 NiFi 분기

NiFi `InvokeHTTP`의 relationship과 1:1로 대응하도록 정한다.

| HTTP | 의미 | `InvokeHTTP` relationship | NiFi 처리 |
|---|---|---|---|
| 200 | 처리 완료(멱등 재요청 포함) | Original(`Response Body Attribute Name` 설정 시) 또는 Response | 응답 본문으로 분기 |
| 409 | 소유권·상태 충돌(`CLAIM_MISMATCH`, `CHUNK_CONFLICT`, `DUPLICATE_ACTIVE_RUN`) | No Retry | WARN 이벤트 후 종료. 재시도하지 않음 |
| 404, 422 | run 없음, 불변식·입력 검증 실패 | No Retry | PG-90 ERROR |
| 5xx | API 또는 DB 일시 장애 | Retry | `RetryFlowFile` 제한 재시도 |
| 연결 실패, timeout | 네트워크 장애 | Failure | `RetryFlowFile` 제한 재시도 |

run이 이미 실패했거나 종료된 상태에서 온 chunk 보고는 409가 아니라 200(`recorded=true`, `runStatus=FAILED_*`)으로 응답한다. 정상적인 경합이므로 NiFi에서 오류로 취급하지 않는다.

## 6. 데이터베이스

전체 DDL의 원본은 가이드 4.1이다. 이 장은 API 관점의 요점만 정리한다. 운영 DDL은 API 저장소의 Alembic migration으로 관리한다(9.9).

| 테이블 | API에서의 용도 |
|---|---|
| `load_run` | run 상태와 행 잠금 대상. `STAGE_VALIDATING` 상태로 검증 flow의 중복 시작을 막는다 |
| `load_partition` | 파티션 manifest, claim token, 파티션 판정 결과 |
| `load_file` | chunk 보고 원장. `(run_id, partition_id, chunk_index)`로 UPSERT |
| `load_validation` | 검증 flow가 보고한 stage별 지표 |
| `load_dispatch` | outbox. run당 `VALIDATE_RUN` 1행을 partial unique index로 강제 |
| `load_event` | 상태 전이 이벤트(API)와 Data plane 이벤트(NiFi PG-90) |

권한 원칙은 다음과 같다(가이드 4.1 "권한").

- API 계정 `load_control_api`: 업무 테이블과 `load_dispatch`에 `SELECT, INSERT, UPDATE`, `load_event`에 `SELECT, INSERT`.
- NiFi 계정 `nifi_runtime`: `load_event` INSERT만. 원장을 직접 바꿀 수 없게 해야 "쓰는 주체는 API 하나"라는 전제가 운영 중에도 유지된다.
- migration 계정: DDL 전용. API 런타임 계정에는 DDL 권한을 주지 않는다.

가이드 초안의 `claim_partition`, `claim_publish` PostgreSQL 함수는 쓰지 않는다. 같은 claim token의 재요청을 `claimed=true`로 돌려주는 멱등 규칙(5.2)이 필요한데, 함수 버전은 응답을 잃고 재요청하면 `false`를 반환해 Worker가 스스로 종료하고 파티션이 sweeper까지 멈춘다. 이 로직은 API repository 계층의 SQL로 구현한다(9.5).

## 7. Sweeper (가이드 PG-70 대체)

API의 worker 프로세스(9.2)가 `recovery.sweeper_interval`마다 실행한다. 여러 worker가 동시에 돌지 않도록 tick마다 `pg_try_advisory_xact_lock`을 얻은 쪽만 실행한다(9.8).

| 대상 | 조건 | 동작 |
|---|---|---|
| 파티션 `RUNNING` | `heartbeat_at < now - recovery.stale` | 정책 `recovery.mode`에 따름: `FAIL`이면 해당 run의 미완료 파티션과 run을 `TIMED_OUT`. `REISSUE`이면 claim 초기화 후 `RETRY`, 이전 chunk 기록 무효화, `REISSUE_PARTITION` dispatch. `attempt_count >= recovery.max_attempts`이면 `FAIL`과 같게 처리 |
| run `CREATED`, `EXTRACTING` | `started_at + recovery.run_timeout < now` | `TIMED_OUT`, 알림 |
| dispatch `SENT` | `sent_at + dispatch.ack_timeout < now` AND (검증 호출이면 run이 아직 `EXTRACTED_VALIDATED`, 재발행이면 파티션이 아직 `RETRY`) | `PENDING`으로 되돌려 재전송 |
| run `STAGE_VALIDATING`, `PUBLISHED` | `heartbeat_at < now - recovery.validation_stale` | `RUN_STALE_ALERT` ERROR 이벤트(같은 run에는 stale 기간마다 1회). 자동 전이하지 않음 |
| run `PUBLISHING` | `publish_started_at + recovery.publish_stale < now` | `PUBLISH_UNKNOWN`, ERROR 알림. 자동 재실행 금지 |

stale 기준은 가이드 13장과 같다. heartbeat는 claim과 chunk 보고 때만 갱신되고 Oracle 쿼리가 실행되는 동안에는 갱신되지 않으므로, `recovery.stale`은 NiFi `EXTRACT.QUERY.TIMEOUT`(= `recovery.extract_query_timeout`)보다 커야 한다. 설정이 이 조건을 어기면 API와 worker가 시작하지 않는다. 그래서 파티션 조건은 heartbeat 하나로 충분하다. 재발행 후 `started_at`은 첫 시작 시각으로 남으므로 조건에 쓰지 않는다.

구현에서 정한 세부 규칙은 다음과 같다.

- 대상 run은 `FOR UPDATE SKIP LOCKED`로 가져온다. 지금 chunk 보고나 claim을 처리 중인 run은 다음 tick으로 미룬다.
- 재발행 전에 해당 파티션의 `load_file` 행을 `status='FAILED'`로 바꾼다. 파티션 판정은 `WRITTEN` 행만 집계하므로 이전 시도의 일부 chunk가 새 판정에 섞이지 않는다. 새 Worker가 같은 chunk를 보고하면 같은 행이 `WRITTEN`으로 갱신된다. API 계정에 DELETE 권한이 없으므로 삭제하지 않는다.
- 재발행 dispatch에는 별도 ACK 엔드포인트가 없다. `RETRY` 파티션이 claim되면 그 파티션의 `REISSUE_PARTITION` dispatch를 `ACKED`로 바꾼다.
- 늦게 살아난 이전 Worker의 chunk 보고는 claim token이 초기화되어 409 `CLAIM_MISMATCH`, 실패 보고는 409 `PARTITION_STATUS_MISMATCH`를 받는다.

1단계 운영은 `recovery.mode=FAIL`을 권장한다. stale 파티션이 생기면 run 전체를 실패시키고 새 `run_id`로 재실행한다. 동일 SCN 재발행(`REISSUE`)은 NiFi 쪽 수신 지점(가이드 13장)과 Oracle UNDO 보존 시간 확인이 끝난 뒤 켠다.

## 8. NiFi 연동 요약

NiFi Processor 단위 설정은 가이드에 있다. 이 장은 API와 맞물리는 지점만 요약한다.

| Process Group | API 호출 | 가이드 |
|---|---|---|
| PG-10 Run Coordinator | `POST /runs`, `POST /runs/{id}/manifest`, 단계 실패 시 `POST /runs/{id}/fail` | 7장 |
| PG-20 Extract Workers | `POST .../claim`, chunk마다 `POST .../chunks`, 최종 실패 시 `POST .../fail` | 8장 |
| (구 PG-30) | 삭제. 완료 판정은 API가 수행 | 9장 |
| PG-05 Control Receiver | API→NiFi `POST /validate/{jobKey}`, `/reissue/{jobKey}` 수신 후 Job PG로 전달 | 9.5 |
| PG-40 Staging Validation | `POST /validation/start`, `POST /validations`, `POST /stage-validated` | 10장 |
| PG-50 Publish | `POST /publish/claim`, `POST /publish/result` | 11장 |
| PG-60 Target Validation | `POST /validations`, `POST /success` | 12장 |
| 재발행 수신(선택) | PG-05 → Job PG `reissue-in` → PG-20 | 13장 |
| PG-90 Audit | API 호출 없음. `load_event` 직접 INSERT | 14장 |

NiFi 쪽 공통 규칙은 다음과 같다.

- `InvokeHTTP`의 `Response Body Attribute Name=api.response`로 응답을 받아 FlowFile content를 보존하고, `${api.response:jsonPath('$.claimed')}`처럼 EL `jsonPath()`로 분기한다.
- 요청 본문은 `AttributesToJSON`(Destination=flowfile-content) 또는 `JoltTransformJSON`으로 만든다. PG-20에서는 PutHDFS **이후**에 content를 바꿔야 Parquet가 요청 본문으로 전송되지 않는다.
- `AttributesToJSON`은 모든 값을 문자열로 만든다. API의 Pydantic 모델은 lax 모드로 `"5000"`을 정수로 받아들인다(9.4).
- relationship 처리는 5.4 표를 따른다.

## 9. FastAPI 구현

### 9.1 기술 스택

| 영역 | 선택 | 비고 |
|---|---|---|
| 런타임 | Python 3.12 | |
| Web | FastAPI, Uvicorn(`uvicorn[standard]`), Gunicorn | Gunicorn + `UvicornWorker`로 멀티 프로세스 |
| 모델·설정 | Pydantic v2, `pydantic-settings[yaml]` | 요청 검증, `config.yaml` 설정 |
| DB | SQLAlchemy 2.0 async Core + `asyncpg` | ORM 대신 `text()` SQL로 CAS·잠금을 명시적으로 작성 |
| Migration | Alembic(async 템플릿) | 가이드 4.1 DDL을 baseline으로 관리 |
| HTTP client | `httpx.AsyncClient` | API→NiFi dispatch, mTLS 지원 |
| 이벤트 | PostgreSQL `LISTEN/NOTIFY`(asyncpg) | dispatcher 즉시 깨우기 |
| 로그·메트릭 | `structlog`(JSON), `prometheus-client` | |
| 테스트 | `pytest`, `pytest-asyncio`, `httpx.ASGITransport`, `testcontainers[postgres]`, `respx` | 실제 PostgreSQL로 동시성 테스트 |
| 품질 | `ruff`, `mypy` | |

ORM을 쓰지 않는 이유: 이 API의 핵심은 `SELECT ... FOR UPDATE`, 조건부 `UPDATE ... RETURNING`, partial unique index를 이용한 `ON CONFLICT`다. 이 SQL이 코드에 그대로 보여야 리뷰와 장애 분석이 쉽다.

### 9.2 프로세스 구성

같은 코드베이스에서 두 가지 진입점으로 실행한다.

| 프로세스 | 진입점 | 역할 | 인스턴스 |
|---|---|---|---|
| `api` | `gunicorn 'load_control.main:create_app()' -k uvicorn.workers.UvicornWorker -w 4` | HTTP 엔드포인트(5장) | 2개 이상, LB 뒤 |
| `worker` | `python -m load_control.worker` | dispatcher(4장), sweeper(7장) | 2개(활성-활성) |

dispatcher와 sweeper를 HTTP 프로세스와 분리하는 이유는 다음과 같다.

- Gunicorn worker마다 백그라운드 작업이 뜨면 인스턴스 수 × worker 수만큼 폴링이 늘어난다.
- API는 요청량에 맞춰, worker는 dispatch량에 맞춰 따로 늘리고 줄일 수 있다.
- worker가 둘 이상 떠도 lease(4.3)와 advisory lock(7장)이 중복 처리를 막으므로 이중화에 별도 리더 선출이 필요 없다.

### 9.3 프로젝트 구조

구현은 저장소의 [`load-control-api/`](./load-control-api/)에 있다. 12장의 API 쪽 작업(1, 2, 4, 5, 6, 8단계의 API 부분)은 구현을 마쳤다.

```text
load-control-api/
├── pyproject.toml
├── alembic.ini
├── config.example.yaml
├── alembic/
│   ├── env.py
│   └── versions/
│       ├── 0001_nifi_ops_baseline.py      # 가이드 4.1 DDL
│       └── 0002_...py
├── src/load_control/
│   ├── main.py            # create_app() factory, lifespan, router 등록, 예외 처리기
│   ├── domain.py          # RunStatus, PartitionStatus, 허용 실패 전이
│   ├── metrics.py         # Prometheus 메트릭
│   ├── config.py          # Settings (config.yaml 로드)
│   ├── db.py              # engine, 트랜잭션 헬퍼(재시도 포함)
│   ├── security.py        # 인증 의존성(role: nifi, operator)
│   ├── errors.py          # ApiError 계층
│   ├── logging.py         # structlog 설정, request id middleware
│   ├── schemas/           # Pydantic 요청·응답 모델
│   │   ├── runs.py  partitions.py  validation.py  publish.py
│   ├── repositories/      # SQL만 둔다(상태 판단 없음)
│   │   ├── runs.py  partitions.py  files.py  validations.py  dispatch.py  events.py  sweeper.py
│   ├── services/          # 트랜잭션 단위 업무 규칙
│   │   ├── runs.py  manifest.py  completion.py  validation.py  publish.py  ops.py
│   ├── routers/
│   │   ├── deps.py  runs.py  partitions.py  validation.py(검증·게시)  ops.py  health.py
│   └── worker/
│       ├── __main__.py    # dispatcher + sweeper 실행
│       ├── dispatcher.py
│       └── sweeper.py
└── tests/
    ├── conftest.py        # testcontainers PostgreSQL, alembic upgrade
    ├── test_partitions.py test_concurrency.py test_dispatcher.py test_sweeper.py
    ├── test_validation_start.py test_publish_flow.py test_ops.py ...
```

계층 규칙은 다음과 같다.
- `routers`는 인증, 입력 검증, 트랜잭션 시작만 맡는다.
- `services`는 하나의 트랜잭션 안에서 업무 규칙을 실행한다.
- `repositories`는 SQL 실행만 하고 판단하지 않는다.

### 9.4 설정과 요청 모델

설정은 `config.yaml` 하나로 관리한다. 전체 예시와 각 항목 설명은 [`load-control-api/config.example.yaml`](./load-control-api/config.example.yaml)에 있다.

```yaml
database:
  url: postgresql+asyncpg://load_control_api:***@meta:5432/nifiops   # API 런타임 계정
  migration_url: postgresql+asyncpg://nifi_ops_migrator:***@meta:5432/nifiops  # alembic 전용
  listen_dsn: postgresql://load_control_api:***@meta:5432/nifiops     # worker LISTEN 전용
  pool_size: 10
auth:
  token_digests:
    nifi: ["<sha256-hex>"]
    operator: ["<sha256-hex>"]
nifi:
  receiver_url: https://nifi-lb.internal:9443
recovery:
  extract_query_timeout: PT60M
  stale: PT90M
  mode: FAIL
dispatch:
  ack_timeout: PT10M
logging:
  format: json
```

| 섹션 | 내용 |
|---|---|
| `database` | DB URL(런타임, migration, LISTEN), pool, 트랜잭션 재시도 횟수 |
| `auth` | role별 Bearer 토큰 SHA-256 digest |
| `nifi` | worker → NiFi PG-05 호출 주소, mTLS 인증서, timeout |
| `recovery` | sweeper 기준: run timeout, stale, 재발행 모드, 정체 경보 |
| `dispatch` | outbox 전달: 최대 시도, backoff, ACK timeout, lease, 폴링 주기 |
| `worker` | worker `/metrics` 포트 |
| `logging` | 로그 수준, 형식(json/console) |

로드 규칙은 다음과 같다(`config.py`, `pydantic-settings`의 `YamlConfigSettingsSource`).

- 파일 위치는 `LCA_CONFIG` 환경변수, 없으면 현재 디렉터리의 `config.yaml`이다. 파일이 없으면 API, worker, alembic 모두 시작하지 않는다.
- 우선순위는 환경변수 > `config.yaml` > 기본값이다. 환경변수 덮어쓰기는 비밀값 주입용이며 섹션 구분자는 `__`이다(예: `LCA_DATABASE__URL`). 비밀값을 파일에 두지 않으려면 YAML에서 해당 키를 빼고 환경변수나 secret 저장소로 주입한다.
- 섹션마다 `extra="forbid"`라서 모르는 키(오타)가 있으면 시작을 거부한다.
- 기간은 ISO 8601(`PT90M`, `PT6H`) 또는 초 단위 숫자로 쓴다.
- `recovery.stale <= recovery.extract_query_timeout`이면 검증 오류로 시작을 거부한다.
- 비밀값(DB URL)은 `SecretStr`이라 로그와 `repr`에 찍히지 않는다.
- `config.yaml`은 git에 올리지 않는다(`.gitignore`). 저장소에는 `config.example.yaml`만 둔다.

```python
# schemas/partitions.py
from typing import Annotated
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from pydantic.alias_generators import to_camel

PartitionId = Annotated[str, StringConstraints(pattern=r"^([0-9]{4}|NULL)$")]
DecimalStr = Annotated[str, StringConstraints(pattern=r"^-?[0-9]{1,38}$")]


class ApiModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True,
                              extra="forbid", coerce_numbers_to_str=True)


class ChunkReport(ApiModel):
    claim_token: UUID
    chunk_index: int = Field(ge=0)
    chunk_count: int = Field(gt=0)
    fragment_identifier: str | None = Field(default=None, max_length=100)
    hdfs_path: str = Field(min_length=1, max_length=1500)
    record_count: int = Field(ge=0)
    byte_count: int | None = Field(default=None, ge=0)


class ChunkResult(ApiModel):
    recorded: bool
    partition_status: str
    run_status: str
    received_chunks: int
    chunk_count: int
    validation_scheduled: bool
```

- JSON 필드는 camelCase, Python 속성은 snake_case로 둔다. FastAPI는 응답을 alias(camelCase)로 직렬화한다.
- Pydantic v2 기본 lax 모드는 `"5000"` 같은 숫자 문자열을 `int`로 받아들이므로, NiFi `AttributesToJSON`의 문자열 값을 그대로 받을 수 있다.
- SCN, 경계값처럼 `numeric(38,0)` 범위의 값은 `DecimalStr`로 받아 정밀도 손실을 막고, DB에는 `CAST(:v AS numeric)`로 넣는다.
- `extra="forbid"`로 알 수 없는 필드를 거부해 NiFi 쪽 attribute 목록 실수를 422로 드러낸다.
- `coerce_numbers_to_str=True`는 Jolt나 `ExecuteSQLRecord`가 경계값을 JSON 숫자로 보낸 경우에도 `DecimalStr`로 받게 한다. 다만 큰 수는 JSON 숫자로 바뀌는 순간 정밀도를 잃을 수 있으므로 NiFi 쪽에서 `TO_CHAR`로 문자열을 보내는 것이 원칙이다(가이드 7.4).

### 9.5 트랜잭션 헬퍼와 잠금 순서

```python
# db.py
import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

T = TypeVar("T")
RETRYABLE_SQLSTATE = {"40P01", "40001"}  # deadlock_detected, serialization_failure


def make_engine(settings) -> AsyncEngine:
    return create_async_engine(
        settings.database.url.get_secret_value(),
        pool_size=settings.database.pool_size,
        max_overflow=settings.database.max_overflow,
        pool_pre_ping=True,
    )


def _sqlstate(e: DBAPIError) -> str | None:
    return getattr(e.orig, "sqlstate", None) or getattr(e.orig, "pgcode", None)


async def in_tx(engine: AsyncEngine,
                fn: Callable[[AsyncConnection], Awaitable[T]],
                attempts: int = 3) -> T:
    for i in range(attempts):
        try:
            async with engine.begin() as conn:   # 예외 시 rollback, 정상 종료 시 commit
                return await fn(conn)
        except DBAPIError as e:
            if _sqlstate(e) in RETRYABLE_SQLSTATE and i < attempts - 1:
                await asyncio.sleep(0.05 * 2 ** i)
                continue
            raise
    raise AssertionError("unreachable")
```

- 격리 수준은 PostgreSQL 기본값 READ COMMITTED를 쓰고, 정합성은 행 잠금과 CAS로 보장한다.
- **잠금 순서를 고정한다**: `load_run` → `load_partition` → `load_file` → `load_dispatch`. 모든 서비스가 이 순서로 잠그면 deadlock이 생기지 않는다. 그래도 생기면(`40P01`) `in_tx`가 트랜잭션 전체를 다시 실행한다. 모든 엔드포인트가 멱등이므로 재실행해도 안전하다.
- 상태를 기록한 뒤 4xx를 반환해야 하는 경우(예: manifest 불변식 위반 → `FAILED_MANIFEST` 저장 후 422)에는 트랜잭션 안에서 예외를 던지면 기록까지 rollback된다. 서비스는 결과 객체를 돌려주고, 라우터가 commit 후에 오류 응답을 만든다.

### 9.6 chunk 판정 서비스

3.2의 트랜잭션을 그대로 옮긴 코드다.

```python
# services/completion.py
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncConnection
from load_control.errors import Conflict, NotFound, Unprocessable
from load_control.repositories import dispatch, events, files, partitions, runs
from load_control.schemas.partitions import ChunkReport, ChunkResult


async def report_chunk(conn: AsyncConnection, run_id: UUID, partition_id: str,
                       req: ChunkReport) -> ChunkResult:
    run = await runs.lock(conn, run_id)                        # SELECT ... FOR UPDATE
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    part = await partitions.lock(conn, run_id, partition_id)   # SELECT ... FOR UPDATE
    if part is None:
        raise NotFound("PARTITION_NOT_FOUND")
    if part.claim_token != req.claim_token:
        raise Conflict("CLAIM_MISMATCH")
    if not req.hdfs_path.startswith(run.hdfs_run_path + "/"):
        raise Unprocessable("HDFS_PATH_OUTSIDE_RUN")

    changed = await files.upsert(conn, run_id, partition_id, req)  # 신규 또는 값 변경이면 True
    if part.status == "SUCCESS" and changed:
        raise Conflict("CHUNK_CONFLICT")                           # rollback되어 기록도 취소
    await partitions.touch(conn, run_id, partition_id)
    await runs.touch(conn, run_id)

    agg = await files.aggregate(conn, run_id, partition_id)
    result = ChunkResult(recorded=True, partition_status=part.status, run_status=run.status,
                         received_chunks=agg.files, chunk_count=req.chunk_count,
                         validation_scheduled=False)
    if run.status != "EXTRACTING" or part.status != "RUNNING" or agg.files < req.chunk_count:
        return result                                              # 판정 대상 아님 또는 진행 중

    if agg.is_complete(req.chunk_count) and agg.rows == part.expected_row_count:
        await partitions.mark_success(conn, run_id, partition_id, req.claim_token, agg)
        await events.record(conn, "PARTITION_SUCCESS", run_id, partition_id, row_count=agg.rows)
        result.partition_status = "SUCCESS"
        if await runs.try_complete_extract(conn, run_id):          # 3.3 run 완료 CAS
            await dispatch.enqueue_validation(conn, run_id)        # INSERT + pg_notify
            await events.record(conn, "EXTRACT_VALIDATED", run_id)
            result.run_status = "EXTRACTED_VALIDATED"
            result.validation_scheduled = True
    else:
        await partitions.mark_failed(conn, run_id, partition_id, "ROW_COUNT_MISMATCH",
                                     f"files={agg.files} rows={agg.rows} "
                                     f"expected={part.expected_row_count}")
        await runs.fail(conn, run_id, expected="EXTRACTING", to="FAILED_EXTRACT",
                        stage="PARTITION_GATE", code="ROW_COUNT_MISMATCH")
        await events.record(conn, "PARTITION_FAILED", run_id, partition_id, level="ERROR")
        result.partition_status, result.run_status = "FAILED", "FAILED_EXTRACT"
    return result
```

`agg.is_complete(n)`는 `files == n AND lo == 0 AND hi == n-1 AND counts == 1`이다(3.2의 4단계).

```python
# routers/partitions.py
router = APIRouter(prefix="/v1/runs/{run_id}/partitions/{partition_id}", tags=["partitions"],
                   dependencies=[Depends(require_role("nifi"))])


@router.post("/chunks", response_model=ChunkResult)
async def report_chunk(run_id: UUID, partition_id: PartitionIdPath, body: ChunkReport,
                       request: Request) -> ChunkResult:
    return await run_tx(request, lambda conn: completion.report_chunk(conn, run_id, partition_id, body))
```

`enqueue_validation`의 SQL은 다음과 같다. `pg_notify`는 트랜잭션이 commit될 때만 전달되므로, rollback된 완료가 dispatcher를 깨우는 일은 없다.

```sql
INSERT INTO nifi_ops.load_dispatch (dispatch_id, run_id, dispatch_type)
VALUES (CAST(:dispatch_id AS uuid), CAST(:run_id AS uuid), 'VALIDATE_RUN')
ON CONFLICT (run_id) WHERE dispatch_type = 'VALIDATE_RUN' DO NOTHING;

SELECT pg_notify('load_dispatch', :run_id);
```

### 9.7 인증과 오류 응답

```python
# security.py
import hashlib, hmac
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer = HTTPBearer(auto_error=True)


def require_role(role: str):
    async def dep(request: Request, cred: HTTPAuthorizationCredentials = Depends(bearer)) -> None:
        digest = hashlib.sha256(cred.credentials.encode()).hexdigest()
        allowed = request.app.state.settings.auth.token_digests.get(role, ())
        if not any(hmac.compare_digest(digest, d) for d in allowed):
            raise HTTPException(status_code=403, detail="FORBIDDEN")
    return dep
```

- role은 `nifi`(NiFi 서비스 계정)와 `operator`(운영자 엔드포인트) 두 가지다. 설정에는 토큰 원문이 아니라 SHA-256 digest 목록을 둔다. 토큰을 교체할 때 이전·신규 토큰이 함께 통과하도록 하기 위해서다.
- mTLS는 LB/ingress에서 종료하고 클라이언트 인증서를 검증한다. Bearer 토큰은 그 위에 추가하는 role 구분 수단이다.

```python
# errors.py / main.py
class ApiError(Exception):
    status = 500
    def __init__(self, code: str, message: str | None = None):
        self.code, self.message = code, message or code

class NotFound(ApiError):      status = 404
class Conflict(ApiError):      status = 409
class Unprocessable(ApiError): status = 422


@app.exception_handler(ApiError)
async def api_error(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status,
                        content={"code": exc.code, "message": exc.message,
                                 "requestId": request.state.request_id})
```

FastAPI의 기본 입력 검증 오류(`RequestValidationError`)는 422이므로 NiFi에서는 No Retry로 분기된다(5.4). 처리하지 못한 예외와 DB 연결 장애는 500/503으로 응답해 NiFi가 재시도하게 한다.

### 9.8 dispatcher와 sweeper

dispatcher는 `LISTEN load_dispatch`로 즉시 깨어나고, 알림을 놓쳐도 `dispatch_poll_interval`마다 폴링한다.

```python
# worker/dispatcher.py
import asyncio
import asyncpg
import httpx


async def run_dispatcher(settings, engine, client: httpx.AsyncClient) -> None:
    wake = asyncio.Event()
    listener = await asyncpg.connect(settings.database.listen_dsn.get_secret_value())
    await listener.add_listener("load_dispatch", lambda *_: wake.set())
    while True:
        try:
            await asyncio.wait_for(wake.wait(), settings.dispatch.poll_interval.total_seconds())
        except TimeoutError:
            pass
        wake.clear()
        while batch := await dispatch.lease_due(engine, settings.dispatch.batch, settings.dispatch.lease):
            await asyncio.gather(*(send_one(settings, engine, client, d) for d in batch))


async def send_one(settings, engine, client, d) -> None:
    action = "validate" if d.dispatch_type == "VALIDATE_RUN" else "reissue"
    url = f"{str(settings.nifi.receiver_url).rstrip('/')}/{action}/{d.job_key}"
    body = await dispatch.build_body(engine, d)   # 5.3 dispatch 본문
    headers = {"X-Run-Id": str(d.run_id), "X-Dispatch-Id": str(d.dispatch_id)}
    try:
        r = await client.post(url, json=body, headers=headers)
    except httpx.HTTPError as e:
        await dispatch.schedule_retry(engine, d, None, repr(e)[:2000], settings)
        return
    if r.is_success:
        await dispatch.mark_sent(engine, d.dispatch_id, r.status_code)
    elif 400 <= r.status_code < 500:
        await dispatch.mark_dead(engine, d.dispatch_id, r.status_code, r.text[:2000])
    else:
        await dispatch.schedule_retry(engine, d, r.status_code, r.text[:2000], settings)
```

- listener 연결이 끊기면 다시 연결하고 바로 한 번 폴링한다(연결 감시 루프는 생략).
- `httpx.AsyncClient`는 프로세스당 하나를 만들고 `cert=(client_cert, client_key)`, `verify=ca_bundle`, `timeout=nifi_timeout_seconds`로 mTLS를 설정한다.
- `mark_sent`는 `WHERE status = 'PENDING'` 조건으로만 갱신한다. NiFi가 202를 응답하자마자 `/validation/start`를 호출해 이미 `ACKED`가 됐을 수 있기 때문이다. 이 조건이 없으면 `ACKED`가 `SENT`로 덮여 ACK timeout 후 불필요한 재전송이 생긴다.
- `schedule_retry`는 `attempt_count >= dispatch.max_attempts`이면 `DEAD`로 바꾸고 `DISPATCH_DEAD` ERROR 이벤트를 남긴다.

sweeper는 tick마다 한 트랜잭션에서 `pg_try_advisory_xact_lock(hashtext('load_control_sweeper'))`을 먼저 얻는다. 잠금을 못 얻으면 다른 worker가 실행 중이므로 건너뛴다. 7장의 규칙은 각각 조건부 `UPDATE ... RETURNING` 한 문장으로 구현하고, 반환된 행마다 `load_event`를 기록한다.

### 9.9 Migration

- Alembic `0001_nifi_ops_baseline`에 가이드 4.1 DDL 전체를 넣는다. 이후 변경은 revision을 추가한다.
- PoC처럼 DDL을 수동으로 이미 만든 DB는 `alembic stamp 0001`로 기준점을 맞춘다.
- migration은 DDL 전용 계정으로 배포 파이프라인에서 실행하고, API 런타임 계정에는 DDL 권한을 주지 않는다.
- `env.py`는 async 템플릿(`alembic init -t async`)을 쓰고 `target_metadata=None`으로 둔다. ORM 모델이 없으므로 autogenerate는 쓰지 않고 SQL을 직접 작성한다.

### 9.10 배포와 리소스

| 항목 | 기준 |
|---|---|
| 패키징 | 컨테이너 이미지 하나, 실행 명령으로 `api`/`worker` 구분 |
| Health | `GET /healthz`(프로세스 생존, DB 미확인), `GET /readyz`(`SELECT 1`) |
| Gunicorn | `-w`는 CPU 코어 수 기준, `--timeout 60`, `--graceful-timeout 30` |
| DB 연결 수 | `api 인스턴스 × gunicorn worker × (pool_size + max_overflow) + worker 인스턴스 × pool + listener` ≤ 관리 DB 승인 연결 수 |
| 종료 | SIGTERM 시 진행 중 요청을 끝내고 종료. 중간에 끊겨도 트랜잭션 rollback과 NiFi 재시도로 복구 |
| 설정 주입 | `config.yaml`을 `/etc/load-control/config.yaml`로 마운트(`LCA_CONFIG`). 비밀값은 조직 표준 secret 저장소에서 `LCA_DATABASE__URL` 같은 환경변수로 덮어쓴다 |

### 9.11 로그와 메트릭

- request id middleware: NiFi `InvokeHTTP` 동적 속성으로 `X-Request-Id: ${UUID()}`를 보내고, 없으면 API가 생성한다. 응답 헤더와 모든 로그에 남긴다.
- `structlog` JSON 로그 필드: 10.3의 필드에 `requestId`, `role`, `httpStatus`를 더한다.
- Prometheus 메트릭(`GET /metrics`):

| 메트릭 | 종류 | 라벨 |
|---|---|---|
| `lca_requests_total` | counter | `endpoint`, `status` |
| `lca_request_seconds` | histogram | `endpoint` |
| `lca_chunk_reports_total` | counter | `result`(`progress`, `partition_success`, `run_complete`, `failed`, `ignored`) |
| `lca_run_lock_wait_seconds` | histogram | |
| `lca_dispatch_total` | counter | `type`, `result`(`sent`, `retry`, `dead`) |
| `lca_dispatch_backlog` | gauge | `status`(`PENDING`, `SENT`, `DEAD`) — sweeper가 갱신 |
| `lca_active_runs` | gauge | `status` |
| `lca_sweeper_actions_total` | counter | `rule` |

## 10. 운영

### 10.1 이중화

- API는 상태를 갖지 않으므로 2개 이상 인스턴스를 LB 뒤에 둔다. 모든 정합성은 PostgreSQL 트랜잭션(run 행 잠금, CAS, unique index)으로 보장한다.
- dispatcher와 sweeper는 별도 worker 프로세스로 2개 띄운다(9.2). 4.3의 lease(`SKIP LOCKED`)와 sweeper advisory lock이 중복 처리를 막으므로 리더 선출이 필요 없다.
- worker가 모두 내려가면 완료 판정은 계속되지만 검증 flow 호출과 timeout 정리가 멈춘다. `lca_dispatch_backlog{status="PENDING"}`의 지속 증가를 알림 조건으로 둔다.
- API가 모두 내려가도 Oracle 조회와 HDFS 기록은 진행된다. 보고는 NiFi `RetryFlowFile`이 재시도하며, 재시도를 다 쓰면 run은 sweeper가 `TIMED_OUT`으로 정리한다. 따라서 `CONTROL.API.RETRY.MAX`와 penalty는 API 재기동 시간보다 길게 잡는다.
- PostgreSQL이 단일 장애점이라는 점은 가이드와 같다.

### 10.2 보안

- NiFi→API: mTLS 또는 Bearer 토큰. 토큰은 Sensitive Parameter로만 관리한다.
- API→NiFi: PG-05의 `HandleHttpRequest`는 내부망에서만 접근 가능하게 방화벽으로 제한하고 mTLS(Client Authentication=REQUIRED)를 사용한다. mTLS를 못 쓰면 요청 헤더의 HMAC 서명이나 토큰을 `RouteOnAttribute`로 검증한다.
- API는 요청 값으로 SQL 식별자를 만들지 않는다. 모든 값은 bind parameter로만 사용한다.
- 운영자 엔드포인트(재전송, `PUBLISH_UNKNOWN` 확정)는 별도 권한과 감사 로그를 둔다.

### 10.3 관측

- API 구조화 로그 필드: `requestId`, `runId`, `partitionId`, `chunkIndex`, `endpoint`, `result`, `durationMs`, `fromStatus`, `toStatus`.
- 상태 전이(`PARTITION_SUCCESS`, `EXTRACT_VALIDATED`, `RUN_FAILED` 등 가이드 14.4의 이벤트)는 API가 같은 트랜잭션에서 `load_event`에 기록한다. NiFi PG-90은 Processor 오류와 Data plane 이벤트만 기록한다.
- 메트릭: 엔드포인트별 지연·오류율, 활성 run 수, `PENDING`/`SENT`/`DEAD` dispatch 수, run 잠금 대기 시간, sweeper 처리 건수.
- 알림: `DEAD` dispatch, `PUBLISH_UNKNOWN`, `TIMED_OUT`, `FAILED_*`, 5xx 비율 급증.

## 11. 테스트와 승인 조건

### 11.1 API 단위·통합 테스트

| 시나리오 | 기대 결과 |
|---|---|
| 마지막 두 파티션의 마지막 chunk를 동시에 보고 | run `EXTRACTED_VALIDATED` 1회, `load_dispatch` 1행 |
| 같은 chunk 보고 2회 | 두 번째도 200, `load_file` 1행, 판정 결과 동일 |
| 파티션 SUCCESS 후 같은 chunk를 다른 row 수로 재보고 | 409 `CHUNK_CONFLICT`, 상태 변화 없음 |
| claim 응답 유실 후 같은 token으로 재요청 | `claimed=true` |
| 다른 token으로 chunk 보고 | 409 `CLAIM_MISMATCH` |
| 파티션 실패 후 다른 파티션의 chunk 보고 | 200, `runStatus=FAILED_EXTRACT`, dispatch 없음 |
| 파일 수는 맞고 row 합계가 다름 | partition `FAILED`, run `FAILED_EXTRACT` |
| manifest 합계 ≠ source count | 422, run `FAILED_MANIFEST` |
| 전부 0건 파티션 + `ALLOW.EMPTY.SOURCE=true` | manifest 응답 시점에 run 완료, dispatch 1행 |
| NiFi 수신 측 5xx | backoff 재시도, 최대 횟수 초과 시 `DEAD`와 알림 |
| 202 응답 후 `/validation/start` 미도착 | `dispatch.ack_timeout` 후 재전송 |
| `/validation/start` 2회 | 첫 번째만 `started=true` |
| worker 2개에서 dispatcher 동시 실행 | 같은 dispatch를 한 번만 전송 |
| 전송 중 worker 종료 | lease 만료 후 다른 worker가 재전송 |
| 커밋 직후 API 프로세스 종료 | worker가 `PENDING` dispatch 전송 |
| NiFi가 202 직후 `/validation/start` 호출(`mark_sent`보다 먼저 ACK) | dispatch는 `ACKED` 유지, 재전송 없음 |
| NOTIFY 유실(listener 끊김) | 폴링 주기 안에 전송 |
| deadlock(`40P01`) 주입 | `in_tx` 재시도 후 정상 응답 |

테스트 구성은 다음과 같다.

- `testcontainers[postgres]`로 PostgreSQL 16을 띄우고 세션 시작 시 `alembic upgrade head`를 실행한다. 동시성 규칙은 SQLite나 mock으로 검증할 수 없으므로 반드시 실제 PostgreSQL을 쓴다.
- API는 `httpx.AsyncClient(transport=ASGITransport(app=app))`로 프로세스 안에서 호출한다. 동시 완료 시나리오는 `asyncio.gather`로 여러 요청을 동시에 보낸다. 요청마다 다른 DB 연결을 쓰므로 실제 잠금 경합이 재현된다.
- 경합을 확실히 만들려면 repository 함수에 테스트용 지연 hook을 두거나, 두 연결에서 SQL을 단계별로 직접 실행해 잠금 대기를 확인한다.
- dispatcher 테스트는 `respx`로 NiFi 응답(2xx, 4xx, 5xx, timeout)을 흉내 낸다.
- 커버리지 기준: `services/`, `repositories/`, `worker/`의 분기 커버리지 90% 이상.

### 11.2 운영 승인 조건

가이드 19장의 조건을 유지하고 다음을 추가한다.

```text
파티션 하나가 실패하면 검증 flow 호출(dispatch)은 0건이다.
동시 완료, 중복 보고, API 재기동 후에도 run당 STAGE_VALIDATING 진입은 1회다.
NiFi 계정으로 load_run/load_partition/load_file/load_validation을 변경할 수 없다.
API 전체 중단 중 시작된 run은 성공으로 판정되지 않고 TIMED_OUT 또는 재시도 후 정상 판정된다.
```

## 12. 전환 순서

1. **프로젝트 골격** (구현 완료): 9.3 구조, 설정, Alembic baseline(가이드 4.1 DDL), 인증, 오류 처리, health, 테스트 환경(testcontainers).
2. **API 1차** (구현 완료): `/runs`, `/manifest`, `/claim`, `/chunks`, `/fail`, `GET /runs/{id}`, 판정 트랜잭션. 11.1의 동시성 테스트를 먼저 통과시킨다.
3. **PG-10, PG-20 전환**: PoC 환경(NiFi 2.4.0 + PostgreSQL)에서 PoC와 같은 시나리오(105,000건, 8파티션, 0건 파티션, 배수 경계, 중복 실행, HDFS 실패 주입)를 다시 실행한다. 이 단계에서는 PG-30을 남겨 두고 API 판정 결과와 PG-30 판정 결과를 비교할 수 있다.
4. **outbox와 검증 수신** (API 구현 완료, NiFi 남음): `load_dispatch`, dispatcher, `/validation/start`, PG-05·PG-40 입구. 이후 PG-30과 DMC Controller Service를 삭제한다.
5. **PG-50, PG-60 연동** (API 구현 완료, NiFi 남음): publish claim·result, validations, stage-validated, success, 운영자 엔드포인트.
6. **Sweeper** (API 구현 완료): `recovery.mode=FAIL`로 시작. 노드 종료, API 재기동, NiFi 재기동, 보고 유실을 주입해 검증한다.
7. **권한 회수**: NiFi 계정의 업무 테이블 쓰기 권한 회수(6장, 가이드 4.1).
8. **재발행(선택)** (API 구현 완료, NiFi 남음): `REISSUE_PARTITION`과 PG-05 `/reissue/{jobKey}` 수신 경로.
