# 통합 검증과 운영·복구

## 1. 검증 전략

검증은 다음 네 층으로 수행한다.

1. 구성 검증: 연결, 권한, Parameter, Controller Service
2. PG 단계 검증: queue/provenance/API 원장으로 각 단계의 입력과 출력 확인
3. E2E 정합성: 원천=추출=staging=target 및 지표 PASS
4. 장애/복구: 중복, API 중단, HDFS 실패, snapshot expired, 게시 결과 불명

시험 run마다 다음 증적을 남긴다.

- 배포 commit과 설정 파일 checksum
- Job key, business key, run ID, snapshot SCN
- `load_run`, `load_partition`, `load_validation`, `load_dispatch`, 오류 `load_event` 조회 결과
- HDFS 파일 목록과 staging/target SQL 결과
- 주요 NiFi provenance와 bulletin
- API server/worker 로그

## 2. 기본 조회 모음

### 2.1 run

```sql
SELECT run_id, job_key, business_key, status, snapshot_scn,
       source_count, extracted_count, staging_count, target_count,
       expected_partition_count, success_partition_count, failed_partition_count,
       error_stage, error_code, started_at, completed_at, cleaned_at
  FROM nifi_ops.load_run
 ORDER BY started_at DESC
 LIMIT 20;
```

### 2.2 partition과 chunk

```sql
SELECT partition_id, status, expected_row_count, actual_row_count,
       file_count, attempt_count, worker_node, heartbeat_at, error_code
  FROM nifi_ops.load_partition
 WHERE run_id = '<run_id>'
 ORDER BY partition_id;

SELECT partition_id, COUNT(*) AS files, SUM(record_count) AS rows, SUM(byte_count) AS bytes
  FROM nifi_ops.load_file
 WHERE run_id = '<run_id>' AND status = 'WRITTEN'
 GROUP BY partition_id
 ORDER BY partition_id;
```

### 2.3 validation, dispatch, event

```sql
SELECT stage, metric_name, query_version, expected_value, actual_value, result, measured_at
  FROM nifi_ops.load_validation
 WHERE run_id = '<run_id>'
 ORDER BY stage, metric_name, query_version;

SELECT dispatch_id, dispatch_type, partition_id, status, attempt_count,
       next_attempt_at, sent_at, acked_at, last_http_status, last_error
  FROM nifi_ops.load_dispatch
 WHERE run_id = '<run_id>' ORDER BY created_at;

SELECT event_time, event_level, event_name, error_class, error_code,
       partition_id, message, details
  FROM nifi_ops.load_event
 WHERE run_id = '<run_id>' ORDER BY event_time;
```

## 3. 정상 E2E 인수 테스트

### 3.1 준비

- 다른 schedule을 중지하거나 시험 Job key를 분리한다.
- 시험 business key의 Oracle 기대 건수와 지표를 미리 계산한다.
- target partition의 기존 건수/지표를 기록한다.
- Trigger 00은 disabled 상태에서 시작한다.

### 3.2 실행

1. Job PG를 start한다.
2. business key를 설정한다.
3. Trigger 00을 enable하고 Run Once한다.
4. 즉시 다시 disable한다.
5. TUI 또는 SQL에서 상태 전이를 관찰한다.

예상 상태 순서:

```text
CREATED → EXTRACTING → EXTRACTED_VALIDATED → STAGE_VALIDATING
→ STAGING_VALIDATED → PUBLISHING → PUBLISHED → SUCCESS
```

### 3.3 합격 기준

| 영역 | 기준 |
|---|---|
| run | 최종 `SUCCESS`, error 필드 없음 |
| 건수 | source = partition 예상 합 = chunk 합 = staging = target |
| partition | 모두 `SUCCESS`, 실패 0 |
| HDFS | ledger와 파일 수/경로 일치, `_SUCCESS` 존재 |
| staging | LOCATION이 해당 run 경로, schema 일치 |
| validation | SOURCE/STAGING/TARGET 지표 모두 PASS |
| dispatch | VALIDATE_RUN 하나, 최종 `ACKED` |
| publish | target의 의도한 partition만 변경 |
| event | 정상 상태 전이 순서, ERROR 없음 |
| queue | 모든 단계 queue가 정상적으로 비워짐 |

### 3.4 독립 SQL 비교

아래 SQL은 API 호스트에서 `bin/oracle.sh -f`, `bin/hive.sh -f`로 실행할 수 있다. HDFS chunk는
`bin/hdfs.sh ls -h <hdfs_run_path>`로 확인한다([운영 조회 도구](./09-query-tools.md)).

Oracle은 run의 SCN을 사용한다.

```sql
SELECT COUNT(*) AS cnt,
       NVL(SUM(AMOUNT), 0) AS amount_sum,
       MIN(REG_TS) AS min_ts,
       MAX(REG_TS) AS max_ts
  FROM APP.INSP_DTL AS OF SCN <snapshot_scn>
 WHERE BASE_DT = DATE '2026-09-28';
```

Hive target:

```sql
SELECT COUNT(*) AS cnt,
       COALESCE(SUM(AMOUNT), 0) AS amount_sum,
       MIN(REG_TS) AS min_ts,
       MAX(REG_TS) AS max_ts,
       COUNT(*) - COUNT(DISTINCT INSP_DTL_SEQ) AS duplicate_count
  FROM dw.insp_dtl
 WHERE base_dt = '2026-09-28';
```

## 4. 필수 장애 시나리오

운영 데이터나 운영 target을 사용하지 말고 격리된 Job/table/path에서 수행한다.

| 시나리오 | 주입 방법 예 | 기대 결과 |
|---|---|---|
| 중복 Trigger | 같은 business key로 연속 실행 | 두 번째 409 `DUPLICATE_ACTIVE_RUN`, 첫 run 영향 없음 |
| 0건 partition | split 범위에 데이터 공백 구성 | API가 즉시 partition `SUCCESS`, PG-20 미전달 |
| 원천 0건 차단 | 빈 business key, `ALLOW.EMPTY.SOURCE=false` | `FAILED_MANIFEST`, 게시 없음 |
| chunk/HDFS 실패 | 격리 경로 권한 또는 시험용 잘못된 경로 | partition/run `FAILED_EXTRACT`, 검증 dispatch 없음 |
| API 일시 중단 | server를 재시도 시간 이내 재시작 | NiFi 재시도 후 계속 진행, 중복 상태 전이 없음 |
| snapshot expired | 충분히 오래된 SCN/짧은 undo 시험 | `FAILED_SNAPSHOT_EXPIRED`, 새 run 필요 |
| STAGING 지표 FAIL | 시험 데이터 PK 중복/타입 불일치 | `FAILED_STAGE_VALIDATION`, 게시 없음 |
| 게시 결과 불명 | 시험 target에서 잘못된 insert column | `PUBLISH_UNKNOWN`, 운영자 확정 전 새 run 차단 |
| TARGET 지표 FAIL | 격리 target 범위 불일치 | `FAILED_TARGET_VALIDATION`, 자동 재게시 없음 |
| dispatch DEAD | PG-05 route 중단 후 max attempt 축소 | DEAD 경보, 복구 후 operator resend |
| ACK 유실 | 202 뒤 Job PG 정지 | SENT→PENDING 재전송, 첫 validation만 시작 |
| REISSUE | 짧은 stale과 격리된 long query | 새 attempt 성공, 이전 token 409, 파일 중복 없음 |
| cleanup | 짧은 retention의 시험 run | 허용 경로/table만 삭제, target 유지 |

## 5. 성능과 용량 검증

### 5.1 측정 항목

- PG-10 manifest SQL 시간과 Oracle 실행 계획
- partition별 query 시간 분포와 skew
- Oracle active session 수와 CPU/I/O
- chunk 크기, 파일 수, HDFS write throughput
- NiFi repository 사용량, heap, GC, queue/back pressure
- staging/target validation query 시간
- `INSERT OVERWRITE` 시간과 HiveServer2/cluster 자원
- API request latency, DB pool 사용, lock wait
- dispatch backlog와 worker 처리량

### 5.2 병렬도 조정 순서

1. split 분포와 인덱스를 먼저 고친다.
2. `PARTITION.COUNT`를 충분히 두어 worker에 작업을 공급한다.
3. PG-20 34의 Concurrent Tasks와 `ORACLE.POOL.MAX`를 함께 조정한다.
4. `EXTRACT.FETCH.SIZE`와 `EXTRACT.ROWS.PER.FILE`을 파일 크기 기준으로 조정한다.
5. HDFS와 Oracle 중 병목 지점을 다시 측정한다.

파티션 수를 무조건 늘리면 manifest의 파티션별 COUNT query, API row 수, small file이 함께 증가한다.

### 5.3 back pressure

빌더 기본값은 모든 연결에 10,000 FlowFile / 1GB다. 운영에서는 다음을 검토한다.

- PG-10→PG-20 입력: 전체 worker 동시성의 최소 2배 이상
- 34→36: 대용량 Parquet가 오래 쌓이지 않도록 100~500개 수준부터 측정
- 재시도 중인 InvokeHTTP/PutHDFS 앞 queue의 체류량
- content repository 여유 공간과 swap 정책

## 6. 일상 운영

### 6.1 TUI

```bash
cd load-control-api
bin/monitor.sh
```

| 화면 | 주요 확인 |
|---|---|
| Dashboard | API ready, 활성/최근 run, dispatch backlog, cleanup, 경보 |
| Run detail | partition 진행률, validation 지표, dispatch, event |
| Log | run ID/request ID 필터, WARN/ERROR |

주요 키는 대시보드 `Enter`, `x`, `s`, `l`, `a`, `r`, run 상세 `s`, `p`, `l`이다. 운영 작업은
operator token이 필요하다.

### 6.2 로그

```bash
grep '<run_id>' load-control-api/logs/server.log
grep '<run_id>' load-control-api/logs/worker.log
grep 'SQOOP_REPLACEMENT' <NIFI_HOME>/logs/nifi-app.log
```

API request 한 건은 `requestId`로 수신, 서비스 판정, 응답을 묶어 추적한다. API의 `dispatchId`는 worker
로그와 PG-05 provenance를 연결한다.

### 6.3 권장 경보

| 경보 | 우선도 | 조치 시작점 |
|---|---|---|
| `PUBLISH_UNKNOWN` | 긴급 | 자동 재실행 중지, Hive 이력/target 확인 |
| `DISPATCH_DEAD` | 높음 | PG-05 port/route/Job PG 확인 |
| `FAILED_*`, `TIMED_OUT` | 높음 | run event와 단계별 원인 확인 |
| `RUN_STALE` | 높음 | NiFi queue/bulletin/Hive query 확인 |
| dispatch PENDING 증가 | 중간 | worker/DB/NiFi receiver 상태 |
| API 5xx 증가 | 높음 | PostgreSQL, pool, server 로그 |
| Oracle session/undo 임계 | 높음 | 병렬도 축소, 실행 시간 조정 |
| HDFS staging 용량 임계 | 높음 | cleanup 실패/보존 정책 확인 |

## 7. 상태별 장애 대응

| 상태 | 가능한 원인 | 확인 순서 | 원칙 |
|---|---|---|---|
| `CREATED` 정체 | SCN/manifest 전 실패, PG-10 queue | NiFi bulletin → server log → run event | PG-90 보고 또는 timeout 후 새 run |
| `EXTRACTING` 정체 | long query, worker 정지, HDFS/API 실패 | partition heartbeat → PG-20 queue → Oracle session | sweeper FAIL/REISSUE 정책 적용 |
| `EXTRACTED_VALIDATED` 정체 | dispatch/PG-05 문제 | `load_dispatch` → worker log → PG-05 | DEAD 원인 복구 후 resend |
| `STAGE_VALIDATING` 정체 | Hive DDL/query 대기 | PG-40 queue/bulletin → Hive query | 자동 상태 변경 없음, 원인 복구/실패 확정 |
| `STAGING_VALIDATED` 정체 | publish FlowFile/claim 문제 | PG-50 queue → server log | token/CAS 확인 |
| `PUBLISHING` 정체 | Hive 게시 장기 실행/결과 유실 | Hive query history → target → worker event | 시간이 지나면 `PUBLISH_UNKNOWN` |
| `PUBLISHED` 정체 | target validation 대기/실패 | PG-60 queue → TARGET 지표 | 자동 재게시 금지 |
| `PUBLISH_UNKNOWN` | 게시 성공 여부 불명 | Hive 이력과 target 독립 검증 | operator가 결과 확정 |

## 8. 오류 코드별 대응

| 코드/이벤트 | 의미 | 대응 |
|---|---|---|
| `DUPLICATE_ACTIVE_RUN` | 같은 업무 key의 활성 run 존재 | 기존 run 처리, 새 Trigger 중지 |
| `CLAIM_MISMATCH` | 이전 attempt의 늦은 보고 또는 잘못된 token | 재발행 직후면 정상, 빈발 시 흐름 조사 |
| `CHUNK_CONFLICT` | 성공 partition에 다른 chunk 내용 | HDFS replace/fragment 흐름과 중복 입력 조사 |
| `ORA-01555`, `ORA-08180` | snapshot undo 소진 | undo/실행 시간을 조정하고 새 run |
| `SQL_ERROR` | ORA code 없는 SQL/decimal 오류 | bulletin과 schema/precision 확인 |
| `HDFS_PATH_OUTSIDE_RUN` | 보고 파일이 run 경로 밖 | 35~37 attribute와 root 설정 확인 |
| `STAGE_VALIDATION_FAILED` | DDL/query/지표 실패 | STAGING 지표, timezone, decimal, bulletin |
| `PUBLISH_UNKNOWN` | 게시 결과 불명 | 운영자 확인 전 절대 자동 재게시 금지 |
| `TARGET_VALIDATION_FAILED` | target 지표 불일치 | 게시/검증 범위, target data 확인 |
| `API_UNREACHABLE` | API 연결 재시도 소진 | API/LB/DB 상태와 network 확인 |
| `CLEANUP_FAILED` | DROP/HDFS 삭제/안전 검사 실패 | 경로와 table을 확인하고 다음 주기 또는 수동 처리 |

## 9. 재처리 원칙

- manifest, extract, staging validation 실패는 설정/원인을 고친 뒤 새 run으로 전체 재실행한다.
- 일부 partition만 새 SCN으로 재실행하지 않는다.
- `REISSUE`만 같은 run/SCN의 정체 partition을 다시 보낸다.
- 게시와 target validation 실패는 자동 게시하지 않는다.
- 같은 business key의 종료 run이 있어도 새 run 생성은 가능하다.
- `PUBLISH_UNKNOWN`은 먼저 확정해야 새 run을 만들 수 있다.
- 실패 run의 HDFS 파일과 staging table은 보존 기간 동안 조사 증적으로 남긴다.

## 10. `PUBLISH_UNKNOWN` 운영 절차

1. 해당 Job의 Trigger와 수동 재실행을 중지한다.
2. run ID, publish 시작 시각, Hive query ID를 확보한다.
3. Hive query history에서 compile/submit/execute/commit 여부를 확인한다.
4. target partition의 건수와 모든 품질 지표를 source/staging과 비교한다.
5. 게시가 확실히 완료되었으면 `PUBLISHED`, 변경되지 않았거나 확실히 실패했으면 `FAILED_PUBLISH`로 확정한다.
6. 판단 근거를 resolution reason과 incident 기록에 남긴다.
7. `PUBLISHED`로 확정한 경우 PG-60이 자동 재개되지 않으므로 target 검증과 후속 상태 처리 방식을 결정한다.

모호하면 임의로 성공 처리하지 않는다. target 복구 또는 업무 승인 절차로 넘긴다.

## 11. DEAD dispatch 복구

1. `last_http_status`, `last_error`, attempt count를 확인한다.
2. API worker에서 PG-05 URL에 network 접근이 되는지 확인한다.
3. PG-05가 running이고 Job route/Output Port/root 연결이 존재하는지 확인한다.
4. Job PG와 목적 PG가 running인지 확인한다.
5. 원인을 복구한 뒤 TUI 또는 operator API로 resend한다.
6. `PENDING → SENT → ACKED`를 확인한다.

원인을 고치지 않고 resend만 반복하지 않는다.

## 12. cleanup 운영

대상 확인:

```bash
curl -fsS -H 'Authorization: Bearer <token>' \
  '<API>/v1/cleanup/candidates?jobKey=<JOB.KEY>&limit=50'
```

현재 root/prefix와 달라 안전 검사에서 거부된 오래된 run은 경로와 table을 두 번 확인한 뒤 수동 삭제하고
API에 기록할 수 있다.

```bash
hdfs dfs -rm -r '<exact-hdfsRunPath>'
beeline -e 'DROP TABLE IF EXISTS <db>.<exact-stage-table>'

curl -X POST \
  -H 'Authorization: Bearer <operator-token>' \
  -H 'Content-Type: application/json' \
  -d '{"droppedTable":"<db.table>","deletedPath":"<exact-path>"}' \
  <API>/v1/runs/<run_id>/cleanup
```

target table/partition은 cleanup 대상이 아니다.

## 13. 변경과 확장 검증

### 새 Job 추가

- 새 `JOB.KEY`와 원천/target 설정을 작성한다.
- `common_params`가 기존 Job과 같은지 diff한다.
- Flow를 생성하고 PG-05의 기존 route가 유지되는지 확인한다.
- 두 Job을 동시에 실행해 Oracle/Hive/API 용량과 상태 격리를 검증한다.

### schema 변경

- `SRC.COLUMNS`, `HIVE.STAGE.DDL.COLUMNS`, `HIVE.INSERT.COLUMNS`, target schema를 함께 변경한다.
- decimal/timestamp 변환과 Parquet schema를 시험한다.
- staging/target 지표 SQL에 사용되는 column도 확인한다.
- 기존 staging table은 run별 이름이므로 새 run과 충돌하지 않지만 target 호환성은 별도 검토한다.

### 공통 설정 변경

공통 Parameter Context 변경은 모든 Job에 영향을 준다. 변경 창을 잡고 Job Trigger를 중지한 뒤 적용하며,
NiFi가 참조 Processor/Controller Service를 잠시 중지·재기동할 수 있음을 고려한다.

## 14. 운영 준비 완료 체크리스트

- [ ] API server/worker 이중화와 PostgreSQL 백업/HA를 검증했다.
- [ ] NiFi→API, API worker→PG-05 양방향 network를 검증했다.
- [ ] Oracle 권한, undo, 인덱스, 세션 한도를 승인받았다.
- [ ] Hive timezone과 target partition 범위를 확인했다.
- [ ] 정상 E2E와 필수 장애 시나리오를 통과했다.
- [ ] `PUBLISH_UNKNOWN`과 DEAD dispatch 운영자를 지정했다.
- [ ] HDFS staging 용량과 cleanup 보존 기간을 승인받았다.
- [ ] TUI/로그/Prometheus/경보가 운영 관제에 연결되었다.
- [ ] Trigger schedule과 재실행 권한을 통제했다.
- [ ] Sqoop rollback 또는 target 복구 절차가 준비되었다.
