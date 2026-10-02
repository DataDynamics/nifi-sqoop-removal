# nifi-sqoop-removal-guide.md 검토 및 NiFi 2.4.0 PoC 결과

> PoC는 두 버전이다.
>
> | 버전 | 구조 | 빌더 | 장 |
> |---|---|---|---|
> | V1 | Load Control API 없음. NiFi가 `PutSQL`로 원장을 직접 기록하고 PG-30 Wait/Notify로 완료 판정. 하나의 PG에 평면 배치 | `poc/build_flow_v1.py` | 1~5장 |
> | V3 | Load Control API 연동. 원장 기록과 완료 판정은 API, NiFi는 데이터 처리. Job PG 아래 자식 PG와 Port로 구성 | `poc/build_flow_v3.py` | 6장 |
> | V4 | V3와 같은 구조에서 원천만 Oracle로 바꾼 버전(SCN 고정, `AS OF SCN`). Oracle 23ai Free 컨테이너에서 V3 시나리오를 재수행했다 | `poc/build_flow_v4.py` | 7장 |
>
> 현재 가이드는 V3 구조를 따른다.

- 검토일: 2026-10-01
- 실행 환경: Apache NiFi 2.4.0 (단일 노드, `http://10.0.1.50:10001`), PostgreSQL 16
- 대체 사항: 원천 Oracle → PostgreSQL `srcdb.app.insp_dtl`, HDFS → PutHDFS + `fs.defaultFS=file:///`
- 추가 설치: `extensions/`에 `nifi-parquet-nar`, `nifi-hadoop-nar`, `nifi-hadoop-libraries-nar` 2.4.0, `jdbc/`에 PostgreSQL·ojdbc 드라이버
- PoC Flow 생성기: `poc/build_flow_v1.py` (Process Group `SQOOP_REPLACEMENT_POC`, Processor 75개, 설정 `poc/config.v1.example.json`)
- 구현 범위: PG-00 Trigger → PG-10 Coordinator → PG-20 Worker → PG-30 Partition/Run Gate → `EXTRACTED_VALIDATED` + `_SUCCESS`, PG-90 실패/이벤트
- 미구현: PG-40 이후(Hive staging, `INSERT OVERWRITE`, Target 검증), PG-70 Recovery. 이 환경에는 Hive가 없고 Apache NiFi 2.x에는 Hive Processor도 없다.

## 1. 시험 결과

| 시나리오 | 결과 |
|---|---|
| 정상 실행 (105,000건, 8 파티션, 5,000행/파일) | `EXTRACTED_VALIDATED`, 8/8 SUCCESS, 21개 Parquet. 원천과 count·distinct·min/max·`SUM(amount)`·NULL 수 일치 |
| 0건 파티션 (seq 30001~45000 공백) | Worker로 보내지 않고 바로 SUCCESS 처리, Run Wait counter 정상 |
| 파티션 건수가 파일 행 수의 정확한 배수 (15,000/5,000) | 빈 4번째 FlowFile 없이 3파일 생성, `fragment.count=3` |
| 동일 업무일자 중복 실행 | partial unique index로 차단, 기존 활성 run 상태 변화 없음, `RUN_FAILED` 이벤트 기록 |
| 파티션 하나의 HDFS 쓰기 실패 주입 (재시도 3회 소진) | 파티션 0004만 FAILED, run `FAILED_EXTRACT`, `_SUCCESS` 미생성. Run Gate가 실패 후 0.1초 만에 확정(RUN.WAIT.TIMEOUT 대기 없음) |
| claim CAS (SQL 단위 시험) | 동일 파티션 2회 claim 시 첫 번째만 `true` |
| Trigger 검증 실패 | `RUN_FAILED`(TRIGGER/VALIDATION) 이벤트 후 종료 |

## 2. 가이드 결함: 실제로 재현했거나 확인한 항목

| # | 위치 | 문제 | 조치 및 권고 |
|---|---|---|---|
| 1 | 4.1 `claim_partition` | `started_at`이 `load_run`과 `load_partition` 양쪽에 있어 `column reference "started_at" is ambiguous` 오류로 **함수 생성이 실패**함 | 가이드 수정 완료 (`p.started_at`, `p.attempt_count`) |
| 2 | 7.2 #22, 15.1 | `PutSQL` Fragmented=true는 fragment 일부만 poll되면 FlowFile을 penalize해 되돌린다. penalized FlowFile은 다음 poll에서 빠지므로 **영구 대기(livelock)**가 발생함. 두 번째 실행에서 재현됨 (`PutSQL.java:724-726`) | 해당 PutSQL의 Penalty Duration=0 sec로 즉시 해소됨. 근본 대책은 split 전에 manifest 전체를 SQL 한 문장으로 INSERT하는 것 (fragment 의존 제거) |
| 3 | 8.1 / 8.2 #29 | `DuplicateFlowFile`에는 `success` relationship 하나뿐이다. `original`/`duplicate` 분기는 존재하지 않음 | `RouteOnAttribute ${copy.index:equals('1')}`로 분기 (PoC에서 검증) |
| 4 | 4장 `CS_DMC_CLIENT` | NiFi 2.x에서 `DistributedMapCacheClientService`가 `MapCacheClientService`로, 서버는 `MapCacheServer`로 이름이 바뀜 | 컴포넌트 이름 갱신 |
| 5 | 10·11·12장 | `PutHive3QL`/`SelectHive3QL`은 Apache NiFi 2.x에 없음 (2.4.0 목록에서 확인) | CFM 4.12.0 지원 Processor 목록에서 실제 이름(예: Cloudera Hive 계열)을 확정해야 함 |
| 6 | 4장, 8.4 | Parquet로 쓴 TZ 없는 `timestamp`가 JVM 시간대(KST) 기준으로 UTC 변환되어 `isAdjustedToUTC=true`, **millis 정밀도**로 저장됨. `2026-09-28 00:00:01` → `2026-09-27T15:00:01Z`. 건수 검증으로는 탐지되지 않음 | Hive에서 읽은 값과 원천 값을 min/max timestamp DQ로 비교. NiFi JVM `-Duser.timezone`과 Hive parquet timestamp 설정을 함께 확정. micro/nano 정밀도가 필요한 컬럼은 문자열 변환 검토 |
| 7 | 8.4 | `ExecuteSQLRecord` 기본값(Use Avro Logical Types=false)이면 DATE/TIMESTAMP/DECIMAL이 문자열로 기록됨 | `dbf-user-logical-types=true` 명시 (PoC에서 DECIMAL(18,2)/DATE 유지 확인) |
| 8 | 7.4 | Manifest 불변식(`SUM(expected)=source_count`) 검사를 언급하지만 7.1/7.2 Processor 흐름에는 해당 단계가 없음 | Manifest SQL에 `SUM() OVER()`를 추가하고 split 전에 RouteOnAttribute로 검사 (PoC 18번) |
| 9 | 2장 표, 8.4 표 | Concurrent Tasks에 `${WORKER.CONCURRENT.TASKS}`/`#{...}`를 사용함. Concurrent Tasks는 정수 스케줄 설정이라 Parameter 참조 대상이 아님 (REST DTO가 Integer) | 배포 스크립트나 환경별 값으로 직접 설정 |
| 10 | 전반 | EL 문자열 리터럴 안의 Parameter(`'#{PARTITION.COUNT}'`)는 치환되지 않음 (PoC에서 2회 재현) | Parameter 값을 UpdateAttribute로 attribute에 옮긴 뒤 EL에서 비교 |

## 3. 설계상 위험 (환경 제약으로 실행 검증은 못 함)

1. **Heartbeat 갱신 지점 없음**: `heartbeat_at`은 claim 시점과 run 생성 시점에만 갱신된다. `RECOVERY.STALE.MINUTES=15` < `EXTRACT.QUERY.TIMEOUT=60 min`이므로 정상적인 장기 run이나 파티션이 Recovery Monitor에서 stale로 판정되어 재발행될 수 있다. PoC에서는 file audit UPSERT에서 partition과 run heartbeat를 함께 갱신했다. 단일 chunk 쓰기가 15분을 넘는 경우에 대한 대책은 별도로 필요하다.
2. **Hive external table과 하위 디렉터리**: 파일이 `run_id=<id>/part=<pid>/` 하위 디렉터리에 있다. 비파티션 external table의 `LOCATION`을 run root로 잡으면 Hive 설정(`hive.mapred.supports.subdirectories`, `mapreduce.input.fileinputformat.input.dir.recursive`)에 따라 0건으로 읽힐 수 있다. 파일명이 이미 고유하므로 run root에 평탄하게 쓰는 방식을 권장한다.
3. **PutSQL CAS 결과 미확인**: PutSQL은 영향 행 수가 0이어도 success로 보낸다. 34(partition SUCCESS), 42/64(run CAS)가 실제로 갱신됐는지 흐름상 알 수 없다. 최종 판정은 DB 재조회로 보호되지만, CAS가 중요한 단계는 `UPDATE ... RETURNING`을 `ExecuteSQLRecord`로 실행해 결과를 분기하는 방식을 권장한다.
4. **실패 후 남는 control FlowFile**: 실패 파티션의 partition-control은 `PARTITION.WAIT.TIMEOUT`까지 Wait에 남는다(PoC에서 1건 관찰). 실패 경로에서 해당 chunk signal을 Notify로 해제하면 즉시 정리된다. Run Wait 쪽은 PoC처럼 실패 시 `delta=${load.partition.count}`로 Notify하면 즉시 해제된다.
5. **PUBLISH_UNKNOWN 판별**: Hive Processor의 failure relationship만으로는 "실행 전 실패"와 "응답 유실"을 구분할 수 없다. 기본값은 PUBLISH_UNKNOWN으로 두고, 명확한 parse/권한 오류만 FAILED_PUBLISH로 분류하는 것이 안전하다.
6. **PostgreSQL을 원천으로 쓰는 경우**: `AS OF SCN`에 대응하는 기능이 없다. 불변 업무 마감 조건이 필수다. 또한 pgjdbc는 autocommit=false일 때만 fetch size(cursor)를 적용하므로 `esql-auto-commit=false`가 필요하다. 이 설정이 없으면 파티션 전체를 메모리에 적재한다.
7. **확인한 정상 동작**: PutSQL 2.4는 실패 시 `error.sql.state`/`error.code`/`error.message`를 붙인다. 따라서 7.2의 "unique violation(23505)과 연결 장애 구분"은 구현 가능하다. 다만 가이드 14.6의 공통 UpdateAttribute가 같은 이름(`error.code`, `error.message`)을 덮어쓰지 않도록 주의해야 한다.

## 4. 재현 방법 (V1)

```bash
# NiFi 2.4.0 기동 후
python3 poc/build_flow_v1.py http://10.0.1.50:10001/nifi-api my-config.json   # config.v1.example.json 사본에 암호와 경로를 채운다
# Trigger(00_Generate_Trigger)는 정지 상태로 두고 Run Once로 실행
```

## 5. 가이드 반영 현황 (2026-10-01)

| 항목 | 반영 위치 |
|---|---|
| 2장 #1~#10, 3장 7번 | 2장, 3.1, 4장, 5장, 7.1~7.4, 8.1~8.4, 10.2, 15.1 (커밋 `28d7e7e`) |
| 3장 1. heartbeat | 3.1 `RECOVERY.STALE.MINUTES`(15→90), 8.5 file audit SQL에 heartbeat 갱신, 13.2 stale 기준 |
| 3장 2. Hive 하위 디렉터리 | 1장 경로 원칙, 8.5 PutHDFS Directory, README 경로. PoC 빌더도 평탄 레이아웃으로 변경 후 재실행 검증(21파일, 105,000건 일치) |
| 3장 3. PutSQL CAS 결과 | 17.1 신설, 9.2 #34·#42, 10.2 #46, 11.2 #56, 12.2 #64 |
| 3장 4. 실패 후 남은 control FlowFile | 9.2 #33 `run_failed` 경로, 9장 Wait 해제 Notify |
| 3장 5. PUBLISH_UNKNOWN 판별 | 11.2 #55A, 판별 기준 |
| 3장 6. PostgreSQL 원천 | 1장 "Oracle 이외 원천 (PostgreSQL)" |

3장 3~5번(PutSQL CAS 결과, Wait 해제, PUBLISH 판별)은 V3(API 연동 구조)에서 문제 자체가 없어졌거나 API가 처리한다(6장). 이후 가이드는 V3 구조로 다시 정리했다(커밋 `36815fe`).

## 6. V3: Load Control API 연동, 자식 PG + Port 구조 (2026-10-01)

`poc/build_flow_v3.py`로 Load Control API와 연동하는 Flow를 만들고, 책임별 자식 PG와 Input/Output Port로 나눴다. V1(75개, PG-30까지)보다 범위가 넓으면서(PG-05 수신, PG-40 입구 포함) Processor는 41개다. 모든 Processor에 한글 COMMENT로 역할을 적었다.

- 실행 환경: Apache NiFi 2.4.0(단일 노드, `http://10.0.1.50:10001`), PostgreSQL 16, Load Control API(`load-control-api`, api 1 프로세스 + worker 1 프로세스, 관리 DB `nifiops_v3`)
- 추가 설치: 1장과 같음
- 데이터: 1장과 같음(`srcdb.app.insp_dtl` 업무일자 `2026-09-28` 105,000건, seq 30001~45000 공백)
- NiFi↔API는 HTTP로 연결했다. 운영도 HTTP만 쓴다(2026-10-03 결정).

| PG | 역할 | Processor | Port |
|---|---|---:|---|
| PG-00 Trigger | 스케줄 트리거, 업무일자 형식 검증 | 3 | out: start-run, errors |
| PG-10 Run Coordinator | run 생성, 원천 지표·manifest 계산(SQL 1문장), manifest 등록 | 8 | in: start-run / out: partitions, errors |
| PG-20 Extract Worker | claim, 파티션 추출, PutHDFS, chunk 보고 | 9 | in: partitions / out: errors |
| PG-05 Control Receiver | API worker의 validate/reissue 수신 | 6 | out: validate, reissue, errors |
| PG-40 Staging Validation | `/validation/start`, `_SUCCESS` (Hive 단계 자리) | 7 | in: validate / out: errors |
| PG-90 Error and Event | 오류 정규화, run/partition 실패 보고 API, load_event 기록 | 8 | in: errors |

PG 간 연결은 상위 PG(`SQOOP_REPLACEMENT_POC_V3`)에 둔다. `partitions`(PG-10→PG-20)와 `reissue`(PG-05→PG-20)는 Round Robin load balance를 쓴다. Controller Service는 상위 PG에 두고 자식 PG가 공유한다. 자식 PG는 Parameter Context를 상속하지 않으므로 각각 지정한다. PoC는 Job이 하나라 PG-05를 상위 PG 안에 두었다(가이드는 root에 둔다).

### 6.1 Processor를 적게 쓰는 규칙

| 규칙 | 효과 |
|---|---|
| `RetryFlowFile` 대신 Processor relationship 재시도(`retryCount`, `retriedRelationships`, PENALIZE_FLOWFILE) | 재시도 loop용 Processor와 Connection이 없다 |
| 실패 지점별 `UpdateAttribute`를 두지 않음. 단계 입구의 기존 `UpdateAttribute`에서 `load.stage`를 지정하고 모든 실패를 `errors` 포트로 보내면, PG-90이 `invokehttp.*`, `executesql.error.message`, `load.stage`로 코드·메시지를 만든다 | 오류 처리가 PG-90 한 곳에 모인다 |
| 요청 본문을 `ReplaceText` 하나로 생성(EL JSON, 문자열은 `escapeJson`) | API 호출 하나가 본문 + `InvokeHTTP` 두 Processor로 끝난다 |
| 원천 지표와 manifest를 SQL 한 문장으로 계산하고 사전 검사를 API manifest 불변식 검사에 맡김 | 지표와 파티션 건수가 같은 statement snapshot 값이 된다 |
| 상태 이벤트(RUN_STARTED, EXTRACT_VALIDATED 등)는 API만 기록 | NiFi는 오류·경고만 기록하고 이벤트가 중복되지 않는다 |
| chunk 보고 응답을 분기하지 않음(판정·이벤트는 API) | Worker 끝이 `InvokeHTTP` 하나다 |

대가: PoC는 파티션 쿼리에 내장 재시도 3회를 걸어 일시 오류와 영구 오류를 구분하지 않는다. 가이드는 Oracle에서 같은 SCN으로 긴 쿼리를 반복하지 않도록 파티션 쿼리를 재시도하지 않게 정했다(가이드 16장).

### 6.2 시험 결과

| 시나리오 | 결과 |
|---|---|
| 정상 실행 | run `STAGE_VALIDATING`, 8/8 SUCCESS, 105,000건, 검증 dispatch `ACKED`(202). Parquet 21개(run root 평탄), `_SUCCESS` 0바이트. 원천과 count·distinct·min/max·`SUM(amount)`·NULL 수 일치. 이벤트 중복 없음 |
| 0건 파티션(seq 30001~45000) | API가 manifest 등록 시 SUCCESS 처리, Worker로 보내지 않음 |
| 파티션 건수가 파일 행 수의 배수(15,000/5,000) | 파티션당 3파일, 빈 FlowFile 없음 |
| 동일 업무일자 중복 실행 | PG-90이 409 + `RUN_CREATE` 단계를 `DUPLICATE_ACTIVE_RUN`(WARN)으로 분류, API 응답 본문을 message로 기록. 기존 run 상태 변화 없음 |
| 파티션 0004 HDFS 쓰기 실패 주입 | PutHDFS 내장 재시도 후 `errors` → PG-90이 파티션 실패 API 호출 → 0004 FAILED, run `FAILED_EXTRACT`, 검증 dispatch 0건, `_SUCCESS` 미생성 |

| API 중단 중 실행 | API 서버를 내린 뒤 Trigger(22:20:11), 약 30초 뒤 재기동(22:20:40). `12_Create_Run`이 연결 거부를 받고 relationship 재시도(penalty 5→10→20초)로 대기하다 22:20:47 재시도에서 run 생성. 이후 `STAGE_VALIDATING`까지 진행, 데이터 일치, NiFi 오류 이벤트 없음 |
| 파티션 재발행(sweeper `REISSUE` 모드) | API를 `mode=REISSUE`, `extract_query_timeout=PT10S`, `stale=PT20S`, `sweeper_interval=PT5S`로 띄우고 0004의 chunk 보고만 닫힌 포트로 보냄. claim 약 20초 뒤 sweeper가 0004를 `RETRY`로 초기화하고 `RECOVERY_REISSUED` 기록, `REISSUE_PARTITION` dispatch → PG-05 `/reissue` → PG-20 재 claim(attempt 2, dispatch `ACKED`) → 0004 SUCCESS → run `STAGE_VALIDATING`. 0004 파일 3개는 같은 이름으로 덮어써 Parquet 21개, 데이터 일치. 보고 경로를 되돌린 뒤 도착한 이전 시도의 chunk 보고 3건은 API가 409 `CLAIM_MISMATCH`로 거부했고, PG-90은 파티션 실패를 보고하지 않고 WARN 이벤트만 남김(run 상태 변화 없음) |

### 6.3 확인한 사항

| # | 내용 |
|---|---|
| 11 | `UpdateAttribute`는 모든 속성을 들어온 attribute 기준으로 평가한다. 같은 Processor 안에서 `partition.claim.token=${UUID()}`를 만들고 본문에서 참조하면 빈 값이 되어 claim이 422(UUID 형식 오류)를 받는다. V3는 token 생성(30)과 본문 생성(31)을 다른 Processor로 나눈다(가이드 8.2) |
| 12 | `InvokeHTTP`의 `Authorization` 동적 속성을 `Bearer #{CONTROL.API.TOKEN}`으로 쓰면 NiFi가 400으로 거부한다. Sensitive 속성은 Parameter 참조 외의 텍스트를 가질 수 없으므로 Parameter `CONTROL.API.AUTHORIZATION`에 `Bearer <token>` 전체를 두고 속성 값은 `#{CONTROL.API.AUTHORIZATION}`만 둔다(가이드 9.2) |
| 13 | `InvokeHTTP`에 `Response Body Attribute Name`을 설정하면 4xx/5xx 응답 본문도 그 attribute(`api.response`)에 들어가고 `invokehttp.response.body`는 비어 있다. 오류 메시지는 두 값을 모두 확인한다 |
| 14 | PutHDFS 실패는 오류 attribute가 없어 PG-90이 `NON_RETRYABLE`로 분류한다. 내장 재시도를 이미 소진한 뒤이므로 동작에는 영향이 없지만, 운영 분류가 필요하면 PG-20에서 `load.stage`를 더 세분한다 |
| 15 | 처음에는 PG-90이 `DUPLICATE_ACTIVE_RUN`만 WARN으로 두어, 재발행 뒤 이전 시도의 409 `CLAIM_MISMATCH`가 ERROR `CHUNK_WRITE_FAILED`로 기록됐다. 409는 모두 정상 경합이므로 90에서 `error.level=WARN`, `error.event=<응답의 $.code>`로 정하도록 고쳤다(93·95가 상태 코드를 덮어쓰므로 미리 계산). 재시험에서 `CLAIM_MISMATCH` 3건이 WARN으로 남았다(가이드 14.2) |
| 16 | 409 `CLAIM_MISMATCH` 응답이 `api.response`를 덮어쓰므로 PG-90의 `report_partition` 조건(`claimed=true`)이 성립하지 않는다. 그래서 이전 token으로 파티션 실패를 잘못 보고하지 않는다 |

### 6.4 재현 방법 (V3)

```bash
# 1. 관리 DB migration과 API·worker 실행(load-control-api/README.md)
LCA_CONFIG=config.yaml alembic upgrade head
python -m load_control.server --config config.yaml
python -m load_control.worker --config config.yaml     # nifi.receiver_url = http://<nifi-host>:<CONTROL.LISTEN.PORT>

# 2. NiFi Flow 생성(config.v3.example.json 사본에 API URL, 인증 헤더, DB, 경로를 채운다)
python3 poc/build_flow_v3.py http://<nifi-host>:<port>/nifi-api my-config.json
# 상위 PG를 시작한다. Trigger(00_Generate_Trigger)는 DISABLED로 만들어지므로 enable 후 Run Once로 실행한다

# 3. 다시 만들 때(같은 config를 주면 그 names의 PG와 Parameter Context를 지운다)
python3 poc/teardown_flow.py http://<nifi-host>:<port>/nifi-api my-config.json
```

## 7. V4: Oracle 원천 빌더 (2026-10-01 작성, 2026-10-02 시험)

`poc/build_flow_v4.py`는 V3(PostgreSQL 원천)와 같은 구조·API 계약에서 원천만 Oracle로 바꾼 빌더다. 설정 예시는 `poc/config.v4.example.json`. 작성 시점에는 dry-run만 했고(7.2), 이후 Oracle 컨테이너에서 실행 시험을 했다(7.4). 빌더는 수정 없이 동작했다.

### 7.1 V3 대비 변경점

| 위치 | 변경 |
|---|---|
| Controller Service | `CS_DBCP_ORACLE`: `oracle.jdbc.OracleDriver`, `#{ORACLE.JDBC.*}`(ojdbc 경로 포함), 검사 쿼리 `SELECT 1 FROM DUAL`, 최대 연결 `#{ORACLE.POOL.MAX}`. 이름은 가이드 3.1·4장과 같다. 관리 DB(`CS_DBCP_META`)는 PostgreSQL 그대로이며 드라이버 경로를 `#{META.JDBC.DRIVER.PATH}`로 분리 |
| PG-10 | 14 SCN 조회(`V$DATABASE`), 15 SCN 추출을 추가(Processor 8 → 10, 번호는 가이드 7.2와 같은 11~20). 16 원천 지표+manifest SQL을 Oracle 문법(`AS OF SCN`, `CONNECT BY`, `TO_CHAR`, 문자열 boolean)으로 작성하고 SCN·`MIN_TS`/`MAX_TS`도 반환. SCN이 숫자가 아니면 `INVALID_SCN`이 들어가 SQL 오류로 실패. 17 Jolt는 대문자 컬럼 키 |
| PG-20 | 33에 SCN 숫자 검사 추가. 34는 `AS OF SCN ${load.snapshot.scn}`, 재시도 없음(가이드 16장), autocommit 기본값, 정밀도 없는 `NUMBER`를 위해 Default Decimal Precision/Scale을 `#{ORACLE.NUMBER.DEFAULT.PRECISION}`/`#{ORACLE.NUMBER.DEFAULT.SCALE}`로 지정 |
| PG-05 | 09에서 재발행 본문의 `snapshotScn`, `isNullPartition`도 추출 |
| Parameter | 원천 연결은 `ORACLE.JDBC.URL/USER/PASSWORD/DRIVER.PATH`(V3의 `SRC.JDBC.*` 대신). 추가: `META.JDBC.DRIVER.PATH`, `ORACLE.POOL.MAX`, `ORACLE.NUMBER.DEFAULT.PRECISION`, `ORACLE.NUMBER.DEFAULT.SCALE`, `DQ.TIMESTAMP.COLUMN`. 제거: `JDBC.DRIVER.PATH` |
| 이름 | `SQOOP_REPLACEMENT_POC_V4`, `PC_SQOOP_REPLACEMENT_COMMON_V4`, `PC_JOB_ORACLE_INSP_DTL_DAILY_V4` |

NULL split 파티션(`SPLIT.NULL.POLICY=SEPARATE`)은 만들지 않는다. split 컬럼에 NULL이 있으면 API가 manifest를 거부한다.

### 7.2 작성 시 점검 (dry-run, NiFi에 생성하지 않음)

빌더의 쓰기 요청을 가로채고 읽기 요청(Processor 타입·정의)만 실제 NiFi 2.4.0에서 받는 dry-run으로 확인했다.

| 항목 | 결과 |
|---|---|
| 구성 | PG 7개(상위 1 + 자식 6), Processor 43개(V3 41 + SCN 2), Connection 75개, Port 13개, Label 7개 |
| 연결 | 모든 연결이 해당 Processor에 실제로 있는 relationship을 사용 |
| COMMENT | 43개 모두 있음 |
| EL | 모든 속성의 `${`·`}` 짝이 맞음 |
| Parameter | 빌더가 참조하는 Parameter가 `config.v4.example.json`에 모두 있음 |

### 7.3 Oracle 환경에서 확인할 것 (결과는 7.4)

1. 조회 계정 권한: 대상 테이블 `SELECT`·`FLASHBACK`, `V$DATABASE` 조회(없으면 14를 `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`로 변경)
2. 16 SQL의 실행 계획: 파티션마다 상관 서브쿼리가 원천을 다시 읽으므로 split 컬럼·업무 조건 인덱스 확인. 느리면 `GROUP BY`/`WIDTH_BUCKET` 방식으로 변경(가이드 7.3)
3. `ExecuteSQLRecord` 타입 매핑: 정밀도 없는 `NUMBER`, `DATE`(시각 포함), `TIMESTAMP`가 Parquet와 Hive DDL에서 의도한 타입·값인지. 특히 JVM 시간대에 따른 timestamp 변환(가이드 4장)
4. JSON writer가 `TO_CHAR` 문자열과 Oracle 대문자 컬럼명을 그대로 내보내는지, API가 manifest를 받아들이는지
5. `UNDO_RETENTION`이 run 최대 소요시간보다 긴지(`ORA-01555` → `FAILED_SNAPSHOT_EXPIRED`)
6. V3에서 수행한 시나리오(정상, 중복 실행, HDFS 실패, API 중단, 재발행) 재수행

### 7.4 Oracle 시험 결과 (2026-10-02)

- 실행 환경: NiFi 2.4.0(V3와 같음), Oracle Database 23ai Free(`gvenzl/oracle-free:23-slim` 컨테이너 `nifi-poc-oracle`, `localhost:1521/FREEPDB1`), ojdbc11 21.15, Load Control API(관리 DB `nifiops_v4`, api 18582, Control Receiver 19545)
- 데이터: `APP.INSP_DTL`에 V3 원천과 같은 데이터(업무일자 `2026-09-28` 105,000건, seq 30001~45000 공백, 다른 일자 5,000건과 NULL split 5건). 컬럼 타입은 `INSP_DTL_SEQ NUMBER(19)`, `BASE_DT DATE`, `AMOUNT NUMBER`(정밀도 없음, `SRC.COLUMNS`에서 `CAST(... AS NUMBER(18,2))`), `REG_TS TIMESTAMP`, `NOTE VARCHAR2`
- 조회 계정 `NIFI_READER`: `CREATE SESSION`, 대상 테이블 `SELECT`·`FLASHBACK`, `SYS.V_$DATABASE` `SELECT`만 부여

| 시나리오 | 결과 |
|---|---|
| 정상 실행 | 43개 Processor 모두 VALID. run `STAGE_VALIDATING`, SCN 고정(`snapshot_scn` 기록), 8/8 SUCCESS, 105,000건, 검증 dispatch `ACKED`(202). Parquet 21개, `_SUCCESS` 생성. Oracle과 count·distinct·min/max·`SUM(AMOUNT)`(71,853,075)·NOTE 비NULL 수(94,500) 일치. 원천 지표 `SOURCE_COUNT`·`AMOUNT_SUM`·`MIN_TS`·`MAX_TS`가 Oracle 값과 같음 |
| 0건 파티션 | 0002(seq 30001~45001)가 manifest 등록 시 SUCCESS, Worker로 가지 않음 |
| 동일 업무일자 중복 실행 | 409 → PG-90이 `DUPLICATE_ACTIVE_RUN`(WARN) 기록, 기존 run 변화 없음 |
| 파티션 0004 HDFS 쓰기 실패 주입(36 Directory를 0004만 `/proc/...`로) | 0004 FAILED, run `FAILED_EXTRACT`(`CHUNK_WRITE_FAILED`), 검증 dispatch 없음, `_SUCCESS` 없음 |
| API 중단 중 실행 | API 중단 후 Trigger(14:09:07), 14:09:45 재기동. `12_Create_Run`이 relationship 재시도로 대기하다 14:10:23 run 생성, `STAGE_VALIDATING`까지 진행. NiFi 오류 이벤트 없음 |
| 파티션 재발행(sweeper `REISSUE`, 6.2와 같은 설정) | 0004의 chunk 보고를 닫힌 포트로 보냄 → 약 20초 뒤 `RECOVERY_REISSUED`, `REISSUE_PARTITION` dispatch 202 → PG-05가 재발행 본문의 `snapshotScn`으로 재추출(attempt 2) → 0004 SUCCESS → `STAGE_VALIDATING`. 이전 시도의 보고 3건은 409 `CLAIM_MISMATCH`(WARN). 데이터 일치(21파일, 105,000건) |

7.3 항목별 확인 결과:

| # | 결과 |
|---|---|
| 1 | 위 권한으로 14(`V$DATABASE`)와 `AS OF SCN` 조회 모두 동작. `DBMS_FLASHBACK` 대체는 필요 없었다 |
| 2 | 16 SQL 실행 계획: 지표 CTE와 파티션별 상관 서브쿼리가 모두 `(BASE_DT, INSP_DTL_SEQ)` 인덱스 범위 스캔. 105,000건에서 0.06초. 이 인덱스가 없는 운영 테이블은 다시 확인한다 |
| 3 | Parquet 타입: `NUMBER(19)` → `decimal(19,0)`(Hive DDL도 `DECIMAL(19,0)` 또는 `CAST`로 `BIGINT`), `CAST(AMOUNT AS NUMBER(18,2))` → `decimal(18,2)`, `DATE`·`TIMESTAMP` → `timestamp[ms, UTC]`. V3와 같이 JVM 시간대(KST) 기준으로 UTC로 바뀐다(`2026-09-28 00:00:01` → `2026-09-27T15:00:01Z`, 2장 #6). Oracle `DATE`는 날짜 컬럼이어도 timestamp가 된다. 정밀도 없는 `NUMBER`를 CAST 없이 읽는 경우는 7.6 |
| 4 | JSON writer가 대문자 컬럼명과 `TO_CHAR` 문자열을 그대로 내보냈고, 17 Jolt와 API manifest 등록이 통과 |
| 5 | `ORA-01555` → `FAILED_SNAPSHOT_EXPIRED` 경로는 7.7에서 확인. `UNDO_RETENTION`이 run 최대 소요시간보다 긴지는 운영 Oracle에서 확인한다(컨테이너 기본값 900초) |
| 6 | 위 표와 같이 V3 시나리오 모두 V3와 같은 결과 |

### 7.6 정밀도 없는 `NUMBER` (2026-10-02)

원천에 정밀도 없는 `NUMBER` 컬럼 `RATE`(= seq / 3, 소수 무한)와 `BIG_NUM`(= seq × 10^17 + 7, 최대 23자리)을 추가하고 `SRC.COLUMNS`를 `..., AMOUNT, ..., RATE, BIG_NUM`(CAST 없음)으로 바꿔 34의 Default Decimal Precision/Scale 동작을 확인했다.

| 설정·데이터 | 결과 |
|---|---|
| 기본값(`ORACLE.NUMBER.DEFAULT.PRECISION=38`, `SCALE=10`) | run 성공. `AMOUNT`·`RATE`·`BIG_NUM` 모두 `decimal(38,10)`. `RATE`는 소수 10자리로 HALF_UP 반올림(`0.6666666667`)되어 합계가 Oracle `SUM(ROUND(RATE,10))`과 같고, `AMOUNT` 합계·`BIG_NUM` 값은 그대로 |
| 기본값 + 한 행의 `BIG_NUM`을 10^30 + 7로 변경 | 0004 파티션이 `AvroTypeException: Cannot encode decimal with precision 41 as max precision 38`로 실패 → PG-90 `SQL_ERROR`(NON_RETRYABLE) → run `FAILED_EXTRACT`. 정수부가 precision − scale(28)자리를 넘으면 값이 깨지지 않고 실패한다 |
| `SCALE=0` | **오류 없이 run 성공.** 모든 컬럼이 `decimal(38,0)`, `AMOUNT` 1.37 → 1, 2.74 → 3, 1368.63 → 1369(반올림). `AMOUNT` 합계 71,853,600(원천 71,853,075). 추출 단계 검증(건수)은 통과하므로 stage·target의 `AMOUNT_SUM` 비교(가이드 10.3)에서만 드러난다 |

정리: 원천 컬럼은 `CAST(... AS NUMBER(p,s))`로 정밀도를 명시하고(가이드 3.2), Default Decimal Precision/Scale은 그 밖의 컬럼을 위한 안전망으로만 둔다. scale을 작게 잡으면 소리 없이 값이 바뀐다. 가이드 3.1·4장에 반영했다.

### 7.7 `ORA-01555` (2026-10-02)

같은 SCN의 undo를 지워 34 파티션 쿼리에서 `ORA-01555`를 일으켰다.

1. PDB(local undo)의 undo를 8MB 고정 크기 `UNDO_TINY`, `UNDO_RETENTION=1`로 바꿈
2. 34를 멈춘 채 Trigger → SCN 2304014 고정, manifest 등록, 7개 파티션 claim 후 34 앞에서 대기
3. 원천 105,000행을 같은 값으로 갱신(`SET ITEM_CD = ITEM_CD`, 500행씩 commit)하고 다른 테이블 갱신으로 undo를 여러 번 순환. 이후 `AS OF SCN 2304014`로 테이블 블록을 읽는 조회가 `ORA-01555`. 인덱스만 읽는 `COUNT(*)`는 성공했다(인덱스 블록은 바뀌지 않음)
4. 34 시작

| 항목 | 결과 |
|---|---|
| 34 | 7개 파티션 모두 1회만 실행하고 `failure` → `errors`(재시도 없음, 가이드 16장) |
| PG-90 | `executesql.error.message`에서 `ORA-01555` 추출, `class=NON_RETRYABLE`, NiFi `EXTRACT_FAILED` 이벤트 7건, 파티션 실패 API 호출 7건 |
| API | 첫 실패에서 run `FAILED_SNAPSHOT_EXPIRED`(`error_stage=EXTRACT`, `error_code=ORA-01555`), 7개 파티션 `FAILED`(0건 파티션 0002는 `SUCCESS`). 검증 dispatch 0건, HDFS run 경로 없음 |
| 재실행 | undo를 되돌린 뒤 Trigger → 새 run이 새 SCN(2310932)으로 8/8 성공. 실패한 run은 그대로 남음 |

Oracle 메시지는 NLS 설정에 따라 한글로 나왔지만(`ORA-01555: 너무 이전 스냅샷: ...`) 코드 추출(`ORA-[0-9]{5}`)에는 영향이 없다.

### 7.8 Cloudera CFM NiFi 클러스터 (2026-10-03)

V4를 Cloudera CFM 4.12(NiFi 2.6.0.4.12.0.1-9) 2노드 비보안 클러스터(`rhel96-vm2`/`vm3`, UI `http://10.0.1.50:18081/nifi`는 haproxy, 노드 API `http://192.168.122.122:8080/nifi-api`)에 설치해 정상 실행을 확인했다.

- 빌더 변경: 00_Generate_Trigger를 Primary Node에서만 실행(`executionNode=PRIMARY`). 클러스터에서 Run Once는 모든 노드에서 실행되므로 그대로 두면 노드 수만큼 trigger가 생기고 두 번째부터 `DUPLICATE_ACTIVE_RUN`이 된다. 단일 노드에서는 영향 없다
- CFM 기본 배포본에 V4가 쓰는 Processor·Controller Service(Parquet, PutHDFS, HikariCP 포함)가 모두 있어 NAR 추가는 필요 없었다
- 노드 준비(두 노드 같은 경로): `/opt/nifi-poc/jdbc/`에 ojdbc11 21.15, PostgreSQL JDBC 42.7.3, `/opt/nifi-poc/conf/core-site.xml`(`fs.defaultFS=file:///`, 이 VM들에는 HDFS가 없음), `/var/lib/nifi-poc/stage`(`nifi` 소유)
- 원천 Oracle은 7.4와 같은 컨테이너(`192.168.122.1:1521/FREEPDB1`). Load Control API는 관리 DB `nifiops_cfm`, api 18583, worker metrics 9103, `nifi.receiver_url=http://192.168.122.122:19546`. PostgreSQL `pg_hba.conf`에 `192.168.122.0/24` → `nifiops_cfm` 허용 추가

| 항목 | 결과 |
|---|---|
| 생성 | 43개 Processor 모두 VALID, Controller Service 5개 ENABLED |
| 정상 실행(업무일자 `2026-09-28`) | run 1개만 생성, `STAGE_VALIDATING`. 8/8 SUCCESS(0002는 0건), 105,000건. 7개 파티션이 Round Robin으로 vm2 4개·vm3 3개에 나뉘어 추출됨(`worker_node`) |
| 결과 파일 | `file:///`라 Parquet가 처리한 노드의 로컬 디스크에 나뉘어 생기고 `_SUCCESS`는 검증 요청을 받은 vm2에만 생긴다. 운영처럼 HDFS를 쓰면 한 경로에 모인다 |

### 7.9 Hive 단계: PG-40 나머지, PG-50, PG-60 (2026-10-03)

가이드 10~12장 설계대로 PG-40의 47~4E, PG-50, PG-60을 V4 빌더에 넣고 7.8의 CFM 클러스터에서 run을 `SUCCESS`까지 실행했다. 구성은 PG 9개, Processor 68개, Connection 127개, Port 19개다.

- 시험 환경: `poc/hdfs-hive/compose.yaml`. Apache Hadoop 3.4.1 단일 NameNode·DataNode(권한 검사 끔)와 Apache Hive 4.0.1 HiveServer2(Derby metastore, Tez local, 인증 없음)를 host network의 `192.168.122.1`에 띄웠다. 7.8의 `file:///` 대신 이 HDFS(`/data/nifi/stage`)에 쓴다. Hive `hive.local.time.zone`은 NiFi 노드와 같은 `America/New_York`
- CFM Hive 구성요소: `ClouderaHiveConnectionPool`(`CS_HIVE3_DBCP`, `DBCPService` 구현), DDL·DML은 `PutClouderaHiveQL`(48, 55), 지표 조회는 `ExecuteSQLRecord` + JSON writer(49, 61). CFM의 Hive 3 JDBC client가 Hive 4.0.1 서버와 문제없이 동작했다
- target: `dw.insp_dtl`(external, Parquet, `PARTITIONED BY (base_dt STRING)`)을 미리 만들고 `INSERT OVERWRITE ... PARTITION (base_dt='<업무일자>')`로 교체
- 새 Parameter: 공통 `HIVE.JDBC.URL`·`USER`·`PASSWORD`·`POOL.MAX`·`QUERY.TIMEOUT`, Job `HIVE.STAGE.DB`, `HIVE.STAGE.DDL.COLUMNS`, `HIVE.TARGET.DB`·`TABLE`, `TARGET.PARTITION.CLAUSE`, `HIVE.INSERT.COLUMNS`, `TARGET.BUSINESS.WHERE`, `DQ.PK.COLUMN`
- PG-90: `STAGE_VALIDATION`(44 이후)과 `TARGET_VALIDATION` 실패도 `report_run`으로 보내고, 기대 상태·실패 상태를 `load.fail.expected`/`load.fail.status`로 정한다(가이드 14.2 91 표). 판정 응답(`reasons`, `changed`)이 있으면 그 응답을 이벤트 메시지로 쓴다

| 시나리오 | 결과 |
|---|---|
| 정상 실행 | run `SUCCESS`, `source = extracted = staging = target = 105,000`. STAGING·TARGET 지표 6개씩(`*_COUNT`, `NULL_SPLIT_COUNT`, `DUP_PK_COUNT`, `AMOUNT_SUM` 71,853,075, `MIN_TS` `2026-09-28 00:00:01`, `MAX_TS` `2026-09-29 09:20:00`) 모두 PASS. 검증 시작부터 `RUN_SUCCESS`까지 약 30초. staging table `stg.tmp_insp_dtl_<run_id>`, target 파티션 `base_dt=2026-09-28` 1개 |
| staging DQ 실패(`DQ.PK.COLUMN=ITEM_CD`) | `DUP_PK_COUNT` 104,983 FAIL → API `STAGING_METRIC_FAILED`(WARN), 4D `stageValidated=false` → PG-90 → run `FAILED_STAGE_VALIDATION`. 게시 없음 |
| 게시 실패(`HIVE.INSERT.COLUMNS`에 없는 컬럼) | 55 `failure`(`SemanticException`) → 56U → run `PUBLISH_UNKNOWN`. 이후 operator `/publish-unknown/resolve`(`FAILED_PUBLISH`)로 확정 |
| target 검증 실패(`TARGET.BUSINESS.WHERE`를 다른 날짜로) | 게시 후 `TARGET_COUNT` 0 등 5개 FAIL → API `TARGET_METRIC_FAILED`, 65 `success=false` → run `FAILED_TARGET_VALIDATION`. 이벤트 메시지에 API `reasons`가 남음. 재게시 없음 |
| HiveServer2 중단 중 실행 | 48이 연결 실패로 세션을 rollback하고 FlowFile을 큐에 남김(failure로 가지 않음). run은 `STAGE_VALIDATING`에서 대기. Hive 재기동(약 1.5분 뒤) 후 이어서 진행해 `SUCCESS` |

확인한 사항:

1. **`PutClouderaHiveQL`은 failure FlowFile에 오류 attribute를 남기지 않는다.** provenance에는 앞 단계의 `executesql.*`와 `query.output.tables`만 있고 Hive 오류는 bulletin에만 있다. 그래서 가이드 11.2에 적은 대로 55A·56F를 두지 않고 failure·retry를 모두 `PUBLISH_UNKNOWN`으로 보고한다
2. 연결 획득 실패는 failure·retry가 아니라 rollback이다. 48·55 모두 SQL을 제출하기 전이므로 Hive가 돌아온 뒤 실행돼도 안전하다. 대기 시간은 API sweeper(`validation_stale`, `publish_stale`)가 감시한다
3. Hive JDBC는 기본으로 결과 컬럼 이름에 table alias를 붙인다(beeline에서 `ptest.insp_dtl_seq` 형태로 확인). `HIVE.JDBC.URL`에 `?hive.resultset.use.unique.column.names=false`를 둬서 4A·62 Jolt가 소문자 이름을 그대로 받게 했다
4. API는 건수 지표 이름 `STAGE_COUNT`·`TARGET_COUNT`의 값을 `staging_count`·`target_count`로 저장한다. 가이드 10.3 예시의 `ROW_COUNT`를 고쳤다
5. 빈 결과에서 Hive `SUM`은 NULL이라 `NULL_SPLIT_COUNT`가 빈 값으로 FAIL했다(target 검증 실패 시험에서 발견). `COALESCE`로 고쳤다
6. 시간대: NiFi JVM과 Hive `hive.local.time.zone`이 같으면 Hive에서 읽은 timestamp가 Oracle 값과 같다. JDBC URL hiveconf로 세션 시간대를 `UTC`로 바꾸는 시험은 지표에 영향이 없어 결론을 내지 못했다. 서버 설정(`hive-site.xml`)으로 맞춘다
7. Hive 4.0.1 컨테이너 운영 주의: PID 파일이 `/opt/hive/conf`에 남아 `docker restart`가 실패하고, Derby metastore 기본 경로가 컨테이너 안이라 컨테이너를 다시 만들면 메타데이터가 사라진다. compose에서 PID를 tmpfs로, metastore를 볼륨으로 옮겼다. 메타데이터가 빈 동안 큐에 있던 run 하나가 `stg` DB가 없어 `FAILED_STAGE_VALIDATION`이 됐다(정상 동작)
8. Parameter Context를 REST로 바꿀 때 `inheritedParameterContexts`를 빼면 상속 해제로 해석돼 409(사용 중인 Parameter 삭제)가 난다. 클러스터에서 Trigger가 enable된 채 상위 PG를 Start하면 Trigger가 바로 실행된다(매뉴얼 5.3 주의와 같음)

### 7.10 staging table·run 경로 정리: PG-70 Cleanup (2026-10-03)

끝난 run의 staging external table과 HDFS run 경로를 보존 기간 뒤에 지우는 PG-70을 V4에 넣었다(가이드 13.3). 대상 판정은 API, 삭제는 NiFi가 한다. 구성은 PG 10개, Processor 79개, Connection 147개, Port 20개다.

- API: migration `0002_run_cleanup`(`load_run.cleaned_at`, 정리 후보 인덱스), 설정 `cleanup.success_retention`(기본 3일)·`failed_retention`(기본 14일)·`max_batch`, `GET /v1/cleanup/candidates`, `POST /v1/runs/{id}/cleanup`(role `nifi`, `operator`). 진행 중 run과 `PUBLISH_UNKNOWN`은 대상이 아니고, 기록 요청도 같은 조건을 다시 확인한다(409 `CLEANUP_NOT_DUE`). 테스트 3개 추가, 전체 119개 통과(서버 전체의 LISTEN 연결 수를 세는 dispatcher 테스트 1개는 같은 서버에서 CFM용 worker가 돌고 있어 제외)
- NiFi: 70 `GenerateFlowFile`(1시간, Primary) → 72 후보 조회 → 75 경로·table 이름 검사 → 77 `DROP TABLE IF EXISTS` → 78 `DeleteHDFS` → 7A 정리 기록. 실패는 PG-90이 `CLEANUP_FAILED` 이벤트(경로·table 이름 포함)만 남긴다

| 시나리오(시험 중 API 보존 기간 2분) | 결과 |
|---|---|
| 끝난 run 11개 정리 | 10개 `RUN_CLEANED`(SUCCESS 6, `FAILED_*` 4). HDFS run 경로 10개와 staging table이 지워지고 target `dw.insp_dtl` 105,000건은 그대로 |
| 7.8의 `file:///` 시절 run(`/var/lib/nifi-poc/stage/...`) | 경로가 현재 `HDFS.STAGE.ROOT`와 달라 75가 거부, 아무것도 지우지 않고 `CLEANUP_FAILED`. 운영자가 노드의 경로를 지우고 operator 토큰으로 `POST /cleanup` 기록 |
| manifest 단계에서 실패한 run(HDFS 경로·staging table 없음) | `DROP TABLE IF EXISTS`와 없는 경로의 `DeleteHDFS` 모두 success. `RUN_CLEANED` |
| 정리 후 후보 조회 | 빈 목록. 보존 기간 안의 최근 SUCCESS run은 대상이 아님 |
| 정상 run(PG-70 추가 후) | `SUCCESS`, 105,000건 |

확인한 사항:

1. **NiFi는 EL 문자열 리터럴 안의 Parameter 참조를 치환하지 않는다.** `${literal('#{HDFS.STAGE.ROOT}')}`는 `#{HDFS.STAGE.ROOT}` 글자 그대로였고(`toLower()` 뒤에는 `#{hive.stage.table.prefix}`), 첫 시험에서 75가 모든 run을 거부했다(삭제 없음). 비교할 값을 71 `UpdateAttribute`에서 attribute로 만들도록 고쳤다. EL 밖(SQL 문자열 등)의 `'#{X}'`는 치환된다
2. `DeleteHDFS`는 `Path`에 glob을 받는다. 그래서 API가 돌려준 경로를 그대로 쓰지 않고 75에서 `#{HDFS.STAGE.ROOT}/#{JOB.KEY}/run_id=<runId>`와 정확히 같은지 검사한다
3. `DeleteHDFS`는 없는 경로를 success로 보낸다. 정리는 중간에 실패해도 다음 주기에 처음부터 다시 하면 된다
4. `GenerateFlowFile`의 Custom Text는 빈 값이면 INVALID다

### 7.11 PG-05 root 이동과 Job 공유 (2026-10-03)

가이드 2장·9.5 구조대로 PG-05 Control Receiver를 Job PG 밖 root에 하나 두고 Job끼리 공유하게 했다.

- 빌더: root에 PG-05가 없으면 만든다(공통 Parameter Context, `CS_HTTP_CONTEXT_MAP`, Processor 6개). 있으면 PG-05를 멈추고 10에 `validate.<JOB>`·`reissue.<JOB>` route, Output Port `validate-<JOB>`·`reissue-<JOB>`, root 연결(→ Job PG `validate-in`·`reissue-in`)을 추가한 뒤 05의 Allowed Paths를 등록된 Job 목록으로 다시 쓰고 시작한다. Job PG 안에서는 `validate-in`→PG-40, `reissue-in`→PG-20(Round Robin)이다. PG-05에는 errors Port를 두지 않는다(가이드 9.5)
- 공통 Parameter Context는 지우고 다시 만들지 않고, 있으면 update request로 값만 맞춘다
- teardown: Job PG와 PG-05를 멈추고 이 Job의 root 연결·Output Port·route를 지운 뒤 Allowed Paths를 갱신한다. 마지막 Job이면 PG-05를 지우고, 공통 Context는 쓰는 PG나 상속하는 Context가 없을 때만 지운다

| 시나리오 | 결과 |
|---|---|
| Job A 생성(PG-05 새로 만듦) | root에 PG-05와 Job PG, root 연결 2개. run `SUCCESS`, 검증 dispatch `ACKED`(202) |
| Job B 추가(`ORACLE_INSP_DTL_DAILY_B`, target `dw.insp_dtl_b`) | PG-05에 route·Port 추가, Allowed Paths `/(validate|reissue)/(ORACLE_INSP_DTL_DAILY|ORACLE_INSP_DTL_DAILY_B)`. 실행 중이던 Job A는 영향 없음 |
| A·B 동시 실행 | 둘 다 `SUCCESS`(각 105,000건). B의 검증 호출은 B Job PG로 가서 `tmp_insp_dtl_b_*` staging과 `dw.insp_dtl_b`에 적재 |
| Job B 삭제 | B 등록만 지워짐. `/validate/ORACLE_INSP_DTL_DAILY_B`는 404, A 경로는 계속 수신(형식 오류 요청에 400). 공통 Context 유지 |
| Job A(마지막) 삭제 후 재생성 | PG-05와 공통 Context까지 삭제되어 root가 비었고, 다시 만든 Flow에서 run `SUCCESS` |

확인한 사항:

1. root 연결을 지우려면 양 끝(PG-05 Output Port와 Job PG Input Port)이 모두 멈춰 있어야 한다(409 `Destination of Connection ... is running`). 첫 teardown이 이 때문에 실패해 PG-05가 멈춘 채 남았다. Job PG를 먼저 멈추도록 고쳤다
2. Job을 추가하거나 지우는 동안 PG-05가 몇 초 멈춘다. 그동안 온 호출은 연결 실패가 되고 API dispatcher가 backoff 후 다시 보낸다
3. 재발행(`reissue-in`) 경로는 검증 경로와 같은 방식으로 연결했지만 이 구조에서 재발행 시나리오를 다시 돌리지는 않았다(7.4에서 PG-05가 Job PG 안에 있을 때 확인)

이름 변경: 가이드 2장·3장 이름으로 맞췄다. Job PG `SQOOP_REPLACEMENT_POC_V4` → `JOB_<JOB.KEY>`(`JOB_ORACLE_INSP_DTL_DAILY`), Job Context `PC_JOB_ORACLE_INSP_DTL_DAILY_V4` → `PC_JOB_<JOB.KEY>`, 공통 Context `PC_SQOOP_REPLACEMENT_COMMON_V4` → `PC_SQOOP_REPLACEMENT_COMMON`. 빌더와 teardown이 `JOB.KEY`로 이름을 정하므로 config에 `names`를 두지 않아도 된다. 새 이름으로 다시 만든 Flow에서 run `SUCCESS`, Job B(`JOB_ORACLE_INSP_DTL_DAILY_B`) 생성·삭제도 확인했다. 7.10 이전 기록의 이름은 당시 이름이다. V1 설정 예시에는 teardown이 V1 이름을 찾도록 `names`를 적었다(V1 빌더는 이름을 코드에 고정해 쓴다).

### 7.5 재현 방법 (V4)

```bash
# Oracle 컨테이너(재부팅 후에도 자동 시작)
docker run -d --name nifi-poc-oracle --restart unless-stopped -p 1521:1521 -e ORACLE_PASSWORD=<pw> gvenzl/oracle-free:23-slim
# APP.INSP_DTL 생성·적재와 NIFI_READER 권한은 7.4 참고
# 관리 DB·API·worker는 6.4와 같다(관리 DB와 포트만 V4용으로)
python3 poc/build_flow_v4.py http://<nifi-host>:<port>/nifi-api my-config.json   # config.v4.example.json 사본
```
