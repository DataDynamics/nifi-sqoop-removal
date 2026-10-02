# 운영 적용 TODO

PoC(V3 PostgreSQL 원천, V4 Oracle 원천)에서 검증한 구조를 CFM 4.12.0 운영 환경에 적용하기 위해 남은 일이다. 근거는 `nifi-sqoop-removal-guide.md`(가이드), `load-control-api-design.md`(API 설계), `poc/REVIEW.md`(PoC 결과)의 해당 절이다.

## 0. 현재 상태

| 영역 | 상태 |
|---|---|
| PG-00 Trigger, PG-10 Coordinator, PG-20 Worker, PG-05 Receiver, PG-90 Error | NiFi 2.4.0 단일 노드에서 검증(V3, V4) |
| PG-40 Staging Validation | 입구 7개만 검증. Hive staging DDL·DQ 미구현 |
| PG-50 Publish, PG-60 Target Validation | 미구현·미검증 |
| Load Control API | 구현·단위 테스트 완료. 단일 인스턴스, 평문 HTTP로만 연동 시험 |
| 시나리오 | 정상, 0건 파티션, 중복 실행, HDFS 실패, API 중단, 재발행, `NUMBER` 정밀도, `ORA-01555` 통과(REVIEW 6.2, 7.4~7.7) |

PoC와 운영 환경의 차이:

| 항목 | PoC | 운영 |
|---|---|---|
| NiFi | Apache NiFi 2.4.0 단일 노드 | CFM 4.12.0(NiFi 2.6.0) cluster |
| Oracle | 23ai Free 컨테이너 | 운영 Oracle(버전 확인 필요) |
| HDFS | `fs.defaultFS=file:///` | 실제 HDFS |
| Hive | 없음 | HiveServer2 |
| NiFi ↔ API | 평문 HTTP, Bearer 토큰 | mTLS(가이드 4장, API 설계 10.2) |
| 배포 | `poc/build_flow_v4.py`가 REST API로 생성 | Registry 또는 조직 표준 배포 절차 |

## 1. 플랫폼과 버전 확정

- [ ] CFM 4.12.0(NiFi 2.6.0)에서 V4 빌더로 Flow를 생성하고 43개 Processor가 VALID인지 확인한다. Processor 속성 이름과 relationship이 2.4.0과 다르면 빌더를 고친다
- [ ] CFM 4.12.0 배포본에 `nifi-parquet-nar`, `nifi-hadoop-nar`, `nifi-hadoop-libraries-nar`가 포함돼 있는지 확인한다(PoC에서는 따로 설치)
- [ ] CFM 4.12.0 Hive Processor·Connection Pool의 실제 이름을 확정하고 `CS_HIVE3_DBCP`에 반영한다(가이드 4장·10장, REVIEW 2장 #5)
- [ ] 운영 Oracle 버전에 맞는 ojdbc 버전을 정하고 모든 노드의 같은 경로에 배포한다(`ORACLE.JDBC.DRIVER.PATH`)
- [ ] 관리 DB PostgreSQL 버전과 JDBC 드라이버 경로를 정한다(`META.JDBC.DRIVER.PATH`)

## 2. 미구현 Flow

- [ ] PG-40 Staging Validation 나머지: Hive external staging 테이블 DDL(run root 경로), staging count·`AMOUNT_SUM`·`MIN_TS`/`MAX_TS` DQ, API 보고(가이드 10장)
- [ ] PG-50 Publish: publish claim CAS, `INSERT OVERWRITE`, `PUBLISH_UNKNOWN` 처리. 55는 재시도하지 않는다(가이드 11장)
- [ ] PG-60 Target Validation: target 지표를 source·staging과 비교하고 최종 상태를 보고한다(가이드 12장)
- [ ] 비운영 target에서 `INSERT OVERWRITE`와 `PUBLISH_UNKNOWN` 경로를 시험한다(가이드 19장 7)
- [ ] PG-05 Control Receiver를 root로 옮기고 Job별 Output Port로 나눈다(가이드 2장). 빌더는 현재 Job PG 안에 둔다
- [ ] 선택: PG-20 `ValidateRecord` + `CS_SCHEMA_REGISTRY`(승인된 target schema), PG-90 DLQ·알림(가이드 8장, 14장)
- [ ] 선택: PutHDFS 실패가 PG-90에서 `NON_RETRYABLE`로 분류된다. 운영 분류가 필요하면 PG-20의 `load.stage`를 세분한다(REVIEW 6.3 #14)

## 3. 운영 Oracle

- [ ] 조회 계정: 대상 테이블 `SELECT`·`FLASHBACK`, `SYS.V_$DATABASE` `SELECT`만 부여한다. `V$DATABASE` 권한을 받을 수 없으면 14를 `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`로 바꾼다(REVIEW 7.4)
- [ ] `UNDO_RETENTION`과 undo tablespace 크기가 run 최대 소요시간보다 넉넉한지 DBA와 확인한다. 부족하면 run이 `FAILED_SNAPSHOT_EXPIRED`로 끝난다(REVIEW 7.7)
- [ ] 대상 테이블마다 `(업무 조건 컬럼, split 컬럼)` 인덱스를 확인하고 16 manifest SQL의 실행 계획을 본다. 인덱스가 없으면 `GROUP BY`/`WIDTH_BUCKET` 방식으로 바꾼다(가이드 7.3)
- [ ] 동시 세션 수: `노드 수 × Worker Concurrent Tasks + 여유`가 승인된 세션 수 이하가 되게 `ORACLE.POOL.MAX`를 정한다(가이드 18장)
- [ ] split 컬럼에 NULL이 있는 테이블을 찾는다. V4는 NULL 파티션(`SPLIT.NULL.POLICY=SEPARATE`)을 만들지 않아 API가 manifest를 거부한다
- [ ] 운영 시간대의 원천 변경량을 확인한다. 변경이 많으면 `AS OF SCN` 조회가 느려지고 undo 사용량이 커진다

## 4. Job별 정의와 타입

- [ ] Job마다 `PC_JOB_<JOB_NAME>`을 확정한다: `SRC.OWNER`, `SRC.TABLE`, `SRC.COLUMNS`(순서 고정), `SRC.SPLIT.COLUMN`, `SRC.BASE.WHERE`, `DQ.AMOUNT.COLUMN`, `DQ.TIMESTAMP.COLUMN`, `PARTITION.COUNT`(가이드 3.2)
- [ ] 정밀도 없는 `NUMBER` 컬럼은 모두 `SRC.COLUMNS`에서 `CAST(... AS NUMBER(p,s))`로 정밀도를 명시한다. scale이 작으면 오류 없이 반올림된다(가이드 4장, REVIEW 7.6)
- [ ] Hive DDL 타입을 Parquet 결과와 맞춘다: `NUMBER(19)` → `DECIMAL(19,0)` 또는 `CAST`로 `BIGINT`, Oracle `DATE` → timestamp(REVIEW 7.4)
- [ ] CLOB·BLOB·`RAW`·`INTERVAL` 등 PoC에서 시험하지 않은 타입이 있는 테이블은 별도로 시험한다
- [ ] 시간대 표준: NiFi JVM `-Duser.timezone`과 Hive parquet timestamp 해석 설정을 정하고, 원천과 Hive에서 읽은 `MIN_TS`/`MAX_TS`가 같은지 확인한다(가이드 4장, REVIEW 2장 #6)
- [ ] 마이크로초 이하 정밀도가 필요한 컬럼은 문자열로 추출하거나 명시적 schema를 둔다(Parquet은 millis로 기록됨)

## 5. HDFS와 Hive

- [ ] `HADOOP.CONF.FILES`(core-site, hdfs-site)를 모든 노드의 같은 경로에 배포한다
- [ ] `HDFS.STAGE.ROOT` 경로, 소유자, umask(`027`)와 Hive가 읽을 수 있는 권한을 정한다. Simple 인증이므로 NiFi OS 사용자가 HDFS 사용자가 된다(가이드 8.5)
- [ ] 실패 run 경로와 오래된 성공 run 경로의 보존·정리 절차를 정한다
- [ ] HiveServer2 인증 방식과 NiFi Hive 계정 권한(staging DDL, target `INSERT OVERWRITE`)을 정한다

## 6. Load Control API 운영 배포

- [ ] API 2개 이상을 LB 뒤에 두고, worker(dispatcher + sweeper) 2개를 띄운다(API 설계 10.1)
- [ ] 관리 DB(`nifi_ops`)를 운영 PostgreSQL에 만들고 migration 전용 계정으로 `alembic upgrade head`를 실행한다
- [ ] 런타임 계정을 나눈다: API는 원장 쓰기, NiFi는 `load_event` INSERT만. 두 계정 모두 `DELETE`·`TRUNCATE`·`DROP` 없음(가이드 4.1 권한 예시)
- [ ] NiFi → API mTLS(`CS_SSL_CLIENT`), API → NiFi PG-05 mTLS(`CS_SSL_SERVER`, Client Auth=Required), 방화벽으로 PG-05 수신 포트를 제한한다(가이드 4장, API 설계 10.2)
- [ ] 토큰을 발급하고 digest를 API 설정에 넣는다. NiFi에는 Sensitive Parameter `CONTROL.API.AUTHORIZATION`으로만 둔다
- [ ] `recovery` 설정을 운영값으로 정한다: `mode=FAIL`, `run_timeout`, `extract_query_timeout`, `stale`. 재발행(`REISSUE`)을 쓸지 결정한다(가이드 13장)
- [ ] `CONTROL.API.RETRY.MAX`와 backoff 합계가 API 재기동 시간보다 길게 정한다(API 설계 10.1)
- [ ] 메트릭 수집과 알림: `DEAD` dispatch, `PUBLISH_UNKNOWN`, `TIMED_OUT`, `FAILED_*`, 5xx 증가, `PENDING` backlog 증가(API 설계 10.3)
- [ ] `load_event` 등 로그 보존 Job을 PostgreSQL scheduler나 외부 Job으로 등록한다(가이드 4.1)
- [ ] 운영자 엔드포인트(`/dispatches/{id}/resend`, `/publish-unknown/resolve`)에 operator 권한과 감사 로그를 둔다

## 7. NiFi cluster 설정과 배포

- [ ] 실행 정책: PG-00·10은 Primary Node, PG-20·05·40·50·60은 All Nodes, `partitions`·`reissue` 연결은 Round Robin(가이드 2.3, 18장)
- [ ] Concurrent Tasks와 Retry Count는 Parameter를 참조할 수 없으므로 배포 스크립트가 환경별 정수로 넣는다(가이드 2.3)
- [ ] Yield Duration, Back Pressure, Provenance 보존 기간, Bulletin 수집 연계를 정한다(가이드 15장, 18장)
- [ ] NiFi Policy를 운영자와 개발자로 나누고, Parameter Context 변경 권한을 제한한다
- [ ] 배포 방식 결정: PoC 빌더를 운영 배포 도구로 쓸지, 빌더로 만든 Flow를 Registry flow definition으로 버전 관리할지 정한다. 민감 Parameter는 flow JSON에 평문으로 넣지 않는다
- [ ] Trigger는 DISABLED로 배포하고 운영 전환 시점에 enable한다. 스케줄(PoC는 1일 Timer)을 운영 일정에 맞춘다

## 8. cluster 환경 재시험

- [ ] 운영과 같은 구성(cluster, 실제 HDFS·Hive, mTLS)의 검증 환경에서 V3·V4 시나리오를 다시 수행한다(REVIEW 6.2, 7.4)
- [ ] PoC에서 못 한 장애를 주입한다: NiFi 노드 종료·재기동(파티션 처리 중), API 인스턴스 1개 종료, worker 종료, 관리 DB 연결 차단, 검증 호출 수신 실패(가이드 19장 9)
- [ ] 2/4/8 파티션과 운영 규모 데이터로 처리 시간, API 응답 시간, run 잠금 대기, Oracle·관리 DB 부하를 측정하고 `PARTITION.COUNT`, `EXTRACT.ROWS.PER.FILE`, Concurrent Tasks를 정한다(가이드 19장 4)
- [ ] 운영 승인 조건(가이드 19장)을 항목별로 확인한다. 특히 NiFi 계정으로 원장 테이블을 변경할 수 없는지, `source = partition sum = staging = target`일 때만 SUCCESS인지

## 9. 운영 전환

- [ ] 대상 Job 목록과 전환 순서를 정한다(작은 테이블부터)
- [ ] AS-IS Sqoop과 일정 기간 병행 실행해 건수·합계·min/max·샘플 행을 비교한다. 병행 중에는 TO-BE가 운영 target을 덮어쓰지 않게 별도 target에 게시한다
- [ ] 되돌림 절차: Kylo `ImportSqoop` Flow 재활성화 방법과 판단 기준을 정한다
- [ ] 운영 Runbook 작성: 실패 run 재실행(새 `run_id`), `FAILED_SNAPSHOT_EXPIRED` 대응, `PUBLISH_UNKNOWN` 확인·확정, `DEAD` dispatch 재전송, 중복 실행 경고 확인
- [ ] 전환 후 Kylo Sqoop Flow와 관련 설정을 제거한다
