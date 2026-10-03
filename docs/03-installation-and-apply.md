# 설치 및 적용 절차

이 장은 빈 환경에서 Load Control API와 NiFi Flow를 배포하는 순서다. 운영 변경 절차와 승인 체계에 맞춰
명령, 계정명, 경로를 조정한다.

## 1. 배포 전 체크리스트

### 1.1 네트워크

| 출발 | 도착 | 용도 |
|---|---|---|
| NiFi 모든 노드 | Oracle JDBC | SCN, manifest, 병렬 추출 |
| NiFi 모든 노드 | HDFS | Parquet 쓰기, cleanup |
| NiFi 모든 노드 | HiveServer2 | staging DDL, 검증, 게시 |
| NiFi 모든 노드 | Load Control API/LB | `/v1` 상태 보고 |
| API server/worker | PostgreSQL | 원장, outbox, LISTEN |
| API worker | NiFi/LB `CONTROL.LISTEN.PORT` | PG-05 validate/reissue 호출 |
| 운영자/모니터링 | API | TUI, health, metrics, 운영 API |

### 1.2 소프트웨어와 파일

- Cloudera CFM 4.12(NiFi 2.6)와 Hive NAR 구성요소
- 모든 NiFi 노드의 동일 경로에 ojdbc11, PostgreSQL JDBC driver
- 모든 NiFi 노드의 동일 경로에 `core-site.xml`, `hdfs-site.xml`
- API 호스트의 Python 3.12 이상
- PostgreSQL 16 권장
- NiFi REST API에 빌더가 접근 가능한 실행 호스트

### 1.3 용량과 정책

- Oracle undo 보존 시간이 최장 run보다 긴지 확인한다.
- 최악 동시 Job의 Oracle 세션 합계를 승인받는다.
- HDFS staging 보존량을 `일 적재량 × 성공/실패 보존 기간`으로 산정한다.
- PostgreSQL 백업, HA, connection 한도를 정한다.
- `FAIL`/`REISSUE`, cleanup 보존 기간, `PUBLISH_UNKNOWN` 승인자를 정한다.

## 2. Oracle 준비

Flow는 PG-10에서 현재 SCN과 원천 manifest를 조회하고, PG-20의 모든 병렬 SELECT가 같은 SCN을
`AS OF SCN`으로 다시 읽는다. 따라서 일반 SELECT 권한만으로는 충분하지 않다. 아래 준비는 다음 장애를
배포 전에 발견하기 위한 것이다.

- Oracle 접속 또는 원천 SELECT 권한 부족
- flashback 권한 부족으로 인한 `AS OF SCN` 실패
- split column의 NULL·중복·잘못된 범위
- 업무 조건과 split 범위 조회의 full scan 및 병렬 처리 지연

### 2.1 전용 읽기 계정과 권한

예시는 별도 읽기 계정을 사용하는 경우다. 적재 Flow의 권한을 업무 애플리케이션 계정과 분리하면 감사,
비밀번호 교체, 권한 회수가 쉬워지고 쓰기 권한을 주지 않아도 된다. 실제 생성과 grant는 DBA 정책에 맞춘다.

```sql
CREATE USER NIFI_READER IDENTIFIED BY <password>;
GRANT CREATE SESSION TO NIFI_READER;
GRANT SELECT ON APP.INSP_DTL TO NIFI_READER;
GRANT FLASHBACK ON APP.INSP_DTL TO NIFI_READER;
GRANT SELECT ON SYS.V_$DATABASE TO NIFI_READER;
```

| 구문 | 필요한 이유 | 사용 위치 |
|---|---|---|
| `CREATE USER` | NiFi 전용 최소 권한 계정으로 분리 | Oracle Controller Service 접속 계정 |
| `CREATE SESSION` | JDBC 연결 생성 | PG-10, PG-20 |
| 원천 table `SELECT` | manifest 집계와 실제 데이터 추출 | PG-10 16, PG-20 34 |
| 원천 table `FLASHBACK` | 모든 파티션이 동일 시점을 읽도록 `AS OF SCN` 허용 | PG-10 16, PG-20 34 |
| `SYS.V_$DATABASE SELECT` | run 시작 시 `CURRENT_SCN` 한 번 조회 | PG-10 14 |

`FLASHBACK` 권한과 충분한 undo 보존은 별개다. 권한이 있어도 실행 중 필요한 과거 블록이 사라지면
`ORA-01555` 또는 `ORA-08180`으로 실패하므로 최장 run보다 충분한 undo 보존 시간을 확보한다.
상세 원리와 산정 범위는 [Oracle SCN 상세 기술](./07-oracle-scn.md)을 참고한다.

`V$DATABASE` 권한을 줄 수 없다면 PG-10의 SCN SQL을 다음으로 바꾸고 필요한 패키지 실행 권한을 부여한다.

```sql
SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) AS SNAPSHOT_SCN FROM DUAL
```

이 쿼리도 목적은 같다. run 전체가 사용할 기준 SCN을 한 번 얻기 위한 대체 방법이며, 파티션마다 새 SCN을
조회하기 위한 것이 아니다. 어느 방법을 사용하든 PG-10 manifest와 모든 PG-20 SELECT는 반환된 하나의 SCN을
공유해야 한다.

권한 확인은 반드시 Flow에서 사용할 `NIFI_READER` 계정으로 수행한다.

```sql
SELECT TO_CHAR(CURRENT_SCN) AS SNAPSHOT_SCN FROM V$DATABASE;

SELECT COUNT(*)
  FROM APP.INSP_DTL AS OF SCN <위에서_조회한_SCN>
 WHERE BASE_DT = DATE '2026-09-28';
```

첫 쿼리는 PG-10 14의 SCN 획득 권한, 두 번째 쿼리는 원천 SELECT·FLASHBACK 권한과 기본 `AS OF SCN`
실행을 검증한다. 이 짧은 확인만으로 최장 run 동안의 undo 보존이 증명되지는 않는다. 두 쿼리 중 하나라도
실패하면 NiFi Flow를 생성하기 전에 DBA와 권한 또는 undo 정책을 수정한다.

### 2.2 split column 사전 검사

대상 업무 범위에 대해 다음을 사전 검사한다. 예제의 `INSP_DTL_SEQ`는
`SRC.SPLIT.COLUMN`이며 Sqoop의 `--split-by`와 같은 역할을 한다.

```sql
SELECT COUNT(*) AS total_rows,
       COUNT(*) - COUNT(INSP_DTL_SEQ) AS null_split_rows,
       COUNT(DISTINCT INSP_DTL_SEQ) AS distinct_split_rows,
       MIN(INSP_DTL_SEQ) AS min_split,
       MAX(INSP_DTL_SEQ) AS max_split
  FROM APP.INSP_DTL
 WHERE BASE_DT = DATE '2026-09-28';
```

각 결과를 확인하는 이유는 다음과 같다.

| 결과 | 확인 이유 | 기대값·이상 시 조치 |
|---|---|---|
| `total_rows` | API가 manifest 전체 건수와 모든 파티션 합계를 비교하는 기준 | 예상 업무 건수와 비교. 0건이면 `ALLOW.EMPTY.SOURCE` 정책 확인 |
| `null_split_rows` | 현재 빌더는 split NULL 전용 파티션을 만들지 않음 | 반드시 0. 0이 아니면 다른 split column 선택 또는 원천 정제 |
| `distinct_split_rows` | 자동 생성되는 unique split 값이 실제 업무 범위에서도 유일한지 확인 | unique 전제라면 `total_rows`와 같아야 함 |
| `min_split`, `max_split` | PG-10이 `PARTITION.COUNT`개의 숫자 범위를 만드는 양 끝값 | NULL이 아니고 예상 범위인지 확인 |

정상적인 unique·NOT NULL split column이라면
`total_rows = distinct_split_rows`이고 `null_split_rows = 0`이다. 중복이 있어도 범위 SELECT 자체는 가능하지만,
사용자가 전제한 자동 unique column과 원천 데이터가 다르다는 뜻이므로 그대로 운영하지 않는다.

이 집계는 큰 테이블에서 비용이 클 수 있다. 이미 PK/UNIQUE·NOT NULL 제약으로 보장된다면 데이터 사전의
제약 조건을 증적으로 사용할 수 있고, 실제 업무 범위의 min/max와 실행 계획은 별도로 확인한다.

### 2.3 인덱스와 실행 계획

업무 조건과 split column을 선두로 하는 인덱스가 권장된다.

```sql
CREATE INDEX APP.IX_INSP_DTL_BASE_SEQ ON APP.INSP_DTL(BASE_DT, INSP_DTL_SEQ);
```

필요한 이유는 PG-10과 PG-20의 실제 조건이 다음 두 조건을 함께 사용하기 때문이다.

```sql
WHERE BASE_DT = :business_date
  AND INSP_DTL_SEQ >= :lower_bound
  AND INSP_DTL_SEQ <  :upper_bound
```

적절한 인덱스가 없으면 PG-10의 파티션별 예상 건수 계산과 PG-20의 각 병렬 SELECT가 같은 원천 범위를
반복해서 full scan할 수 있다. `(업무 조건 컬럼, split column)` 순서는 먼저 업무일자 범위를 좁힌 뒤
split 범위를 range scan하도록 돕는다.

`IX_INSP_DTL_BASE_SEQ`는 Flow 설정에 넣는 값이 아니다. Flow에는 인덱스 객체명이 아니라
`SRC.SPLIT.COLUMN=INSP_DTL_SEQ`를 넣고, Oracle Optimizer가 실행 계획에서 인덱스를 선택한다.
파티션 테이블이면 local/global 인덱스 정책과 물리 파티션 pruning을 DBA와 함께 검토한다.

운영 테이블에 인덱스를 바로 생성하지 않는다. 여섯 테이블 각각에 대해 통계, 실행 계획, 기존 인덱스 중복,
DML·저장 공간 부하를 확인한 뒤 필요한 경우에만 생성한다. 최소한 PG-10 manifest SQL과 대표 PG-20 범위
SQL의 실행 계획에서 의도한 partition pruning 또는 index range scan이 발생하는지 확인한다.

## 3. HDFS와 Hive 준비

### 3.1 HDFS

`HDFS.STAGE.ROOT`를 만들고 NiFi 실행 사용자가 run 하위 경로를 만들고 삭제할 수 있게 한다.

```bash
hdfs dfs -mkdir -p /data/nifi/stage
hdfs dfs -ls -d /data/nifi/stage
```

운영 환경에서는 보안 정책에 맞는 owner, group, ACL을 사용한다. 저장소의 시험 compose는 HDFS 권한 검사를
끄지만 운영 권장 설정이 아니다.

### 3.2 Hive

staging DB와 target table은 Flow 배포 전에 만든다. Flow는 staging table만 run별로 만든다.

```sql
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS dw;

CREATE EXTERNAL TABLE IF NOT EXISTS dw.insp_dtl (
  INSP_DTL_SEQ DECIMAL(19,0),
  ITEM_CD STRING,
  AMOUNT DECIMAL(18,2),
  REG_TS TIMESTAMP,
  NOTE STRING
)
PARTITIONED BY (base_dt STRING)
STORED AS PARQUET;
```

NiFi JVM timezone과 `hive.local.time.zone`을 같게 설정하고, JDBC URL에는
`hive.resultset.use.unique.column.names=false`를 붙인다.

## 4. PostgreSQL과 Load Control API 설치

### 4.1 계정 원칙

권장 역할은 다음과 같다.

- migration 계정: `nifi_ops` DDL 수행
- API 계정: 원장 SELECT/INSERT/UPDATE, DELETE/DDL 없음
- NiFi 계정: `load_event` INSERT만

역할을 먼저 만들고 migration을 실행하면 migration이 필요한 grant도 적용한다. 정확한 DDL 원본은
`load-control-api/src/migrations/versions/`다.

### 4.2 설치

```bash
cd load-control-api

# 인터넷이 되는 환경
bin/install.sh --online

# 또는 air-gap: 인터넷 장비에서 먼저 실행
# bin/download-packages.sh
# packages/를 포함한 전체 디렉터리를 옮긴 뒤 대상에서 bin/install.sh

cp config/config.example.yaml config/config.yaml
chmod 600 config/config.yaml
```

`config.yaml`에서 최소한 다음을 채운다.

- `database.url`, `database.migration_url`, `database.listen_dsn`
- `auth.token_digests.nifi`, `auth.token_digests.operator`
- `nifi.receiver_url`
- `recovery.extract_query_timeout`, `recovery.stale`
- 운영 방식에 맞는 로그와 cleanup 보존 기간

`auth.token_digests.nifi`는 NiFi 로그인 token이 아니다. NiFi가 인증 없이 실행되는 시험 환경에서도
Load Control API용 난수 token을 별도로 생성한다. 원문은 NiFi 공통 Parameter
`CONTROL.API.AUTHORIZATION=Bearer <token>`에 넣고, SHA-256 digest만 API `config.yaml`에 넣는다.
6개 Job은 이 공통 token 하나를 공유한다. 생성과 digest 등록 절차는
[설정 레퍼런스의 auth 절](./02-configuration.md#83-auth)을 따른다.

### 4.3 migration과 기동

```bash
bin/migrate.sh
bin/migrate.sh current
bin/start.sh
bin/status.sh --wait 30
curl -fsS http://127.0.0.1:8080/readyz
```

정상 응답:

```json
{"status":"ok"}
```

systemd를 사용할 때는 bin 방식과 섞지 않는다.

```bash
bin/stop.sh
sudo bin/systemd/install.sh <service-user>
sudo systemctl start load-control-api load-control-worker
systemctl status load-control-api load-control-worker
```

### 4.4 API 설치 검증

```bash
curl -fsS http://<api-host>:8080/healthz
curl -fsS http://<api-host>:8080/readyz
curl -fsS -H 'Authorization: Bearer <nifi-token>' \
  'http://<api-host>:8080/v1/runs?limit=1'
curl -fsS http://<api-host>:8080/metrics | head
curl -fsS http://<worker-host>:9100/metrics | head
```

확인할 로그:

```bash
tail -f logs/server.log logs/worker.log
```

## 5. NiFi 사전 준비

1. 모든 노드에 JDBC driver와 Hadoop 설정 파일을 배포한다.
2. `CONTROL.LISTEN.PORT`가 비어 있고 API worker에서 접근 가능한지 확인한다.
3. CFM의 `ClouderaHiveConnectionPool`, `PutClouderaHiveQL` 타입이 설치되어 있는지 확인한다.
4. NiFi 노드와 Hive timezone을 맞춘다.
5. NiFi REST API URL을 확인한다. 예: `http://nifi-host:8080/nifi-api`.
6. 기존에 같은 이름의 Job PG나 Job Parameter Context가 없는지 확인한다.

빌더는 `/flow/processor-types`와 `/flow/controller-service-types`를 조회하여 실제 설치된 bundle/type을
사용한다. CFM Hive type이 없으면 빌드를 중단한다.

## 6. Flow 설정 파일 작성

```bash
cp nifi-flow/job-config.example.json /secure/path/job-insp-dtl.json
chmod 600 /secure/path/job-insp-dtl.json
```

[설정 레퍼런스](./02-configuration.md)에 따라 공통값과 Job값을 채운다. 배포 전 다음을 별도 리뷰한다.

- `SRC.BASE.WHERE`가 의도한 업무 범위만 선택하는가
- `SRC.COLUMNS`와 `HIVE.STAGE.DDL.COLUMNS`가 순서와 타입까지 일치하는가
- `TARGET.PARTITION.CLAUSE`가 table 전체 overwrite가 아닌가
- `TARGET.BUSINESS.WHERE`가 게시 범위와 같은가
- API token, JDBC password, driver path가 올바른가
- 여러 Job의 `common_params`가 동일한가

## 7. Flow 생성

> [!IMPORTANT]
> 이 절차는 설정 파일 하나당 Job PG 전체(PG-00·10·20·40·50·60·70·90)를 하나 생성한다. 원천 Oracle
> 테이블이 6개면 서로 다른 `JOB.KEY`와 설정 파일로 이 절차를 6번 수행해야 한다. PG-05만 root에서
> 공유한다. 빌더가 생성을 자동화하지만 한 Processor 세트를 6개 테이블이 공유하는 구조는 아니다.

저장소 root에서 실행한다.

```bash
python3 nifi-flow/deploy_job_flow.py \
  http://<nifi-host>:<port>/nifi-api \
  /secure/path/job-insp-dtl.json \
  > /secure/path/job-insp-dtl-flow-ids.json
```

빌더가 수행하는 작업:

1. NiFi에 설치된 Processor/Controller Service 타입을 조회한다.
2. 공통 Parameter Context를 생성하거나 제공된 값으로 갱신한다.
3. Job Parameter Context를 생성하고 공통 Context를 상속한다.
4. Job PG, 8개 자식 PG, Input/Output Port, 연결을 만든다.
5. Oracle, PostgreSQL, Hive pool과 JSON/Parquet writer를 생성·enable한다.
6. 공유 PG-05를 만들거나 기존 PG-05에 Job route를 등록한다.
7. PG-05를 실행 상태로 되돌린다.
8. Trigger 00은 `DISABLED`로 유지하고 생성된 ID를 JSON으로 출력한다.

중요한 동작:

- 빌드는 트랜잭션이 아니다. 중간 실패를 자동 rollback하지 않는다.
- 같은 Job PG가 있으면 덮어쓰지 않고 중단한다.
- 기존 공통 Context를 갱신할 때 이를 참조하는 구성요소가 NiFi에 의해 잠시 정지될 수 있다.
- PG-05에 Job을 등록할 때 PG-05가 잠시 멈춘다. 그동안 실패한 API worker 호출은 재시도된다.

중간 실패나 재배포 시 먼저 기존 Job Flow를 제거한다.

```bash
python3 nifi-flow/remove_job_flow.py \
  http://<nifi-host>:<port>/nifi-api \
  /secure/path/job-insp-dtl.json
```

제거 스크립트는 Job PG와 Job Context 및 PG-05의 해당 Job route만 지운다. PostgreSQL 원장, HDFS 산출물,
Hive target은 지우지 않는다. 마지막 Job이면 공유 PG-05와 공통 Context도 제거한다.

## 8. 생성 직후 UI 점검

### 8.1 Job PG

- 자식 PG 8개가 모두 보이는가
- `validate-in`, `reissue-in` Input Port가 연결되어 있는가
- PG-10→PG-20과 reissue-in→PG-20 연결이 Round Robin인가
- 모든 Controller Service가 enabled인가
- Processor에 invalid 표시가 없는가
- 00 Trigger가 disabled인가

### 8.2 Parameter Context

- Job Context가 공통 Context를 상속하는가
- password/authorization 값이 sensitive인가
- Job별 값이 다른 Job Context에 섞이지 않았는가

자식 PG는 부모 Context를 자동 상속하지 않으므로 빌더가 각 자식 PG에 같은 Job Context를 지정한다. UI에서
Context가 빠진 PG가 없는지 확인한다.

### 8.3 PG-05

- root에 공유 PG-05가 하나만 있는가
- `Allowed Paths`가 현재 등록된 Job 목록을 포함하는가
- `validate-<JOB>`, `reissue-<JOB>` Output Port가 해당 Job Input Port로 연결되는가
- `CS_HTTP_CONTEXT_MAP`이 enabled인가
- PG-05가 running인가

등록되지 않은 경로가 404인지 확인하는 것은 안전한 수신 smoke test다.

```bash
curl -i -X POST http://<nifi-or-lb>:<CONTROL.LISTEN.PORT>/not-registered
```

등록된 validate/reissue 경로에는 임의 요청을 보내지 않는다. 유효한 형태면 실제 Job FlowFile이 만들어질 수
있으므로 정상 run의 API dispatch로 시험한다.

## 9. 첫 실행

1. Job PG를 Start한다. 00은 disabled라 run이 시작되지 않는다.
2. PG-70이 시작 직후 실행될 수 있으므로 cleanup API와 queue를 확인한다.
3. Job Context의 `BUSINESS.KEY`를 비운영 시험 일자로 설정한다.
4. 00 Trigger를 enable한다.
5. 00에서 **Run Once**를 실행한다.
6. 즉시 Trigger를 다시 disable하여 오실행을 막는다.
7. [PG별 동작 원리와 검증](./04-process-groups.md)에 따라 단계별로 확인한다.

Job PG를 다시 Start할 때 Trigger가 enabled이면 즉시 새 run이 생길 수 있다. 수동 검증 기간에는 항상
disabled를 기본 상태로 둔다.

## 10. 정기 실행 전환

- Trigger schedule을 운영 시간으로 변경한다.
- 업무일자를 Parameter로 수동 변경할지 NiFi EL로 자동 계산할지 결정한다.
- 자동 계산 예: 전일

```text
${now():toNumber():minus(86400000):format('yyyy-MM-dd')}
```

- Primary Node 변경 시에도 중복 run이 차단되는지 시험한다.
- 운영 알림과 대시보드가 `FAILED_*`, `TIMED_OUT`, `PUBLISH_UNKNOWN`, `DISPATCH_DEAD`를 수집하는지 확인한다.
- 배포 commit, 설정 checksum, flow ID JSON, 검증 run ID를 변경 기록에 남긴다.

## 11. 롤백 원칙

Flow 생성 전에는 기존 Sqoop schedule을 유지한다. 새 Flow 검증 중에는 target의 동일 파티션을 Sqoop과
NiFi가 동시에 게시하지 않게 한다.

전환 실패 시:

1. NiFi Trigger를 disable하고 Job PG를 stop한다.
2. 진행 중 run과 게시 여부를 API/TUI에서 확인한다.
3. `PUBLISH_UNKNOWN`이면 자동 재실행하지 말고 Hive 이력과 target을 확인한다.
4. 필요하면 `remove_job_flow.py`로 Flow만 제거한다.
5. target 정합성을 확인한 뒤 기존 Sqoop schedule을 복구한다.

이미 게시된 target을 되돌리는 작업은 이 프로젝트가 자동으로 수행하지 않는다. 원본 snapshot이나 기존
파티션 백업 등 별도의 업무 복구 절차가 필요하다.
