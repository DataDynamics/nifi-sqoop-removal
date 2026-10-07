# 부록 B. Oracle→Oracle 복제 Flow: ExecuteSQL + PutDatabaseRecord

이 부록은 Oracle 테이블을 NiFi에서 SQL로 조회하고, 같은 스키마로 만든 다른 Oracle 테이블에
`PutDatabaseRecord`로 적재하는 시험 Flow를 정리합니다. 조회는 `ExecuteSQL`(Avro 출력)로 합니다. timestamp
컬럼은 조회 SQL에서 `RR/MM/DD HH24:MI:SSXFF` 형식의 문자열로 바꿔 넘깁니다.

시험 환경(CFM 4.12 / NiFi 2.6, Oracle 23 Free `FREEPDB1`)에서 2026-10-07에 실행했고, 5만 건이
원본과 완전히 일치했습니다. 예시 값은 시험 환경 기준입니다.

![ExecuteSQL + PutDatabaseRecord Oracle copy flow](./assets/oracle-copy-putdatabaserecord-flow.png)

## B.1 핵심: timestamp는 NiFi가 아니라 Oracle이 변환

`CREATED_AT`을 문자열로 꺼낸 뒤 적재할 때 NiFi에서 timestamp로 다시 해석하면 안 됩니다. NiFi는 문자열을
`java.sql.Timestamp`로 바꿀 때 JVM 기본 시간대를 쓰는데, CFM 노드의 JVM 시간대는 `America/New_York`입니다.
서머타임이 시작되는 2026-03-08 02:00~03:00은 이 시간대에 존재하지 않으므로, 그 사이 값은 1시간 뒤로
밀려 저장됩니다.

처음 시도(조회는 `ExecuteSQLRecord` + JSON, 적재 Reader 스키마에서 `CREATED_AT`을 `timestamp-micros`,
Timestamp Format `yy/MM/dd HH:mm:ss.SSSSSS`로 지정)의 결과는 다음과 같았습니다. 49,998건은 일치하고
2건이 어긋났습니다.

| ID | 원본 | 적재본 |
|---:|---|---|
| 177 | 2026-03-08 02:52:36.547885 | 2026-03-08 **03**:52:36.547885 |
| 12106 | 2026-03-08 02:09:52.219361 | 2026-03-08 **03**:09:52.219361 |

그래서 다음과 같이 바꿨습니다.

1. `CREATED_AT`을 끝까지 문자열로 둡니다. `ExecuteSQL`은 `TO_CHAR` 결과(VARCHAR2)를 Avro `string`으로
   쓰고, `AvroReader`는 이 내장 스키마를 그대로 쓰므로 NiFi는 값을 해석하지 않습니다.
2. `PutDatabaseRecord`가 TIMESTAMP 컬럼에 문자열을 바인딩하면 Oracle이 세션의
   `NLS_TIMESTAMP_FORMAT`으로 암묵 변환합니다.
3. Oracle JDBC thin 드라이버는 세션 NLS를 JVM locale(`en_US` → `DD-MON-RR HH.MI.SSXFF AM`)로 정하고,
   NLS 형식을 지정하는 연결 속성이 없습니다. 그래서 logon trigger로 이 Flow의 세션에만
   `NLS_TIMESTAMP_FORMAT='RR/MM/DD HH24:MI:SSXFF'`를 적용합니다.
4. 이 Flow의 세션을 구분하려고 Connection Pool에 JDBC 연결 속성 `v$session.program`을 줍니다.
   같은 `NIFI_READER` 계정을 쓰는 V4 세션(`JDBC Thin Client`)에는 trigger가 적용되지 않습니다.

> [!IMPORTANT]
> NiFi JVM 시간대를 UTC로 바꾸면 근본 원인이 사라지지만, 클러스터 재시작이 필요하고
> "NiFi JVM 시간대 = Hive `hive.local.time.zone`" 원칙([설정 레퍼런스](./02-configuration.md)) 때문에
> Hive 설정과 V4 Flow에도 영향을 줍니다. 이 시험에서는 JVM 설정을 바꾸지 않았습니다.

## B.2 Oracle 준비

모든 문장은 `FREEPDB1`에서 관리자 권한으로 실행합니다.

### B.2.1 원천 테이블과 시험 데이터

문자열 10개와 timestamp 1개 컬럼, 행 구분용 `ID`를 가진 테이블에 5만 건을 만듭니다. `CREATED_AT`은
2026-01-01부터 약 280일 범위의 무작위 시각(마이크로초 포함)입니다.

```sql
create table APP.TMP_TEST (
  ID      number(10) primary key,
  COL01   varchar2(100), COL02 varchar2(100), COL03 varchar2(100), COL04 varchar2(100), COL05 varchar2(100),
  COL06   varchar2(100), COL07 varchar2(100), COL08 varchar2(100), COL09 varchar2(100), COL10 varchar2(100),
  CREATED_AT timestamp
);

insert /*+ append */ into APP.TMP_TEST
select level,
  'A-'||level, dbms_random.string('U',10), dbms_random.string('L',20), dbms_random.string('A',15), dbms_random.string('X',12),
  'CODE'||lpad(mod(level,100),3,'0'), dbms_random.string('U',30), '홍길동'||mod(level,1000), dbms_random.string('A',50), to_char(level,'FM00000000'),
  timestamp '2026-01-01 00:00:00' + numtodsinterval(dbms_random.value(0, 280*86400), 'SECOND')
from dual connect by level <= 50000;
commit;

grant select on APP.TMP_TEST to NIFI_READER;
```

GLOBAL TEMPORARY TABLE은 다른 세션에서 데이터가 보이지 않아 NiFi가 읽을 수 없으므로 일반 테이블로 만듭니다.

### B.2.2 대상 테이블과 권한

원천 스키마를 데이터 없이 복제하고 기본키를 겁니다. 별도 적재 계정은 만들지 않고 `NIFI_READER`에
이 테이블에 한한 쓰기 권한을 줍니다.

```sql
create table APP.TMP_TEST_COPY as select * from APP.TMP_TEST where 1=0;
alter table APP.TMP_TEST_COPY add primary key (ID);
grant select, insert, delete on APP.TMP_TEST_COPY to NIFI_READER;
```

### B.2.3 NLS logon trigger

```sql
create or replace trigger SYS.TRG_NIFI_TMP_TEST_NLS after logon on database
declare
  v_prog varchar2(100);
begin
  if sys_context('USERENV','SESSION_USER') = 'NIFI_READER' then
    select program into v_prog from v$session where sid = sys_context('USERENV','SID');
    if v_prog = 'NIFI_TMP_TEST_COPY' then
      execute immediate q'[alter session set nls_timestamp_format = 'RR/MM/DD HH24:MI:SSXFF']';
    end if;
  end if;
exception when others then null;
end;
/
```

`exception when others then null`은 trigger 오류로 모든 로그인이 막히는 것을 방지합니다. 대신 trigger가
실패하면 NLS가 적용되지 않아 적재가 변환 오류로 실패하므로 결과는 곧바로 드러납니다.

#### Connection Pool과 logon trigger

logon trigger는 Pool에서 커넥션을 빌릴 때마다 실행되는 것이 아니라, Pool이 Oracle에 물리 연결을 새로
맺어 세션이 생길 때 한 번 실행됩니다. 그때 설정한 NLS는 세션이 끝날 때까지 유지되므로, 같은 물리 연결을
여러 번 빌려 써도 계속 적용됩니다.

![Connection Pool and logon trigger](./assets/oracle-pool-logon-trigger.png)

1. HikariCP가 물리 연결을 만들 때 드라이버가 로그인 정보에 `v$session.program = NIFI_TMP_TEST_COPY`를
   함께 보냅니다. 따라서 trigger가 실행되는 시점에 `v$session.program` 값이 이미 들어 있습니다.
2. 로그인이 끝나면 trigger가 `NIFI_READER`이면서 program이 `NIFI_TMP_TEST_COPY`인 세션에만
   `ALTER SESSION SET NLS_TIMESTAMP_FORMAT`을 실행합니다.
3. Processor가 커넥션을 빌렸다가 반납해도 물리 연결은 닫히지 않고 같은 세션이 재사용됩니다. HikariCP는
   반납할 때 autoCommit, 트랜잭션 격리 수준, readOnly 같은 JDBC 상태만 되돌리고 `ALTER SESSION`으로 바꾼
   NLS는 건드리지 않습니다.
4. 연결이 수명 만료나 오류로 정리되어 Pool이 새로 만들면, 새 세션에서 trigger가 다시 실행됩니다. 따라서
   Pool 안의 모든 세션은 항상 같은 NLS를 갖습니다.

주의할 점은 다음과 같습니다.

- trigger를 만들거나 고치기 전에 이미 열려 있던 연결에는 적용되지 않습니다. trigger를 바꾼 뒤에는
  `CS_DBCP_ORACLE_RW`를 Disable→Enable해 연결을 새로 맺습니다.
- 같은 Pool을 쓰는 ExecuteSQL 세션에도 NLS가 적용됩니다. 조회 SQL은 `TO_CHAR`에 형식을 직접 지정하므로
  영향이 없습니다.
- 같은 세션에서 누군가 `ALTER SESSION`으로 NLS를 다시 바꾸면 그 값이 Pool에 남아 다음 사용에 이어집니다.
  이 Flow에는 그런 SQL이 없습니다.
- trigger가 적용되었는지는 `v$session`의 program 값(B.5)과 적재 결과로 확인합니다. NLS가 적용되지
  않았다면 세션 기본 형식(`DD-MON-RR HH.MI.SSXFF AM`)으로 `26/03/08 02:52:36.547885`를 변환하지 못해
  failure에 쌓입니다.

## B.3 NiFi 구성

루트 PG 아래 `TEST_TMP_TEST_COPY` PG를 만들고 Parameter Context `PC_SQOOP_REPLACEMENT_COMMON`을
연결합니다. 접속 정보는 이 Context의 `ORACLE.*` 파라미터를 그대로 씁니다.

### B.3.1 Controller Service

| 이름 | 유형 | 주요 속성 |
|---|---|---|
| `CS_DBCP_ORACLE_RW` | `HikariCPConnectionPool` | URL `#{ORACLE.JDBC.URL}`, Driver `oracle.jdbc.OracleDriver`, Driver Location `#{ORACLE.JDBC.DRIVER.PATH}`, User `#{ORACLE.JDBC.USER}`, Password `#{ORACLE.JDBC.PASSWORD}`, Max Total Connections `4`, Minimum Idle `0`, Validation Query `SELECT 1 FROM DUAL` |
| | | 동적 속성 `v$session.program` = `NIFI_TMP_TEST_COPY`(B.1의 4) |
| `CS_AVRO_READER` | `AvroReader` | Schema Access Strategy `Use Embedded Avro Schema`(기본값) |

`ExecuteSQL`은 Record Writer 없이 결과를 Avro로 쓰고, 스키마를 Avro 파일 안에 넣습니다. 따라서 Reader에
스키마를 따로 적을 필요가 없습니다. 컬럼 타입은 `ExecuteSQL`이 JDBC 메타데이터로 정합니다.

### B.3.2 ExecuteSQL - TMP_TEST

| 속성 | 값 |
|---|---|
| Database Connection Pooling Service | `CS_DBCP_ORACLE_RW` |
| SQL Query | 아래 |
| Max Rows Per Flow File | `10000` |
| Fetch Size | `1000` |
| Use Avro Logical Types | `false`(기본값) |
| Scheduling | Timer driven `1 day`, Execution `Primary node` |
| 자동 종료 관계 | `failure` |

```sql
SELECT ID, COL01, COL02, COL03, COL04, COL05, COL06, COL07, COL08, COL09, COL10,
       TO_CHAR(CREATED_AT, 'RR/MM/DD HH24:MI:SSXFF') AS CREATED_AT
  FROM APP.TMP_TEST
```

입력 연결이 없는 Processor라 노드마다 실행되지 않도록 Primary node로 둡니다. `CREATED_AT`은
`26/03/21 03:58:04.706016`처럼 나옵니다(`TIMESTAMP(6)`이라 소수부는 6자리).

### B.3.3 PutDatabaseRecord - TMP_TEST_COPY

| 속성 | 값 |
|---|---|
| Record Reader | `CS_AVRO_READER` |
| Database Type | `Oracle 12+` |
| Statement Type | `INSERT` |
| Database Connection Pooling Service | `CS_DBCP_ORACLE_RW` |
| Schema Name / Table Name | `APP` / `TMP_TEST_COPY` |
| Unmatched Column Behavior | `Fail on Unmatched Columns` |
| Maximum Batch Size | `1000` |
| 자동 종료 관계 | `success` |

### B.3.4 연결

| 출발 | 관계 | 도착 |
|---|---|---|
| ExecuteSQL | `success` | PutDatabaseRecord |
| PutDatabaseRecord | `failure`, `retry` | Funnel(실패 FlowFile 보관·확인용) |

## B.4 실행

1. Controller Service 두 개를 Enable합니다.
2. PutDatabaseRecord를 Start합니다.
3. ExecuteSQL에서 **Run Once**를 실행합니다.
4. PG queue가 0이고 Funnel 앞 queue가 비어 있으면 완료입니다. 시험 환경에서는 FlowFile 5개가 수 초
   안에 처리됐습니다.

다시 실행할 때는 `truncate table APP.TMP_TEST_COPY`를 먼저 합니다. `ID`가 기본키라 비우지 않으면
중복 키로 failure에 쌓입니다.

## B.5 검증

```sql
select count(*) from APP.TMP_TEST_COPY;                                                   -- 50000
select count(*) from (select * from APP.TMP_TEST      minus select * from APP.TMP_TEST_COPY); -- 0
select count(*) from (select * from APP.TMP_TEST_COPY minus select * from APP.TMP_TEST);      -- 0

-- 서머타임 갭 구간 값이 그대로인지 확인
select ID, to_char(CREATED_AT, 'YYYY-MM-DD HH24:MI:SSXFF')
  from APP.TMP_TEST_COPY where ID in (177, 12106);

-- trigger가 이 Flow 세션에만 적용되는지 확인
select program, count(*) from v$session where username = 'NIFI_READER' group by program;
```

시험 결과는 다음과 같습니다.

| 항목 | 결과 |
|---|---|
| 적재 건수 | 50,000 |
| 양방향 `MINUS` 차이 | 0 / 0 |
| ID 177, 12106 | 2026-03-08 02:52:36.547885, 02:09:52.219361(원본과 같음) |
| `NIFI_READER` 세션 | `JDBC Thin Client` 16, `NIFI_TMP_TEST_COPY` 1 |

## B.6 주의 사항

- 적재는 Oracle 세션 NLS에 의존합니다. trigger가 없거나 Pool의 `v$session.program`이 빠지면
  Oracle 날짜 변환 오류로 failure에 쌓입니다.
- 같은 원리로, NiFi에서 문자열을 timestamp로 해석해 적재하는 다른 Flow도 JVM 시간대의 서머타임 갭에서
  값이 바뀔 수 있습니다.
- `RR`은 연도 두 자리를 현재 세기 기준(00~49 → 20xx, 50~99 → 19xx)으로 해석합니다. 1950년 이전이나
  2050년 이후 값이 있으면 `YYYY` 형식을 써야 합니다.
- `NIFI_READER`는 `APP.TMP_TEST_COPY`에 한해 쓰기 권한이 있습니다. 시험이 끝나면 회수합니다.

## B.7 정리

```sql
drop trigger SYS.TRG_NIFI_TMP_TEST_NLS;
drop table APP.TMP_TEST_COPY purge;
drop table APP.TMP_TEST purge;
```

NiFi에서는 `TEST_TMP_TEST_COPY` PG의 Processor를 Stop하고 queue를 비운 뒤 Controller Service를
Disable하고 PG를 삭제합니다.
