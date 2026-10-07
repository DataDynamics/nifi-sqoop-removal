# 부록 A. 운영 조회 도구 사용법

이 부록은 Load Control API 설치 디렉터리의 `bin/`에 있는 세 조회 도구의 사용법을 다룹니다. 운영자는 이
도구로 적재 결과를 원천(Oracle), HDFS chunk, Hive staging·target에서 직접 확인합니다. 세 도구 모두 psql처럼
대화형으로 쓸 수 있고 `-c`·`-f`로 일괄 실행할 수도 있습니다. sqlplus, beeline, hadoop 클라이언트(JVM)를 따로
설치하지 않아도 API 호스트에서 바로 실행됩니다.

| 도구 | 대상 | 접속 방식 | 주 용도 |
|---|---|---|---|
| `bin/oracle.sh` | 원천 Oracle | python-oracledb thin 모드 | `AS OF SCN`으로 NiFi가 추출한 시점의 원천 재조회 |
| `bin/hive.sh` | HiveServer2 | Thrift(impyla) | staging external table과 target 건수·합계·중복 확인 |
| `bin/hdfs.sh` | HDFS | WebHDFS REST | run 경로의 Parquet chunk, `_SUCCESS`, 정리 여부 확인 |

모든 예시는 설치 디렉터리(`load-control-api/`)에서 실행합니다. 예시 결과는 시험 환경에서 실제로 실행한 출력입니다.

## A.1 준비

### A.1.1 설치 확인

조회 도구의 의존 패키지(`oracledb`, `impyla` 등)는 API와 같은 `packages/` wheel 묶음에 들어 있습니다.
`bin/install.sh`를 실행했다면 추가 설치는 없습니다. 기존 설치를 갱신한 경우에는 `bin/install.sh`를 다시
실행합니다.

```bash
.venv/bin/python -c "import oracledb, impala.dbapi; print('ok')"
```

### A.1.2 접속 정보

접속 정보는 모두 `config/config.yaml`의 `clients` 섹션에 적습니다. 도구는 실행할 때마다 이 섹션만 읽으므로
환경변수나 Hadoop 설정 파일(`core-site.xml`, `hdfs-site.xml`)을 따로 준비하지 않습니다. API와 같은 파일을
쓰고, 비밀번호도 평문으로 적습니다(파일 권한 600). 쓰지 않는 도구는 `null`로 두거나 생략합니다.

다음은 모든 키를 적은 예입니다. 기본값과 같은 키는 생략해도 됩니다.

```yaml
clients:
  oracle:
    dsn: oracle-host:1521/ORCLPDB   # host:port/service_name
    user: NIFI_READER               # 원천 조회 계정(읽기 전용 권한 계정 권장)
    password: CHANGE_ME
    current_schema: APP             # 스키마 없이 쓴 이름을 이 스키마에서 찾는다
    call_timeout: PT10M             # 문장 하나 최대 실행 시간
    arraysize: 1000                 # fetch 단위
  hive:
    host: hs2-host
    port: 10000
    database: default               # 처음 USE할 database
    user: nifi
    password: CHANGE_ME             # auth가 LDAP·CUSTOM이면 필수. NONE이면 서버가 검사하지 않는다
    auth: NONE                      # 서버의 hive.server2.authentication: NONE, LDAP, CUSTOM, NOSASL
    transport: binary               # 서버의 hive.server2.transport.mode: binary, http
    http_path: cliservice           # transport가 http일 때 hive.server2.thrift.http.path
    connect_timeout: 30             # 연결 대기 초
    query_timeout: PT30M            # 문장마다 hive.query.timeout.seconds로 보낸다
  hdfs:
    namenode_urls:                  # NameNode WebHDFS 주소. HA면 둘 다(standby는 건너뛴다)
      - http://nn1:9870
      - http://nn2:9870
    user: nifi                      # simple 인증 user.name
    home: /data/nifi/stage          # 대화형 시작 디렉터리, 상대 경로의 기준
    datanode_hosts: {}              # DataNode 이름이 안 풀릴 때 {이름: IP}(A.5.4)
    timeout_seconds: 30             # HTTP timeout 초
```

| 키 | 필수·기본 | 값을 확인하는 곳 |
|---|---|---|
| `oracle.dsn` | 필수 | 원천 DB의 `호스트:포트/service_name`. NiFi `ORACLE.JDBC.URL`의 `jdbc:oracle:thin:@//` 뒤 부분과 같습니다 |
| `oracle.user`, `password` | 필수 | 원천 조회 계정 |
| `oracle.current_schema` | `null` | 원천 테이블 소유 스키마(예: `APP`) |
| `hive.host`, `port` | 필수, `10000` | HiveServer2 주소. NiFi `HIVE.JDBC.URL`(`jdbc:hive2://호스트:포트/db`)과 같습니다 |
| `hive.user`, `password` | `nifi`, `null` | HS2 접속 계정. LDAP·CUSTOM 인증 서버면 그 계정의 비밀번호 |
| `hive.auth`, `transport`, `http_path` | `NONE`, `binary`, `cliservice` | HS2 `hive-site.xml`의 `hive.server2.authentication`, `hive.server2.transport.mode`, `hive.server2.thrift.http.path`([A.4.4](#a44-인증)) |
| `hdfs.namenode_urls` | 필수 | NameNode Web UI 주소. `hdfs-site.xml`의 `dfs.namenode.http-address`(HA면 `dfs.namenode.http-address.<nameservice>.<nn>` 전부) |
| `hdfs.user` | `nifi` | NiFi가 HDFS에 쓰는 사용자 |
| `hdfs.home` | `/` | NiFi `HDFS.STAGE.ROOT` |

항목별 설명은 [설정 레퍼런스 8.8](./02-configuration.md)과 `config/config.example.yaml`에도 있습니다.

설정 파일을 고치지 않고 한 번만 다른 대상에 접속할 때는 명령행 옵션을 씁니다. 옵션은 `clients`의 같은
항목만 덮어쓰고, 나머지는 설정 파일 값을 그대로 씁니다.

```bash
bin/oracle.sh --dsn other-host:1521/SVC --user APP_READER -W        # -W: 비밀번호를 물어본다
bin/hive.sh --host other-hs2 --user etl_reader -W -d stg
bin/hdfs.sh --url http://nn2:9870 ls /
```

### A.1.3 접속 확인

```console
$ bin/oracle.sh -c '\conninfo' -c 'select 1 from dual'
oracle NIFI_READER@127.0.0.1:1521/FREEPDB1, Oracle 23.26.3.0.0, 읽기 전용
 1
---
 1
(1행)

$ bin/hive.sh -c 'show databases'
 database_name
---------------
 default
 dw
 stg
(3행)

$ bin/hdfs.sh pwd
/data/nifi/stage
```

세 명령 모두 종료 코드 0이면 준비가 끝난 것입니다. 실패할 때의 메시지와 조치는 [A.8 문제 해결](#a8-문제-해결)에 있습니다.

## A.2 공통 사용법

### A.2.1 실행 방식

| 실행 | 동작 |
|---|---|
| `bin/oracle.sh` (인자 없이 터미널에서) | 대화형. 줄 편집·이력(`~/.lca_<도구>_history`)을 지원합니다 |
| `bin/oracle.sh -c '문장' -c '문장'` | 순서대로 실행하고 첫 오류에서 멈춥니다. `;`가 없는 마지막 문장도 실행합니다 |
| `bin/oracle.sh -f check.sql` | 파일을 실행합니다. `-f -`는 표준입력입니다. `-c`와 섞으면 입력한 순서대로 실행합니다 |
| `echo 'select 1 from dual' \| bin/oracle.sh` | 파이프로 받은 입력을 실행합니다 |
| `bin/hdfs.sh ls -h /data` | HDFS 명령 하나를 실행합니다(HDFS만 해당) |

대화형 SQL 셸은 문장이 `;`로 끝날 때까지 여러 줄을 받습니다. 이어서 입력하는 중에는 프롬프트가 `=>`에서
`->`로 바뀝니다. Ctrl-C는 입력 중인 문장을 버리고, 실행 중이면 서버에서 문장을 취소합니다. Ctrl-D나 `\q`로
끝냅니다(HDFS는 `exit`도 쓸 수 있습니다).

### A.2.2 공통 옵션

| 옵션 | 설명 |
|---|---|
| `-F table\|vertical\|csv\|tsv\|json` | 출력 형식(기본 table) |
| `-t`, `--no-header` | 열 이름 머리글을 뺍니다. 값 하나만 받을 때 `-t -F tsv`와 함께 씁니다 |
| `--max-rows N` | SQL 문장마다 최대 출력 행 수. 기본 1000이고 0이면 제한이 없습니다. 잘리면 안내를 출력합니다 |
| `--timing` | 문장마다 실행 시간을 보여 줍니다 |
| `--echo` | 실행 전에 문장을 출력합니다(`-f` 실행 기록용) |
| `--null 문자열` | table·vertical에서 NULL을 이 문자열로 보여 줍니다(기본 빈칸) |
| `--write` | 쓰기를 허용합니다([A.6](#a6-읽기-전용과-쓰기-모드)) |

`--max-rows`, `--timing`, `--echo`, `--null`은 SQL 도구(oracle, hive)에만 있습니다.

### A.2.3 출력 형식

| 형식 | 쓰임 | 예 |
|---|---|---|
| `table` | 사람이 읽는 기본 표. 숫자는 오른쪽 정렬, 한글은 2칸 폭으로 맞춥니다 | `bin/hive.sh -c "..."` |
| `vertical` | 열이 많은 행을 `열 \| 값` 세로로 보여 줍니다(대화형 `\x`) | `bin/oracle.sh -F vertical -c "select * from insp_dtl fetch first 1 rows only"` |
| `csv` | 엑셀·다른 도구로 넘깁니다(RFC 4180) | `bin/hive.sh -F csv --max-rows 0 -c "..." > out.csv` |
| `tsv` | 셸 스크립트에서 값만 꺼냅니다. 값 안의 탭·줄바꿈은 `\t`, `\n`으로 이스케이프합니다 | `bin/oracle.sh -t -F tsv -c "select count(*) ..."` |
| `json` | 객체 배열. Decimal은 정밀도를 지키려고 문자열로 냅니다 | `bin/hdfs.sh -F json count /data/nifi/stage` |

csv·tsv·json에서는 행 수, 잘림, 실행 시간 같은 안내를 표준오류로 보냅니다. 그래서 표준출력을 파일이나
파이프로 넘겨도 결과만 남습니다.

### A.2.4 종료 코드

| 코드 | 의미 | 예 |
|---:|---|---|
| 0 | 성공 | |
| 1 | 실행 오류 | `ORA-00942`, Hive `SemanticException`, HDFS `FileNotFoundException` |
| 2 | 사용법·설정·접속 오류 | `clients` 설정 없음, 잘못된 옵션, `ORA-01017`, HS2·NameNode 연결 실패 |
| 3 | 거부 | 읽기 전용 모드에서 쓰기 문장·명령, `--write`여도 보호 경로 삭제 |
| 130 | Ctrl-C로 취소 | |

## A.3 Oracle: bin/oracle.sh

### A.3.1 대화형 예

```console
$ bin/oracle.sh
oracle NIFI_READER@127.0.0.1:1521/FREEPDB1, Oracle 23.26.3.0.0, 읽기 전용
도움말 \?, 종료 \q
oracle:NIFI_READER=> select count(*)
oracle:NIFI_READER-> from insp_dtl;
 COUNT(*)
----------
   110000
(1행)
oracle:NIFI_READER=> \x
확장 출력 켬
oracle:NIFI_READER=> select * from insp_dtl fetch first 1 rows only;
-[ RECORD 1 ]----
INSP_DTL_SEQ |
BASE_DT      | 2026-09-27 00:00:00
ITEM_CD      | X
AMOUNT       | 1
REG_TS       | 2026-10-02 05:03:51.078321
NOTE         |
...
oracle:NIFI_READER=> \q
```

### A.3.2 메타 명령

| 명령 | 동작 |
|---|---|
| `\dt [패턴]` | 테이블·뷰·materialized view·synonym 목록과 통계 행 수(`NUM_ROWS`). Oracle 관리 계정은 뺍니다. 예: `\dt APP.*`, `\dt INSP*` |
| `\d 이름`, `DESC 이름` | 열 번호, 이름, 타입(`NUMBER(18,2)`, `VARCHAR2(100 CHAR)`), NOT NULL |
| `\d+ 이름` | 위 내용에 기본값과 열 주석을 더합니다 |
| `\dn [패턴]` | 스키마(사용자) 목록 |
| `\scn` | 현재 SCN. `v$database` 권한이 없으면 `TIMESTAMP_TO_SCN(SYSTIMESTAMP)`로 근사합니다 |
| `\x`, `\format 형식`, `\t`, `\timing`, `\maxrows N` | 출력 설정 |
| `\o 파일`, `\o` | 결과를 파일로 쓴다 / 화면으로 되돌립니다 |
| `\i 파일` | SQL 파일을 실행합니다 |
| `\conninfo`, `\?`, `\q` | 접속 정보, 도움말, 종료 |

이름은 Oracle 규칙대로 대문자로 바꿉니다. 소문자나 특수문자가 들어간 이름은 `"Name"`처럼 큰따옴표로
감쌉니다. 스키마를 생략했는데 `current_schema`도 없고 같은 이름이 여러 스키마에 있으면 스키마를 붙이라고
알려 줍니다.

```console
$ bin/oracle.sh -c '\d insp_dtl'
 # | column       | type                | null
---+--------------+---------------------+----------
 1 | INSP_DTL_SEQ | NUMBER(19)          |
 2 | BASE_DT      | DATE                | NOT NULL
 3 | ITEM_CD      | VARCHAR2(10 BYTE)   | NOT NULL
 4 | AMOUNT       | NUMBER              | NOT NULL
 5 | REG_TS       | TIMESTAMP(6)        | NOT NULL
 6 | NOTE         | VARCHAR2(4000 BYTE) |
...
```

### A.3.3 SCN 시점 조회

NiFi는 run마다 SCN 하나(`snapshot_scn`)를 고정하고 모든 파티션을 `AS OF SCN`으로 읽습니다. 같은 SCN으로
조회하면 NiFi가 본 원천과 같은 데이터를 다시 볼 수 있습니다. 원리는 [Oracle SCN 상세 기술](./07-oracle-scn.md)에
있습니다.

```console
$ bin/oracle.sh -c "SELECT COUNT(*) cnt, SUM(amount) amount_sum FROM insp_dtl AS OF SCN 2390399
                    WHERE base_dt = DATE '2026-09-28'"
 CNT    | AMOUNT_SUM
--------+------------
 105000 |   71853075
(1행)
```

`ORA-01555`(snapshot too old)나 `ORA-08181`(유효하지 않은 SCN)이 나면 그 시점의 undo가 남아 있지 않은
것입니다. `\scn`으로 현재 SCN을 확인하고 run의 `snapshot_scn`과 비교합니다. 이때는 같은 시점으로 대조할 수
없으므로 Hive target 기준으로만 확인합니다.

### A.3.4 PL/SQL과 쓰기

PL/SQL 블록(BEGIN, DECLARE, CREATE PROCEDURE 등)은 SQL*Plus처럼 `/`만 있는 줄에서 끝납니다. 읽기 전용
모드에서는 실행하지 않으므로 `--write`가 필요합니다. 쓰기 모드는 autocommit을 끕니다. `COMMIT`을 직접
입력해야 반영되고, 커밋하지 않고 종료하면 ROLLBACK합니다.

```text
oracle:NIFI_READER[write]=> begin
oracle:NIFI_READER[write]-> dbms_stats.gather_table_stats('APP', 'INSP_DTL');
oracle:NIFI_READER[write]-> end;
oracle:NIFI_READER[write]-> /
```

## A.4 Hive: bin/hive.sh

### A.4.1 대화형 예

```console
$ bin/hive.sh
hive nifi@192.168.122.1:10000/default (binary, auth NONE), 읽기 전용
도움말 \?, 종료 \q
hive:default=> use stg;
완료
hive:stg=> \dt *17b*
 database | table
----------+-----------------------------------------------
 stg      | tmp_insp_dtl_17b86fadc90d4f819bc36c0cdded1fca
(1행)
hive:stg=> \q
```

프롬프트에는 현재 database가 나옵니다. `USE`가 성공하면 프롬프트가 바뀌고, 연결이 끊겨 다시 맺을 때도 그
database로 들어갑니다. 시작할 database는 `-d stg`로 정합니다.

### A.4.2 메타 명령

| 명령 | 동작 |
|---|---|
| `\dt [db.패턴]` | `SHOW TABLES [IN db]` 결과를 `*`, `?` 패턴으로 거릅니다. 예: `\dt stg.tmp_*` |
| `\d 이름` | `DESCRIBE 이름`(열, 타입, 파티션 열) |
| `\d+ 이름` | `DESCRIBE FORMATTED 이름`(Location, InputFormat, 테이블 속성, 통계) |
| `\dn [패턴]` | `SHOW DATABASES`를 패턴으로 거릅니다 |
| 나머지 | Oracle과 같습니다(`\scn` 제외) |

`SHOW ... LIKE`의 패턴 문법은 Hive 3(`*`)과 Hive 4(`%`)에서 다릅니다. 메타 명령은 전체 목록을 받아 도구가
거르므로 두 버전 모두 `*`를 씁니다. SQL로 직접 `SHOW TABLES LIKE`를 쓸 때는 서버 버전의 문법을 따릅니다.

staging table이 올바른 run 경로를 보는지 확인하는 예:

```console
$ bin/hive.sh -c '\d+ stg.tmp_insp_dtl_d4a0ede736e14befad2e21973a3bd7d2' | grep -E 'Location|InputFormat'
 Location:                    | hdfs://192.168.122.1:8020/data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=d4a0ede7-36e1-4bef-ad2e-21973a3bd7d2 |
 InputFormat:                 | org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat                                               |
```

### A.4.3 시간 제한과 취소

문장마다 `hive.query.timeout.seconds`(설정 `clients.hive.query_timeout`, 기본 30분)를 보내므로, 오래
걸리는 조회는 서버가 끊습니다. 실행 중에 Ctrl-C를 누르면 HS2 operation을 취소합니다. 큰 테이블을 조회할 때는
target 파티션 조건(`base_dt = '...'`)을 꼭 넣습니다.

### A.4.4 인증

`clients.hive.auth`는 서버 `hive-site.xml`의 `hive.server2.authentication` 값과 맞춥니다.

| 서버 설정 | `auth` | 보내는 것 | `password` |
|---|---|---|---|
| `NONE`(HS2 기본) | `NONE` | SASL PLAIN으로 user·password | 검사하지 않습니다(아무 값이나 됩니다) |
| `LDAP` | `LDAP` | SASL PLAIN으로 user·password | 필수. LDAP 계정 비밀번호 |
| `CUSTOM` | `CUSTOM` | SASL PLAIN으로 user·password | 필수. 서버의 사용자 정의 인증기가 검사합니다 |
| `NOSASL` | `NOSASL` | SASL 없이 접속 | 쓰지 않습니다 |
| `KERBEROS` | 지원하지 않음 | | |

`transport: http`면 같은 user·password를 HTTP Basic 헤더로 보냅니다. `auth`가 `LDAP`·`CUSTOM`인데
`password`가 비어 있으면 접속하지 않고 종료 코드 2로 끝납니다. 비밀번호를 파일에 두고 싶지 않으면
`password`를 비우고 실행할 때마다 `-W`로 입력합니다.

```console
$ bin/hive.sh -W -c '\conninfo'
Hive 비밀번호:
hive etl_reader@hs2-host:10000/default (binary, auth LDAP), 읽기 전용
```

연결 구간 암호화(`hive.server2.use.SSL`)는 지원하지 않습니다. LDAP 비밀번호가 평문으로 네트워크를 지나므로
SSL이 켜진 HS2에는 접속할 수 없고, SSL이 꺼진 HS2라도 신뢰할 수 있는 내부망에서만 씁니다.

## A.5 HDFS: bin/hdfs.sh

### A.5.1 대화형 예

```console
$ bin/hdfs.sh
hdfs nifi@http://192.168.122.1:9870, 읽기 전용
도움말 help, 종료 exit
hdfs:/data/nifi/stage> cd ORACLE_INSP_DTL_DAILY
hdfs:/data/nifi/stage/ORACLE_INSP_DTL_DAILY> ls -d 'run_id=1*'
 permission | repl | owner | group      | size | modified         | path
------------+------+-------+------------+------+------------------+------------------------------------------------------------------------------------
 drwxr-x--- | -    | nifi  | supergroup |    0 | 2026-10-03 10:49 | /data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=17b86fad-c90d-4f81-9bc3-6c0cdded1fca
hdfs:/data/nifi/stage/ORACLE_INSP_DTL_DAILY> cd ..
hdfs:/data/nifi/stage> exit
```

### A.5.2 명령

| 명령 | 설명 |
|---|---|
| `ls [-R] [-h] [-d] [경로...]` | 권한, 복제 수, 소유자, 그룹, 크기, 수정 시각, 경로. `-d`는 디렉터리 자체, `-R`은 하위 전체, `-h`는 K·M·G 단위 |
| `cd [경로]`, `pwd` | 현재 디렉터리. 인자 없는 `cd`는 `home`(설정)으로 갑니다 |
| `stat 경로...` | 종류, 크기, 권한, 소유자, 복제 수, 블록 크기, 수정·접근 시각 |
| `du [-s] [-h] 경로...` | 바로 아래 항목별 크기. `-s`면 경로 합계 하나 |
| `count [-h] 경로...` | 디렉터리 수(자신 포함), 파일 수, 크기 |
| `find [경로] [-name 패턴] [-type f\|d]` | 하위 전체 검색. 경로 목록만 출력합니다 |
| `cat [-f] 경로...` | 내용 전체 |
| `head [-c N] [-f] 경로`, `tail [-c N] [-f] 경로` | 앞·끝 N바이트(기본 1024) |
| `get [-f] 경로 [로컬경로]` | 파일 하나를 로컬로 받습니다. `-f`면 덮어씁니다 |
| `mkdir`, `rm [-r] [-f]`, `mv`, `put [-f]`, `chmod 750` | 쓰기 명령. `--write`일 때만 동작합니다 |
| `format 형식`, `help`, `exit` | 출력 형식, 도움말, 종료 |

- 경로는 절대 경로나 현재 디렉터리 기준 상대 경로로 씁니다. `..`과 `.`도 쓸 수 있습니다.
- 경로의 어느 조각에든 glob(`*`, `?`, `[..]`)을 쓸 수 있습니다. 로컬 셸이 먼저 펼치지 않도록 명령행에서는
  따옴표로 감쌉니다. 예: `bin/hdfs.sh du -s -h '/data/nifi/stage/*/*'`
- 옵션은 경로 뒤에 와도 됩니다(`find . -type f -name '*.parquet'`). `-`로 시작하는 경로는 `--` 뒤에 씁니다.

### A.5.3 Parquet 파일 다루기

chunk 파일은 Snappy Parquet입니다. `cat`, `head`, `tail`은 바이너리 파일을 터미널에 출력하지 않습니다.
내용은 Hive staging table로 조회합니다. 파일 자체가 필요하면 `get`으로 받습니다.

```console
$ bin/hdfs.sh cat /data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=.../part-0000-000000.parquet
오류: 바이너리 파일(Parquet 등)이라 터미널에 출력하지 않습니다. get으로 받거나 Hive staging 테이블로 조회한다(강제 출력은 -f)

$ bin/hdfs.sh head -c 4 /data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=.../part-0000-000000.parquet | xxd
00000000: 5041 5231                                PAR1
```

파이프나 파일로 보낼 때는 막지 않습니다. 위 예처럼 매직 바이트(`PAR1`)로 파일이 온전한지 확인할 수 있습니다.

### A.5.4 DataNode 주소

파일 내용을 읽거나 쓸 때 NameNode는 DataNode 호스트 이름으로 redirect합니다. `ls`, `du`는 되는데
`cat`, `get`만 `DataNode에서 읽지 못했습니다`로 실패하면, 그 이름이 조회 호스트에서 풀리지 않거나 다른
주소로 가야 하는 경우입니다. 이때 `clients.hdfs.datanode_hosts`에 바꿀 주소를 적습니다.

```yaml
  hdfs:
    datanode_hosts:
      dn1.cluster.local: 10.0.0.11
```

## A.6 읽기 전용과 쓰기 모드

기본은 읽기 전용입니다. 쓰기가 꼭 필요할 때만 `--write`를 주고, 프롬프트에 `[write]`가 붙었는지 확인합니다.

| 대상 | 읽기 전용(기본)에서 실행되는 것 | `--write` |
|---|---|---|
| Oracle | 첫 키워드가 `SELECT`, `WITH`, `DESC`인 문장만 실행합니다. 쓰기 키워드나 `FOR UPDATE`가 들어 있으면 거부합니다. 실행할 때마다 `SET TRANSACTION READ ONLY`로 시작해 ROLLBACK으로 끝내므로 DB가 한 번 더 막습니다 | 모든 문장. autocommit을 끄므로 `COMMIT`이 필요합니다 |
| Hive | `SELECT`, `WITH`, `FROM`, `VALUES`, `EXPLAIN`, `SHOW`, `DESCRIBE`, `USE`, `SET`, `RESET`만 실행합니다. `WITH … INSERT`, `FROM … INSERT`, `EXPLAIN ANALYZE INSERT`는 거부합니다 | 모든 문장(Hive는 문장마다 바로 반영됩니다) |
| HDFS | 조회 명령만 실행합니다 | `mkdir`, `rm`, `mv`, `put`, `chmod`도 실행합니다. 단 `/`, `/data`처럼 깊이 2 미만인 경로는 지우거나 옮기지 않습니다 |

```console
$ bin/oracle.sh -c "delete from insp_dtl"
오류: 읽기 전용 모드라 실행하지 않습니다(DELETE 문장). 쓰기가 필요하면 --write로 실행한다
$ echo $?
3
```

- 판정은 문자열·주석을 가린 뒤 단어로 합니다. 따라서 `WHERE note = 'delete'` 같은 값에는 걸리지 않습니다.
  판정이 보수적이라 열 이름이 정확히 `DELETE`, `LOAD`인 드문 경우에도 거부합니다. 이때는 열 이름을
  따옴표로 감쌉니다.
- 정리(PG-70)가 맡는 staging table DROP과 run 경로 삭제는 원칙적으로 수동으로 하지 않습니다. 수동 정리가
  꼭 필요하면 [통합 검증과 운영·복구](./06-validation-and-operations.md)의 정리 절차를 따르고, 정리했다는
  기록(`POST /v1/runs/{id}/cleanup`)도 남깁니다.

## A.7 운영 시나리오별 사용 예

run 정보(`snapshot_scn`, `hdfs_run_path`, `stage_table_name`, `business_key`)는 TUI run 상세
(`bin/monitor.sh`)나 `GET /v1/runs/{id}`에서 확인합니다.

### A.7.1 성공한 run을 네 단계에서 대조

```bash
RUN=d4a0ede7-36e1-4bef-ad2e-21973a3bd7d2; SCN=2390399; BK=2026-09-28
bin/oracle.sh -c "SELECT COUNT(*) cnt, SUM(amount) amount_sum FROM insp_dtl AS OF SCN $SCN
                  WHERE base_dt = DATE '$BK'"                                     # 원천
bin/hdfs.sh count -h "/data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=$RUN"          # chunk 파일
bin/hive.sh -c "SELECT COUNT(*) cnt, SUM(amount) amount_sum FROM stg.tmp_insp_dtl_${RUN//-/}" \
            -c "SELECT COUNT(*) cnt, SUM(amount) amount_sum FROM dw.insp_dtl WHERE base_dt = '$BK'"
```

시험 환경 결과는 원천, staging, target 모두 105,000행과 금액 합계 71,853,075로 같았습니다. HDFS run 경로에는
파일 8개(chunk 7개와 `_SUCCESS`)가 있었습니다. HDFS 파일 수는 API의 chunk 기록(`load_file`) 수에
`_SUCCESS` 1개를 더한 값과 같아야 합니다.

### A.7.2 실패한 run 조사

1. 실패 단계(`error_stage`)를 TUI에서 확인합니다.
2. 추출 실패(`FAILED_EXTRACT`)라면 run 경로에 남은 chunk와 `_SUCCESS`가 있는지 봅니다.
   ```bash
   bin/hdfs.sh ls -h "/data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=$RUN"
   ```
3. staging 검증 실패(`FAILED_STAGE_VALIDATION`)라면 staging table의 위치·스키마와 건수를 봅니다.
   ```bash
   bin/hive.sh -c "\d+ stg.tmp_insp_dtl_${RUN//-/}" -c "SELECT COUNT(*) FROM stg.tmp_insp_dtl_${RUN//-/}"
   ```
4. 원천과 비교하려면 A.7.1의 Oracle 문장을 같은 SCN으로 실행합니다. `ORA-01555`가 나면 A.3.3을 봅니다.
5. target 검증 실패(`FAILED_TARGET_VALIDATION`)라면 중복과 범위를 확인합니다.
   ```bash
   bin/hive.sh -c "SELECT COUNT(*) - COUNT(DISTINCT insp_dtl_seq) dup FROM dw.insp_dtl WHERE base_dt = '$BK'"
   ```

### A.7.3 정리(PG-70) 확인

정리가 끝난 run(`cleaned_at`이 있음)은 run 경로와 staging table이 없어야 합니다.

```console
$ bin/hdfs.sh stat /data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=23831ba3-42b4-4a3c-b53a-47788a8f8ad0
오류: FileNotFoundException: File does not exist: /data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=23831ba3-...
$ bin/hive.sh -c '\dt stg.tmp_insp_dtl_23831ba3*'
 database | table
----------+-------
(0행)
```

남은 용량과 staging table 수는 다음으로 봅니다.

```bash
bin/hdfs.sh du -s -h '/data/nifi/stage/*'          # Job별 합계
bin/hdfs.sh du -h '/data/nifi/stage/*'             # run 경로별 크기
bin/hive.sh -c '\dt stg.tmp_*' | tail -1           # staging table 수
```

### A.7.4 스크립트로 판정

종료 코드와 `-t -F tsv`를 함께 쓰면 셸 스크립트에서 값만 받아 비교할 수 있습니다.

```bash
#!/usr/bin/env bash
# 사용: check.sh <business_key> <snapshot_scn>   원천(SCN 시점)과 target 건수가 같으면 0
set -u
cd /opt/load-control-api
src=$(bin/oracle.sh -t -F tsv -c "SELECT COUNT(*) FROM insp_dtl AS OF SCN $2 WHERE base_dt = DATE '$1'") || exit $?
tgt=$(bin/hive.sh -t -F tsv -c "SELECT COUNT(*) FROM dw.insp_dtl WHERE base_dt = '$1'") || exit $?
echo "source=$src target=$tgt"
[[ "$src" == "$tgt" ]]
```

```console
$ ./check.sh 2026-09-28 2390399; echo $?
source=105000 target=105000
0
```

반복해서 쓰는 SQL은 파일로 두고 `-f`로 실행합니다. `--echo`를 주면 어떤 문장의 결과인지 함께 남습니다.

```bash
bin/hive.sh --echo --timing -f checks/target_daily.sql > logs/target_check_$(date +%F).txt
```

### A.7.5 결과 내보내기

```bash
bin/hive.sh -F csv --max-rows 0 -c "SELECT * FROM dw.insp_dtl WHERE base_dt = '2026-09-28'" > insp_dtl_0928.csv
bin/oracle.sh -F json -c "SELECT * FROM insp_dtl AS OF SCN 2390399 WHERE insp_dtl_seq = 10"
```

대화형에서는 `\o 파일`로 이후 결과를 파일로 보내고, 인자 없는 `\o`로 화면에 되돌립니다.

## A.8 문제 해결

| 증상(메시지) | 종료 코드 | 원인과 조치 |
|---|---:|---|
| `config.yaml clients.oracle 설정이 없거나 잘못됐습니다(dsn, user, password)` | 2 | `clients` 섹션이 없거나 필수 항목이 빠졌습니다. A.1.2대로 채우거나 명령행 옵션으로 줍니다 |
| `설정을 읽을 수 없습니다: config file not found` | 2 | `config/config.yaml`이 없습니다. `bin/*.sh`로 실행하고, 설치 디렉터리에 설정 파일이 있는지 확인합니다 |
| `Oracle 연결 실패(...): ORA-01017: invalid credential` | 2 | 계정·비밀번호 오류. NiFi `ORACLE.JDBC.USER`·`PASSWORD`와 비교합니다 |
| `Oracle 연결 실패(...): DPY-6005` 또는 `ORA-12514` | 2 | 호스트·포트·service name 오류나 방화벽. `dsn`을 JDBC URL의 `@//` 뒤 부분과 비교합니다 |
| `DPY-3010: connections to this database server version are not supported` | 2 | Oracle 11g 이하. thin 모드가 지원하지 않습니다 |
| `HiveServer2 연결 실패(...): Could not connect to any of [...]` | 2 | HS2 주소·포트 오류나 HS2 중지 |
| HS2 연결 직후 `TSocket read 0 bytes` | 2 | 인증·전송 방식 불일치. 서버의 `hive.server2.authentication`과 `transport.mode`를 `auth`·`transport`에 맞춥니다(A.4.4). SSL이 켜진 HS2도 이렇게 실패합니다 |
| `HiveServer2 연결 실패(...): Error validating the login` | 2 | LDAP·CUSTOM 인증 실패. `clients.hive.user`·`password`를 확인합니다 |
| `clients.hive.auth가 LDAP이면 password가 필요합니다` | 2 | `clients.hive.password`를 적거나 `-W`로 입력합니다 |
| `NameNode에 연결할 수 없습니다: ...: standby; ...` | 2 | 모든 NameNode가 standby거나 접속 불가. `namenode_urls`에 active NameNode가 있는지 확인합니다 |
| `DataNode에서 읽지 못했습니다(http://dn...:9864/...)` | 2 | A.5.4의 `datanode_hosts`를 설정합니다 |
| `AccessControlException: Permission denied` | 1 | `clients.hdfs.user` 권한이 부족합니다. NiFi와 같은 사용자(`nifi`)를 씁니다 |
| `ORA-00942: table or view ... does not exist` | 1 | 이름이나 스키마가 틀렸습니다. `\dt` 패턴으로 찾고 스키마를 붙입니다(`APP.INSP_DTL`) |
| `ORA-01555` / `ORA-08181` | 1 | 그 SCN의 undo가 없습니다. A.3.3을 봅니다 |
| `읽기 전용 모드라 실행하지 않습니다(...)` | 3 | 의도한 쓰기라면 `--write`로 다시 실행합니다. 조회인데 거부됐다면 열 이름을 따옴표로 감쌉니다 |
| `보호 경로라 변경하지 않습니다: /data` | 3 | 깊이 2 미만 경로는 도구로 지우거나 옮기지 않습니다 |
| `처음 1000행만 출력했습니다` | 0 | 결과를 잘랐습니다. `--max-rows 0` 또는 `\maxrows 0`으로 전체를 받습니다 |
| `python not found: .../.venv/bin/python` | 1 | `bin/install.sh`를 먼저 실행합니다 |

## A.9 명령 요약

```text
# 공통: -c 문장(반복) -f 파일 -F table|vertical|csv|tsv|json -t --write
bin/oracle.sh [--dsn D] [--user U] [-W] [--max-rows N] [--timing] [--echo] [--null S]
bin/hive.sh   [--host H] [--port P] [-d DB] [--user U] [-W] [--max-rows N] [--timing] [--echo] [--null S]
bin/hdfs.sh   [--url URL]... [--user U] [명령 [인자...]]

# SQL 메타 명령
\dt [패턴]  \d 이름  \d+ 이름  \dn [패턴]  \scn(Oracle)  \conninfo
\x [on|off]  \format 형식  \t [on|off]  \timing [on|off]  \maxrows N
\o [파일]  \i 파일  \?  \q

# HDFS 명령
ls [-R] [-h] [-d]  cd  pwd  stat  du [-s] [-h]  count [-h]  find [-name P] [-type f|d]
cat [-f]  head [-c N]  tail [-c N]  get [-f]  format  help  exit
--write: mkdir  rm [-r] [-f]  mv  put [-f]  chmod 750

# 종료 코드: 0 성공, 1 실행 오류, 2 사용법·설정·접속 오류, 3 거부, 130 Ctrl-C
```

## A.10 구현 메모와 제약

| 모듈(`load-control-api/src/load_control/query/`) | 역할 |
|---|---|
| `__main__.py` | 하위 명령 `oracle`·`hive`·`hdfs`, 옵션, 실행 방식 선택, 종료 코드 |
| `statements.py` | `;`(Oracle PL/SQL은 `/` 줄)로 문장 나누기, 읽기 전용 판정 |
| `sqlshell.py` | psql식 대화형·일괄 실행, 메타 명령, DB별 백엔드 인터페이스 |
| `oracle.py`, `hive.py` | DB별 연결, 실행, `\dt`·`\d`·`\dn`, Ctrl-C 취소 |
| `webhdfs.py`, `hdfsshell.py` | WebHDFS 클라이언트(HA failover, DataNode redirect), HDFS 명령 |
| `output.py`, `console.py` | 출력 형식, readline 이력 |

- 의존 패키지는 `oracledb`와 `impyla`(thrift, thrift-sasl, pure-sasl, bitarray 등)입니다.
  `packages/requirements.txt`에 버전이 고정되어 있습니다. `pure-sasl`은 소스 배포본만 있어서
  `bin/download-packages.sh`가 인터넷이 되는 장비에서 순수 python wheel로 만들어 함께 둡니다.
- API server·worker는 `clients` 섹션을 읽지 않습니다. 조회 도구가 실패해도 적재에는 영향이 없습니다.
- 지원하지 않는 것:
  - Kerberos(Oracle·Hive·HDFS)
  - Hive SSL(`hive.server2.use.SSL`). Hive LDAP·CUSTOM 인증은 지원하지만 시험 환경 HS2가 `NONE`이라
    실제 LDAP 서버로는 검증하지 않았습니다
  - Oracle 11g 이하(thin 모드 미지원)
  - HDFS 디렉터리 단위 `get`·`put`
  - Parquet 내용 직접 해석(Hive staging table로 조회합니다)
