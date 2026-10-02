#!/usr/bin/env python3
"""NiFi 2.4 REST API로 Sqoop 대체 PoC Flow V4를 만든다. V3(build_flow_v3.py)와 같은 구조(Load Control API 연동,
자식 PG + Port)에서 원천을 PostgreSQL 대신 Oracle로 바꾼 버전이다. 가이드 7.2~7.3, 8.4의 Oracle 기준을 따른다.

    PG-00 Trigger ─start-run▶ PG-10 Run Coordinator ─partitions(RR)▶ PG-20 Extract Worker
    PG-05 Control Receiver ─validate▶ PG-40 Staging Validation ─staging-valid▶ PG-50 Publish ─published▶ PG-60 Target Validation
    PG-05 ─reissue(RR)▶ PG-20,  PG-70 Cleanup(주기 실행),  모든 PG ─errors▶ PG-90 Error and Event

V3 대비 변경점
- 원천 Connection Pool(CS_DBCP_ORACLE): oracle.jdbc.OracleDriver, ORACLE.JDBC.* Parameter(ojdbc 경로 포함),
  검사 쿼리 SELECT 1 FROM DUAL. 이름은 가이드 3.1·4장과 같다.
  관리 DB(load_event)는 PostgreSQL 그대로이므로 드라이버 경로를 META.JDBC.DRIVER.PATH로 나눈다.
- PG-10: SCN 조회(14)·추출(15)을 추가해 모든 원천 SQL이 같은 SCN(AS OF SCN)을 읽는다. 원천 지표+manifest SQL(16)은
  Oracle 문법(CONNECT BY, TO_CHAR, 문자열 boolean)이며 결과 컬럼은 대문자다. SCN과 timestamp 지표(MIN_TS/MAX_TS)도
  함께 반환한다. Processor 번호는 가이드 7.2(11~20)와 같다.
- PG-20: 33에서 SCN도 숫자 검사, 34는 AS OF SCN으로 조회하고 재시도하지 않는다(같은 SCN으로 긴 쿼리를 반복하지
  않도록, 가이드 16장). 정밀도 없는 NUMBER는 ORACLE.NUMBER.DEFAULT.PRECISION/SCALE로 기록된다(SRC.COLUMNS에서
  CAST로 정밀도를 명시하는 것을 권장).
- PG-05: 재발행 본문의 snapshotScn을 load.snapshot.scn으로 꺼낸다.
- NULL split 파티션(SPLIT.NULL.POLICY=SEPARATE)은 만들지 않는다. NULL이 있으면 API가 manifest를 거부한다.
- PG-40 나머지(47~4E), PG-50, PG-60(가이드 10~12장): Hive staging external table과 지표 검증, INSERT OVERWRITE 게시,
  target 지표 검증으로 run을 SUCCESS까지 끝낸다. Hive 구성요소는 CFM의 ClouderaHiveConnectionPool(CS_HIVE3_DBCP)과
  PutClouderaHiveQL이며, 지표 조회는 ExecuteSQLRecord(JSON)다. Apache NiFi에는 Hive 번들이 없어 이 빌더를 쓸 수 없다.
- PG-70 Cleanup: 1시간마다 API에 보존 기간이 지난 run을 묻고 staging table을 DROP, HDFS run 경로를 삭제한 뒤
  API에 기록한다. 보존 기간과 대상 판정은 API(cleanup 설정)가 한다.

Oracle Database 23ai Free에서 V3 시나리오와 NUMBER 정밀도, ORA-01555를 시험했다(REVIEW.md 7.4~7.7).
Cloudera CFM 4.12(NiFi 2.6.0) 2노드 클러스터에서도 정상 실행을 확인했다(REVIEW.md 7.8). 클러스터에서 trigger가
노드마다 생기지 않도록 00은 Primary Node에서만 실행한다. Hive 단계(PG-40~60)는 같은 클러스터와 Apache Hive 4.0.1
(poc/hdfs-hive)에서 정상·staging DQ 실패·게시 실패·target 검증 실패·Hive 중단을 시험했다(REVIEW.md 7.9).
사용 방법은 V4-MANUAL.md.

사용법: build_flow_v4.py <nifi-api-url> <config.json>
config.json 형식은 config.v4.example.json. `names.process_group` 기본값은 SQOOP_REPLACEMENT_POC_V4.
Trigger(00_Generate_Trigger)는 DISABLED로 만든다. 실행하려면 enable 후 Run Once 한다.
"""
import json
import sys
import urllib.error
import urllib.request

API = sys.argv[1].rstrip("/")
CFG = json.load(open(sys.argv[2]))
NAMES = CFG.get("names", {})
TOP_NAME = NAMES.get("process_group", "SQOOP_REPLACEMENT_POC_V4")
PC_COMMON = NAMES.get("common_context", "PC_SQOOP_REPLACEMENT_COMMON_V4")
PC_JOB = NAMES.get("job_context", "PC_JOB_ORACLE_INSP_DTL_DAILY_V4")
REV = {"version": 0, "clientId": "poc-builder-v4"}


def call(method, path, body=None):
    req = urllib.request.Request(API + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            data = r.read()
            return json.loads(data) if data else None
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code}: {e.read().decode()[:2000]}")


TYPES = {t["type"].split(".")[-1]: t for t in
         call("GET", "/flow/processor-types")["processorTypes"]
         + call("GET", "/flow/controller-service-types")["controllerServiceTypes"]}


def bundle(short):
    if short not in TYPES:
        raise SystemExit(f"{short} not found in this NiFi (Hive components need Cloudera CFM)")
    return TYPES[short]["bundle"], TYPES[short]["type"]


# ---------------------------------------------------------------- Parameter Context
def drop_param_ctx(name):
    for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"]:
        if pc["component"]["name"] == name:
            call("DELETE", f"/parameter-contexts/{pc['id']}?version={pc['revision']['version']}&clientId=poc-builder-v4")


def param_ctx(name, params, inherited=None):
    comp = {"name": name, "parameters": [
        {"parameter": {"name": k, "value": v, "sensitive": k.endswith(("PASSWORD", "AUTHORIZATION"))}}
        for k, v in params.items()]}
    if inherited:
        comp["inheritedParameterContexts"] = [{"id": inherited, "component": {"id": inherited}}]
    return call("POST", "/parameter-contexts", {"revision": REV, "component": comp})["id"]


root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
for g in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]:
    if g["component"]["name"] == TOP_NAME:
        raise SystemExit(f"{TOP_NAME} already exists ({g['id']}); delete it first")

drop_param_ctx(PC_JOB)
drop_param_ctx(PC_COMMON)
common = param_ctx(PC_COMMON, CFG["common_params"])
job = param_ctx(PC_JOB, CFG["job_params"], inherited=common)


def new_pg(parent, name, x, y, comments=""):
    """자식 PG는 Parameter Context를 상속하지 않으므로 매번 지정한다."""
    ent = call("POST", f"/process-groups/{parent}/process-groups",
               {"revision": REV, "component": {"name": name, "position": {"x": x, "y": y}, "comments": comments}})
    call("PUT", f"/process-groups/{ent['id']}", {"revision": ent["revision"], "component": {
        "id": ent["id"], "parameterContext": {"id": job}}})
    return ent["id"]


TOP = new_pg(root, TOP_NAME, 1000, 300, "Sqoop 대체 PoC V4: Oracle 원천, 자식 PG + Port 구조")

# ---------------------------------------------------------------- Controller Services (TOP에 두고 자식 PG가 공유)
services = {}


def cs(name, short, props):
    b, t = bundle(short)
    ent = call("POST", f"/process-groups/{TOP}/controller-services",
               {"revision": REV, "component": {"type": t, "bundle": b, "name": name, "properties": props}})
    services[name] = ent
    return ent["id"]


def hikari(prefix, driver_class, validation_query, max_conns="10"):
    return {"hikaricp-connection-url": f"#{{{prefix}.JDBC.URL}}",
            "hikaricp-driver-classname": driver_class,
            "hikaricp-driver-locations": f"#{{{prefix}.JDBC.DRIVER.PATH}}",
            "hikaricp-username": f"#{{{prefix}.JDBC.USER}}",
            "hikaricp-password": f"#{{{prefix}.JDBC.PASSWORD}}",
            "hikaricp-max-total-conns": max_conns,
            "hikaricp-validation-query": validation_query}


# 관리 DB는 PostgreSQL(load_event INSERT 전용), 원천은 Oracle. 드라이버가 다르므로 경로 Parameter를 나눈다.
META = cs("CS_DBCP_META", "HikariCPConnectionPool", hikari("META", "org.postgresql.Driver", "SELECT 1"))
SRC = cs("CS_DBCP_ORACLE", "HikariCPConnectionPool",
         hikari("ORACLE", "oracle.jdbc.OracleDriver", "SELECT 1 FROM DUAL", "#{ORACLE.POOL.MAX}"))
JARR = cs("CS_JSON_WRITER_ARRAY", "JsonRecordSetWriter", {"output-grouping": "output-array"})
PARQ = cs("CS_PARQUET_WRITER", "ParquetRecordSetWriter", {"compression-type": "SNAPPY"})
HTTPCTX = cs("CS_HTTP_CONTEXT_MAP", "StandardHttpContextMap", {"Request Expiration": "1 min"})
# Hive(가이드 10~12장). 지표 조회 SQL의 결과 컬럼 이름이 table alias 없이 오도록 HIVE.JDBC.URL에
# hive.resultset.use.unique.column.names=false를 둔다.
HIVE = cs("CS_HIVE3_DBCP", "ClouderaHiveConnectionPool", {
    "hive-db-connect-url": "#{HIVE.JDBC.URL}", "hive-db-user": "#{HIVE.JDBC.USER}",
    "hive-db-password": "#{HIVE.JDBC.PASSWORD}", "hive-max-total-connections": "#{HIVE.POOL.MAX}",
    "hive-max-wait-time": "10 secs", "Validation-query": "SELECT 1"})


# Processor COMMENT(한글). NiFi UI의 Processor 설정 > Comments에 표시된다.
COMMENTS = {
    # PG-00 Trigger
    "P00": "적재 실행을 시작하는 트리거. 스케줄마다 빈 FlowFile 하나를 만든다. 배포 시 DISABLED로 두고 운영 전환 시 enable한다.",
    "P01": "Job 키와 업무일자를 attribute로 넣고 현재 단계를 RUN_CREATE로 표시한다.",
    "P02": "업무일자가 yyyy-MM-dd 형식인지 검사한다. 업무일자는 SQL에 들어가므로 SQL 주입을 막는 경계다. 형식이 틀리면 errors로 보낸다.",
    # PG-10 Run Coordinator
    "P11": "run 생성 요청 본문(JSON)을 만든다. jobKey, businessKey, HDFS root, stage table prefix, 0건 허용 여부를 넣는다.",
    "P12": "Load Control API에 run을 생성한다(POST /runs). 같은 업무일자의 활성 run이 있으면 409로 거부된다. 5xx·연결 오류는 자체 재시도 후 errors로 보낸다.",
    "P13": "run 생성 응답에서 runId, run 전용 HDFS 경로, stage table 이름을 꺼내고 현재 단계를 MANIFEST로 표시한다.",
    "P14": "Oracle 현재 SCN을 조회한다. 이후 원천 지표·manifest(16)와 모든 Worker 조회(34)가 이 SCN(AS OF SCN)으로 같은 시점 데이터를 읽는다.",
    "P15": "SCN 조회 결과에서 load.snapshot.scn을 꺼낸다. 이 값은 파티션 FlowFile까지 따라가 Worker 조회에 쓰인다.",
    "P16": "같은 SCN에서 원천 건수·NULL 수·최솟값·최댓값·금액 합계·timestamp 범위와 파티션별 경계·예상 건수를 SQL 한 문장으로 계산한다. SCN이 숫자가 아니면 SQL 오류로 실패한다.",
    "P17": "SQL 결과 배열을 manifest 등록 요청 형식으로 바꾼다. 0번 행에서 SCN과 원천 지표를 꺼내고 파티션 목록을 partitions로 감싼다. Oracle 결과 컬럼은 대문자다.",
    "P18": "manifest를 API에 등록한다(POST /runs/{id}/manifest). API가 불변식(합계, 파티션 수, 0건 허용, NULL)을 검사하고 Worker로 보낼 파티션 목록을 돌려준다.",
    "P19": "API가 돌려준 dispatchPartitions를 파티션 하나당 FlowFile 하나로 나눈다. 0건 파티션은 API가 이미 SUCCESS로 처리해 목록에 없다.",
    "P20": "파티션 FlowFile의 JSON에서 파티션 ID, 하한, 상한, 상한 포함 여부, 예상 건수를 attribute로 꺼내 PG-20으로 보낸다.",
    # PG-20 Extract Worker
    "P30": "파티션 소유권 확인용 claim token(UUID)을 만들고 현재 단계를 EXTRACT로 표시한다. token은 재시도해도 바뀌지 않는다.",
    "P31": "claim 요청 본문(claimToken, workerNode)을 만든다. token은 앞 Processor에서 만든 값을 쓴다.",
    "P32": "API에 파티션 처리 소유권을 요청한다(POST .../claim). 다른 Worker가 이미 처리 중이거나 run이 끝났으면 claimed=false를 받는다.",
    "P33": "claim에 성공했고 파티션 경계와 SCN이 숫자인 경우만 추출로 보낸다(SQL에 직접 들어가는 값). claimed=false는 정상 경합이므로 오류 없이 종료한다.",
    "P34": "고정 SCN(AS OF SCN)으로 파티션 범위의 Oracle 원천을 조회해 Parquet chunk FlowFile로 만든다. 재시도하지 않는다. 실패(ORA-01555 등)하면 errors로 보내 파티션 실패를 보고한다.",
    "P35": "chunk 파일 이름(part-<파티션>-<chunk>.parquet)을 정하고 현재 단계를 CHUNK_WRITE로 표시한다.",
    "P36": "Parquet chunk를 run 전용 HDFS 경로에 쓴다(Write and rename). 실패하면 자체 재시도 후 errors로 보낸다.",
    "P37": "HDFS 기록 후 content를 chunk 보고 JSON(claimToken, chunk 번호·개수, HDFS 경로, 건수, 크기)으로 바꾼다.",
    "P38": "API에 chunk 기록을 보고한다(POST .../chunks). 파티션·run 완료 판정과 검증 호출 예약은 API가 하므로 응답을 더 보지 않고 끝낸다.",
    # PG-05 Control Receiver
    "R05": "Load Control API worker가 보내는 검증 시작(/validate/<Job>)·파티션 재발행(/reissue/<Job>) 요청을 받는다. 등록된 Job 경로만 허용한다.",
    "R06": "요청이 POST이고 X-Run-Id, X-Dispatch-Id 헤더가 UUID 형식인지 검사한다.",
    "R07": "형식이 잘못된 요청에 400으로 응답한다. API가 ACK timeout 뒤 다시 보낸다.",
    "R08": "정상 요청에 바로 202로 응답한다. 검증은 오래 걸리므로 HTTP 연결을 붙잡지 않는다.",
    "R09": "요청 본문에서 runId, dispatchId와 재발행에 필요한 파티션 정보·SCN을 attribute로 꺼낸다.",
    "R10": "요청 경로로 검증(validate)과 재발행(reissue)을 나눈다. 재발행은 PG-20 Worker로 보낸다.",
    # PG-40 Staging Validation
    "V40": "현재 단계를 VALIDATION_START로 표시한다.",
    "V41": "검증 시작 요청 본문(dispatchId, node)을 만든다.",
    "V42": "API에 검증 시작을 알린다(POST /validation/start). 같은 run에 대해 한 번만 started=true를 받는다. 이 호출이 dispatch ACK가 된다.",
    "V43": "started=true인 경우만 진행한다. 중복 검증 요청은 오류 없이 종료한다.",
    "V44": "검증 시작 응답에서 HDFS 경로, stage table, 업무일자, 원천·추출 건수, 원천 지표(AMOUNT_SUM, MIN_TS, MAX_TS)를 꺼내고 _SUCCESS 파일 이름을 정한다. 이후 실패는 run을 FAILED_STAGE_VALIDATION으로 보고한다.",
    "V45": "_SUCCESS marker를 빈 파일로 쓰기 위해 content를 비운다.",
    "V46": "run 경로에 _SUCCESS marker를 쓴다. 이 파일은 API가 run 완료를 확정한 뒤에만 생긴다. Hive는 _로 시작하는 파일을 읽지 않는다.",
    "V47": "run 경로를 LOCATION으로 하는 Hive external staging 테이블 DDL을 만든다. stage table 이름이 영문·숫자·_가 아니면 DDL이 실패하도록 바꾼다.",
    "V48": "staging 테이블을 만든다(PutClouderaHiveQL). Hive retry는 3회 재시도 후 errors로 보낸다.",
    "V49": "staging 테이블에서 건수, split NULL 수, PK 중복 수, 금액 합계, MIN_TS/MAX_TS를 계산하고 원천 기대값과 비교한 PASS/FAIL을 지표마다 한 행으로 돌려준다.",
    "V4A": "지표 행 배열을 /validations 요청(stage=STAGING, queryVersion=v1, metrics)으로 바꾼다.",
    "V4B": "staging 지표를 API에 기록한다(POST /validations). FAIL이 있어도 먼저 기록한다.",
    "V4C": "stage-validated 요청 본문({})을 만든다.",
    "V4D": "API에 staging 통과 판정을 요청한다(POST /stage-validated). API가 저장된 지표가 모두 PASS일 때만 STAGING_VALIDATED로 바꾼다.",
    "V4E": "stageValidated=true면 PG-50으로 보낸다. false면 errors로 보내 run을 FAILED_STAGE_VALIDATION으로 보고한다.",
    # PG-50 Publish
    "B50": "게시 소유권 token(UUID)을 만들고 현재 단계를 PUBLISH로 표시한다. 이 단계의 실패는 PG-90이 이벤트만 남긴다(결과는 57이 직접 보고).",
    "B51": "publish claim 요청 본문(publishToken)을 만든다.",
    "B52": "API에 게시 소유권을 요청한다(POST /publish/claim). STAGING_VALIDATED → PUBLISHING CAS에 성공하거나 같은 token이면 claimed=true.",
    "B53": "claimed=true인 경우만 게시한다. 중복 요청은 오류 없이 종료한다.",
    "B54": "승인된 target·partition·column으로 INSERT OVERWRITE SQL을 만든다. FlowFile에서 받은 값은 stage table 이름(형식 검사)만 쓴다.",
    "B55": "target에 INSERT OVERWRITE를 실행한다. 재시도하지 않는다. failure·retry는 실행 여부를 알 수 없으므로 PUBLISH_UNKNOWN으로 보고한다(가이드 11.2).",
    "B56": "게시 결과 PUBLISHED 본문을 만든다.",
    "B56U": "게시 결과 PUBLISH_UNKNOWN 본문을 만든다. 운영자가 Hive 이력과 target을 확인해 /publish-unknown/resolve로 확정한다.",
    "B57": "API에 게시 결과를 보고한다(POST /publish/result). token이 일치할 때만 run 상태가 바뀐다.",
    "B58": "run이 PUBLISHED면 PG-60으로 보낸다. 그 밖(PUBLISH_UNKNOWN 등)은 errors로 보내 이벤트를 남긴다.",
    # PG-60 Target Validation
    "T60": "현재 단계를 TARGET_VALIDATION으로 표시한다. 이후 실패는 run을 FAILED_TARGET_VALIDATION으로 보고한다.",
    "T61": "target 업무 범위(TARGET.BUSINESS.WHERE)에서 건수, split NULL 수, PK 중복 수, 금액 합계, MIN_TS/MAX_TS를 계산하고 원천 기대값과 비교한다.",
    "T62": "지표 행 배열을 /validations 요청(stage=TARGET)으로 바꾼다.",
    "T63": "target 지표를 API에 기록한다(POST /validations).",
    "T64": "success 요청 본문({})을 만든다. target 건수는 API가 TARGET_COUNT 지표에서 읽는다.",
    "T65": "API에 최종 성공을 요청한다(POST /success). 저장된 TARGET 지표가 모두 PASS일 때만 SUCCESS로 바꾸고 RUN_SUCCESS를 기록한다.",
    "T66": "success=true면 끝낸다. false면 errors로 보내 run을 FAILED_TARGET_VALIDATION으로 보고한다.",
    # PG-90 Error and Event
    # PG-70 Cleanup
    "C70": "정리 주기 트리거(1시간). Primary Node에서만 실행한다. 대상 판정은 API가 하므로 PG 시작과 함께 RUNNING이어도 된다.",
    "C71": "현재 단계를 CLEANUP으로 표시한다. 이 단계의 실패는 PG-90이 이벤트만 남기고 다음 주기에 다시 시도한다.",
    "C72": "API에 이 Job의 정리 대상 run을 묻는다(GET /cleanup/candidates). 보존 기간(SUCCESS 3일, 실패 14일 등)은 API 설정이다.",
    "C73": "정리 대상 목록을 run 하나당 FlowFile 하나로 나눈다. 대상이 없으면 아무것도 내보내지 않는다.",
    "C74": "run ID, HDFS run 경로, staging table 이름, run 상태를 attribute로 꺼낸다.",
    "C75": "지우기 전에 경로가 #{HDFS.STAGE.ROOT}/#{JOB.KEY}/run_id=<runId>와 정확히 같은지, table 이름이 접두사·형식에 맞는지 검사한다. 다르면 지우지 않고 errors로 보낸다.",
    "C76": "staging external table DROP 문을 만든다.",
    "C77": "staging external table을 DROP한다(IF EXISTS). external table이므로 데이터 파일은 지우지 않는다.",
    "C78": "HDFS run 경로(Parquet chunk, _SUCCESS)를 재귀 삭제한다.",
    "C79": "정리 보고 본문(droppedTable, deletedPath)을 만든다.",
    "C7A": "API에 정리 완료를 기록한다(POST /runs/{id}/cleanup). 이미 기록된 run이면 changed=false로 끝난다.",
    "E90": "모든 PG의 오류를 정규화한다. load.stage와 Processor가 남긴 attribute(HTTP 상태, SQL 오류, 연결 예외)로 오류 단계·코드·분류·메시지와 이벤트 수준·이름을 만든다. 409(중복 실행, claim 불일치)는 정상 경합이므로 WARN이다.",
    "E91": "오류 단계에 따라 run 실패 보고(MANIFEST, STAGE_VALIDATION, TARGET_VALIDATION), 파티션 실패 보고, 이벤트 기록만 중 하나로 나눈다.",
    "E92": "run 실패 보고 본문(load.fail.expected → load.fail.status, 오류 단계·코드·메시지)을 만든다. SCN 조회 실패도 여기로 온다.",
    "E93": "API에 run 실패를 보고한다(POST /runs/{id}/fail). 보고하지 않으면 활성 run lock이 남는다.",
    "E94": "파티션 실패 보고 본문(claimToken, 오류 단계·분류·코드·메시지)을 만든다.",
    "E95": "API에 파티션 실패를 보고한다(POST .../partitions/{pid}/fail). API가 파티션과 run을 실패 처리한다.",
    "E96": "오류 이벤트를 관리 DB nifi_ops.load_event에 기록한다. 409 정상 경합은 WARN(이름은 API 오류 코드), 그 밖은 ERROR(<단계>_FAILED)로 남긴다.",
    "E97": "오류를 구조화 JSON으로 nifi-app.log에 남긴다(prefix SQOOP_REPLACEMENT).",
}

# ---------------------------------------------------------------- 구성요소 생성 helper
procs = {}   # key -> (entity, group id)
ports = {}   # (group id, name, "in"/"out") -> port id
conns = []   # (group, src, rels, dst, extra)
COLW, ROWH = 420, 190


def p(g, key, name, short, props=None, col=0, row=0, tasks=1, sched="0 sec", sensitive=(), retry=None,
      primary=False):
    """retry=(relationships, count): Processor 내장 재시도. 소진되면 해당 relationship 연결로 간다.
    primary=True: 클러스터에서 Primary Node에서만 실행(단일 노드에서는 영향 없음)."""
    b, t = bundle(short)
    config = {"properties": props or {}, "concurrentlySchedulableTaskCount": tasks,
              "schedulingPeriod": sched, "penaltyDuration": "5 sec", "yieldDuration": "5 sec",
              "comments": COMMENTS[key]}  # 설명이 없는 Processor는 KeyError로 빌드를 멈춘다
    if sensitive:
        config["sensitiveDynamicPropertyNames"] = list(sensitive)
    if primary:
        config["executionNode"] = "PRIMARY"
    if retry:
        config.update({"retriedRelationships": retry[0], "retryCount": retry[1],
                       "backoffMechanism": "PENALIZE_FLOWFILE", "maxBackoffPeriod": "1 min"})
    ent = call("POST", f"/process-groups/{g}/processors", {"revision": REV, "component": {
        "type": t, "bundle": b, "name": name, "position": {"x": col * COLW, "y": row * ROWH}, "config": config}})
    procs[key] = (ent, g)
    return key


def label(g, text, x, y, width, height):
    """PG 설명 Label. Processor 영역 위쪽에 둔다."""
    call("POST", f"/process-groups/{g}/labels", {"revision": REV, "component": {
        "label": text, "position": {"x": x, "y": y}, "width": width, "height": height,
        "style": {"font-size": "14px"}}})


def port(g, name, kind, col, row):
    path = "input-ports" if kind == "in" else "output-ports"
    ent = call("POST", f"/process-groups/{g}/{path}",
               {"revision": REV, "component": {"name": name, "position": {"x": col * COLW, "y": row * ROWH}}})
    ports[(g, name, kind)] = ent["id"]
    return ent["id"]


def c(g, src, rels, dst, **extra):
    """같은 PG 안의 연결. src/dst는 processor key 또는 ("in"|"out", port name)."""
    conns.append((g, src, rels if isinstance(rels, list) else [rels], dst, extra))


def ua(g, key, name, attrs, col, row):
    return p(g, key, name, "UpdateAttribute", attrs, col, row)


def route(g, key, name, routes, col, row):
    return p(g, key, name, "RouteOnAttribute", {"Routing Strategy": "Route to Property name", **routes}, col, row)


def body(g, key, name, value, col, row):
    """FlowFile content를 value로 바꾼다(API 요청 본문, _SUCCESS marker 등)."""
    return p(g, key, name, "ReplaceText", {"Replacement Strategy": "Always Replace", "Evaluation Mode": "Entire text",
                                           "Replacement Value": value}, col, row)


def invoke(g, key, name, path, col, row, attr_response=True, tasks=1, method="POST"):
    """Load Control API 호출(가이드 9.2). Retry(5xx)·Failure(연결 오류)는 내장 재시도 후 errors로 간다."""
    props = {
        "HTTP Method": method, "HTTP URL": f"#{{CONTROL.API.URL}}{path}",
        "Request Content-Type": "application/json", "Connection Timeout": "5 secs",
        "Socket Read Timeout": "#{CONTROL.API.TIMEOUT}", "Response Generation Required": "false",
        "Authorization": "#{CONTROL.API.AUTHORIZATION}", "X-Request-Id": "${UUID()}", "X-Run-Id": "${load.run.id}"}
    if attr_response:
        props["Response Body Attribute Name"] = "api.response"
        props["Response Body Attribute Size"] = "16384"
    p(g, key, name, "InvokeHTTP", props, col, row, tasks=tasks, sensitive=("Authorization",),
      retry=(["Retry", "Failure"], 5))
    c(g, key, ["No Retry", "Retry", "Failure"], ("out", "errors"))
    return key


def esql(g, key, name, pool, sql, col, row, writer=JARR, extra=None, tasks=1, retry=None):
    props = {"Database Connection Pooling Service": pool, "SQL Query": sql, "esqlrecord-record-writer": writer}
    props.update(extra or {})
    return p(g, key, name, "ExecuteSQLRecord", props, col, row, tasks=tasks, retry=retry)


def put_hdfs(g, key, name, col, row, tasks=1):
    p(g, key, name, "PutHDFS", {
        "Hadoop Configuration Resources": "#{HADOOP.CONF.FILES}", "Directory": "${load.hdfs.path}",
        "Conflict Resolution Strategy": "replace", "writing-strategy": "writeAndRename",
        "Permissions umask": "#{HDFS.PERMISSIONS.UMASK}"}, col, row, tasks=tasks, retry=(["failure"], 3))
    c(g, key, "failure", ("out", "errors"))
    return key


def hive_ql(g, key, name, col, row, retry=None):
    """content의 HiveQL 한 문장을 실행한다(PutClouderaHiveQL). 오류 attribute를 남기지 않는다."""
    return p(g, key, name, "PutClouderaHiveQL", {
        "hive3-dbcp-service": HIVE, "hive-batch-size": "1", "hive3-query-timeout": "#{HIVE.QUERY.TIMEOUT}",
        "rollback-on-failure": "false"}, col, row, retry=retry)


NUM = "'^-?[0-9]+$'"
SRC_TABLE = "#{SRC.OWNER}.#{SRC.TABLE}"
SPLIT = "#{SRC.SPLIT.COLUMN}"
N = "#{PARTITION.COUNT}"

# 최상위 배치: 왼쪽 열은 실행 흐름, 오른쪽 열은 제어 수신·검증, 아래는 공통 오류
G00 = new_pg(TOP, "PG-00 Trigger", 0, 0, "스케줄 트리거와 업무일자 형식 검증")
G10 = new_pg(TOP, "PG-10 Run Coordinator", 0, 260, "run 생성, 원천 지표·manifest 계산, manifest 등록")
G20 = new_pg(TOP, "PG-20 Extract Worker", 0, 520, "claim, 파티션 추출, PutHDFS, chunk 보고")
G05 = new_pg(TOP, "PG-05 Control Receiver", 700, 0, "API worker의 validate/reissue 호출 수신")
G40 = new_pg(TOP, "PG-40 Staging Validation", 700, 260, "/validation/start, _SUCCESS, Hive staging 테이블과 지표 검증")
G50 = new_pg(TOP, "PG-50 Publish", 1400, 0, "publish claim, INSERT OVERWRITE, 게시 결과 보고")
G60 = new_pg(TOP, "PG-60 Target Validation", 1400, 260, "target 지표 검증, 최종 SUCCESS")
G70 = new_pg(TOP, "PG-70 Cleanup", 1400, 520, "보존 기간이 지난 run의 staging table·HDFS run 경로 정리")
G90 = new_pg(TOP, "PG-90 Error and Event", 700, 520, "공통 오류 정규화, 실패 보고 API, load_event 기록")

# 각 PG 안 위쪽에 역할·흐름·입출력·주의점을 적은 Label을 둔다. 상위 PG에는 전체 흐름 Label을 둔다.
PG_LABELS = {
    G00: """PG-00 Trigger
역할: 스케줄마다 적재 실행을 시작한다.
흐름: 00 트리거 → 01 Job 키·업무일자 설정(load.stage=RUN_CREATE) → 02 업무일자 형식 검증
출력: start-run → PG-10 / errors → PG-90
주의: 00은 DISABLED로 배포하고 운영 전환 시 enable한다. 업무일자는 SQL에 들어가므로 02의 정규식이 SQL 주입을 막는 경계다.""",
    G10: """PG-10 Run Coordinator
역할: Load Control API에 run을 만들고, 원천 지표와 파티션 manifest를 계산해 등록한다.
흐름: 11·12 run 생성(POST /runs) → 13 run 정보 설정(load.stage=MANIFEST) → 14·15 Oracle SCN 고정
      → 16 원천 지표+manifest SQL 1문장(AS OF SCN) → 17 요청 변환(Jolt) → 18 manifest 등록(API가 불변식 검사) → 19·20 파티션별 FlowFile
입력: start-run / 출력: partitions → PG-20(Round Robin), errors → PG-90
주의: 0건 파티션은 API가 바로 SUCCESS 처리해 Worker로 보내지 않는다. 12의 409는 같은 업무일자의 활성 run(중복 실행)이다.
      SCN 조회에는 V$DATABASE 조회 권한이 필요하다(없으면 DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER).""",
    G20: """PG-20 Extract Worker
역할: 파티션을 claim하고 원천을 조회해 Parquet로 HDFS에 쓴 뒤 chunk마다 API에 보고한다.
흐름: 30 claim token(load.stage=EXTRACT) → 31·32 claim → 33 소유·SCN 확인 → 34 파티션 조회(AS OF SCN, Parquet chunk)
      → 35 파일 이름(load.stage=CHUNK_WRITE) → 36 PutHDFS → 37·38 chunk 보고
입력: partitions(PG-10 manifest, PG-05 재발행) / 출력: errors → PG-90
주의: 파티션·run 완료 판정과 검증 호출 예약은 API가 한다. claimed=false는 정상 경합이므로 조용히 끝낸다.
      34는 재시도하지 않는다. ORA-01555면 API가 run을 FAILED_SNAPSHOT_EXPIRED로 바꾼다(UNDO 보존 시간 확인).""",
    G05: """PG-05 Control Receiver
역할: Load Control API worker가 보내는 검증 시작·파티션 재발행 요청을 받는다.
흐름: 05 HTTP 수신(/validate·/reissue/<Job>) → 06 헤더 검증(07: 400 응답) → 08 202 응답 → 09 본문 추출 → 10 동작별 분기
출력: validate → PG-40, reissue → PG-20, errors → PG-90
주의: 202를 먼저 응답한다. 처리 확인(ACK)은 PG-40의 /validation/start 또는 PG-20의 재 claim이다.
      운영에서는 root에 두고 여러 Job이 공유한다(가이드 9.5).""",
    G40: """PG-40 Staging Validation
역할: API에 검증 시작을 알리고, _SUCCESS를 쓴 뒤 Hive staging 테이블로 원천 지표와 비교한다.
흐름: 40 load.stage=VALIDATION_START → 41·42 검증 시작(POST /validation/start) → 43 started 확인
      → 44 run 정보·원천 지표(load.stage=STAGE_VALIDATION) → 45·46 _SUCCESS → 47·48 external staging DDL
      → 49 staging 지표·PASS/FAIL(SQL 1문장) → 4A·4B 지표 기록 → 4C·4D staging 통과 판정 → 4E
입력: validate / 출력: staging-valid → PG-50, errors → PG-90
주의: started=true는 run당 한 번만 온다(중복 요청은 조용히 종료). 44 이후 실패는 run을 FAILED_STAGE_VALIDATION으로 만든다.
      판정은 API가 저장된 지표로 다시 한다. 시간대가 어긋나면 MIN_TS/MAX_TS가 FAIL이 된다.""",
    G50: """PG-50 Publish
역할: staging을 target에 INSERT OVERWRITE로 게시하고 결과를 API에 직접 보고한다.
흐름: 50 publish token(load.stage=PUBLISH) → 51·52 publish claim → 53 소유 확인 → 54 INSERT OVERWRITE SQL
      → 55 실행(재시도 없음) → 56 PUBLISHED / 56U PUBLISH_UNKNOWN → 57 결과 보고 → 58 PUBLISHED 확인
입력: staging-valid / 출력: published → PG-60, errors → PG-90(이벤트만)
주의: 55를 자동 재실행하지 않는다. 결과가 불명확하면 PUBLISH_UNKNOWN으로 남기고 운영자가 확정한다.""",
    G60: """PG-60 Target Validation
역할: 게시된 target 업무 범위를 원천 지표와 비교하고 API에 최종 성공을 요청한다.
흐름: 60 load.stage=TARGET_VALIDATION → 61 target 지표·PASS/FAIL → 62·63 지표 기록 → 64·65 success → 66 확인
입력: published / 출력: errors → PG-90
주의: SUCCESS와 RUN_SUCCESS 이벤트는 API가 기록한다. 실패해도 재게시하지 않는다(FAILED_TARGET_VALIDATION).""",
    G70: """PG-70 Cleanup
역할: 보존 기간이 지난 끝난 run의 staging external table과 HDFS run 경로를 지운다.
흐름: 70 1시간 주기(Primary) → 71 load.stage=CLEANUP → 72 정리 대상 조회(GET /cleanup/candidates) → 73 run별 분할
      → 74 attribute → 75 경로·table 이름 검사 → 76·77 DROP TABLE → 78 DeleteHDFS → 79·7A 정리 기록(POST /cleanup)
출력: errors → PG-90(이벤트만)
주의: 무엇을 언제 지울지는 API가 정한다(SUCCESS 3일, 실패·TIMED_OUT 14일, PUBLISH_UNKNOWN 제외).
      75가 경로를 HDFS.STAGE.ROOT/JOB.KEY/run_id=<runId>로 고정해 다른 경로를 지우지 않게 한다. 실패하면 다음 주기에 다시 한다.""",
    G90: """PG-90 Error and Event
역할: 모든 PG의 실패를 한 곳에서 처리한다.
흐름: 90 오류 정규화(load.stage, HTTP 상태, SQL 오류, 연결 예외 → 코드·수준·메시지) → 91 분기
      → 92·93 run 실패 보고(MANIFEST 단계) / 94·95 파티션 실패 보고(claim한 파티션) → 96 load_event 기록 → 97 로그
입력: errors(모든 PG)
주의: 409(중복 실행, CLAIM_MISMATCH)는 정상 경합이므로 WARN. 상태 전이 이벤트는 API가 기록하므로 여기서는 오류·경고만 남긴다.""",
}
for g, text in PG_LABELS.items():
    label(g, text, 0, -2 * ROWH, 5 * COLW, 170)
label(TOP, """SQOOP_REPLACEMENT_POC_V4 — Load Control API 연동 Sqoop 대체 PoC (Oracle 원천)
PG-00 →(start-run)→ PG-10 →(partitions, Round Robin)→ PG-20 → API가 완료 판정 → API worker가 PG-05 호출
PG-05 →(validate)→ PG-40 →(staging-valid)→ PG-50 →(published)→ PG-60,  PG-05 →(reissue, Round Robin)→ PG-20
PG-70 Cleanup(1시간 주기),  모든 PG →(errors)→ PG-90
원장 기록·완료 판정은 Load Control API가 하고, NiFi는 데이터 처리와 API 호출만 한다.
상세: nifi-sqoop-removal-guide.md 2장, poc/REVIEW.md 6장""", 0, -250, 1100, 170)

# ===== PG-00 Trigger
port(G00, "start-run", "out", 3, 0)
port(G00, "errors", "out", 3, 1)
p(G00, "P00", "00_Generate_Trigger", "GenerateFlowFile",
  {"generate-ff-custom-text": "{}", "Unique FlowFiles": "false"}, 0, 0, sched="1 day",
  primary=True)  # 클러스터에서 노드마다 trigger가 생기지 않게 한다
ua(G00, "P01", "01_Set_Trigger_Attributes", {
    "load.job.key": "#{JOB.KEY}", "load.business.key": "#{BUSINESS.KEY}",
    "load.trigger.type": "SCHEDULE", "load.stage": "RUN_CREATE"}, 1, 0)
# business key는 SQL(SRC.BASE.WHERE)에 들어가므로 API 검증과 별개로 형식을 고정한다.
route(G00, "P02", "02_Validate_Trigger", {
    "valid": "${load.business.key:matches('^[0-9]{4}-[0-9]{2}-[0-9]{2}$')}"}, 2, 0)
c(G00, "P00", "success", "P01"); c(G00, "P01", "success", "P02")
c(G00, "P02", "valid", ("out", "start-run")); c(G00, "P02", "unmatched", ("out", "errors"))

# ===== PG-10 Run Coordinator (가이드 7장)
port(G10, "start-run", "in", 0, 0)
port(G10, "partitions", "out", 4, 2)
port(G10, "errors", "out", 4, 1)
body(G10, "P11", "11_Build_Run_Body",
     '{"jobKey":"${load.job.key}","businessKey":"${load.business.key}","hdfsRoot":"#{HDFS.STAGE.ROOT}",'
     '"stageTablePrefix":"#{HIVE.STAGE.TABLE.PREFIX}","allowEmptySource":#{ALLOW.EMPTY.SOURCE}}', 1, 0)
invoke(G10, "P12", "12_Create_Run", "/runs", 2, 0)
ua(G10, "P13", "13_Set_Run_Attrs", {
    "load.run.id": "${api.response:jsonPath('$.runId')}",
    "load.hdfs.path": "${api.response:jsonPath('$.hdfsRunPath')}",
    "load.stage.table": "${api.response:jsonPath('$.stageTable')}",
    "load.stage": "MANIFEST"}, 3, 0)
# 14·15: Oracle SCN을 고정한다. 이후 모든 원천 조회(16, PG-20 34)가 같은 SCN을 읽는다(가이드 7.2).
# V$DATABASE 조회 권한이 없으면 SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) AS SNAPSHOT_SCN FROM DUAL로 바꾼다.
esql(G10, "P14", "14_Query_Current_SCN", SRC, "SELECT TO_CHAR(CURRENT_SCN) AS SNAPSHOT_SCN FROM V$DATABASE", 0, 1)
p(G10, "P15", "15_Extract_SCN", "EvaluateJsonPath", {
    "Destination": "flowfile-attribute", "load.snapshot.scn": "$[0].SNAPSHOT_SCN"}, 1, 1)
# 16: 원천 지표와 파티션 경계·건수를 같은 SCN에서 한 문장으로 계산한다(가이드 7.3).
# - SCN이 숫자가 아니면 INVALID_SCN이 들어가 SQL 오류로 실패한다(별도 RouteOnAttribute 없음).
# - 경계·건수·SCN은 TO_CHAR로 문자열 반환: NUMBER(38)이 JSON 숫자로 바뀌며 정밀도를 잃지 않게 한다.
#   API는 숫자 문자열을 정수로 받아들인다. boolean은 19c 이하에 SQL 타입이 없어 'true'/'false' 문자열이다.
# - Oracle은 따옴표 없는 별칭을 대문자로 돌려주므로 결과 컬럼을 대문자로 두고 17의 Jolt도 대문자 키를 쓴다.
# - MIN_TS/MAX_TS는 stage 검증에서 시간대 해석 차이를 잡기 위한 지표다(가이드 4장, 10.3).
SCN = "${load.snapshot.scn:matches('^[0-9]+$'):ifElse(${load.snapshot.scn},'INVALID_SCN')}"
MANIFEST_SQL = """WITH m AS (
  SELECT COUNT(*) AS source_count, COUNT(*) - COUNT(@SPLIT@) AS null_cnt,
         NVL(MIN(@SPLIT@), 0) AS mn, NVL(MAX(@SPLIT@), 0) AS mx,
         NVL(SUM(#{DQ.AMOUNT.COLUMN}), 0) AS amount_sum,
         MIN(#{DQ.TIMESTAMP.COLUMN}) AS min_ts, MAX(#{DQ.TIMESTAMP.COLUMN}) AS max_ts
    FROM @TABLE@ AS OF SCN @SCN@
   WHERE #{SRC.BASE.WHERE}
), g AS (
  SELECT LEVEL - 1 AS pid FROM DUAL CONNECT BY LEVEL <= @N@
), b AS (
  SELECT g.pid,
         m.mn + FLOOR(g.pid * (m.mx - m.mn + 1) / @N@) AS lo,
         CASE WHEN g.pid = @N@ - 1 THEN m.mx
              ELSE m.mn + FLOOR((g.pid + 1) * (m.mx - m.mn + 1) / @N@) END AS hi,
         CASE WHEN g.pid = @N@ - 1 THEN 1 ELSE 0 END AS incl
    FROM m CROSS JOIN g
), c AS (
  SELECT b.pid, b.lo, b.hi, b.incl,
         (SELECT COUNT(*) FROM @TABLE@ AS OF SCN @SCN@ s
           WHERE #{SRC.BASE.WHERE}
             AND s.@SPLIT@ >= b.lo
             AND (s.@SPLIT@ < b.hi OR (b.incl = 1 AND s.@SPLIT@ = b.hi))) AS cnt
    FROM b
)
SELECT LPAD(c.pid, 4, '0') AS PARTITION_ID,
       TO_CHAR(c.lo) AS LOWER_BOUND, TO_CHAR(c.hi) AS UPPER_BOUND,
       CASE c.incl WHEN 1 THEN 'true' ELSE 'false' END AS UPPER_INCLUSIVE,
       'false' AS IS_NULL_PARTITION, TO_CHAR(c.cnt) AS EXPECTED_ROW_COUNT,
       TO_CHAR(@SCN@) AS SNAPSHOT_SCN,
       TO_CHAR(m.source_count) AS SOURCE_COUNT, TO_CHAR(m.null_cnt) AS SOURCE_NULL_SPLIT_COUNT,
       TO_CHAR(m.mn) AS SOURCE_MIN, TO_CHAR(m.mx) AS SOURCE_MAX,
       TO_CHAR(@N@) AS PLANNED_PARTITION_COUNT,
       TO_CHAR(m.amount_sum) AS AMOUNT_SUM,
       TO_CHAR(m.min_ts, 'YYYY-MM-DD HH24:MI:SS') AS MIN_TS,
       TO_CHAR(m.max_ts, 'YYYY-MM-DD HH24:MI:SS') AS MAX_TS
  FROM c CROSS JOIN m
 ORDER BY c.pid"""
MANIFEST_SQL = (MANIFEST_SQL.replace("@TABLE@", SRC_TABLE).replace("@SPLIT@", SPLIT)
                .replace("@N@", N).replace("@SCN@", SCN))
esql(G10, "P16", "16_Query_Source_Manifest", SRC, MANIFEST_SQL, 2, 1,
     extra={"Max Wait Time": "#{EXTRACT.QUERY.TIMEOUT}"})
# 17: 배열 → manifest 요청. 0번 행에서 SCN과 원천 지표를 꺼낸다. Jolt shift는 "0"과 "*"가 겹치면 "0"만 적용하므로
# 0번 행에도 파티션 필드 매핑을 함께 둔다.
PART_MAP = {"PARTITION_ID": "partitions[&1].partitionId", "LOWER_BOUND": "partitions[&1].lowerBound",
            "UPPER_BOUND": "partitions[&1].upperBound", "UPPER_INCLUSIVE": "partitions[&1].upperInclusive",
            "IS_NULL_PARTITION": "partitions[&1].isNullPartition",
            "EXPECTED_ROW_COUNT": "partitions[&1].expectedRowCount"}
JOLT_MANIFEST = json.dumps([
    {"operation": "shift", "spec": {
        "0": {**PART_MAP, "SNAPSHOT_SCN": "snapshotScn", "SOURCE_COUNT": "sourceCount",
              "SOURCE_NULL_SPLIT_COUNT": "sourceNullSplitCount", "SOURCE_MIN": "sourceMinSplit",
              "SOURCE_MAX": "sourceMaxSplit", "PLANNED_PARTITION_COUNT": "plannedPartitionCount",
              "AMOUNT_SUM": "sourceMetrics.AMOUNT_SUM", "MIN_TS": "sourceMetrics.MIN_TS",
              "MAX_TS": "sourceMetrics.MAX_TS"},
        "*": PART_MAP}},
], indent=1)
p(G10, "P17", "17_Build_Manifest_Body", "JoltTransformJSON", {
    "Jolt Transform": "jolt-transform-chain", "Jolt Specification": JOLT_MANIFEST}, 3, 1)
invoke(G10, "P18", "18_Register_Manifest", "/runs/${load.run.id}/manifest", 0, 2, attr_response=False)
p(G10, "P19", "19_Split_Dispatch_Partitions", "SplitJson", {"JsonPath Expression": "$.dispatchPartitions"}, 1, 2)
p(G10, "P20", "20_Extract_Partition_Attrs", "EvaluateJsonPath", {
    "Destination": "flowfile-attribute",
    "partition.id": "$.partitionId", "partition.lower": "$.lowerBound", "partition.upper": "$.upperBound",
    "partition.upper.inclusive": "$.upperInclusive", "partition.is.null": "$.isNullPartition",
    "partition.expected.rows": "$.expectedRowCount"}, 2, 2)
c(G10, ("in", "start-run"), [], "P11"); c(G10, "P11", "success", "P12")
c(G10, "P12", "Original", "P13"); c(G10, "P13", "success", "P14")
c(G10, "P14", "success", "P15"); c(G10, "P14", "failure", ("out", "errors"))
c(G10, "P15", "matched", "P16"); c(G10, "P15", ["unmatched", "failure"], ("out", "errors"))
c(G10, "P16", "success", "P17"); c(G10, "P16", "failure", ("out", "errors"))
c(G10, "P17", "success", "P18"); c(G10, "P17", "failure", ("out", "errors"))
c(G10, "P18", "Response", "P19")
c(G10, "P19", "split", "P20"); c(G10, "P19", "failure", ("out", "errors"))
c(G10, "P20", "matched", ("out", "partitions")); c(G10, "P20", ["unmatched", "failure"], ("out", "errors"))

# ===== PG-20 Extract Worker (가이드 8장). 입력은 PG-10 manifest와 PG-05 reissue 두 곳
port(G20, "partitions", "in", 0, 0)
port(G20, "errors", "out", 4, 2)
# UpdateAttribute는 들어온 attribute 기준으로 평가하므로 token 생성과 사용(31 본문)을 다른 Processor에 둔다.
ua(G20, "P30", "30_Set_Claim_Token", {"partition.claim.token": "${UUID()}", "load.stage": "EXTRACT"}, 1, 0)
body(G20, "P31", "31_Build_Claim_Body",
     '{"claimToken":"${partition.claim.token}","workerNode":"${hostname(true):escapeJson()}"}', 2, 0)
invoke(G20, "P32", "32_Claim_Partition", "/runs/${load.run.id}/partitions/${partition.id}/claim", 3, 0, tasks=2)
# claim 거절(다른 worker 소유)은 정상 경합이므로 조용히 끝낸다(unmatched auto-terminate).
route(G20, "P33", "33_Is_Owner", {
    "owner": f"${{api.response:jsonPath('$.claimed'):equals('true'):and(${{partition.lower:matches({NUM})}})"
             f":and(${{partition.upper:matches({NUM})}}):and(${{load.snapshot.scn:matches({NUM})}})}}"}, 4, 0)
# 고정 SCN으로 조회한다. 재시도하지 않는다: ORA-01555처럼 같은 SCN으로 다시 해도 실패할 오류에 긴 쿼리를
# 반복하지 않도록 바로 실패 보고하고 새 run으로 재실행한다(가이드 16장).
# 정밀도 없는 NUMBER 컬럼은 Default Decimal Precision/Scale로 기록된다. 기본값(10, 0)이면 소수점 이하가 잘리거나
# 큰 값이 깨질 수 있으므로 Parameter로 지정하고, 가능하면 SRC.COLUMNS에서 CAST(col AS NUMBER(p,s))로 명시한다.
esql(G20, "P34", "34_Execute_Partition_Query", SRC,
     f"SELECT #{{SRC.COLUMNS}}\n  FROM {SRC_TABLE} AS OF SCN ${{load.snapshot.scn}}\n WHERE #{{SRC.BASE.WHERE}}\n"
     f"   AND {SPLIT} >= ${{partition.lower}}\n"
     f"   AND {SPLIT} ${{partition.upper.inclusive:equals('true'):ifElse('<=','<')}} ${{partition.upper}}",
     0, 1, writer=PARQ, tasks=4, extra={
         "esql-max-rows": "#{EXTRACT.ROWS.PER.FILE}", "esql-output-batch-size": "0",
         "esql-fetch-size": "#{EXTRACT.FETCH.SIZE}", "Max Wait Time": "#{EXTRACT.QUERY.TIMEOUT}",
         "dbf-user-logical-types": "true",     # DATE/TIMESTAMP/DECIMAL 타입 유지
         "dbf-default-precision": "#{ORACLE.NUMBER.DEFAULT.PRECISION}",
         "dbf-default-scale": "#{ORACLE.NUMBER.DEFAULT.SCALE}"})
ua(G20, "P35", "35_Set_Chunk_Attrs", {
    "filename": "part-${partition.id}-${fragment.index:padLeft(6,'0')}.parquet",  # run root에 평탄하게 기록
    "chunk.index": "${fragment.index}", "load.stage": "CHUNK_WRITE"}, 1, 1)
put_hdfs(G20, "P36", "36_PutHDFS", 2, 1, tasks=4)
# PutHDFS 이후 content를 보고용 JSON으로 바꾼다(Parquet가 요청 본문으로 가지 않게).
body(G20, "P37", "37_Build_Chunk_Report",
     '{"claimToken":"${partition.claim.token}","chunkIndex":${fragment.index},"chunkCount":${fragment.count},'
     '"fragmentIdentifier":"${fragment.identifier}","hdfsPath":"${absolute.hdfs.path:escapeJson()}/${filename}",'
     '"recordCount":${record.count},"byteCount":${fileSize}}', 3, 1)
# 판정(파티션 SUCCESS/FAILED, run 완료, 검증 예약)과 이벤트는 API가 한다. NiFi는 응답을 더 보지 않는다.
invoke(G20, "P38", "38_Report_Chunk", "/runs/${load.run.id}/partitions/${partition.id}/chunks", 4, 1, tasks=2)
c(G20, ("in", "partitions"), [], "P30"); c(G20, "P30", "success", "P31"); c(G20, "P31", "success", "P32")
c(G20, "P32", "Original", "P33"); c(G20, "P33", "owner", "P34")
c(G20, "P34", "success", "P35"); c(G20, "P34", "failure", ("out", "errors"))
c(G20, "P35", "success", "P36"); c(G20, "P36", "success", "P37"); c(G20, "P37", "success", "P38")

# ===== PG-05 Control Receiver (가이드 9.5). 등록된 Job 경로만 받는다(그 외는 HandleHttpRequest가 404).
port(G05, "validate", "out", 4, 0)
port(G05, "reissue", "out", 4, 1)
port(G05, "errors", "out", 4, 2)
p(G05, "R05", "05_Listen_Control", "HandleHttpRequest", {
    "Listening Port": "#{CONTROL.LISTEN.PORT}", "HTTP Context Map": HTTPCTX,
    "Allowed Paths": "/(validate|reissue)/#{JOB.KEY}",
    "Allow GET": "false", "Allow POST": "true", "Allow PUT": "false", "Allow DELETE": "false",
    "Allow HEAD": "false", "Allow OPTIONS": "false"}, 0, 0)
route(G05, "R06", "06_Validate_Request", {
    "valid": "${http.method:equals('POST'):and(${http.headers.X-Dispatch-Id:matches("
             "'^[0-9a-fA-F-]{36}$')}):and(${http.headers.X-Run-Id:matches('^[0-9a-fA-F-]{36}$')})}"}, 1, 0)
p(G05, "R07", "07_Respond_400", "HandleHttpResponse", {"HTTP Status Code": "400", "HTTP Context Map": HTTPCTX}, 1, 1)
# 검증은 오래 걸릴 수 있으므로 먼저 202를 응답하고 HTTP 연결을 붙잡지 않는다.
p(G05, "R08", "08_Respond_202", "HandleHttpResponse", {"HTTP Status Code": "202", "HTTP Context Map": HTTPCTX}, 2, 0)
# validate·reissue 본문을 한 번에 추출한다(없는 경로는 빈 값).
p(G05, "R09", "09_Extract_Control_Body", "EvaluateJsonPath", {
    "Destination": "flowfile-attribute", "Path Not Found Behavior": "ignore",
    "load.run.id": "$.runId", "load.dispatch.id": "$.dispatchId",
    "partition.id": "$.partitionId", "load.business.key": "$.businessKey", "load.hdfs.path": "$.hdfsRunPath",
    "partition.lower": "$.lowerBound", "partition.upper": "$.upperBound",
    "partition.upper.inclusive": "$.upperInclusive", "partition.is.null": "$.isNullPartition",
    "partition.expected.rows": "$.expectedRowCount", "load.snapshot.scn": "$.snapshotScn"}, 3, 0)
route(G05, "R10", "10_Route_By_Action", {
    "validate": "${http.request.uri:startsWith('/validate/')}",
    "reissue": "${http.request.uri:startsWith('/reissue/')}"}, 3, 1)
c(G05, "R05", "success", "R06"); c(G05, "R06", "valid", "R08"); c(G05, "R06", "unmatched", "R07")
c(G05, "R07", "success", ("out", "errors")); c(G05, "R08", "success", "R09")
c(G05, "R09", "matched", "R10"); c(G05, "R09", ["unmatched", "failure"], ("out", "errors"))
c(G05, "R10", "validate", ("out", "validate")); c(G05, "R10", "reissue", ("out", "reissue"))
c(G05, "R10", "unmatched", ("out", "errors"))

# ===== PG-40 Staging Validation (가이드 10장)
port(G40, "validate", "in", 0, 0)
port(G40, "errors", "out", 5, 1)
port(G40, "staging-valid", "out", 5, 3)
ua(G40, "V40", "40_Set_Validation_Stage", {"load.stage": "VALIDATION_START"}, 1, 0)
body(G40, "V41", "41_Build_Start_Body", '{"dispatchId":"${load.dispatch.id}","node":"${hostname(true):escapeJson()}"}', 2, 0)
invoke(G40, "V42", "42_Validation_Start", "/runs/${load.run.id}/validation/start", 3, 0)
# started=false는 중복 dispatch이므로 조용히 끝낸다.
route(G40, "V43", "43_Is_Started", {"started": "${api.response:jsonPath('$.started'):equals('true')}"}, 4, 0)
# 검증 flow는 API 호출로 새로 시작되므로 기대값(원천 건수·지표)을 /validation/start 응답에서 받는다(가이드 10.2).
# 여기부터 load.stage=STAGE_VALIDATION: 실패하면 PG-90이 run을 FAILED_STAGE_VALIDATION으로 보고한다.
ua(G40, "V44", "44_Set_Run_Attrs", {
    "load.hdfs.path": "${api.response:jsonPath('$.hdfsRunPath')}",
    "load.stage.table": "${api.response:jsonPath('$.stageTable')}",
    "load.business.key": "${api.response:jsonPath('$.businessKey')}",
    "load.job.key": "${api.response:jsonPath('$.jobKey')}",
    "load.snapshot.scn": "${api.response:jsonPath('$.snapshotScn')}",
    "load.source.count": "${api.response:jsonPath('$.sourceCount')}",
    "load.extracted.count": "${api.response:jsonPath('$.extractedCount')}",
    "validation.source.AMOUNT_SUM": "${api.response:jsonPath('$.sourceMetrics.AMOUNT_SUM')}",
    "validation.source.MIN_TS": "${api.response:jsonPath('$.sourceMetrics.MIN_TS')}",
    "validation.source.MAX_TS": "${api.response:jsonPath('$.sourceMetrics.MAX_TS')}",
    "filename": "_SUCCESS", "load.stage": "STAGE_VALIDATION"}, 1, 1)
body(G40, "V45", "45_Empty_Content", "", 2, 1)
put_hdfs(G40, "V46", "46_PutHDFS_SUCCESS_Marker", 3, 1)

# SQL에 들어가는 FlowFile 값은 형식을 고정한다. 형식이 틀리면 SQL이 실패하도록 바꿔 errors로 보낸다.
STAGE_TBL = "#{HIVE.STAGE.DB}.${load.stage.table:matches('^[A-Za-z0-9_]{1,128}$'):ifElse(${load.stage.table},'invalid-stage-table')}"


def num_attr(a):
    return "${" + a + ":matches('^-?[0-9]+$'):ifElse(${" + a + "},'NULL')}"


def str_attr(a):
    return "${" + a + ":replaceAll('[^0-9A-Za-z:. _-]', '')}"


def metrics_sql(table_where, count_name):
    """지표마다 (metric_name, expected_value, actual_value, result) 한 행(가이드 10.3).
    MIN_TS/MAX_TS는 원천 TO_CHAR와 같은 형식의 문자열로 비교한다. AMOUNT_SUM은 DECIMAL(38,2)로 맞춰 비교한다."""
    return f"""WITH s AS (
  SELECT COUNT(*) AS cnt,
         COALESCE(SUM(CASE WHEN #{{SRC.SPLIT.COLUMN}} IS NULL THEN 1 ELSE 0 END), 0) AS null_cnt,
         COUNT(*) - COUNT(DISTINCT #{{DQ.PK.COLUMN}}) AS dup_cnt,
         COALESCE(SUM(#{{DQ.AMOUNT.COLUMN}}), 0) AS amount_sum,
         COALESCE(DATE_FORMAT(MIN(#{{DQ.TIMESTAMP.COLUMN}}), 'yyyy-MM-dd HH:mm:ss'), '') AS min_ts,
         COALESCE(DATE_FORMAT(MAX(#{{DQ.TIMESTAMP.COLUMN}}), 'yyyy-MM-dd HH:mm:ss'), '') AS max_ts
    FROM {table_where}
)
SELECT '{count_name}' AS metric_name, '{num_attr('load.source.count')}' AS expected_value,
       CAST(cnt AS STRING) AS actual_value,
       IF(cnt = {num_attr('load.source.count')} AND cnt = {num_attr('load.extracted.count')}, 'PASS', 'FAIL') AS result
  FROM s
UNION ALL
SELECT 'NULL_SPLIT_COUNT', '0', CAST(null_cnt AS STRING), IF(null_cnt = 0, 'PASS', 'FAIL') FROM s
UNION ALL
SELECT 'DUP_PK_COUNT', '0', CAST(dup_cnt AS STRING), IF(dup_cnt = 0, 'PASS', 'FAIL') FROM s
UNION ALL
SELECT 'AMOUNT_SUM', '{str_attr('validation.source.AMOUNT_SUM')}', CAST(amount_sum AS STRING),
       IF(CAST(amount_sum AS DECIMAL(38,2)) = CAST('{str_attr('validation.source.AMOUNT_SUM')}' AS DECIMAL(38,2)), 'PASS', 'FAIL') FROM s
UNION ALL
SELECT 'MIN_TS', '{str_attr('validation.source.MIN_TS')}', min_ts, IF(min_ts = '{str_attr('validation.source.MIN_TS')}', 'PASS', 'FAIL') FROM s
UNION ALL
SELECT 'MAX_TS', '{str_attr('validation.source.MAX_TS')}', max_ts, IF(max_ts = '{str_attr('validation.source.MAX_TS')}', 'PASS', 'FAIL') FROM s"""


def jolt_metrics(stage):
    """ExecuteSQLRecord 결과 배열 → /validations 요청. Hive 결과 컬럼은 소문자다."""
    return json.dumps([
        {"operation": "shift", "spec": {"*": {
            "metric_name": "metrics[&1].metricName", "expected_value": "metrics[&1].expectedValue",
            "actual_value": "metrics[&1].actualValue", "result": "metrics[&1].result"}}},
        {"operation": "default", "spec": {"stage": stage, "queryVersion": "v1"}}], indent=1)


# _SUCCESS(숨김 파일)는 Hive가 읽지 않으므로 run 경로를 그대로 LOCATION으로 쓴다.
body(G40, "V47", "47_Build_External_DDL",
     f"CREATE EXTERNAL TABLE IF NOT EXISTS {STAGE_TBL} (\n  #{{HIVE.STAGE.DDL.COLUMNS}}\n)\n"
     "STORED AS PARQUET\nLOCATION '${load.hdfs.path}'", 0, 2)
hive_ql(G40, "V48", "48_Create_External_Table", 1, 2, retry=(["retry"], 3))
esql(G40, "V49", "49_Query_Stage_Metrics", HIVE, metrics_sql(STAGE_TBL, "STAGE_COUNT"), 2, 2,
     extra={"Max Wait Time": "#{HIVE.QUERY.TIMEOUT} secs"})
p(G40, "V4A", "4A_Build_Validations_Body", "JoltTransformJSON", {
    "Jolt Transform": "jolt-transform-chain", "Jolt Specification": jolt_metrics("STAGING")}, 3, 2)
invoke(G40, "V4B", "4B_Report_Validations", "/runs/${load.run.id}/validations", 4, 2)
body(G40, "V4C", "4C_Empty_Json", "{}", 1, 3)
invoke(G40, "V4D", "4D_Stage_Validated", "/runs/${load.run.id}/stage-validated", 2, 3)
route(G40, "V4E", "4E_Is_Stage_Validated", {
    "validated": "${api.response:jsonPath('$.stageValidated'):equals('true')}"}, 3, 3)
c(G40, ("in", "validate"), [], "V40"); c(G40, "V40", "success", "V41"); c(G40, "V41", "success", "V42")
c(G40, "V42", "Original", "V43"); c(G40, "V43", "started", "V44")
c(G40, "V44", "success", "V45"); c(G40, "V45", "success", "V46"); c(G40, "V45", "failure", ("out", "errors"))
c(G40, "V46", "success", "V47"); c(G40, "V47", "success", "V48"); c(G40, "V47", "failure", ("out", "errors"))
c(G40, "V48", "success", "V49"); c(G40, "V48", ["failure", "retry"], ("out", "errors"))
c(G40, "V49", "success", "V4A"); c(G40, "V49", "failure", ("out", "errors"))
c(G40, "V4A", "success", "V4B"); c(G40, "V4A", "failure", ("out", "errors"))
c(G40, "V4B", "Original", "V4C"); c(G40, "V4C", "success", "V4D"); c(G40, "V4C", "failure", ("out", "errors"))
c(G40, "V4D", "Original", "V4E")
c(G40, "V4E", "validated", ("out", "staging-valid")); c(G40, "V4E", "unmatched", ("out", "errors"))

# ===== PG-50 Publish (가이드 11장). 결과는 57이 직접 보고하므로 PG-90은 이벤트만 남긴다.
port(G50, "staging-valid", "in", 0, 0)
port(G50, "errors", "out", 5, 1)
port(G50, "published", "out", 5, 2)
ua(G50, "B50", "50_Set_Publish_Token", {"publish.token": "${UUID()}", "load.stage": "PUBLISH"}, 1, 0)
body(G50, "B51", "51_Build_Claim_Body", '{"publishToken":"${publish.token}"}', 2, 0)
invoke(G50, "B52", "52_Claim_Publish", "/runs/${load.run.id}/publish/claim", 3, 0)
route(G50, "B53", "53_Is_Publish_Owner", {"claimed": "${api.response:jsonPath('$.claimed'):equals('true')}"}, 4, 0)
body(G50, "B54", "54_Build_Insert_Overwrite_SQL",
     "INSERT OVERWRITE TABLE #{HIVE.TARGET.DB}.#{HIVE.TARGET.TABLE}\n#{TARGET.PARTITION.CLAUSE}\n"
     f"SELECT #{{HIVE.INSERT.COLUMNS}}\n  FROM {STAGE_TBL}", 0, 1)
# 재시도 없음. PutClouderaHiveQL은 오류 attribute를 남기지 않아 실행 전 실패(FAILED_PUBLISH)를 가려낼 수 없으므로
# failure·retry를 모두 PUBLISH_UNKNOWN으로 보고한다(가이드 11.2, 55A·56F 생략).
hive_ql(G50, "B55", "55_Insert_Overwrite", 1, 1)
body(G50, "B56", "56_Body_PUBLISHED", '{"publishToken":"${publish.token}","outcome":"PUBLISHED"}', 2, 1)
body(G50, "B56U", "56U_Body_PUBLISH_UNKNOWN",
     '{"publishToken":"${publish.token}","outcome":"PUBLISH_UNKNOWN","errorCode":"HIVE_PUBLISH_FAILED",'
     '"message":"INSERT OVERWRITE routed to failure/retry on ${hostname(true):escapeJson()}; check Hive query history and NiFi bulletin"}',
     2, 2)
invoke(G50, "B57", "57_Report_Publish_Result", "/runs/${load.run.id}/publish/result", 3, 1)
route(G50, "B58", "58_Is_Published", {"published": "${api.response:jsonPath('$.runStatus'):equals('PUBLISHED')}"}, 4, 1)
c(G50, ("in", "staging-valid"), [], "B50"); c(G50, "B50", "success", "B51"); c(G50, "B51", "success", "B52")
c(G50, "B52", "Original", "B53"); c(G50, "B53", "claimed", "B54")
c(G50, "B54", "success", "B55"); c(G50, "B54", "failure", ("out", "errors"))
c(G50, "B55", "success", "B56"); c(G50, "B55", ["failure", "retry"], "B56U")
c(G50, "B56", "success", "B57"); c(G50, "B56U", "success", "B57")
c(G50, "B56", "failure", ("out", "errors")); c(G50, "B56U", "failure", ("out", "errors"))
c(G50, "B57", "Original", "B58")
c(G50, "B58", "published", ("out", "published")); c(G50, "B58", "unmatched", ("out", "errors"))

# ===== PG-60 Target Validation (가이드 12장)
port(G60, "published", "in", 0, 0)
port(G60, "errors", "out", 5, 1)
ua(G60, "T60", "60_Set_Target_Stage", {"load.stage": "TARGET_VALIDATION"}, 1, 0)
esql(G60, "T61", "61_Query_Target_Metrics", HIVE,
     metrics_sql("#{HIVE.TARGET.DB}.#{HIVE.TARGET.TABLE}\n   WHERE #{TARGET.BUSINESS.WHERE}", "TARGET_COUNT"), 2, 0,
     extra={"Max Wait Time": "#{HIVE.QUERY.TIMEOUT} secs"})
p(G60, "T62", "62_Build_Validations_Body", "JoltTransformJSON", {
    "Jolt Transform": "jolt-transform-chain", "Jolt Specification": jolt_metrics("TARGET")}, 3, 0)
invoke(G60, "T63", "63_Report_Validations", "/runs/${load.run.id}/validations", 4, 0)
body(G60, "T64", "64_Empty_Json", "{}", 1, 1)
invoke(G60, "T65", "65_Report_Success", "/runs/${load.run.id}/success", 2, 1)
route(G60, "T66", "66_Is_Success", {"success": "${api.response:jsonPath('$.success'):equals('true')}"}, 3, 1)
c(G60, ("in", "published"), [], "T60"); c(G60, "T60", "success", "T61")
c(G60, "T61", "success", "T62"); c(G60, "T61", "failure", ("out", "errors"))
c(G60, "T62", "success", "T63"); c(G60, "T62", "failure", ("out", "errors"))
c(G60, "T63", "Original", "T64"); c(G60, "T64", "success", "T65"); c(G60, "T64", "failure", ("out", "errors"))
c(G60, "T65", "Original", "T66"); c(G60, "T66", "unmatched", ("out", "errors"))

# ===== PG-70 Cleanup. 대상 판정은 API, NiFi는 지우고 기록만 한다.
port(G70, "errors", "out", 5, 1)
p(G70, "C70", "70_Generate_Cleanup_Trigger", "GenerateFlowFile",
  {"generate-ff-custom-text": "{}", "Unique FlowFiles": "false"}, 0, 0, sched="1 hour", primary=True)
# Parameter는 EL 문자열 리터럴 안(예: literal('#{X}'))에서는 치환되지 않으므로, 75가 비교할 값을 여기서 attribute로 만든다.
ua(G70, "C71", "71_Set_Cleanup_Stage", {
    "load.stage": "CLEANUP", "load.job.key": "#{JOB.KEY}",
    "cleanup.path.prefix": "#{HDFS.STAGE.ROOT}/#{JOB.KEY}/run_id=",
    "cleanup.table.prefix": "#{HIVE.STAGE.TABLE.PREFIX}"}, 1, 0)
invoke(G70, "C72", "72_Get_Cleanup_Candidates", "/cleanup/candidates?jobKey=#{JOB.KEY}&limit=#{CLEANUP.BATCH}",
       2, 0, attr_response=False, method="GET")
p(G70, "C73", "73_Split_Runs", "SplitJson", {"JsonPath Expression": "$.runs"}, 3, 0)
p(G70, "C74", "74_Extract_Run_Attrs", "EvaluateJsonPath", {
    "Destination": "flowfile-attribute", "load.run.id": "$.runId", "load.hdfs.path": "$.hdfsRunPath",
    "load.stage.table": "$.stageTable", "load.business.key": "$.businessKey",
    "cleanup.run.status": "$.status"}, 4, 0)
# DeleteHDFS는 glob도 받으므로 지울 경로를 API 응답 그대로 믿지 않고 이 Job의 run 경로 형식과 정확히 비교한다.
route(G70, "C75", "75_Check_Cleanup_Target", {
    "safe": "${load.run.id:matches('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')"
            ":and(${load.hdfs.path:equals(${cleanup.path.prefix:replaceAll('/+', '/'):append(${load.run.id})})})"
            ":and(${load.stage.table:matches('^[a-z0-9_]{1,128}$')})"
            ":and(${load.stage.table:startsWith(${cleanup.table.prefix:toLower()})})}"}, 0, 1)
body(G70, "C76", "76_Build_Drop_SQL", "DROP TABLE IF EXISTS #{HIVE.STAGE.DB}.${load.stage.table}", 1, 1)
hive_ql(G70, "C77", "77_Drop_Stage_Table", 2, 1, retry=(["retry"], 3))
p(G70, "C78", "78_Delete_Run_Path", "DeleteHDFS", {
    "Hadoop Configuration Resources": "#{HADOOP.CONF.FILES}", "file_or_directory": "${load.hdfs.path}",
    "recursive": "true"}, 3, 1, retry=(["failure"], 3))
body(G70, "C79", "79_Build_Cleanup_Body",
     '{"droppedTable":"#{HIVE.STAGE.DB}.${load.stage.table}","deletedPath":"${load.hdfs.path:escapeJson()}"}', 4, 1)
invoke(G70, "C7A", "7A_Report_Cleanup", "/runs/${load.run.id}/cleanup", 1, 2)
c(G70, "C70", "success", "C71"); c(G70, "C71", "success", "C72")
c(G70, "C72", "Response", "C73")
c(G70, "C73", "split", "C74"); c(G70, "C73", "failure", ("out", "errors"))
c(G70, "C74", "matched", "C75"); c(G70, "C74", ["unmatched", "failure"], ("out", "errors"))
c(G70, "C75", "safe", "C76"); c(G70, "C75", "unmatched", ("out", "errors"))
c(G70, "C76", "success", "C77"); c(G70, "C76", "failure", ("out", "errors"))
c(G70, "C77", "success", "C78"); c(G70, "C77", ["failure", "retry"], ("out", "errors"))
c(G70, "C78", "success", "C79"); c(G70, "C78", "failure", ("out", "errors"))
c(G70, "C79", "success", "C7A"); c(G70, "C79", "failure", ("out", "errors"))

# ===== PG-90 Error and Event (가이드 14장). 모든 PG의 errors가 여기로 온다.
port(G90, "errors", "in", 0, 0)
# 같은 UpdateAttribute 안에서는 방금 만든 값을 참조할 수 없으므로 식마다 원천 attribute를 직접 쓴다.
# - 4xx/5xx 응답: invokehttp.status.code (2xx 값은 앞 단계 성공 흔적이므로 무시)
# - API 연결 실패: invokehttp.java.exception.class
# - SQL 실패: executesql.error.message
# - 그 밖(PutHDFS, 형식 검증, Control Receiver 400): load.stage로 식별
HTTP_ERR = "${invokehttp.status.code:matches('[3-5][0-9]{2}')}"
# 409는 정상 경합(DUPLICATE_ACTIVE_RUN, CLAIM_MISMATCH, CHUNK_CONFLICT)이므로 API 오류 코드를 이름으로 쓰고 WARN으로 남긴다(가이드 9.3).
# Response Body Attribute Name을 쓰면 4xx/5xx 본문도 api.response로 들어간다.
BODY_CODE = "${invokehttp.response.body:replaceNull(${api.response}):jsonPath('$.code'):replaceEmpty('HTTP_409')}"
STAGE_FAILED = "${load.stage:replaceNull('CONTROL_RECEIVER'):append('_FAILED')}"
ua(G90, "E90", "90_Normalize_Error", {
    "error.stage": "${load.stage:replaceNull('CONTROL_RECEIVER')}",
    "error.code": "${invokehttp.status.code:equals('409'):ifElse(" + BODY_CODE + ","
                  "${invokehttp.status.code:matches('[3-5][0-9]{2}'):ifElse(${invokehttp.status.code:prepend('HTTP_')},"
                  "${invokehttp.java.exception.class:isEmpty():not():ifElse('API_UNREACHABLE',"
                  "${executesql.error.message:find('ORA-[0-9]{5}'):ifElse("
                  "${executesql.error.message:replaceAll('(?s)^.*?(ORA-[0-9]{5}).*$','$1')},"
                  "${executesql.error.message:isEmpty():not():ifElse('SQL_ERROR'," + STAGE_FAILED + ")})})})})}",
    # 실패 보고 호출(93, 95)이 invokehttp.status.code를 덮어쓰므로 이벤트 수준과 이름을 여기서 정해 둔다.
    "error.level": "${invokehttp.status.code:equals('409'):ifElse('WARN','ERROR')}",
    "error.event": "${invokehttp.status.code:equals('409'):ifElse(" + BODY_CODE + "," + STAGE_FAILED + ")}",
    "error.class": "${invokehttp.java.exception.class:isEmpty():not():ifElse('TRANSIENT',"
                   "${invokehttp.status.code:matches('4[0-9]{2}'):ifElse('VALIDATION','NON_RETRYABLE')})}",
    # run 실패 보고 대상 단계의 기대 상태와 실패 상태(가이드 14.2 91 표)
    "load.fail.expected": "${load.stage:equals('MANIFEST'):ifElse('CREATED',"
                          "${load.stage:equals('STAGE_VALIDATION'):ifElse('STAGE_VALIDATING','PUBLISHED')})}",
    "load.fail.status": "${load.stage:equals('MANIFEST'):ifElse('FAILED_MANIFEST',"
                        "${load.stage:equals('STAGE_VALIDATION'):ifElse('FAILED_STAGE_VALIDATION','FAILED_TARGET_VALIDATION')})}",
    "error.message": f"${{{HTTP_ERR[2:-1]}:ifElse(${{invokehttp.response.body:replaceNull(${{api.response}})}},"
                     "${executesql.error.message:replaceNull(${invokehttp.java.exception.message:replaceNull("
                     # 판정 응답(stage-validated·success의 reasons, publish/result의 changed)이 거부 사유를 담고 있다.
                     "${api.response:matches('(?s).*\"(reasons|changed)\".*'):ifElse(${api.response},"
                     # 정리 실패는 어느 경로·테이블인지 남긴다(75 검사 거부, DROP·DeleteHDFS 실패).
                     "${load.stage:equals('CLEANUP'):ifElse(${literal('cleanup not done (target check, Hive DROP or "
                     "HDFS delete failed; see bulletin): path='):append(${load.hdfs.path}):append(' table=')"
                     ":append(${load.stage.table})},"
                     "'processor routed failure; see bulletin and provenance')})})})})}"},
   1, 0)
route(G90, "E91", "91_Route_Failure_Report", {
    # manifest·staging 검증·target 검증 단계 실패는 run 실패로 보고한다. manifest 422면 API가 이미 기록했다.
    # VALIDATION_START(API가 재전송)와 PUBLISH(57이 직접 보고)는 이벤트만 남긴다.
    "report_run": "${load.run.id:isEmpty():not():and(${load.stage:equals('MANIFEST')"
                  ":and(${invokehttp.status.code:equals('422'):not()})"
                  ":or(${load.stage:in('STAGE_VALIDATION','TARGET_VALIDATION')})})}",
    # claim에 성공한 파티션의 추출·기록 실패는 파티션 실패로 보고한다.
    "report_partition": "${load.stage:in('EXTRACT','CHUNK_WRITE'):and(${api.response:jsonPath('$.claimed'):equals('true')})}"},
      2, 0)
MSG_JSON = "${error.message:replaceAll('(?s)^(.{0,1500}).*$','$1'):escapeJson()}"
body(G90, "E92", "92_Build_Run_Fail_Body",
     '{"expectedStatus":"${load.fail.expected}","failStatus":"${load.fail.status}","errorStage":"${error.stage}",'
     '"errorCode":"${error.code}","message":"' + MSG_JSON + '"}', 3, 0)
p(G90, "E93", "93_Report_Run_Fail", "InvokeHTTP", {
    "HTTP Method": "POST", "HTTP URL": "#{CONTROL.API.URL}/runs/${load.run.id}/fail",
    "Request Content-Type": "application/json", "Connection Timeout": "5 secs",
    "Socket Read Timeout": "#{CONTROL.API.TIMEOUT}", "Authorization": "#{CONTROL.API.AUTHORIZATION}",
    "X-Request-Id": "${UUID()}", "X-Run-Id": "${load.run.id}"}, 4, 0, sensitive=("Authorization",),
  retry=(["Retry", "Failure"], 5))
body(G90, "E94", "94_Build_Partition_Fail_Body",
     '{"claimToken":"${partition.claim.token}","errorStage":"${error.stage}","errorClass":"${error.class}",'
     '"errorCode":"${error.code}","message":"' + MSG_JSON + '"}', 3, 1)
p(G90, "E95", "95_Report_Partition_Fail", "InvokeHTTP", {
    "HTTP Method": "POST", "HTTP URL": "#{CONTROL.API.URL}/runs/${load.run.id}/partitions/${partition.id}/fail",
    "Request Content-Type": "application/json", "Connection Timeout": "5 secs",
    "Socket Read Timeout": "#{CONTROL.API.TIMEOUT}", "Authorization": "#{CONTROL.API.AUTHORIZATION}",
    "X-Request-Id": "${UUID()}", "X-Run-Id": "${load.run.id}"}, 4, 1, sensitive=("Authorization",),
  retry=(["Retry", "Failure"], 5))
SQL_MSG = "${error.message:replaceAll(\"'\",\"''\"):replaceAll('[\\r\\n]+',' '):replaceAll('(?s)^(.{0,1500}).*$','$1')}"
p(G90, "E96", "96_Insert_Load_Event", "PutSQL", {
    "JDBC Connection Pool": META, "Support Fragmented Transactions": "false", "Batch Size": "1",
    "putsql-sql-statement":
        "INSERT INTO nifi_ops.load_event (event_id, event_level, event_name, run_id, job_key, business_key,\n"
        "    partition_id, chunk_index, process_group, processor_name, node_id, row_count,\n"
        "    error_class, error_code, message)\n"
        "VALUES (gen_random_uuid(),\n"
        "    '${error.level}', '${error.event}',\n"
        "    CAST(NULLIF('${load.run.id}', '') AS uuid), COALESCE(NULLIF('${load.job.key}', ''), '#{JOB.KEY}'),\n"
        "    NULLIF('${load.business.key}', ''), NULLIF('${partition.id}', ''), CAST(NULLIF('${chunk.index}', '') AS integer),\n"
        "    '" + TOP_NAME + "', NULL, '${hostname(true)}', NULL,\n"
        "    NULLIF('${error.class}', ''), NULLIF('${error.code}', ''), NULLIF('" + SQL_MSG + "', ''))"}, 2, 2)
p(G90, "E97", "97_LogMessage", "LogMessage", {
    "log-level": "${error.level:toLower()}",
    "log-prefix": "SQOOP_REPLACEMENT ",
    "log-message": "{\"stage\":\"${error.stage}\",\"code\":\"${error.code}\",\"class\":\"${error.class}\","
                   "\"run_id\":\"${load.run.id}\",\"job_key\":\"${load.job.key}\",\"business_key\":\"${load.business.key}\","
                   "\"partition_id\":\"${partition.id}\",\"message\":\"" + MSG_JSON + "\"}"}, 3, 2)
c(G90, ("in", "errors"), [], "E90"); c(G90, "E90", "success", "E91")
c(G90, "E91", "report_run", "E92"); c(G90, "E91", "report_partition", "E94"); c(G90, "E91", "unmatched", "E96")
c(G90, "E92", ["success", "failure"], "E93"); c(G90, "E94", ["success", "failure"], "E95")
c(G90, "E93", ["Original", "No Retry", "Retry", "Failure"], "E96")
c(G90, "E95", ["Original", "No Retry", "Retry", "Failure"], "E96")
c(G90, "E96", ["success", "failure", "retry"], "E97")

# ---------------------------------------------------------------- PG 간 연결(TOP)
links = [
    (G00, "start-run", G10, "start-run", {}),
    (G10, "partitions", G20, "partitions", {"loadBalanceStrategy": "ROUND_ROBIN"}),  # Coordinator → Worker
    (G05, "reissue", G20, "partitions", {"loadBalanceStrategy": "ROUND_ROBIN"}),
    (G05, "validate", G40, "validate", {}),
    (G40, "staging-valid", G50, "staging-valid", {}),
    (G50, "published", G60, "published", {}),
] + [(g, "errors", G90, "errors", {}) for g in (G00, G10, G20, G05, G40, G50, G60, G70)]


def endpoint(g, ref):
    if isinstance(ref, tuple):
        kind, name = ref
        return {"id": ports[(g, name, kind)], "groupId": g, "type": "INPUT_PORT" if kind == "in" else "OUTPUT_PORT"}
    ent, pg_id = procs[ref]
    return {"id": ent["component"]["id"], "groupId": pg_id, "type": "PROCESSOR"}


for g, src, rels, dst, extra in conns:
    comp = {"source": endpoint(g, src), "destination": endpoint(g, dst),
            "backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB", **extra}
    if not isinstance(src, tuple):
        comp["selectedRelationships"] = rels
    call("POST", f"/process-groups/{g}/connections", {"revision": REV, "component": comp})

for sg, sname, dg, dname, extra in links:
    comp = {"source": {"id": ports[(sg, sname, "out")], "groupId": sg, "type": "OUTPUT_PORT"},
            "destination": {"id": ports[(dg, dname, "in")], "groupId": dg, "type": "INPUT_PORT"},
            "backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB", **extra}
    call("POST", f"/process-groups/{TOP}/connections", {"revision": REV, "component": comp})

# 연결되지 않은 relationship은 auto-terminate(정상 종료 지점: claim 거절, 중복 dispatch, 마지막 단계 success 등)
used = {}
for g, src, rels, _, _ in conns:
    if not isinstance(src, tuple):
        used.setdefault(src, set()).update(rels)
for key, (ent, _) in procs.items():
    cur = call("GET", f"/processors/{ent['component']['id']}")
    rels = {r["name"] for r in cur["component"]["relationships"]}
    call("PUT", f"/processors/{ent['component']['id']}", {"revision": cur["revision"], "component": {
        "id": ent["component"]["id"], "config": {"autoTerminatedRelationships": sorted(rels - used.get(key, set()))}}})

for name, ent in services.items():
    cur = call("GET", f"/controller-services/{ent['id']}")
    call("PUT", f"/controller-services/{ent['id']}/run-status", {"revision": cur["revision"], "state": "ENABLED"})

# Trigger는 PG 전체 시작에 섞여 즉시 실행되지 않도록 DISABLED로 둔다.
trig = procs["P00"][0]["component"]["id"]
cur = call("GET", f"/processors/{trig}")
call("PUT", f"/processors/{trig}/run-status", {"revision": cur["revision"], "state": "DISABLED"})

print(json.dumps({"process_group": TOP, "trigger": trig,
                  "groups": {"PG-00": G00, "PG-10": G10, "PG-20": G20, "PG-05": G05, "PG-40": G40, "PG-50": G50,
                             "PG-60": G60, "PG-70": G70, "PG-90": G90},
                  "processors": {k: v[0]["component"]["id"] for k, v in procs.items()}}, indent=1))
