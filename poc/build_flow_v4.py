#!/usr/bin/env python3
"""NiFi 2.4 REST API로 Sqoop 대체 PoC Flow V4를 만든다. V3(build_flow_v3.py)와 같은 구조(Load Control API 연동,
자식 PG + Port)에서 원천을 PostgreSQL 대신 Oracle로 바꾼 버전이다. 가이드 7.2~7.3, 8.4의 Oracle 기준을 따른다.

    PG-00 Trigger ─start-run▶ PG-10 Run Coordinator ─partitions(RR)▶ PG-20 Extract Worker
    PG-05 Control Receiver ─validate▶ PG-40 Staging Validation,  ─reissue(RR)▶ PG-20
    모든 PG ─errors▶ PG-90 Error and Event

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

이 빌더는 Oracle 환경에서 실행 시험을 하지 않았다(PostgreSQL 원천 V3로 구조만 검증).

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
    "V44": "검증 시작 응답에서 HDFS 경로, stage table, 업무일자를 꺼내고 _SUCCESS 파일 이름을 정한다.",
    "V45": "_SUCCESS marker를 빈 파일로 쓰기 위해 content를 비운다.",
    "V46": "run 경로에 _SUCCESS marker를 쓴다. 이 파일은 API가 run 완료를 확정한 뒤에만 생긴다. 운영에서는 이후 Hive staging 검증으로 이어진다.",
    # PG-90 Error and Event
    "E90": "모든 PG의 오류를 정규화한다. load.stage와 Processor가 남긴 attribute(HTTP 상태, SQL 오류, 연결 예외)로 오류 단계·코드·분류·메시지와 이벤트 수준·이름을 만든다. 409(중복 실행, claim 불일치)는 정상 경합이므로 WARN이다.",
    "E91": "오류 단계에 따라 run 실패 보고, 파티션 실패 보고, 이벤트 기록만 중 하나로 나눈다.",
    "E92": "run 실패 보고 본문(CREATED → FAILED_MANIFEST, 오류 단계·코드·메시지)을 만든다. SCN 조회 실패도 여기로 온다.",
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


def p(g, key, name, short, props=None, col=0, row=0, tasks=1, sched="0 sec", sensitive=(), retry=None):
    """retry=(relationships, count): Processor 내장 재시도. 소진되면 해당 relationship 연결로 간다."""
    b, t = bundle(short)
    config = {"properties": props or {}, "concurrentlySchedulableTaskCount": tasks,
              "schedulingPeriod": sched, "penaltyDuration": "5 sec", "yieldDuration": "5 sec",
              "comments": COMMENTS[key]}  # 설명이 없는 Processor는 KeyError로 빌드를 멈춘다
    if sensitive:
        config["sensitiveDynamicPropertyNames"] = list(sensitive)
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


def invoke(g, key, name, path, col, row, attr_response=True, tasks=1):
    """Load Control API 호출(가이드 9.2). Retry(5xx)·Failure(연결 오류)는 내장 재시도 후 errors로 간다."""
    props = {
        "HTTP Method": "POST", "HTTP URL": f"#{{CONTROL.API.URL}}{path}",
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


NUM = "'^-?[0-9]+$'"
SRC_TABLE = "#{SRC.OWNER}.#{SRC.TABLE}"
SPLIT = "#{SRC.SPLIT.COLUMN}"
N = "#{PARTITION.COUNT}"

# 최상위 배치: 왼쪽 열은 실행 흐름, 오른쪽 열은 제어 수신·검증, 아래는 공통 오류
G00 = new_pg(TOP, "PG-00 Trigger", 0, 0, "스케줄 트리거와 업무일자 형식 검증")
G10 = new_pg(TOP, "PG-10 Run Coordinator", 0, 260, "run 생성, 원천 지표·manifest 계산, manifest 등록")
G20 = new_pg(TOP, "PG-20 Extract Worker", 0, 520, "claim, 파티션 추출, PutHDFS, chunk 보고")
G05 = new_pg(TOP, "PG-05 Control Receiver", 700, 0, "API worker의 validate/reissue 호출 수신")
G40 = new_pg(TOP, "PG-40 Staging Validation", 700, 260, "/validation/start, _SUCCESS marker (Hive 단계 자리)")
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
    G40: """PG-40 Staging Validation (입구)
역할: API에 검증 시작을 알리고 run 경로에 _SUCCESS marker를 쓴다.
흐름: 40 load.stage=VALIDATION_START → 41·42 검증 시작(POST /validation/start) → 43 started 확인
      → 44 run 정보 → 45·46 _SUCCESS 기록
입력: validate / 출력: errors → PG-90
주의: started=true는 run당 한 번만 온다(중복 요청은 조용히 종료). Hive staging 검증(가이드 10장 47 이후)은
      이 PoC 환경에 없어 46에서 끝난다.""",
    G90: """PG-90 Error and Event
역할: 모든 PG의 실패를 한 곳에서 처리한다.
흐름: 90 오류 정규화(load.stage, HTTP 상태, SQL 오류, 연결 예외 → 코드·수준·메시지) → 91 분기
      → 92·93 run 실패 보고(MANIFEST 단계) / 94·95 파티션 실패 보고(claim한 파티션) → 96 load_event 기록 → 97 로그
입력: errors(모든 PG)
주의: 409(중복 실행, CLAIM_MISMATCH)는 정상 경합이므로 WARN. 상태 전이 이벤트는 API가 기록하므로 여기서는 오류·경고만 남긴다.""",
}
for g, text in PG_LABELS.items():
    label(g, text, 0, -2 * ROWH, 5 * COLW, 170)
label(TOP, """SQOOP_REPLACEMENT_POC_V4 — Load Control API 연동 Sqoop 대체 PoC (Oracle 원천, 미검증)
PG-00 →(start-run)→ PG-10 →(partitions, Round Robin)→ PG-20 → API가 완료 판정 → API worker가 PG-05 호출
PG-05 →(validate)→ PG-40,  PG-05 →(reissue, Round Robin)→ PG-20,  모든 PG →(errors)→ PG-90
원장 기록·완료 판정은 Load Control API가 하고, NiFi는 데이터 처리와 API 호출만 한다.
상세: nifi-sqoop-removal-guide.md 2장, poc/REVIEW.md 6장""", 0, -230, 1100, 150)

# ===== PG-00 Trigger
port(G00, "start-run", "out", 3, 0)
port(G00, "errors", "out", 3, 1)
p(G00, "P00", "00_Generate_Trigger", "GenerateFlowFile",
  {"generate-ff-custom-text": "{}", "Unique FlowFiles": "false"}, 0, 0, sched="1 day")
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

# ===== PG-40 Staging Validation 입구 (가이드 10장). Hive 단계는 이 환경에 없어 _SUCCESS까지만 한다.
port(G40, "validate", "in", 0, 0)
port(G40, "errors", "out", 4, 1)
ua(G40, "V40", "40_Set_Validation_Stage", {"load.stage": "VALIDATION_START"}, 1, 0)
body(G40, "V41", "41_Build_Start_Body", '{"dispatchId":"${load.dispatch.id}","node":"${hostname(true):escapeJson()}"}', 2, 0)
invoke(G40, "V42", "42_Validation_Start", "/runs/${load.run.id}/validation/start", 3, 0)
# started=false는 중복 dispatch이므로 조용히 끝낸다.
route(G40, "V43", "43_Is_Started", {"started": "${api.response:jsonPath('$.started'):equals('true')}"}, 4, 0)
ua(G40, "V44", "44_Set_Run_Attrs", {
    "load.hdfs.path": "${api.response:jsonPath('$.hdfsRunPath')}",
    "load.stage.table": "${api.response:jsonPath('$.stageTable')}",
    "load.business.key": "${api.response:jsonPath('$.businessKey')}",
    "filename": "_SUCCESS", "load.stage": "SUCCESS_MARKER"}, 1, 1)
body(G40, "V45", "45_Empty_Content", "", 2, 1)
put_hdfs(G40, "V46", "46_PutHDFS_SUCCESS_Marker", 3, 1)
# 운영에서는 V46 success를 Hive staging DDL·지표 조회(PG-40 본체)로 연결한다.
c(G40, ("in", "validate"), [], "V40"); c(G40, "V40", "success", "V41"); c(G40, "V41", "success", "V42")
c(G40, "V42", "Original", "V43"); c(G40, "V43", "started", "V44")
c(G40, "V44", "success", "V45"); c(G40, "V45", "success", "V46"); c(G40, "V45", "failure", ("out", "errors"))

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
    "error.message": f"${{{HTTP_ERR[2:-1]}:ifElse(${{invokehttp.response.body:replaceNull(${{api.response}})}},"
                     "${executesql.error.message:replaceNull(${invokehttp.java.exception.message:replaceNull("
                     "'processor routed failure; see bulletin and provenance')})})}"},
   1, 0)
route(G90, "E91", "91_Route_Failure_Report", {
    # manifest 단계 실패는 run을 FAILED_MANIFEST로 보고한다. 422면 API가 이미 기록했다.
    "report_run": "${load.stage:equals('MANIFEST'):and(${load.run.id:isEmpty():not()})"
                  ":and(${invokehttp.status.code:equals('422'):not()})}",
    # claim에 성공한 파티션의 추출·기록 실패는 파티션 실패로 보고한다.
    "report_partition": "${load.stage:in('EXTRACT','CHUNK_WRITE'):and(${api.response:jsonPath('$.claimed'):equals('true')})}"},
      2, 0)
MSG_JSON = "${error.message:replaceAll('(?s)^(.{0,1500}).*$','$1'):escapeJson()}"
body(G90, "E92", "92_Build_Run_Fail_Body",
     '{"expectedStatus":"CREATED","failStatus":"FAILED_MANIFEST","errorStage":"${error.stage}",'
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
] + [(g, "errors", G90, "errors", {}) for g in (G00, G10, G20, G05, G40)]


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
                  "groups": {"PG-00": G00, "PG-10": G10, "PG-20": G20, "PG-05": G05, "PG-40": G40, "PG-90": G90},
                  "processors": {k: v[0]["component"]["id"] for k, v in procs.items()}}, indent=1))
