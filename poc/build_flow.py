#!/usr/bin/env python3
"""NiFi 2.4 REST API로 Sqoop 대체 PoC Flow를 만든다. Load Control API 연동 구조(가이드 7~10장).

NiFi는 데이터 처리만 하고, 상태 기록과 완료 판정은 Load Control API가 한다.

    PG-00 Trigger → PG-10 Coordinator ──POST /runs, /manifest──▶ API
                            │ partitions
                            ▼
                    PG-20 Worker ──claim, chunks, fail──▶ API ──(run 완료 시 outbox)──┐
                                                                                     │ POST /validate/{jobKey}
    PG-05 Control Receiver ◀─────────────────────────────────────────────────────────┘
          │ validate-in
          ▼
    PG-40 입구: /validation/start → _SUCCESS marker  (Hive가 없으므로 STAGE_VALIDATING까지)

- 원천: PostgreSQL(Oracle 대체). AS OF SCN 대신 불변 업무일자 조건을 쓰므로 snapshotScn은 보내지 않는다.
- HDFS: PutHDFS + core-site.xml(fs.defaultFS=file:///) 로컬 파일시스템
- 관리 DB: NiFi는 load_event만 기록한다(PG-90). 원장 쓰기는 모두 API 호출이다.
- PG-05는 운영에서는 root 수준의 공통 PG지만 PoC에서는 Job이 하나뿐이라 같은 PG 안에 둔다.
- PoC는 NiFi↔API를 평문 HTTP로 연결한다. 운영에서는 InvokeHTTP/HandleHttpRequest에 SSL Context Service(mTLS)를 붙인다.

가이드 초안 구조(PG-30 Wait/Notify, PutSQL로 원장 직접 기록)는 git 이력(커밋 28d7e7e~ec5f074)의 이전 버전에 있다.

사용법: build_flow.py <nifi-api-url> <config.json>

config.json의 선택 키 `names`로 기존 Flow와 나란히 만들 수 있다(기본값은 아래 상수).
  "names": {"process_group": "...", "common_context": "...", "job_context": "..."}
"""
import json
import sys
import urllib.error
import urllib.request

API = sys.argv[1].rstrip("/")
CFG = json.load(open(sys.argv[2]))
NAMES = CFG.get("names", {})
PG_NAME = NAMES.get("process_group", "SQOOP_REPLACEMENT_POC")
PC_COMMON = NAMES.get("common_context", "PC_SQOOP_REPLACEMENT_COMMON")
PC_JOB = NAMES.get("job_context", "PC_JOB_PG_INSP_DTL_DAILY")


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


REV = {"version": 0, "clientId": "poc-builder"}

TYPES = {t["type"].split(".")[-1]: t for t in
         call("GET", "/flow/processor-types")["processorTypes"]
         + call("GET", "/flow/controller-service-types")["controllerServiceTypes"]}


def bundle(short):
    return TYPES[short]["bundle"], TYPES[short]["type"]


# ---------------------------------------------------------------- Parameter Context
SENSITIVE_SUFFIXES = ("PASSWORD", "AUTHORIZATION")


def drop_param_ctx(name):
    for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"]:
        if pc["component"]["name"] == name:
            call("DELETE", f"/parameter-contexts/{pc['id']}?version={pc['revision']['version']}&clientId=poc-builder")


def param_ctx(name, params, inherited=None):
    comp = {"name": name, "parameters": [
        {"parameter": {"name": k, "value": v, "sensitive": k.endswith(SENSITIVE_SUFFIXES)}}
        for k, v in params.items()]}
    if inherited:
        comp["inheritedParameterContexts"] = [{"id": inherited, "component": {"id": inherited}}]
    return call("POST", "/parameter-contexts", {"revision": REV, "component": comp})["id"]


root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
for pg in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]:
    if pg["component"]["name"] == PG_NAME:
        raise SystemExit(f"{PG_NAME} already exists ({pg['id']}); delete it first")

drop_param_ctx(PC_JOB)
drop_param_ctx(PC_COMMON)
common = param_ctx(PC_COMMON, CFG["common_params"])
job = param_ctx(PC_JOB, CFG["job_params"], inherited=common)

pg = call("POST", f"/process-groups/{root}/process-groups",
          {"revision": REV, "component": {"name": PG_NAME, "position": {"x": 0, "y": 0}}})
PG = pg["id"]
call("PUT", f"/process-groups/{PG}", {"revision": pg["revision"],
     "component": {"id": PG, "parameterContext": {"id": job}}})

# ---------------------------------------------------------------- Controller Services
services = {}


def cs(name, short, props):
    b, t = bundle(short)
    ent = call("POST", f"/process-groups/{PG}/controller-services",
               {"revision": REV, "component": {"type": t, "bundle": b, "name": name, "properties": props}})
    services[name] = ent
    return ent["id"]


def hikari(prefix):
    return {"hikaricp-connection-url": f"#{{{prefix}.JDBC.URL}}",
            "hikaricp-driver-classname": "org.postgresql.Driver",
            "hikaricp-driver-locations": "#{JDBC.DRIVER.PATH}",
            "hikaricp-username": f"#{{{prefix}.JDBC.USER}}",
            "hikaricp-password": f"#{{{prefix}.JDBC.PASSWORD}}",
            "hikaricp-max-total-conns": "10",
            "hikaricp-validation-query": "SELECT 1"}


META = cs("CS_DBCP_META", "HikariCPConnectionPool", hikari("META"))  # load_event INSERT 전용
SRC = cs("CS_DBCP_SRC", "HikariCPConnectionPool", hikari("SRC"))
JARR = cs("CS_JSON_WRITER_ARRAY", "JsonRecordSetWriter", {"output-grouping": "output-array"})
PARQ = cs("CS_PARQUET_WRITER", "ParquetRecordSetWriter", {"compression-type": "SNAPPY"})
HTTPCTX = cs("CS_HTTP_CONTEXT_MAP", "StandardHttpContextMap", {"Request Expiration": "1 min"})

# ---------------------------------------------------------------- Processors
procs = {}
conns = []
COLW, ROWH = 460, 200


def p(key, name, short, props=None, col=0, row=0, tasks=1, penalty="5 sec", sched="0 sec", sensitive=()):
    b, t = bundle(short)
    config = {"properties": props or {}, "concurrentlySchedulableTaskCount": tasks,
              "schedulingPeriod": sched, "penaltyDuration": penalty, "yieldDuration": "5 sec"}
    if sensitive:
        config["sensitiveDynamicPropertyNames"] = list(sensitive)
    ent = call("POST", f"/process-groups/{PG}/processors", {"revision": REV, "component": {
        "type": t, "bundle": b, "name": name, "position": {"x": col * COLW, "y": row * ROWH}, "config": config}})
    procs[key] = ent
    return key


def c(src, rels, dst):
    conns.append((src, rels if isinstance(rels, list) else [rels], dst))


def ua(key, name, attrs, col, row):
    return p(key, name, "UpdateAttribute", attrs, col, row)


def ejp(key, name, paths, col, row):
    return p(key, name, "EvaluateJsonPath", {"Destination": "flowfile-attribute", **paths}, col, row)


def route(key, name, routes, col, row):
    return p(key, name, "RouteOnAttribute", {"Routing Strategy": "Route to Property name", **routes}, col, row)


def esql(key, name, pool, sql, col, row, writer=JARR, extra=None, tasks=1):
    props = {"Database Connection Pooling Service": pool, "SQL Query": sql, "esqlrecord-record-writer": writer}
    props.update(extra or {})
    return p(key, name, "ExecuteSQLRecord", props, col, row, tasks=tasks)


def retry(key, name, attr, maximum, col, row):
    return p(key, name, "RetryFlowFile", {"retry-attribute": attr, "maximum-retries": maximum,
                                          "penalize-retries": "true", "Fail on Non-numerical Overwrite": "true",
                                          "reuse-mode": "fail"}, col, row)


def to_json(key, name, attrs, col, row):
    """요청 본문 생성. API는 모르는 필드를 422로 거부하므로 attribute 목록을 명시한다(가이드 9.2)."""
    return p(key, name, "AttributesToJSON", {
        "Attributes List": ",".join(attrs), "Destination": "flowfile-content",
        "Include Core Attributes": "false", "Null Value": "false"}, col, row)


def invoke(key, name, path, col, row, attr_response=True, tasks=1):
    """Load Control API 호출(가이드 9.2 공통 설정).

    attr_response=True면 2xx 응답 본문이 Original relationship의 api.response attribute로 붙는다.
    False면 응답 본문이 Response relationship의 content가 된다(manifest 응답처럼 클 때).
    Authorization은 Sensitive Parameter를 참조하므로 Sensitive 동적 속성으로 만들고, 값은 참조 하나만 둔다.
    """
    props = {
        "HTTP Method": "POST",
        "HTTP URL": f"#{{CONTROL.API.URL}}{path}",
        "Request Content-Type": "application/json",
        "Connection Timeout": "5 secs",
        "Socket Read Timeout": "#{CONTROL.API.TIMEOUT}",
        "Response Generation Required": "false",
        "Authorization": "#{CONTROL.API.AUTHORIZATION}",
        "X-Request-Id": "${UUID()}",
        "X-Run-Id": "${load.run.id}",
    }
    if attr_response:
        props["Response Body Attribute Name"] = "api.response"
        props["Response Body Attribute Size"] = "16384"
    return p(key, name, "InvokeHTTP", props, col, row, tasks=tasks, sensitive=("Authorization",))


def api_call(key, name, path, col, row, ok, no_retry, attr_response=True, tasks=1):
    """InvokeHTTP + 제한 재시도 + 4xx 분기(가이드 9.3)."""
    invoke(key, name, path, col, row, attr_response=attr_response, tasks=tasks)
    retry(key + "R", name.split("_")[0] + "R_Retry_API", "api.retry.count", "#{CONTROL.API.RETRY.MAX}", col, row + 1)
    c(key, "Original" if attr_response else "Response", ok)
    c(key, ["Retry", "Failure"], key + "R")
    c(key + "R", "retry", key)
    c(key + "R", ["retries_exceeded", "failure"], "EAPI")
    c(key, "No Retry", no_retry)


def err(key, name, stage, cls, msg, col, row, target, code="INTERNAL", event="NIFI_ERROR", level="ERROR"):
    """오류 attribute 설정(가이드 14.6) 후 target(run 실패 보고, 파티션 실패 보고, 이벤트만)으로 보낸다.

    event.name을 매번 지정한다. 앞 단계에서 붙은 event.name(예: RUN_CREATED)이 따라오기 때문이다.
    """
    ua(key, name, {"error.stage": stage, "error.processor": name, "error.class": cls,
                   "error.code": code, "error.message": msg, "event.name": event, "event.level": level},
       col, row)
    c(key, "success", target)


NUM = "'^-?[0-9]+$'"
SRC_TABLE = "#{SRC.OWNER}.#{SRC.TABLE}"
SPLIT = "#{SRC.SPLIT.COLUMN}"

# ===== PG-00 Trigger (row 0)
p("P00", "00_Generate_Trigger", "GenerateFlowFile",
  {"generate-ff-custom-text": "{}", "Unique FlowFiles": "false"}, 0, 0, sched="1 day")
ua("P01", "01_Set_Trigger_Attributes", {
    "load.job.key": "#{JOB.KEY}", "load.business.key": "#{BUSINESS.KEY}", "load.trigger.type": "SCHEDULE"}, 1, 0)
route("P02", "02_Validate_Trigger", {
    "valid": "${load.business.key:matches('^[0-9]{4}-[0-9]{2}-[0-9]{2}$'):and(${load.job.key:isEmpty():not()})}"}, 2, 0)
err("E02", "02E_Invalid_Trigger", "TRIGGER", "VALIDATION", "invalid business key or job key", 3, 0, "EV", event="TRIGGER_INVALID")
c("P00", "success", "P01"); c("P01", "success", "P02"); c("P02", "valid", "P10"); c("P02", "unmatched", "E02")

# ===== PG-10 Run Coordinator (rows 1-5, 가이드 7장)
# Parameter는 EL 문자열 리터럴 안에서 치환되지 않으므로(REVIEW #10) 비교·본문용 값을 먼저 attribute로 옮긴다.
ua("P10", "10_Set_Run_Request", {
    "jobKey": "${load.job.key}", "businessKey": "${load.business.key}",
    "hdfsRoot": "#{HDFS.STAGE.ROOT}", "stageTablePrefix": "#{HIVE.STAGE.TABLE.PREFIX}",
    "allowEmptySource": "#{ALLOW.EMPTY.SOURCE}", "load.partition.planned": "#{PARTITION.COUNT}",
    "load.allow.empty": "#{ALLOW.EMPTY.SOURCE}"}, 0, 1)
to_json("P10J", "10J_Build_Run_Body",
        ["jobKey", "businessKey", "hdfsRoot", "stageTablePrefix", "allowEmptySource"], 1, 1)
api_call("P11", "11_Create_Run", "/runs", 2, 1, ok="P11A", no_retry="P11D")
ua("P11A", "11A_Set_Run_Attrs", {
    "load.run.id": "${api.response:jsonPath('$.runId')}",
    "load.hdfs.path": "${api.response:jsonPath('$.hdfsRunPath')}",
    "load.stage.table": "${api.response:jsonPath('$.stageTable')}",
    "event.name": "RUN_CREATED", "event.level": "INFO"}, 3, 1)
route("P11D", "11D_Route_Create_Error", {"duplicate": "${invokehttp.status.code:equals('409')}"}, 2, 3)
err("E11D", "11E_Duplicate_Active_Run", "RUN_CREATE", "VALIDATION",
    "active run exists for ${load.job.key}/${load.business.key}", 3, 3, "EV", code="DUPLICATE_ACTIVE_RUN",
    event="DUPLICATE_ACTIVE_RUN", level="WARN")
err("E11", "11E_Create_Run_Rejected", "RUN_CREATE", "NON_RETRYABLE",
    "${invokehttp.response.body:replaceNull('create run rejected')}", 3, 4, "EV",
    code="${invokehttp.status.code}", event="RUN_CREATE_REJECTED")
c("P10", "success", "P10J"); c("P10J", "success", "P11"); c("P10J", "failure", "E11")
c("P11A", "success", "P12"); c("P11A", "success", "EV")
c("P11D", "duplicate", "E11D"); c("P11D", "unmatched", "E11")

esql("P12", "12_Query_Source_Metrics", SRC,
     f"SELECT COUNT(*) AS source_count,\n       MIN({SPLIT})::text AS min_seq,\n       MAX({SPLIT})::text AS max_seq,\n"
     f"       COUNT(*) - COUNT({SPLIT}) AS null_seq_count,\n"
     f"       COALESCE(SUM(#{{DQ.AMOUNT.COLUMN}}), 0)::text AS amount_sum\n  FROM {SRC_TABLE}\n WHERE #{{SRC.BASE.WHERE}}",
     4, 1)
ejp("P13", "13_Extract_Source_Metrics", {
    "load.source.count": "$[0].source_count", "load.source.min": "$[0].min_seq", "load.source.max": "$[0].max_seq",
    "load.source.null.count": "$[0].null_seq_count", "load.source.amount": "$[0].amount_sum"}, 5, 1)
# PoC는 NULL 파티션을 쓰지 않는다(SPLIT.NULL.POLICY=FAIL). 0건 원천은 ALLOW.EMPTY.SOURCE일 때만 허용
route("P14", "14_Source_Precheck", {
    "valid": "${load.source.null.count:equals('0')"
             ":and(${load.source.count:gt(0):or(${load.allow.empty:equals('true')})})"
             f":and(${{load.source.min:matches({NUM}):or(${{load.source.count:equals('0')}})}})}}"}, 6, 1)
err("E12", "12E_Source_Metrics_Failed", "SOURCE_METRICS", "NON_RETRYABLE",
    "${executesql.error.message:replaceNull('source metrics query failed')}", 4, 2, "RF")
err("E14", "14E_Source_Precheck_Failed", "SOURCE_PRECHECK", "VALIDATION",
    "empty source or NULL split key: count=${load.source.count} nulls=${load.source.null.count}", 6, 2, "RF")
c("P12", "success", "P13"); c("P12", "failure", "E12")
c("P13", "matched", "P14"); c("P13", ["unmatched", "failure"], "E14")
c("P14", "valid", "P16"); c("P14", "unmatched", "E14")

# 경계는 lower(i+1) = upper(i)인 식 하나로 만든다. 경계값은 정밀도 손실을 막기 위해 text로 반환(가이드 7.4).
MIN, MAX, N = "${load.source.min}", "${load.source.max}", "#{PARTITION.COUNT}"
esql("P16", "16_Query_Partition_Manifest", SRC,
     "WITH b AS (\n"
     f"  SELECT g AS pid,\n         {MIN} + floor(g * ({MAX} - {MIN} + 1)::numeric / {N}) AS lo,\n"
     f"         CASE WHEN g = {N} - 1 THEN {MAX}\n"
     f"              ELSE {MIN} + floor((g + 1) * ({MAX} - {MIN} + 1)::numeric / {N}) END AS hi,\n"
     f"         g = {N} - 1 AS incl\n    FROM generate_series(0, {N} - 1) g\n), c AS (\n"
     f"  SELECT b.*, (SELECT COUNT(*) FROM {SRC_TABLE} s\n                WHERE #{{SRC.BASE.WHERE}}\n"
     f"                  AND s.{SPLIT} >= b.lo\n                  AND (s.{SPLIT} < b.hi OR (b.incl AND s.{SPLIT} = b.hi))) AS cnt\n"
     "    FROM b\n)\nSELECT lpad(pid::text, 4, '0') AS partition_id, lo::bigint::text AS lower_bound,\n"
     "       hi::bigint::text AS upper_bound, incl AS upper_inclusive, false AS is_null_partition,\n"
     "       cnt AS expected_row_count\n  FROM c ORDER BY pid", 7, 1)
# manifest 배열을 partitions로 감싸고 source 지표를 상위 필드로 붙인다(가이드 7.2의 20번).
JOLT_MANIFEST = json.dumps([
    {"operation": "shift", "spec": {"*": {
        "partition_id": "partitions[&1].partitionId", "lower_bound": "partitions[&1].lowerBound",
        "upper_bound": "partitions[&1].upperBound", "upper_inclusive": "partitions[&1].upperInclusive",
        "is_null_partition": "partitions[&1].isNullPartition",
        "expected_row_count": "partitions[&1].expectedRowCount"}}},
    {"operation": "default", "spec": {
        "sourceCount": "${load.source.count}", "sourceNullSplitCount": "${load.source.null.count}",
        "sourceMinSplit": "${load.source.min}", "sourceMaxSplit": "${load.source.max}",
        "plannedPartitionCount": "${load.partition.planned}",
        "sourceMetrics": {"AMOUNT_SUM": "${load.source.amount}"}}},
], indent=1)
p("P17", "17_Build_Manifest_Body", "JoltTransformJSON", {
    "Jolt Transform": "jolt-transform-chain", "Jolt Specification": JOLT_MANIFEST}, 8, 1)
api_call("P21", "21_Register_Manifest", "/runs/${load.run.id}/manifest", 9, 1,
         ok="P23", no_retry="E21", attr_response=False)
err("E16", "16E_Manifest_Query_Failed", "MANIFEST", "NON_RETRYABLE",
    "${executesql.error.message:replaceNull('manifest query or body build failed')}", 7, 2, "RF")
# 422면 API가 FAILED_MANIFEST를 이미 기록했다. 이벤트만 남긴다.
err("E21", "21E_Manifest_Rejected", "MANIFEST", "VALIDATION",
    "${invokehttp.response.body:replaceNull('manifest rejected')}", 9, 3, "EV", code="MANIFEST_INVALID",
    event="MANIFEST_INVALID")
c("P16", "success", "P17"); c("P16", "failure", "E16"); c("P17", "failure", "E16")
c("P17", "success", "P21")
p("P23", "23_Split_Dispatch_Partitions", "SplitJson", {"JsonPath Expression": "$.dispatchPartitions"}, 10, 1)
ejp("P24", "24_Extract_Partition_Attrs", {
    "partition.id": "$.partitionId", "partition.lower": "$.lowerBound", "partition.upper": "$.upperBound",
    "partition.upper.inclusive": "$.upperInclusive", "partition.expected.rows": "$.expectedRowCount"}, 10, 2)
err("E23", "23E_Split_Failed", "MANIFEST", "INTERNAL", "dispatchPartitions split failed", 10, 3, "EV", event="MANIFEST_SPLIT_FAILED")
c("P23", "split", "P24"); c("P23", "failure", "E23"); c("P24", "matched", "P30"); c("P24", ["unmatched", "failure"], "E23")

# run 단계 실패 보고: POST /runs/{id}/fail (CREATED → FAILED_MANIFEST). 보고하지 않으면 active lock이 남는다.
ua("RF", "25_Set_Run_Fail_Body", {
    "expectedStatus": "CREATED", "failStatus": "FAILED_MANIFEST", "errorStage": "${error.stage}",
    "errorCode": "${error.code}", "message": "${error.message:replaceAll('(?s)^(.{0,1500}).*$','$1')}",
    "event.name": "RUN_FAILED", "event.level": "ERROR"}, 0, 4)
to_json("RFJ", "25J_Build_Run_Fail_Body", ["expectedStatus", "failStatus", "errorStage", "errorCode", "message"], 1, 4)
api_call("RFI", "25_Report_Run_Fail", "/runs/${load.run.id}/fail", 1, 5, ok="EV", no_retry="EV")
c("RF", "success", "RFJ"); c("RFJ", "success", "RFI"); c("RFJ", "failure", "EV")

# ===== PG-20 Extract Workers (rows 6-10, 가이드 8장)
# UpdateAttribute는 모든 속성을 "들어온" attribute 기준으로 평가한다. 같은 processor 안에서 방금 만든
# partition.claim.token을 참조하면 빈 값이 되므로 token 생성과 본문용 복사를 두 단계로 나눈다(PoC에서 재현).
ua("P30", "30_Set_Claim_Token", {"partition.claim.token": "${UUID()}"}, 0, 6)
ua("P30B", "30B_Set_Claim_Request", {"claimToken": "${partition.claim.token}", "workerNode": "${hostname(true)}"}, 0, 7)
to_json("P30J", "30J_Build_Claim_Body", ["claimToken", "workerNode"], 1, 6)
api_call("P31", "31_Claim_Partition", "/runs/${load.run.id}/partitions/${partition.id}/claim", 2, 6,
         ok="P33", no_retry="E31", tasks=2)
route("P33", "33_Is_Owner", {
    "owner": f"${{api.response:jsonPath('$.claimed'):equals('true'):and(${{partition.lower:matches({NUM})}})"
             f":and(${{partition.upper:matches({NUM})}})}}"}, 3, 6)
p("P33X", "33X_Log_Not_Owner", "LogMessage", {
    "log-level": "info", "log-prefix": "SQOOP_REPLACEMENT ",
    "log-message": "claim refused run=${load.run.id} partition=${partition.id} "
                   "response=${api.response}"}, 3, 7)
err("E31", "31E_Claim_Rejected", "PARTITION_CLAIM", "NON_RETRYABLE",
    "${invokehttp.response.body:replaceNull('claim rejected')}", 2, 8, "EV", code="${invokehttp.status.code}", event="CLAIM_REJECTED")
c("P30", "success", "P30B"); c("P30B", "success", "P30J"); c("P30J", "success", "P31"); c("P30J", "failure", "E31")
c("P33", "owner", "P34"); c("P33", "unmatched", "P33X")

esql("P34", "34_Execute_Partition_Query", SRC,
     f"SELECT #{{SRC.COLUMNS}}\n  FROM {SRC_TABLE}\n WHERE #{{SRC.BASE.WHERE}}\n   AND {SPLIT} >= ${{partition.lower}}\n"
     f"   AND {SPLIT} ${{partition.upper.inclusive:equals('true'):ifElse('<=','<')}} ${{partition.upper}}",
     4, 6, writer=PARQ, tasks=4, extra={
         "esql-max-rows": "#{EXTRACT.ROWS.PER.FILE}", "esql-output-batch-size": "0",
         "esql-fetch-size": "#{EXTRACT.FETCH.SIZE}", "Max Wait Time": "#{EXTRACT.QUERY.TIMEOUT}",
         # PostgreSQL JDBC는 autocommit=false일 때만 fetch size(cursor)를 적용한다
         "esql-auto-commit": "false",
         # DATE/TIMESTAMP/DECIMAL을 Parquet logical type으로 유지
         "dbf-user-logical-types": "true"})
route("P34C", "34A_Classify_DB_Error", {
    "transient": "${executesql.error.message:find('(?i)(connection|timed out|timeout|I/O error|SQLState: 08|57P0[1-3])')}"},
      4, 7)
retry("R34", "34R_Retry_Partition_Query", "partition.retry.count", "#{PARTITION.RETRY.MAX}", 5, 7)
err("E34", "34E_Partition_Query_Failed", "SOURCE_EXTRACT", "NON_RETRYABLE",
    "${executesql.error.message:replaceNull('partition query failed')}", 4, 8, "PF")
c("P34", "success", "P38"); c("P34", "failure", "P34C")
c("P34C", "transient", "R34"); c("P34C", "unmatched", "E34")
c("R34", "retry", "P34"); c("R34", ["retries_exceeded", "failure"], "E34")

# chunk 속성. Output Batch Size=0이므로 모든 chunk에 fragment.count가 있어 control FlowFile이 필요 없다(가이드 8.1).
ua("P38", "38_Set_Chunk_Attrs", {
    "chunk.index": "${fragment.index:padLeft(6,'0')}", "chunk.record.count": "${record.count}",
    # Hive external table이 하위 디렉터리를 읽지 않을 수 있으므로 run root에 평탄하게 기록
    "filename": "part-${partition.id}-${fragment.index:padLeft(6,'0')}.parquet"}, 6, 6)
p("P39", "39_PutHDFS", "PutHDFS", {
    "Hadoop Configuration Resources": "#{HADOOP.CONF.FILES}", "Directory": "${load.hdfs.path}",
    "Conflict Resolution Strategy": "replace", "writing-strategy": "writeAndRename",
    "Permissions umask": "#{HDFS.PERMISSIONS.UMASK}"}, 7, 6, tasks=4)
retry("R39", "39R_Retry_HDFS", "hdfs.retry.count", "#{PARTITION.RETRY.MAX}", 7, 7)
err("E39", "39E_HDFS_Write_Failed", "HDFS_WRITE", "TRANSIENT",
    "PutHDFS routed failure; see bulletin and provenance for run_id/partition_id", 7, 8, "PF",
    code="HDFS_RETRY_EXHAUSTED")
c("P38", "success", "P39"); c("P39", "failure", "R39")
c("R39", "retry", "P39"); c("R39", ["retries_exceeded", "failure"], "E39")

# chunk 보고(가이드 8.5). PutHDFS 이후에 content를 JSON으로 바꿔야 Parquet가 요청 본문으로 가지 않는다.
ua("P40", "40_Set_Chunk_Report", {
    "chunkIndex": "${fragment.index}", "chunkCount": "${fragment.count}",
    "fragmentIdentifier": "${fragment.identifier}", "hdfsPath": "${absolute.hdfs.path}/${filename}",
    "recordCount": "${record.count}", "byteCount": "${fileSize}"}, 8, 6)
to_json("P40J", "40J_Build_Chunk_Report",
        ["claimToken", "chunkIndex", "chunkCount", "fragmentIdentifier", "hdfsPath", "recordCount", "byteCount"], 9, 6)
api_call("P42", "42_Report_Chunk", "/runs/${load.run.id}/partitions/${partition.id}/chunks", 10, 6,
         ok="P43", no_retry="E42", tasks=2)
route("P43", "43_Route_Chunk_Result", {
    "run_complete": "${api.response:jsonPath('$.validationScheduled'):equals('true')}",
    "partition_failed": "${api.response:jsonPath('$.partitionStatus'):equals('FAILED')}"}, 11, 6)
ua("P43C", "43C_Event_Extract_Validated", {"event.name": "EXTRACT_VALIDATED", "event.level": "INFO",
                                            "event.row.count": "${api.response:jsonPath('$.receivedChunks')}"}, 11, 7)
err("E43", "43E_Partition_Row_Mismatch", "PARTITION_GATE", "VALIDATION",
    "API judged partition FAILED: ${api.response}", 12, 7, "EV", code="ROW_COUNT_MISMATCH",
    event="PARTITION_FAILED")
err("E42", "42E_Chunk_Rejected", "CHUNK_REPORT", "NON_RETRYABLE",
    "${invokehttp.response.body:replaceNull('chunk report rejected')}", 10, 8, "EV", code="${invokehttp.status.code}", event="CHUNK_REPORT_REJECTED")
c("P39", "success", "P40"); c("P40", "success", "P40J"); c("P40J", "success", "P42"); c("P40J", "failure", "E42")
c("P43", "run_complete", "P43C"); c("P43", "partition_failed", "E43"); c("P43C", "success", "EV")

# 파티션 최종 실패 보고: POST .../fail (가이드 8.3)
ua("PF", "29_Set_Partition_Fail_Body", {
    "errorStage": "${error.stage}", "errorClass": "${error.class}", "errorCode": "${error.code}",
    "message": "${error.message:replaceAll('(?s)^(.{0,1500}).*$','$1')}", "attempt": "${partition.retry.count:replaceNull('0')}",
    "event.name": "PARTITION_FAILED", "event.level": "ERROR"}, 0, 9)
to_json("PFJ", "29J_Build_Partition_Fail_Body",
        ["claimToken", "errorStage", "errorClass", "errorCode", "message", "attempt"], 1, 9)
api_call("PFI", "29_Report_Partition_Fail", "/runs/${load.run.id}/partitions/${partition.id}/fail", 2, 9,
         ok="EV", no_retry="EV")
c("PF", "success", "PFJ"); c("PFJ", "success", "PFI"); c("PFJ", "failure", "EV")

# ===== PG-05 Control Receiver (rows 11-12, 가이드 9.5)
p("R05", "05_Listen_Control", "HandleHttpRequest", {
    "Listening Port": "#{CONTROL.LISTEN.PORT}", "HTTP Context Map": HTTPCTX,
    "Allowed Paths": "/(validate|reissue)/[A-Z0-9_]{1,200}",
    "Allow GET": "false", "Allow POST": "true", "Allow PUT": "false", "Allow DELETE": "false",
    "Allow HEAD": "false", "Allow OPTIONS": "false"}, 0, 11)
route("R06", "06_Validate_Request", {
    "valid": "${http.method:equals('POST'):and(${http.headers.X-Dispatch-Id:matches("
             "'^[0-9a-fA-F-]{36}$')}):and(${http.headers.X-Run-Id:matches('^[0-9a-fA-F-]{36}$')})}"}, 1, 11)
p("R07", "07_Respond_400", "HandleHttpResponse", {"HTTP Status Code": "400", "HTTP Context Map": HTTPCTX}, 1, 12)
# 검증은 오래 걸릴 수 있으므로 먼저 202를 응답하고 HTTP 연결을 붙잡지 않는다.
p("R08", "08_Respond_202", "HandleHttpResponse", {"HTTP Status Code": "202", "HTTP Context Map": HTTPCTX}, 2, 11)
ejp("R09", "09_Extract_Control_Body", {"load.run.id": "$.runId", "load.dispatch.id": "$.dispatchId"}, 3, 11)
ua("R09B", "09B_Set_Control_Attrs", {
    "control.action": "${http.request.uri:substringAfter('/'):substringBefore('/')}",
    "load.job.key": "${http.request.uri:substringAfterLast('/')}",
    "control.expected.job": "#{JOB.KEY}"}, 4, 11)
route("R10", "10_Route_By_Job", {
    "validate": "${load.job.key:equals(${control.expected.job}):and(${control.action:equals('validate')})}",
    "reissue": "${load.job.key:equals(${control.expected.job}):and(${control.action:equals('reissue')})}"}, 5, 11)
err("E10", "10E_Unknown_Job", "CONTROL_RECEIVER", "VALIDATION",
    "unregistered jobKey=${load.job.key} action=${control.action}", 5, 12, "EV", event="CONTROL_UNKNOWN_JOB")
err("E06", "06E_Invalid_Control_Request", "CONTROL_RECEIVER", "VALIDATION",
    "invalid control request uri=${http.request.uri}", 2, 12, "EV", event="CONTROL_REQUEST_INVALID")
c("R05", "success", "R06"); c("R06", "valid", "R08"); c("R06", "unmatched", "R07")
c("R07", "success", "E06"); c("R08", "success", "R09")
c("R09", "matched", "R09B"); c("R09", ["unmatched", "failure"], "E06")
c("R09B", "success", "R10"); c("R10", "validate", "V40"); c("R10", "reissue", "R70"); c("R10", "unmatched", "E10")

# 재발행 수신(가이드 13.2): 본문에 Worker 실행에 필요한 값이 모두 있다.
ejp("R70", "70_Extract_Reissue_Body", {
    "partition.id": "$.partitionId", "load.business.key": "$.businessKey", "load.hdfs.path": "$.hdfsRunPath",
    "partition.lower": "$.lowerBound", "partition.upper": "$.upperBound",
    "partition.upper.inclusive": "$.upperInclusive", "partition.expected.rows": "$.expectedRowCount"}, 6, 12)
ua("R72", "72_Event_Reissued", {"event.name": "RECOVERY_REISSUED", "event.level": "WARN"}, 7, 12)
c("R70", "matched", "R72"); c("R70", ["unmatched", "failure"], "E06"); c("R72", "success", "P30"); c("R72", "success", "EV")

# ===== PG-40 Staging Validation 입구 (row 13, 가이드 10장). Hive 단계는 이 환경에 없다.
ua("V40", "40S_Set_Start_Body", {"dispatchId": "${load.dispatch.id}", "node": "${hostname(true)}"}, 0, 13)
to_json("V40J", "40J_Build_Start_Body", ["dispatchId", "node"], 1, 13)
api_call("V41", "40S_Validation_Start", "/runs/${load.run.id}/validation/start", 2, 13, ok="V42", no_retry="E41")
route("V42", "40T_Is_Started", {"started": "${api.response:jsonPath('$.started'):equals('true')}"}, 3, 13)
p("V42X", "40X_Log_Duplicate_Dispatch", "LogMessage", {
    "log-level": "info", "log-prefix": "SQOOP_REPLACEMENT ",
    "log-message": "duplicate validation dispatch run=${load.run.id} response=${api.response}"}, 3, 14)
# 검증에 필요한 값은 /validation/start 응답에서 받는다(PG-10의 attribute가 없으므로).
ua("V43", "40U_Set_Run_Attrs", {
    "load.hdfs.path": "${api.response:jsonPath('$.hdfsRunPath')}",
    "load.stage.table": "${api.response:jsonPath('$.stageTable')}",
    "load.business.key": "${api.response:jsonPath('$.businessKey')}",
    "load.source.count": "${api.response:jsonPath('$.sourceCount')}",
    "load.extracted.count": "${api.response:jsonPath('$.extractedCount')}",
    "validation.source.amount": "${api.response:jsonPath('$.sourceMetrics.AMOUNT_SUM')}",
    "filename": "_SUCCESS"}, 4, 13)
p("V44", "40A_Empty_Content", "ReplaceText", {
    "Replacement Strategy": "Always Replace", "Replacement Value": "", "Evaluation Mode": "Entire text"}, 5, 13)
p("V45", "40C_PutHDFS_SUCCESS_Marker", "PutHDFS", {
    "Hadoop Configuration Resources": "#{HADOOP.CONF.FILES}", "Directory": "${load.hdfs.path}",
    "Conflict Resolution Strategy": "replace", "writing-strategy": "writeAndRename",
    "Permissions umask": "#{HDFS.PERMISSIONS.UMASK}"}, 6, 13)
ua("V46", "41_Event_Hive_Not_Available", {
    "event.name": "STAGE_VALIDATION_STARTED", "event.level": "INFO",
    "event.row.count": "${load.extracted.count}"}, 7, 13)
err("E41", "40E_Validation_Start_Rejected", "STAGE_VALIDATE", "NON_RETRYABLE",
    "${invokehttp.response.body:replaceNull('validation start rejected')}", 2, 15, "EV", code="${invokehttp.status.code}", event="VALIDATION_START_REJECTED")
err("E45", "40E_SUCCESS_Marker_Failed", "HDFS_WRITE", "TRANSIENT", "_SUCCESS marker write failed", 6, 14, "EV", event="SUCCESS_MARKER_FAILED")
c("V40", "success", "V40J"); c("V40J", "success", "V41"); c("V40J", "failure", "E41")
c("V42", "started", "V43"); c("V42", "unmatched", "V42X")
c("V43", "success", "V44"); c("V44", "success", "V45"); c("V44", "failure", "E45")
c("V45", "success", "V46"); c("V45", "failure", "E45"); c("V46", "success", "EV")

# ===== PG-90 이벤트 기록 (row 16, 가이드 14장). NiFi Processor 오류와 Data plane 이벤트만 기록한다.
ua("EAPI", "90_API_Unreachable", {
    "event.name": "CONTROL_API_UNREACHABLE", "event.level": "ERROR", "error.stage": "CONTROL_API",
    "error.class": "TRANSIENT", "error.code": "${invokehttp.status.code:replaceNull('CONNECT')}",
    "error.message": "API call retries exhausted; sweeper will clean up the run"}, 0, 16)
ua("EV0", "90_Prepare_Event", {
    "event.name": "${event.name:replaceNull('NIFI_ERROR')}",
    "event.level": "${event.level:replaceNull(${error.stage:isEmpty():ifElse('INFO','ERROR')})}",
    # 따옴표 이스케이프, 줄바꿈 제거, 1500자 제한
    "error.message.safe": "${error.message:replaceAll(\"'\",\"''\"):replaceAll('[\\r\\n]+',' '):replaceAll('(?s)^(.{0,1500}).*$','$1')}"},
   1, 16)
c("EAPI", "success", "EV")
p("EV", "95_Insert_Load_Event", "PutSQL", {
    "JDBC Connection Pool": META, "Support Fragmented Transactions": "false", "Batch Size": "1",
    "putsql-sql-statement":
        "INSERT INTO nifi_ops.load_event (event_id, event_level, event_name, run_id, job_key, business_key,\n"
        "    partition_id, chunk_index, process_group, processor_name, node_id, attempt_no, row_count,\n"
        "    error_class, error_code, message)\n"
        "VALUES (gen_random_uuid(), '${event.level}', '${event.name}',\n"
        "    CAST(NULLIF('${load.run.id}', '') AS uuid), '${load.job.key}', '${load.business.key}',\n"
        "    NULLIF('${partition.id}', ''), CAST(NULLIF('${chunk.index}', '') AS integer), '" + PG_NAME + "',\n"
        "    NULLIF('${error.processor}', ''), '${hostname(true)}', CAST(NULLIF('${partition.retry.count}', '') AS integer),\n"
        "    CAST(NULLIF('${event.row.count}', '') AS bigint), NULLIF('${error.class}', ''), NULLIF('${error.code}', ''),\n"
        "    NULLIF('${error.message.safe}', ''))"}, 2, 16)
p("EV2", "96_LogMessage", "LogMessage", {
    "log-level": "${event.level:toLower()}", "log-prefix": "SQOOP_REPLACEMENT ",
    "log-message": "{\"event\":\"${event.name}\",\"run_id\":\"${load.run.id}\",\"job_key\":\"${load.job.key}\","
                   "\"business_key\":\"${load.business.key}\",\"partition_id\":\"${partition.id}\","
                   "\"rows\":\"${event.row.count}\",\"error_stage\":\"${error.stage}\",\"error_class\":\"${error.class}\","
                   "\"error_code\":\"${error.code}\",\"message\":\"${error.message.safe}\"}"}, 3, 16)
c("EV", ["success", "failure", "retry"], "EV2")

# 모든 이벤트 입력을 EV0(준비) → EV(INSERT)로 보낸다: c(x, rel, "EV")를 EV0로 바꾼다.
conns = [(s, r, "EV0" if d == "EV" else d) for s, r, d in conns]
c("EV0", "success", "EV")

# ---------------------------------------------------------------- Connections
for src, rels, dst in conns:
    s, d = procs[src]["component"], procs[dst]["component"]
    body = {"revision": REV, "component": {
        "source": {"id": s["id"], "groupId": PG, "type": "PROCESSOR"},
        "destination": {"id": d["id"], "groupId": PG, "type": "PROCESSOR"},
        "selectedRelationships": rels,
        "backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB"}}
    if src == "P24" and rels == ["matched"]:
        body["component"]["loadBalanceStrategy"] = "ROUND_ROBIN"  # Coordinator → Worker
    call("POST", f"/process-groups/{PG}/connections", body)

# 연결되지 않은 relationship은 auto-terminate
used = {}
for src, rels, _ in conns:
    used.setdefault(src, set()).update(rels)
for key, ent in procs.items():
    cur = call("GET", f"/processors/{ent['id']}")
    rels = {r["name"] for r in cur["component"]["relationships"]}
    auto = sorted(rels - used.get(key, set()))
    call("PUT", f"/processors/{ent['id']}", {"revision": cur["revision"], "component": {
        "id": ent["id"], "config": {"autoTerminatedRelationships": auto}}})

# ---------------------------------------------------------------- Enable services
for name, ent in services.items():
    cur = call("GET", f"/controller-services/{ent['id']}")
    call("PUT", f"/controller-services/{ent['id']}/run-status", {"revision": cur["revision"], "state": "ENABLED"})

print(json.dumps({"process_group": PG, "trigger": procs["P00"]["id"],
                  "processors": {k: v["id"] for k, v in procs.items()}}, indent=1))
