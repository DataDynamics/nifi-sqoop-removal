#!/usr/bin/env python3
"""build_flow_v1.py 또는 build_flow_v3.py가 만든 Process Group과 Parameter Context를 지운다.

사용법: teardown_flow.py <nifi-api-url> [config.json]
config.json을 주지 않으면 V1 기본 이름(SQOOP_REPLACEMENT_POC)을 지운다. V3는 build_flow_v3.py에 쓴 config를 준다.
config.json에 `names`가 있으면 그 이름의 PG와 Parameter Context를 지운다.
"""
import json
import sys
import time
import urllib.error
import urllib.request

API = sys.argv[1].rstrip("/")
NAMES = json.load(open(sys.argv[2])).get("names", {}) if len(sys.argv) > 2 else {}
PG_NAME = NAMES.get("process_group", "SQOOP_REPLACEMENT_POC")
PC_COMMON = NAMES.get("common_context", "PC_SQOOP_REPLACEMENT_COMMON")
PC_JOB = NAMES.get("job_context", "PC_JOB_PG_INSP_DTL_DAILY")
CID = "poc-builder"


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


root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
for pg in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]:
    if pg["component"]["name"] != PG_NAME:
        continue
    pid = pg["id"]
    call("PUT", f"/flow/process-groups/{pid}", {"id": pid, "state": "STOPPED"})
    # 실행 중인 thread가 끝나야 controller service를 끌 수 있다.
    for _ in range(60):
        procs = call("GET", f"/process-groups/{pid}/processors")["processors"]
        if all(pr["status"]["aggregateSnapshot"]["activeThreadCount"] == 0
               and pr["component"]["state"] != "RUNNING" for pr in procs):
            break
        time.sleep(1)
    call("PUT", f"/flow/process-groups/{pid}/controller-services", {"id": pid, "state": "DISABLED"})
    for _ in range(60):
        svcs = call("GET", f"/flow/process-groups/{pid}/controller-services")["controllerServices"]
        if all(sv["component"]["state"] == "DISABLED" for sv in svcs):
            break
        time.sleep(1)
    # 큐에 남은 FlowFile을 비운다
    drop = call("POST", f"/process-groups/{pid}/empty-all-connections-requests")
    req_id = drop["dropRequest"]["id"]
    for _ in range(30):
        st = call("GET", f"/process-groups/{pid}/empty-all-connections-requests/{req_id}")
        if st["dropRequest"]["finished"]:
            break
        time.sleep(1)
    call("DELETE", f"/process-groups/{pid}/empty-all-connections-requests/{req_id}")
    cur = call("GET", f"/process-groups/{pid}")
    call("DELETE", f"/process-groups/{pid}?version={cur['revision']['version']}&clientId={CID}")
    print("deleted", pid)
# Job context가 common을 상속하므로 Job부터 지운다.
contexts = {pc["component"]["name"]: pc["id"] for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"]}
for name in (PC_JOB, PC_COMMON):
    if name in contexts:
        cur = call("GET", f"/parameter-contexts/{contexts[name]}")
        call("DELETE", f"/parameter-contexts/{contexts[name]}?version={cur['revision']['version']}&clientId={CID}")
        print("deleted parameter context", name)
