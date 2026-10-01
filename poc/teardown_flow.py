#!/usr/bin/env python3
"""build_flow.py가 만든 SQOOP_REPLACEMENT_POC Process Group과 Parameter Context를 지운다.

사용법: teardown_flow.py <nifi-api-url>
"""
import json
import sys
import time
import urllib.error
import urllib.request

API = sys.argv[1].rstrip("/")
PG_NAME = "SQOOP_REPLACEMENT_POC"
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
for name in ("PC_JOB_PG_INSP_DTL_DAILY", "PC_SQOOP_REPLACEMENT_COMMON"):
    if name in contexts:
        cur = call("GET", f"/parameter-contexts/{contexts[name]}")
        call("DELETE", f"/parameter-contexts/{contexts[name]}?version={cur['revision']['version']}&clientId={CID}")
        print("deleted parameter context", name)
