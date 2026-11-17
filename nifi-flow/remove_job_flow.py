#!/usr/bin/env python3
"""deploy_job_flow.py가 만든 Job을 지운다.

사용법: remove_job_flow.py <nifi-api-url> <config.json> [process-group-name] [--parent-id <pg-id>]
        배포에 쓴 config, PG 이름, 상위 PG id를 그대로 준다(--parent-id 기본 root).

- Job PG(`JOB_<JOB.KEY>`)와 Job Parameter Context(`PC_JOB_<JOB.KEY>`)를 지운다.
- root PG-05 Control Receiver에서는 이 Job의 route·Output Port·연결만 지운다.
  Job PG가 상위 PG 안에 있으면 중간 PG의 중계 Input Port(validate-<JOB>, reissue-<JOB>)와 연결도 지운다.
- 남은 Job이 없으면 PG-05와 공통 Parameter Context도 지운다.
"""
import argparse
import datetime
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("api_url")
ap.add_argument("config")
ap.add_argument("process_group_name", nargs="?")
ap.add_argument("--parent-id", help="Job PG가 있는 상위 PG id(기본 root)")
ap.add_argument("--timeout", type=float, default=60,
                help="NiFi REST 호출 하나의 timeout 초(기본 60). 넘으면 어느 호출에서 멈췄는지 남기고 종료한다")
ap.add_argument("--log-file", help="진행 로그를 이 파일에 남긴다(append). 지정하지 않으면 콘솔(stdout)에 출력한다")
ap.add_argument("--debug", action="store_true",
                help="요청 본문과 응답 앞부분도 로그에 남긴다(PASSWORD·AUTHORIZATION 값은 가린다)")
ARGS = ap.parse_args()
API = ARGS.api_url.rstrip("/")

# ---------------------------------------------------------------- 진행 로그
# 진행 로그는 --log-file이 있으면 그 파일에, 없으면 콘솔(stdout)에 즉시(flush) 쓴다.
# --log-file이 있어도 ERROR는 콘솔(stderr)에도 출력해 실패를 바로 알 수 있게 한다.
LOG_FILE = open(ARGS.log_file, "a", encoding="utf-8") if ARGS.log_file else None
T0 = time.monotonic()
CALLS = 0


def log(msg, level="INFO"):
    """시각·경과 시간과 함께 진행 로그 한 줄을 --log-file 또는 콘솔(stdout)에 바로 쓴다."""
    line = (f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} +{time.monotonic() - T0:7.1f}s "
            f"{level:5} {msg}")
    if LOG_FILE:
        LOG_FILE.write(line + "\n")
        LOG_FILE.flush()
        if level == "ERROR":
            print(line, file=sys.stderr, flush=True)
    else:
        print(line, flush=True)


def die(msg):
    """오류를 로그에 남기고 종료한다."""
    log(msg, "ERROR")
    log(f"삭제 실패(NiFi API 호출 {CALLS}회). 원인을 해결한 뒤 다시 실행하면 남은 것부터 지운다",
        "ERROR")
    raise SystemExit(1)


def _excepthook(tp, val, tb):
    """예상하지 못한 예외도 로그 파일에 남긴다(traceback은 stderr에 기본 형식으로 출력)."""
    import traceback
    log("예상하지 못한 오류로 중단: " + "".join(traceback.format_exception(tp, val, tb)).rstrip(), "ERROR")


sys.excepthook = _excepthook

SENSITIVE = ("PASSWORD", "AUTHORIZATION")


def mask(obj):
    """로그용으로 sensitive 값(이름이 PASSWORD·AUTHORIZATION으로 끝나는 키·Parameter)을 가린다."""
    if isinstance(obj, dict):
        out = {}
        sens_param = str(obj.get("name", "")).upper().endswith(SENSITIVE) or obj.get("sensitive") is True
        for k, v in obj.items():
            if (k.upper().replace("-", ".").endswith(SENSITIVE) or (k == "value" and sens_param)) \
                    and isinstance(v, str) and not v.startswith("#{"):
                out[k] = "****"
            else:
                out[k] = mask(v)
        return out
    if isinstance(obj, list):
        return [mask(x) for x in obj]
    return obj


log(f"remove_job_flow 시작: python {sys.version.split()[0]}, host {socket.gethostname()}, pid {os.getpid()}")
log(f"NiFi API: {API} (호출 timeout {ARGS.timeout:g}s)")
for _k in ("http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"):
    if os.environ.get(_k):
        log(f"환경변수 {_k}={os.environ[_k]} (NiFi 호출이 proxy를 거치면 응답이 없을 수 있다)", "WARN")
log(f"config 읽기: {os.path.abspath(ARGS.config)}")
try:
    with open(ARGS.config, encoding="utf-8") as _f:
        CFG = json.load(_f)
except (OSError, ValueError) as e:
    die(f"config를 읽을 수 없다: {e}")
NAMES = CFG.get("names", {})
RECEIVER_NAME = NAMES.get("control_receiver", "PG-05 Control Receiver")
JOB_KEY = CFG["job_params"]["JOB.KEY"]
PG_NAME = ARGS.process_group_name or NAMES.get("process_group", f"JOB_{JOB_KEY}")
PC_COMMON = NAMES.get("common_context", "PC_SQOOP_REPLACEMENT_COMMON")
PC_JOB = NAMES.get("job_context", f"PC_JOB_{JOB_KEY}")
CID = "nifi-flow-remover"   # DELETE 요청의 clientId 쿼리 값. revision version과 함께 보낸다.
log(f"JOB.KEY={JOB_KEY}, Job PG={PG_NAME}, Job Context={PC_JOB}, 공통 Context={PC_COMMON}, "
    f"Receiver={RECEIVER_NAME}, parent={ARGS.parent_id or 'root'}")


def call(method, path, body=None):
    """NiFi REST API를 호출하고 응답 JSON을 돌려준다(본문이 비면 None).

    HTTP 오류는 상태 코드와 본문 앞 2000자를 담아 SystemExit로 멈춘다. 중간에 멈춰도 다시 실행하면
    남은 것부터 지운다(이미 지워진 PG·Context는 건너뛴다).
    """
    global CALLS
    CALLS += 1
    n = CALLS
    payload = None if body is None else json.dumps(body).encode()
    log(f"[#{n}] {method} {path}" + (f" ({len(payload)} bytes)" if payload else ""), "DEBUG" if method == "GET" else "INFO")
    if ARGS.debug and body is not None:
        log(f"[#{n}] request body: {json.dumps(mask(body), ensure_ascii=False)[:4000]}", "DEBUG")
    req = urllib.request.Request(API + path, method=method, data=payload,
                                 headers={"Content-Type": "application/json"})
    t = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=ARGS.timeout) as r:
            data = r.read()
            status = r.status
    except urllib.error.HTTPError as e:
        die(f"[#{n}] {method} {path} -> HTTP {e.code} ({time.monotonic() - t:.2f}s): "
            f"{e.read().decode(errors='replace')[:2000]}")
    except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, OSError) as e:
        reason = getattr(e, "reason", e)
        die(f"[#{n}] {method} {API + path} 연결 실패 ({time.monotonic() - t:.2f}s): "
            f"{type(reason).__name__}: {reason}. NiFi 주소·포트·방화벽·proxy 설정을 확인한다")
    log(f"[#{n}] -> {status} ({time.monotonic() - t:.2f}s, {len(data)} bytes)", "DEBUG")
    if ARGS.debug and data:
        try:
            shown = json.dumps(mask(json.loads(data)), ensure_ascii=False)
        except ValueError:
            shown = data.decode(errors="replace")
        log(f"[#{n}] response: {shown[:1000]}", "DEBUG")
    try:
        return json.loads(data) if data else None
    except ValueError:
        die(f"[#{n}] {method} {path} 응답이 JSON이 아니다(NiFi API URL이 /nifi-api로 끝나는지 확인): "
            f"{data[:500].decode(errors='replace')}")


def stop_pg(pid):
    """PG(하위 PG 포함)를 STOPPED로 바꾸고, 하위 PG까지 active thread가 0이 될 때까지 최대 60초 기다린다.

    Job PG(`JOB_<KEY>`)는 Processor를 모두 하위 PG(PG-00~90)에 두므로 바로 아래 Processor만 보면 기다리지
    않고 지나간다. 그래서 PG status의 aggregateSnapshot(하위 PG 합계)으로 thread 수를 본다.
    시간 안에 멈추지 않아도 예외 없이 돌아간다. 이후 삭제 요청이 실패하면 call()이 멈춘다.
    """
    log(f"PG 정지 요청: {pid}")
    call("PUT", f"/flow/process-groups/{pid}", {"id": pid, "state": "STOPPED"})
    # 실행 중인 thread가 끝나야 controller service를 끄고 연결·PG를 지울 수 있다.
    for i in range(60):
        snap = call("GET", f"/flow/process-groups/{pid}/status")["processGroupStatus"]["aggregateSnapshot"]
        if snap["activeThreadCount"] == 0:
            log(f"PG 정지 완료: {pid}")
            return
        log(f"PG 정지 대기 {i + 1}/60s: active thread {snap['activeThreadCount']}")
        time.sleep(1)
    log(f"PG가 60초 안에 멈추지 않았다: {pid}. 계속 진행한다(이후 삭제가 거부될 수 있다)", "WARN")


def empty_queue(conn_id):
    """연결 하나의 큐를 비운다.

    drop-request를 만들고 끝날 때까지 최대 30초 기다린 뒤 요청을 DELETE로 정리한다.
    큐에 FlowFile이 남아 있는 연결은 지울 수 없다.
    """
    log(f"연결 큐 비우기: {conn_id}")
    req = call("POST", f"/flowfile-queues/{conn_id}/drop-requests")["dropRequest"]
    for i in range(30):
        req = call("GET", f"/flowfile-queues/{conn_id}/drop-requests/{req['id']}")["dropRequest"]
        if req["finished"]:
            log(f"연결 큐 비움: {conn_id} (dropped {req.get('dropped', '-')})")
            break
        log(f"연결 큐 비우기 대기 {i + 1}/30s: {req.get('percentCompleted')}%")
        time.sleep(1)
    else:
        log(f"연결 큐가 30초 안에 비워지지 않았다: {conn_id}", "WARN")
    call("DELETE", f"/flowfile-queues/{conn_id}/drop-requests/{req['id']}")


def delete_connection(cn):
    """연결을 비우고 지운다. 삭제에는 최신 revision version이 필요하므로 다시 읽는다."""
    c_ = cn["component"]
    log(f"연결 삭제: {c_['source'].get('name', c_['source']['id'])} --> {c_['destination'].get('name', c_['destination']['id'])}"
        f" ({cn['id']})")
    empty_queue(cn["id"])
    cur = call("GET", f"/connections/{cn['id']}")
    call("DELETE", f"/connections/{cn['id']}?version={cur['revision']['version']}&clientId={CID}")


def delete_pg(pid):
    """PG를 통째로 지운다.

    NiFi는 실행 중인 구성요소, 켜진 Controller Service, 비어 있지 않은 큐가 있는 PG를 지우지 않으므로
    1. PG를 멈추고 2. PG 안 Controller Service를 모두 DISABLED로 바꿔 끝날 때까지(최대 60초) 기다리고
    3. 모든 연결의 큐를 비운 뒤(empty-all-connections-requests, 최대 30초) 4. 최신 revision으로 DELETE한다.
    """
    log(f"== PG 삭제 시작: {pid}")
    stop_pg(pid)
    # Controller Service 비활성화는 비동기다. 참조하는 Processor가 멈춰 있어야 끝난다.
    log(f"Controller Service DISABLE 요청: {pid}")
    call("PUT", f"/flow/process-groups/{pid}/controller-services", {"id": pid, "state": "DISABLED"})
    for i in range(60):
        svcs = call("GET", f"/flow/process-groups/{pid}/controller-services")["controllerServices"]
        left = [sv["component"]["name"] for sv in svcs if sv["component"]["state"] != "DISABLED"]
        if not left:
            log(f"Controller Service 모두 DISABLED ({len(svcs)}개)")
            break
        log(f"Controller Service DISABLE 대기 {i + 1}/60s: {left}")
        time.sleep(1)
    else:
        log(f"Controller Service가 60초 안에 DISABLED 되지 않았다: {left}", "WARN")
    # 큐에 남은 FlowFile을 비운다
    log(f"PG 안 모든 연결 큐 비우기: {pid}")
    drop = call("POST", f"/process-groups/{pid}/empty-all-connections-requests")
    req_id = drop["dropRequest"]["id"]
    for i in range(30):
        st = call("GET", f"/process-groups/{pid}/empty-all-connections-requests/{req_id}")
        if st["dropRequest"]["finished"]:
            log(f"큐 비움 완료 (dropped {st['dropRequest'].get('dropped', '-')})")
            break
        log(f"큐 비우기 대기 {i + 1}/30s: {st['dropRequest'].get('percentCompleted')}%")
        time.sleep(1)
    else:
        log(f"큐가 30초 안에 비워지지 않았다: {pid}", "WARN")
    call("DELETE", f"/process-groups/{pid}/empty-all-connections-requests/{req_id}")
    cur = call("GET", f"/process-groups/{pid}")
    log(f"PG 삭제: {cur['component'].get('name')} ({pid})")
    call("DELETE", f"/process-groups/{pid}?version={cur['revision']['version']}&clientId={CID}")
    log(f"PG 삭제됨: {cur['component'].get('name')} ({pid})")


def stop_input_port(pid):
    """Input Port가 RUNNING이면 멈춘다. 연결·Port를 지우려면 양 끝이 멈춰 있어야 한다."""
    cur = call("GET", f"/input-ports/{pid}")
    if cur["component"]["state"] == "RUNNING":
        log(f"Input Port 정지: {cur['component']['name']} ({pid})")
        call("PUT", f"/input-ports/{pid}/run-status", {"revision": cur["revision"], "state": "STOPPED"})


def delete_route_chain(out_port, job_pg):
    """PG-05 Output Port에서 Job PG Input Port까지 이어진 연결과 중계 Input Port를 지운다.

    root에서 시작해 연결의 destination을 따라 내려간다. destination이 Job PG가 아니면 중계 Input Port이므로
    멈추고 다음 단계로 간다. 중계 Port는 들어오는 연결과 나가는 연결을 모두 지운 뒤에 지운다.
    """
    gid, src = root, out_port
    while True:
        cn = next((x for x in call("GET", f"/process-groups/{gid}/connections")["connections"]
                   if x["component"]["source"]["id"] == src), None)
        nxt = None
        if cn:
            dst = cn["component"]["destination"]
            if dst["groupId"] != job_pg:
                stop_input_port(dst["id"])
                nxt = (dst["groupId"], dst["id"])
            delete_connection(cn)
        if gid != root:   # src는 중계 Input Port
            cur = call("GET", f"/input-ports/{src}")
            log(f"중계 Input Port 삭제: {cur['component']['name']} in {gid} ({src})")
            call("DELETE", f"/input-ports/{src}?version={cur['revision']['version']}&clientId={CID}")
        if nxt is None:
            return
        gid, src = nxt


def unregister_job(receiver, job_pg):
    """root PG-05에서 이 Job의 연결·Output Port·route를 지운다. 남은 Job 목록을 돌려준다.

    순서:
    1. Job PG와 PG-05를 멈춘다.
    2. 이 Job의 PG-05 Output Port(validate-<JOB>, reissue-<JOB>)마다 Job PG까지 이어진 연결과 중계 Input Port를
       지우고(delete_route_chain), PG-05 안에서 그 Port로 가는 연결을 지운 뒤 Port를 지운다.
    3. PARENT에 Job PG로 들어가는 연결이 남아 있으면(중간에 멈춘 배포 등) 비우고 지운다.
    4. 10_Route_By_Job_Action에서 validate.<JOB>·reissue.<JOB> 동적 속성을 지운다(값 None이 속성 삭제다).
       연결이 먼저 지워져 있어야 해당 relationship이 사라질 수 있다.
    5. 남은 Job이 있으면 05_Listen_Control의 Allowed Paths를 남은 Job으로 줄이고 PG-05를 다시 시작한다.
       남은 Job이 없으면 PG-05를 멈춘 채로 두고, 호출한 쪽이 PG-05를 지운다.
    """
    log(f"== {RECEIVER_NAME}에서 Job 등록 해제: {JOB_KEY}")
    # 연결의 양 끝(PG-05 Output Port, Job PG Input Port)이 멈춰 있어야 연결을 지울 수 있다.
    stop_pg(job_pg)
    stop_pg(receiver)
    procs = call("GET", f"/process-groups/{receiver}/processors")["processors"]
    r10 = next(p for p in procs if p["component"]["name"].startswith("10_"))
    r05 = next(p for p in procs if p["component"]["name"].startswith("05_"))
    names = {f"validate-{JOB_KEY}", f"reissue-{JOB_KEY}"}   # register_job이 만든 Output Port 이름
    for port in call("GET", f"/process-groups/{receiver}/output-ports")["outputPorts"]:
        if port["component"]["name"] not in names:
            continue
        log(f"{RECEIVER_NAME} Output Port와 Job PG까지의 경로 삭제: {port['component']['name']} ({port['id']})")
        delete_route_chain(port["id"], job_pg)
        for cn in call("GET", f"/process-groups/{receiver}/connections")["connections"]:
            if cn["component"]["destination"]["id"] == port["id"]:
                delete_connection(cn)
        cur = call("GET", f"/output-ports/{port['id']}")
        log(f"Output Port 삭제: {port['component']['name']} ({port['id']})")
        call("DELETE", f"/output-ports/{port['id']}?version={cur['revision']['version']}&clientId={CID}")
    for cn in call("GET", f"/process-groups/{PARENT}/connections")["connections"]:
        if cn["component"]["destination"]["groupId"] == job_pg:
            log(f"Job PG로 들어가는 남은 연결 삭제: {cn['id']}")
            delete_connection(cn)
    log(f"route 삭제: validate.{JOB_KEY}, reissue.{JOB_KEY} ← {r10['component']['name']}")
    r10 = call("GET", f"/processors/{r10['id']}")
    call("PUT", f"/processors/{r10['id']}", {"revision": r10["revision"], "component": {"id": r10["id"], "config": {
        "properties": {f"validate.{JOB_KEY}": None, f"reissue.{JOB_KEY}": None}}}})
    # 지운 뒤 남은 route 속성 이름에서 남은 Job 목록을 얻는다.
    r10 = call("GET", f"/processors/{r10['id']}")
    jobs = sorted({k.split(".", 1)[1] for k in r10["component"]["config"]["properties"]
                   if k.startswith(("validate.", "reissue."))})
    if jobs:
        log(f"남은 Job {jobs}, Allowed Paths 갱신 후 {RECEIVER_NAME} 다시 시작")
        r05 = call("GET", f"/processors/{r05['id']}")
        call("PUT", f"/processors/{r05['id']}", {"revision": r05["revision"], "component": {
            "id": r05["id"], "config": {"properties": {"Allowed Paths": f"/(validate|reissue)/({'|'.join(jobs)})"}}}})
        call("PUT", f"/flow/process-groups/{receiver}", {"id": receiver, "state": "RUNNING"})
    log(f"등록 해제 완료: {JOB_KEY} from {RECEIVER_NAME}, remaining jobs: {jobs}")
    return jobs


# 순서: PG-05에서 이 Job 등록 해제(남은 Job이 없으면 PG-05 삭제) → Job PG 삭제 → Parameter Context 삭제.
# Job PG로 들어가는 연결이 남아 있으면 Job PG를 지울 수 없으므로 등록 해제를 먼저 한다.
# Job PG는 PARENT(--parent-id, 기본 root)에서, PG-05는 root에서 찾는다.
log("NiFi 연결 확인: /flow/about")
_about = call("GET", "/flow/about")["about"]
log(f"NiFi 연결 OK: {_about.get('title')} {_about.get('version')}")
root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
PARENT = ARGS.parent_id or root
log(f"root PG: {root}, 상위 PG: {PARENT}")


def child_groups(gid):
    return {pg["component"]["name"]: pg["id"]
            for pg in call("GET", f"/flow/process-groups/{gid}")["processGroupFlow"]["flow"]["processGroups"]}


root_children = child_groups(root)
children = child_groups(PARENT)
receiver_left = False   # PG-05가 남았는지(다른 Job이 쓰는지). 남으면 공통 Context도 지우지 않는다.
log(f"Job PG {PG_NAME}: " + (f"있음 ({children[PG_NAME]})" if PG_NAME in children else "없음(이미 지워짐)"))
log(f"{RECEIVER_NAME}: " + (f"있음 ({root_children[RECEIVER_NAME]})" if RECEIVER_NAME in root_children else "없음"))
if PG_NAME in children and RECEIVER_NAME in root_children:
    if unregister_job(root_children[RECEIVER_NAME], children[PG_NAME]):
        receiver_left = True
    else:
        log(f"남은 Job이 없으므로 {RECEIVER_NAME}도 지운다")
        delete_pg(root_children[RECEIVER_NAME])
if PG_NAME in children:
    delete_pg(children[PG_NAME])
log("== Parameter Context 정리")
# Job context가 common을 상속하므로 Job부터 지운다. common은 PG-05나 다른 Job이 쓰면 남긴다.
contexts = {pc["component"]["name"]: pc["id"] for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"]}
for name in (PC_JOB, PC_COMMON):
    if name not in contexts:
        log(f"Parameter Context 없음(건너뜀): {name}")
        continue
    cur = call("GET", f"/parameter-contexts/{contexts[name]}")
    # 공통 Context는 PG-05가 남았거나, 아직 붙어 있는 PG가 있거나, 다른 Job Context가 상속하면 남긴다.
    if name == PC_COMMON and (receiver_left or cur["component"].get("boundProcessGroups")
                              or any(pc["component"].get("inheritedParameterContexts")
                                     and any(i["id"] == contexts[name] for i in pc["component"]["inheritedParameterContexts"])
                                     for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"])):
        log(f"Parameter Context 유지: {name} (아직 사용 중)")
        continue
    call("DELETE", f"/parameter-contexts/{contexts[name]}?version={cur['revision']['version']}&clientId={CID}")
    log(f"Parameter Context 삭제됨: {name} ({contexts[name]})")
log(f"삭제 완료: {PG_NAME} (NiFi API 호출 {CALLS}회)")
