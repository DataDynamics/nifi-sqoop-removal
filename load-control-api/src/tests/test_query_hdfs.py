"""운영 조회 도구 WebHDFS 클라이언트와 HDFS 셸을 respx로 흉내 낸 NameNode·DataNode로 검증한다."""

import io

import httpx
import pytest
import respx

from load_control.query.hdfsshell import HdfsShell
from load_control.query.sqlshell import EXIT_REFUSED, EXIT_USAGE
from load_control.query.webhdfs import WebHdfs, normalize

NN1, NN2 = "http://nn1:9870", "http://nn2:9870"
API = "/webhdfs/v1"


def status(name: str, kind: str = "FILE", length: int = 0) -> dict[str, object]:
    return {"pathSuffix": name, "type": kind, "length": length, "owner": "nifi", "group": "hadoop",
            "permission": "750", "replication": 1, "blockSize": 134217728,
            "modificationTime": 1_700_000_000_000, "accessTime": 0}


def listing(*items: dict[str, object]) -> httpx.Response:
    return httpx.Response(200, json={"FileStatuses": {"FileStatus": list(items)}})


def make_shell(**kw: object) -> tuple[HdfsShell, io.StringIO, io.StringIO, io.BytesIO]:
    fs = WebHdfs([NN1, NN2], "nifi", datanode_hosts={"dn1.cluster": "10.0.0.11"})
    out, err, raw = io.StringIO(), io.StringIO(), io.BytesIO()
    sh = HdfsShell(fs, home="/data/stage", out=out, err=err, binary_out=raw, **kw)  # type: ignore[arg-type]
    return sh, out, err, raw


def test_normalize() -> None:
    assert normalize("/a//b/./c/../d/") == "/a/b/d"
    assert normalize("/../..") == "/"


@respx.mock
def test_standby_namenode_is_skipped() -> None:
    """첫 NameNode가 StandbyException이면 두 번째로 넘어가고, 이후 요청은 활성 NameNode로 먼저 간다."""
    standby = httpx.Response(403, json={"RemoteException": {"exception": "StandbyException", "message": "s"}})
    r1 = respx.get(f"{NN1}{API}/data/stage").mock(return_value=standby)
    respx.get(f"{NN2}{API}/data/stage").mock(return_value=listing(status("a", "DIRECTORY")))
    fs = WebHdfs([NN1, NN2], "nifi")
    assert [s.path for s in fs.listdir("/data/stage")] == ["/data/stage/a"]
    assert fs.listdir("/data/stage")[0].is_dir
    assert r1.call_count == 1
    assert fs.active_url == NN2


@respx.mock
def test_ls_relative_path_and_glob() -> None:
    """상대 경로는 home 기준이고, glob은 부모 목록을 걸러 펼친다."""
    respx.get(f"{NN1}{API}/data/stage").mock(
        return_value=listing(status("run=1", "DIRECTORY"), status("run=2", "DIRECTORY"), status("x")))
    respx.get(f"{NN1}{API}/data/stage/run%3D1").mock(return_value=listing(status("p.parquet", length=10)))
    respx.get(f"{NN1}{API}/data/stage/run%3D2").mock(return_value=listing())
    sh, out, _, _ = make_shell()
    assert sh.run(["ls", "run=*"])
    text = out.getvalue()
    assert "/data/stage/run=1/p.parquet" in text and "-rwxr-x---" in text


@respx.mock
def test_read_follows_redirect_with_host_mapping() -> None:
    """OPEN의 DataNode redirect 호스트를 datanode_hosts로 바꿔 읽는다."""
    respx.get(f"{NN1}{API}/data/stage/a.txt").mock(return_value=httpx.Response(
        307, headers={"Location": "http://dn1.cluster:9864/webhdfs/v1/data/stage/a.txt?op=OPEN"}))
    dn = respx.get("http://10.0.0.11:9864/webhdfs/v1/data/stage/a.txt").mock(
        return_value=httpx.Response(200, content=b"hello"))
    sh, _, _, raw = make_shell()
    assert sh.run(["cat", "a.txt"])
    assert raw.getvalue() == b"hello"
    assert dn.called


def test_write_commands_need_write_flag() -> None:
    """읽기 전용이면 rm 등을 요청 없이 거부하고 종료 코드 3."""
    sh, _, err, _ = make_shell()
    assert not sh.run(["rm", "-r", "/data/stage/x"])
    assert sh.status == EXIT_REFUSED
    assert "읽기 전용" in err.getvalue()


def test_shallow_paths_are_protected_even_with_write() -> None:
    """--write여도 /data 같은 얕은 경로는 지우지 않는다."""
    sh, _, err, _ = make_shell(allow_write=True)
    assert not sh.run(["rm", "-r", "/data"])
    assert sh.status == EXIT_REFUSED
    assert "보호 경로" in err.getvalue()


@respx.mock
def test_rm_with_write_deletes() -> None:
    route = respx.delete(f"{NN1}{API}/data/stage/run%3D1").mock(
        return_value=httpx.Response(200, json={"boolean": True}))
    sh, _, _, _ = make_shell(allow_write=True)
    assert sh.run(["rm", "-r", "run=1"])
    assert route.calls[0].request.url.params["recursive"] == "true"


@respx.mock
def test_find_accepts_options_after_path() -> None:
    """find 경로 -type f -name 패턴."""
    respx.get(f"{NN1}{API}/data/stage").mock(
        return_value=listing(status("r", "DIRECTORY"), status("a.parquet")))
    respx.get(f"{NN1}{API}/data/stage/r").mock(return_value=listing(status("b.parquet"), status("_SUCCESS")))
    sh, out, _, _ = make_shell()
    assert sh.run(["find", ".", "-type", "f", "-name", "*.parquet"])
    assert out.getvalue().split() == ["/data/stage/a.parquet", "/data/stage/r/b.parquet"]


@pytest.mark.parametrize("argv", [["nope"], ["head", "-c", "x", "a"], ["ls", "-Z"]])
def test_usage_errors(argv: list[str]) -> None:
    sh, _, _, _ = make_shell()
    assert not sh.run(argv)
    assert sh.status == EXIT_USAGE
