"""WebHDFS REST 클라이언트(httpx 동기). bin/hdfs.sh가 쓴다.

Hadoop 클라이언트(JVM, hdfs 명령)가 없는 호스트에서도 NameNode HTTP 포트(기본 9870)만 열려 있으면
HDFS를 조회할 수 있다. 인증은 simple(`user.name` 질의 인자)만 지원한다(이 환경은 Kerberos 없음).

NameNode HA: namenode_urls를 순서대로 시도한다. 연결 실패이거나 StandbyException(standby NameNode가
돌려주는 403)이면 다음 주소로 넘어가고, 성공한 주소를 기억해 다음 요청부터 먼저 쓴다.
OPEN(파일 읽기)과 CREATE(업로드)는 NameNode가 DataNode 주소로 307 redirect한다. redirect는 직접 따라가며,
DataNode 호스트 이름이 이 호스트에서 풀리지 않거나 다른 주소로 가야 하면 datanode_hosts로 바꾼다
(예: {"dn1.cluster.local": "10.0.0.11"}).
"""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

API = "/webhdfs/v1"


class HdfsError(Exception):
    """WebHDFS가 돌려준 RemoteException 또는 연결 실패. exception은 Java 예외 이름(없으면 빈 문자열)."""

    def __init__(self, message: str, exception: str = "", status: int = 0) -> None:
        super().__init__(message)
        self.exception = exception
        self.status = status


@dataclass(frozen=True)
class FileStatus:
    """LISTSTATUS·GETFILESTATUS의 FileStatus 하나. path는 절대 경로로 채운다."""

    path: str
    type: str  # FILE, DIRECTORY, SYMLINK
    length: int
    owner: str
    group: str
    permission: str  # 8진수 문자열(예: 750)
    replication: int
    block_size: int
    modification_time: int  # epoch milliseconds
    access_time: int

    @property
    def is_dir(self) -> bool:
        """디렉터리 여부."""
        return self.type == "DIRECTORY"

    @classmethod
    def from_json(cls, parent: str, raw: dict[str, Any]) -> "FileStatus":
        """WebHDFS JSON에서 만든다. LISTSTATUS는 pathSuffix에 이름만, GETFILESTATUS는 빈 문자열을 준다."""
        suffix = raw.get("pathSuffix") or ""
        path = join(parent, suffix) if suffix else parent
        return cls(path=path, type=raw.get("type", "FILE"), length=int(raw.get("length", 0)),
                   owner=raw.get("owner", ""), group=raw.get("group", ""),
                   permission=str(raw.get("permission", "")), replication=int(raw.get("replication", 0)),
                   block_size=int(raw.get("blockSize", 0)),
                   modification_time=int(raw.get("modificationTime", 0)),
                   access_time=int(raw.get("accessTime", 0)))


@dataclass(frozen=True)
class ContentSummary:
    """GETCONTENTSUMMARY 결과. length는 논리 크기, space_consumed는 복제본을 포함한 실제 사용량."""

    directory_count: int
    file_count: int
    length: int
    space_consumed: int
    quota: int
    space_quota: int


def join(parent: str, child: str) -> str:
    """HDFS 경로 결합. child가 절대 경로면 child를 쓴다."""
    if child.startswith("/"):
        return normalize(child)
    return normalize(parent.rstrip("/") + "/" + child)


def normalize(path: str) -> str:
    """`.`, `..`, 중복 `/`를 정리한 절대 경로. 루트 위로는 올라가지 않는다."""
    parts: list[str] = []
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
        else:
            parts.append(part)
    return "/" + "/".join(parts)


class WebHdfs:
    """WebHDFS 동기 클라이언트. 경로 인자는 모두 절대 경로다(상대 경로 해석은 hdfsshell이 한다)."""

    def __init__(self, namenode_urls: list[str], user: str, timeout: float = 30.0,
                 client: httpx.Client | None = None, datanode_hosts: dict[str, str] | None = None) -> None:
        if not namenode_urls:
            raise ValueError("namenode_urls is empty")
        self.urls = [u.rstrip("/") for u in namenode_urls]
        self.user = user
        self._client = client or httpx.Client(timeout=timeout)
        self._active = 0  # 마지막으로 성공한 NameNode 인덱스
        self.datanode_hosts = datanode_hosts or {}

    def close(self) -> None:
        """HTTP 연결을 닫는다."""
        self._client.close()

    @property
    def active_url(self) -> str:
        """마지막으로 응답한(또는 처음 시도할) NameNode 주소."""
        return self.urls[self._active]

    def _datanode(self, resp: httpx.Response) -> str:
        """NameNode의 307 응답에서 DataNode 주소를 꺼내 datanode_hosts로 호스트를 바꾼다."""
        location = resp.headers.get("location")
        if resp.status_code != 307 or not location:
            raise HdfsError(f"DataNode redirect가 없습니다(HTTP {resp.status_code})")
        url = httpx.URL(location)
        if url.host in self.datanode_hosts:
            url = url.copy_with(host=self.datanode_hosts[url.host])
        return str(url)

    def _send(self, method: str, path: str, op: str, params: dict[str, Any] | None = None,
              stream: bool = False, **kw: Any) -> httpx.Response:
        """NameNode를 순서대로 시도해 응답을 돌려준다. 2xx·3xx가 아니면 HdfsError. redirect는 따라가지 않는다.

        stream이면 본문을 읽지 않은 응답을 돌려주므로 호출자가 close해야 한다.
        """
        query = {"op": op, "user.name": self.user, **(params or {})}
        errors: list[str] = []
        order = [self._active, *[i for i in range(len(self.urls)) if i != self._active]]
        for idx in order:
            url = f"{self.urls[idx]}{API}{quote(path)}"
            try:
                request = self._client.build_request(method, url, params=query, **kw)
                resp = self._client.send(request, stream=stream)
            except httpx.HTTPError as exc:
                errors.append(f"{self.urls[idx]}: {exc}")
                continue
            if resp.status_code < 400:
                self._active = idx
                return resp
            if stream:
                resp.read()
                resp.close()
            err = _remote_error(resp)
            if err.exception == "StandbyException":
                errors.append(f"{self.urls[idx]}: standby")
                continue
            self._active = idx
            raise err
        raise HdfsError("NameNode에 연결할 수 없습니다: " + "; ".join(errors))

    def status(self, path: str) -> FileStatus:
        """GETFILESTATUS. 없으면 HdfsError(FileNotFoundException)."""
        raw = self._send("GET", path, "GETFILESTATUS").json()["FileStatus"]
        return FileStatus.from_json(path, raw)

    def exists(self, path: str) -> bool:
        """경로가 있는지. FileNotFoundException만 False로 바꾸고 나머지 오류는 그대로 낸다."""
        try:
            self.status(path)
        except HdfsError as exc:
            if exc.exception == "FileNotFoundException":
                return False
            raise
        return True

    def listdir(self, path: str) -> list[FileStatus]:
        """LISTSTATUS. 파일 경로를 주면 그 파일 하나를 돌려준다(hdfs dfs -ls와 같다). 이름순."""
        raw = self._send("GET", path, "LISTSTATUS").json()["FileStatuses"]["FileStatus"]
        items = [FileStatus.from_json(path, r) for r in raw]
        return sorted(items, key=lambda s: s.path)

    def walk(self, path: str) -> Iterator[FileStatus]:
        """path 아래를 깊이 우선으로 모두 돌려준다(path 자신은 빼고). 디렉터리는 하위보다 먼저 나온다."""
        for item in self.listdir(path):
            if item.path == path:  # path가 파일이면 LISTSTATUS가 자기 자신을 준다
                continue
            yield item
            if item.is_dir:
                yield from self.walk(item.path)

    def content_summary(self, path: str) -> ContentSummary:
        """GETCONTENTSUMMARY. 큰 디렉터리는 NameNode가 하위 전체를 세므로 오래 걸릴 수 있다."""
        raw = self._send("GET", path, "GETCONTENTSUMMARY").json()["ContentSummary"]
        return ContentSummary(directory_count=int(raw.get("directoryCount", 0)),
                              file_count=int(raw.get("fileCount", 0)), length=int(raw.get("length", 0)),
                              space_consumed=int(raw.get("spaceConsumed", 0)),
                              quota=int(raw.get("quota", -1)), space_quota=int(raw.get("spaceQuota", -1)))

    def read(self, path: str, offset: int = 0, length: int | None = None,
             chunk_size: int = 65536) -> Iterator[bytes]:
        """OPEN. DataNode로 redirect된 응답 본문을 chunk 단위로 내준다. length가 None이면 끝까지."""
        params: dict[str, Any] = {"offset": offset}
        if length is not None:
            params["length"] = length
        url = self._datanode(self._send("GET", path, "OPEN", params))
        try:
            with self._client.stream("GET", url) as resp:
                if resp.status_code >= 400:
                    resp.read()
                    raise _remote_error(resp)
                yield from resp.iter_bytes(chunk_size)
        except httpx.HTTPError as exc:
            raise HdfsError(f"DataNode에서 읽지 못했습니다({url.split('?')[0]}): {exc}") from None

    def mkdirs(self, path: str, permission: str | None = None) -> bool:
        """MKDIRS(상위 디렉터리도 만든다). 이미 있으면 True."""
        params = {"permission": permission} if permission else None
        return bool(self._send("PUT", path, "MKDIRS", params).json()["boolean"])

    def delete(self, path: str, recursive: bool = False) -> bool:
        """DELETE. 없으면 False. 비어 있지 않은 디렉터리를 recursive 없이 지우면 HdfsError."""
        return bool(self._send("DELETE", path, "DELETE", {"recursive": str(recursive).lower()})
                    .json()["boolean"])

    def rename(self, src: str, dst: str) -> bool:
        """RENAME. dst가 이미 있는 디렉터리면 그 안으로 옮긴다. 실패하면 False(HDFS 동작 그대로)."""
        return bool(self._send("PUT", src, "RENAME", {"destination": dst}).json()["boolean"])

    def set_permission(self, path: str, permission: str) -> None:
        """SETPERMISSION. permission은 8진수 문자열(예: 750)."""
        self._send("PUT", path, "SETPERMISSION", {"permission": permission})

    def create(self, path: str, data: bytes | Iterator[bytes], overwrite: bool = False) -> None:
        """CREATE. NameNode가 준 DataNode 주소(Location)로 본문을 다시 PUT한다."""
        url = self._datanode(self._send("PUT", path, "CREATE", {"overwrite": str(overwrite).lower()}))
        try:
            put = self._client.put(url, content=data, headers={"Content-Type": "application/octet-stream"})
        except httpx.HTTPError as exc:
            raise HdfsError(f"DataNode에 쓰지 못했습니다({url.split('?')[0]}): {exc}") from None
        if put.status_code >= 400:
            raise _remote_error(put)


def _remote_error(resp: httpx.Response) -> HdfsError:
    """오류 응답을 HdfsError로 바꾼다. RemoteException JSON이 아니면 HTTP 상태와 본문 앞부분을 쓴다."""
    try:
        remote = resp.json()["RemoteException"]
        return HdfsError(remote.get("message", ""), remote.get("exception", ""), resp.status_code)
    except (ValueError, KeyError, TypeError):
        return HdfsError(f"HTTP {resp.status_code}: {resp.text[:200]}", status=resp.status_code)
