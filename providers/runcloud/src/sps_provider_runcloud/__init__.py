# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""A 1.0 `Provider` over run.cloud microVM sandboxes (https://docs.run.cloud/sandboxes).

Each operation maps to the run.cloud REST API under `/run-cloud/sandboxes`:

- `create` is `POST /run-cloud/sandboxes` with the `Idempotency-Key` header, then `GET` until the
  sandbox runs. run.cloud returns the first sandbox for a repeated key whatever the body, so the
  adapter puts a digest of the request in the sandbox's `name` and compares it. Labels are
  run.cloud tags, which `list` filters on server-side. The lifetime is `timeoutSeconds` with
  `onTimeout: "destroy"`, and idle pausing is off (`idlePauseSeconds: 0`).
- `run` and the process operations exec a supervisor script in the sandbox (`guest.py`), because
  run.cloud's exec call holds the HTTP request for the whole command and returns output as text.
- `files.read` and `files.write` move the file through `POST .../fs` in 512 KiB chunks.
- `cost` reads `GET .../cost`, which states the billed amount per meter (source `reported`), final
  once the destroyed sandbox is fully metered and readable for 30 days. Where that endpoint is not
  deployed it falls back to `GET .../usage` (billed seconds) and the sandbox's CPU counter, and
  reports usage quantities; discovery holds the list prices.
"""

from __future__ import annotations

import asyncio
import base64
import codecs
import hashlib
import json
import re
import secrets
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
from sandbox_provider_spec.v1 import (
    Cost,
    CostLine,
    CreateRequest,
    DestroyResult,
    Discovery,
    FileInfo,
    FileStream,
    OutputChunk,
    OutputPage,
    Process,
    ProcessStartRequest,
    Provider,
    ProviderTime,
    RunRequest,
    RunResult,
    Sandbox,
    SandboxPage,
    Size,
    SpsError,
)

from sps_provider_runcloud.facts import (
    CLASS,
    COST_SETTLE_S,
    DEFAULT_ENDPOINT,
    DEFAULT_SIZE,
    FS_CHUNK,
    FS_WINDOW,
    IMAGE,
    IO_CHUNK,
    LIMITS,
    LIST_LIMIT,
    MAX_LIFETIME_S,
    MAX_RUN_S,
    MAX_WAIT_S,
    POLL_S,
    SIZES,
    build_discovery,
)
from sps_provider_runcloud.guest import ROOT, SUPERVISOR

BASE = "/run-cloud/sandboxes"
NANODOLLARS_PER_USD = Decimal(1_000_000_000)
NAME_PREFIX = "sps-"
START_TIMEOUT_S = 11 * 60
NOT_ENDED = {"running", "paused", "stopped", "interrupted", "archived", "pending", "starting"}
ENDED = {"destroying", "destroyed", "failed", "deleted"}
_PROCESS_ID = re.compile(r"^p[0-9a-f]{24}$")
_STATES = {
    "running": "running",
    "pending": "running",
    "starting": "running",
    "paused": "paused",
    "pausing": "paused",
    "resuming": "paused",
    "stopped": "stopped",
    "interrupted": "stopped",
    "archived": "stopped",
}


def _time(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


def _fingerprint(request: CreateRequest, plan: Mapping[str, Any]) -> str:
    doc = {
        "class": request.sandbox_class,
        "resources": dict(request.resources),
        "requirements": dict(request.requirements),
        "image": request.image,
        "lifetime_seconds": request.lifetime_seconds,
        "at_end": request.at_end,
        "env": dict(request.env),
        "labels": dict(request.labels),
        "plan": plan,
    }
    raw = json.dumps(doc, sort_keys=True, separators=(",", ":"), default=str)
    return NAME_PREFIX + hashlib.sha256(raw.encode()).hexdigest()


def _pick_size(resources: Mapping[str, object]) -> Size:
    floors = {"cpu_min": 0.0, "memory_min_mib": 0.0, "disk_min_mib": 0.0}
    for key, value in resources.items():
        if value is None or (key == "root" and value == "by_default"):
            continue
        if key in floors:
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise SpsError("invalid_request", f"resources.{key} must be a number")
            floors[key] = float(value)
        elif key == "arch":
            wanted = value if isinstance(value, list) else [value]
            if "amd64" not in wanted:
                raise SpsError("unsupported", "run.cloud sandboxes are amd64")
        else:
            raise SpsError("unsupported", f"run.cloud cannot meet resources.{key}")
    if floors["disk_min_mib"] > 40960:
        raise SpsError("unsupported", "run.cloud sandboxes have a 40 GiB disk")
    if not floors["cpu_min"] and not floors["memory_min_mib"]:
        return DEFAULT_SIZE
    for size in SIZES:
        if size.cpu >= floors["cpu_min"] and size.memory_mib >= floors["memory_min_mib"]:
            return size
    raise SpsError("unsupported", "No run.cloud size meets the request")


def _plan(request: CreateRequest) -> dict[str, Any]:
    if request.sandbox_class != CLASS:
        raise SpsError("unsupported", f"run.cloud has no class {request.sandbox_class!r}")
    if request.snapshot is not None:
        raise SpsError("unsupported", "This adapter does not claim snapshots")
    if request.secrets:
        raise SpsError("unsupported", "This adapter does not claim hidden_secrets")
    if request.requirements:
        gate = next(iter(request.requirements))
        raise SpsError("unsupported", f"run.cloud does not claim the section {gate!r}")
    if request.image not in (None, IMAGE):
        raise SpsError("unsupported", f"run.cloud offers only the {IMAGE!r} image here")
    if request.at_end != "destroyed":
        raise SpsError("unsupported", "A run.cloud sandbox ends destroyed")
    size = _pick_size(request.resources)
    lifetime = request.lifetime_seconds
    if lifetime is not None:
        if isinstance(lifetime, bool) or not isinstance(lifetime, int) or lifetime < 1:
            raise SpsError("invalid_request", "lifetime_seconds must be an integer >= 1")
        if lifetime > MAX_LIFETIME_S:
            raise SpsError("unsupported", f"lifetime is capped at {MAX_LIFETIME_S}s")
    body: dict[str, Any] = {
        "image": IMAGE,
        "cpu": size.cpu,
        "memory": size.memory_mib,
        "idlePauseSeconds": 0,
        "timeoutSeconds": lifetime or 0,
        "tags": dict(request.labels),
        "env": dict(request.env),
        "wait": False,
    }
    if lifetime:
        body["onTimeout"] = "destroy"
    return body


class _Utf8:
    """Decode a byte stream read in pieces, holding back a character cut at a piece's end."""

    @staticmethod
    def decode(data: bytes, final: bool) -> tuple[str, int]:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        text = decoder.decode(data, final=final)
        held = len(decoder.getstate()[0])
        return text, len(data) - held


class RuncloudProvider(Provider):
    """A 1.0 provider over the run.cloud sandbox API."""

    def __init__(
        self,
        credentials: Mapping[str, str],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(credentials)
        endpoint = (self.credentials.get("endpoint") or DEFAULT_ENDPOINT).rstrip("/")
        parts = urlsplit(endpoint)
        if parts.scheme != "https" or parts.hostname not in self.api_domains:
            raise SpsError(
                "invalid_request", "endpoint must be an https URL on a run.cloud API host"
            )
        self._http = httpx.AsyncClient(
            base_url=endpoint,
            headers={
                "Authorization": f"Bearer {self.credentials['api_key']}",
                "User-Agent": "sps-provider-runcloud",
            },
            transport=transport,
            timeout=httpx.Timeout(60.0, connect=10.0),
        )

    # -- transport ------------------------------------------------------------

    @staticmethod
    def _error(resp: httpx.Response) -> SpsError:
        try:
            body = resp.json()
        except ValueError:
            body = None
        message = ""
        code_name = ""
        if isinstance(body, dict):
            error = body.get("error")
            if isinstance(error, dict):
                message = str(error.get("message") or "")
                code_name = str(error.get("code") or "")
            else:
                message = str(error or body.get("message") or "")
                code_name = str(body.get("code") or "")
        status = resp.status_code
        message = (message or f"run.cloud answered HTTP {status}")[:1000]
        retry = resp.headers.get("retry-after")
        retry_s = int(retry) if retry and retry.isdigit() else None
        if status == 409 and code_name == "sandbox_capacity_unavailable":
            return SpsError("unavailable", message, outcome="failed", retry_after_seconds=retry_s)
        if status == 409 and re.search(r"\bdestroy(ing|ed)\b", message):
            return SpsError("not_found", message)
        table = {
            400: "invalid_request",
            401: "unauthenticated",
            402: "quota_exceeded",
            403: "forbidden",
            404: "not_found",
            409: "conflict",
            413: "too_large",
            422: "unsupported",
        }
        if status in table:
            return SpsError(table[status], message)
        if status == 429:
            code = "rate_limited" if "rate" in message.lower() else "quota_exceeded"
            return SpsError(code, message, retry_after_seconds=retry_s)
        if status == 504:
            return SpsError("timeout", message, outcome="unknown")
        if status in (502, 503):
            return SpsError("unavailable", message, outcome="unknown", retry_after_seconds=retry_s)
        return SpsError("provider_error", message, outcome="unknown")

    async def _req(
        self, method: str, path: str, *, ok: tuple[int, ...] = (), **kwargs: Any
    ) -> httpx.Response:
        try:
            resp = await self._http.request(method, path, **kwargs)
        except httpx.TimeoutException:
            raise SpsError("timeout", "run.cloud did not answer", outcome="unknown") from None
        except httpx.HTTPError:
            raise SpsError("unavailable", "run.cloud is unreachable", outcome="unknown") from None
        if resp.status_code >= 400 and resp.status_code not in ok:
            raise self._error(resp)
        return resp

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = await self._req(method, path, **kwargs)
        return resp.json() if resp.content else None

    @staticmethod
    def _path(sandbox_id: str, suffix: str = "") -> str:
        return f"{BASE}/{quote(sandbox_id, safe='')}{suffix}"

    async def _sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return await self._json("GET", self._path(sandbox_id))

    def _to_sandbox(self, raw: Mapping[str, Any]) -> Sandbox:
        state = raw.get("state") or ""
        mapped = "destroyed" if state in ENDED or state not in _STATES else _STATES[state]
        created = _time(raw["created_at"])
        timeout = int(raw.get("timeout_seconds") or 0)
        ends = created + timedelta(seconds=timeout) if timeout > 0 else None
        if ends is None:
            at_end = None
        else:
            at_end = "destroyed" if raw.get("on_timeout") == "destroy" else "paused"
        times = [ProviderTime("created", created, "created_at")]
        if raw.get("destroyed_at"):
            times.append(ProviderTime("ended", _time(raw["destroyed_at"]), "destroyed_at"))
        if ends is not None:
            times.append(ProviderTime("deadline", ends, "timeout_seconds"))
        disk = raw.get("disk_mb")
        return Sandbox(
            id=raw["id"],
            sandbox_class=CLASS,
            state=mapped,
            size=Size(
                cpu=int(raw.get("milli_cpu") or 0) / 1000,
                memory_mib=int(raw.get("mem_mb") or 0),
                disk_mib=int(disk) if disk else None,
            ),
            region=raw.get("region") or None,
            image=raw.get("image") or None,
            labels=dict(raw.get("tags") or {}),
            created_at=created,
            ends_at=ends,
            at_end=at_end,
            times=tuple(times),
        )

    # -- the supervisor -------------------------------------------------------

    async def _exec(self, sandbox_id: str, argv: list[str], timeout_s: int) -> dict[str, Any]:
        """Exec one supervisor call and return the JSON document it printed."""
        body = {"cmd": ["python3", "-c", SUPERVISOR, *argv], "timeout_seconds": timeout_s}
        res = await self._json(
            "POST",
            self._path(sandbox_id, "/exec"),
            json=body,
            timeout=httpx.Timeout(timeout_s + 30.0, connect=10.0),
        )
        try:
            doc = json.loads(res["stdout"])
        except (TypeError, KeyError, ValueError):
            detail = str((res or {}).get("stderr", ""))[-300:] if isinstance(res, dict) else ""
            raise SpsError(
                "provider_error",
                f"The in-sandbox supervisor failed: {detail}".strip(),
                outcome="unknown",
            ) from None
        if not isinstance(doc, dict):
            raise SpsError("provider_error", "The in-sandbox supervisor failed", outcome="unknown")
        return doc

    async def _start(
        self,
        sandbox_id: str,
        directory: str,
        spec: Mapping[str, Any],
        wait_s: float,
    ) -> dict[str, Any]:
        doc = await self._exec(
            sandbox_id,
            ["start", directory, json.dumps(spec), str(wait_s), str(IO_CHUNK)],
            int(wait_s) + 30,
        )
        if doc.get("error") == "cwd":
            raise SpsError("not_found", f"No such directory: {spec['cwd']}")
        if doc.get("error"):
            raise SpsError("provider_error", "Could not start the command", outcome="unknown")
        return doc

    async def _poll(
        self,
        sandbox_id: str,
        directory: str,
        wait_s: float,
        offsets: tuple[int, int],
        caps: tuple[int, int],
        remove: bool,
    ) -> dict[str, Any]:
        argv = [
            "poll",
            directory,
            str(wait_s),
            str(offsets[0]),
            str(offsets[1]),
            str(caps[0]),
            str(caps[1]),
            str(IO_CHUNK),
            "1" if remove else "0",
        ]
        return await self._exec(sandbox_id, argv, int(wait_s) + 30)

    @staticmethod
    def _spec(argv: Any, cwd: str | None, env: Mapping[str, str], timeout: float | None) -> dict:
        if not argv or not all(isinstance(a, str) for a in argv):
            raise SpsError("invalid_request", "argv must be a non-empty list of strings")
        if cwd is not None and not cwd.startswith("/"):
            raise SpsError("invalid_request", "cwd must be an absolute path")
        return {"argv": list(argv), "cwd": cwd, "env": dict(env), "timeout": timeout}

    # -- operations -----------------------------------------------------------

    @classmethod
    def static_discovery(cls) -> Discovery:
        return build_discovery(provider_id=cls.provider_id, provider_name=cls.name)

    async def discovery(self) -> Discovery:
        # No run.cloud call returns these facts; listing one sandbox proves the key.
        await self._req("GET", BASE, params={"limit": 1})
        return build_discovery(provider_id=self.provider_id, provider_name=self.name)

    async def create(self, request: CreateRequest, *, idempotency_key: str) -> Sandbox:
        body = _plan(request)
        body["name"] = _fingerprint(request, body)
        resp = await self._req(
            "POST", BASE, json=body, headers={"Idempotency-Key": idempotency_key}
        )
        raw = resp.json()
        if raw.get("name") not in (None, body["name"]):
            raise SpsError("conflict", "Idempotency-Key was used with a different body")
        replay = resp.status_code == 200
        sandbox_id = raw["id"]
        deadline = asyncio.get_running_loop().time() + START_TIMEOUT_S
        delay = 0.25
        while True:
            raw = await self._sandbox(sandbox_id)
            if raw.get("name") not in (None, body["name"]):
                raise SpsError("conflict", "Idempotency-Key was used with a different body")
            state = raw.get("state")
            if state == "running":
                return self._to_sandbox(raw)
            if replay and state in ("pending", "starting"):
                raise SpsError(
                    "conflict",
                    "The first create with this key is in progress",
                    retry_after_seconds=2,
                )
            if replay:
                return self._to_sandbox(raw)  # the first sandbox, in whatever state it is now
            if state not in ("pending", "starting"):
                reason = str(raw.get("last_error") or raw.get("lastError") or state)[:500]
                await self._req("DELETE", self._path(sandbox_id), ok=(404,))
                if "capacity" in reason.lower():
                    raise SpsError("unavailable", f"run.cloud has no capacity: {reason}")
                raise SpsError(
                    "provider_error", f"The sandbox did not start: {reason}", outcome="failed"
                )
            if asyncio.get_running_loop().time() >= deadline:
                await self._req("DELETE", self._path(sandbox_id), ok=(404,))
                raise SpsError("timeout", "The sandbox did not start in time", outcome="failed")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 1.0)

    async def get(self, sandbox_id: str) -> Sandbox:
        return self._to_sandbox(await self._sandbox(sandbox_id))

    async def list(
        self,
        *,
        labels: Mapping[str, str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SandboxPage:
        if not 1 <= limit <= LIST_LIMIT:
            raise SpsError("invalid_request", f"limit must be 1 to {LIST_LIMIT}")
        params: list[tuple[str, str]] = [("tag", f"{k}:{v}") for k, v in (labels or {}).items()]
        params.append(("limit", str(limit)))
        if cursor:
            params.append(("cursor", cursor))
        page = await self._json("GET", BASE, params=params)
        boxes = [
            self._to_sandbox(item)
            for item in page.get("items") or []
            if item.get("state") in NOT_ENDED and item.get("created_at")
        ]
        return SandboxPage(sandboxes=boxes, next_cursor=page.get("nextCursor") or None)

    async def destroy(self, sandbox_id: str) -> DestroyResult:
        # DELETE answers 202 while the sandbox is ending and 204 once it has ended.
        await self._req("DELETE", self._path(sandbox_id))
        return DestroyResult(sandbox_id=sandbox_id, state="destroyed")

    async def run(self, sandbox_id: str, request: RunRequest) -> RunResult:
        if request.user is not None:
            raise SpsError("unsupported", "This adapter does not claim user_selection")
        if request.output != "text":
            raise SpsError("unsupported", "This adapter does not claim run.base64")
        limit = request.output_limit_bytes
        if not 0 <= limit <= LIMITS.output_bytes:
            raise SpsError("invalid_request", f"output_limit_bytes over {LIMITS.output_bytes}")
        timeout_ms = request.timeout_ms
        if not 1 <= timeout_ms <= MAX_RUN_S * 1000:
            raise SpsError("invalid_request", f"timeout_ms must be 1 to {MAX_RUN_S * 1000}")
        spec = self._spec(request.argv, request.cwd, request.env, timeout_ms / 1000)
        directory = f"{ROOT}/r/{secrets.token_hex(12)}"
        caps = (limit + 1, limit + 1)  # one byte over the limit shows that it was cut
        wait = min(timeout_ms / 1000 + 2, POLL_S)
        doc = await self._start(sandbox_id, directory, {**spec, "caps": caps, "remove": True}, wait)
        out = bytearray(base64.b64decode(doc["out"]))
        err = bytearray(base64.b64decode(doc["err"]))
        while True:
            done = doc["done"]
            out_end = min(doc["out_size"], caps[0])
            err_end = min(doc["err_size"], caps[1])
            if done and len(out) >= out_end and len(err) >= err_end:
                break
            doc = await self._poll(
                sandbox_id, directory, 0 if done else POLL_S, (len(out), len(err)), caps, True
            )
            if doc.get("error"):
                raise SpsError("provider_error", "The command's record is gone", outcome="unknown")
            out += base64.b64decode(doc["out"])
            err += base64.b64decode(doc["err"])
        exit_doc = doc["exit"]
        timed_out = bool(exit_doc["timed_out"])
        return RunResult(
            exit_code=None if timed_out else int(exit_doc["code"]),
            stdout=bytes(out[:limit]).decode("utf-8", "replace"),
            stderr=bytes(err[:limit]).decode("utf-8", "replace"),
            stdout_truncated=len(out) > limit,
            stderr_truncated=len(err) > limit,
            timed_out=timed_out,
        )

    async def files_read(self, sandbox_id: str, path: str) -> FileStream:
        _check_path(path)
        first = await self._read_at(sandbox_id, path, 0)
        data = base64.b64decode(first.get("content") or "")
        if first.get("size") is not None:
            size = int(first["size"])
        elif first.get("eof") is True or first.get("next_offset") is None:
            size = len(data)
        else:
            raise SpsError("provider_error", "run.cloud did not state the size", outcome="unknown")

        async def chunks() -> AsyncIterator[bytes]:
            # Later chunks are read FS_WINDOW at a time and yielded in order.
            if data:
                yield data
            offsets = list(range(len(data), size, FS_CHUNK)) if data else []
            reads: list[asyncio.Task[dict[str, Any]]] = []
            try:
                for offset in offsets[:FS_WINDOW]:
                    reads.append(asyncio.ensure_future(self._read_at(sandbox_id, path, offset)))
                queued = len(reads)
                for index, offset in enumerate(offsets):
                    page = await reads[index]
                    if queued < len(offsets):
                        reads.append(
                            asyncio.ensure_future(self._read_at(sandbox_id, path, offsets[queued]))
                        )
                        queued += 1
                    piece = base64.b64decode(page.get("content") or "")
                    want = min(FS_CHUNK, size - offset)
                    if len(piece) != want:
                        raise SpsError(
                            "provider_error", "The file changed during the read", outcome="unknown"
                        )
                    yield piece
            finally:
                for task in reads:
                    task.cancel()

        return FileStream(size=size, chunks=chunks())

    async def _read_at(self, sandbox_id: str, path: str, offset: int) -> dict[str, Any]:
        body = {"op": "read", "path": path, "offset": offset, "length": FS_CHUNK}
        return await self._fs(sandbox_id, body)

    async def files_write(
        self,
        sandbox_id: str,
        path: str,
        data: AsyncIterable[bytes],
        *,
        size: int,
        user: str | None = None,
    ) -> FileInfo:
        if user is not None:
            raise SpsError("unsupported", "This adapter does not claim user_selection")
        _check_path(path)
        if size > LIMITS.file_bytes:
            raise SpsError("too_large", f"The file is over {LIMITS.file_bytes} bytes")
        # The first chunk truncates the file and goes alone; later chunks write at their own
        # offsets, FS_WINDOW at a time.
        received = 0
        pending = bytearray()
        writes: set[asyncio.Task[None]] = set()
        first_sent = False

        async def send(offset: int, piece: bytes) -> None:
            body = {
                "op": "write",
                "path": path,
                "offset": offset,
                "content": base64.b64encode(piece).decode(),
                "truncate": offset == 0,
            }
            page = await self._fs(sandbox_id, body)
            if page.get("next_offset") != offset + len(piece):
                raise SpsError(
                    "provider_error", "A file chunk was not acknowledged", outcome="unknown"
                )

        async def dispatch(offset: int, piece: bytes) -> None:
            nonlocal first_sent
            if not first_sent:
                await send(offset, piece)
                first_sent = True
                return
            while len(writes) >= FS_WINDOW:
                done, _ = await asyncio.wait(writes, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    writes.discard(task)
                    task.result()
            writes.add(asyncio.ensure_future(send(offset, piece)))

        try:
            async for part in data:
                pending += part
                if received + len(pending) > size:
                    raise SpsError(
                        "invalid_request", "The body is longer than its size", outcome="unknown"
                    )
                while len(pending) >= FS_CHUNK:
                    await dispatch(received, bytes(pending[:FS_CHUNK]))
                    received += FS_CHUNK
                    del pending[:FS_CHUNK]
            if received + len(pending) != size:
                raise SpsError(
                    "invalid_request", "The body is shorter than its size", outcome="unknown"
                )
            if pending or not first_sent:
                await dispatch(received, bytes(pending))
            if writes:
                await asyncio.gather(*writes)
        finally:
            for task in writes:
                task.cancel()
        return FileInfo(path=path, size=size)

    async def _fs(self, sandbox_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        page = await self._json("POST", self._path(sandbox_id, "/fs"), json=body)
        error = page.get("error") if isinstance(page, dict) else "invalid response"
        if not error:
            return page
        text = str(error)
        if "no such file or directory" in text:
            raise SpsError("not_found", f"No such file: {body['path']}")
        if "is a directory" in text:
            raise SpsError("invalid_request", f"{body['path']} is a directory")
        if "not a directory" in text:
            raise SpsError("not_found", f"No such file: {body['path']}")
        if "permission denied" in text:
            raise SpsError("forbidden", f"Permission denied: {body['path']}")
        raise SpsError("provider_error", "The file operation failed", outcome="unknown")

    async def process_start(self, sandbox_id: str, request: ProcessStartRequest) -> Process:
        if request.stdin:
            raise SpsError("unsupported", "This adapter does not claim processes.input")
        if request.user is not None:
            raise SpsError("unsupported", "This adapter does not claim user_selection")
        spec = self._spec(request.argv, request.cwd, request.env, None)
        process_id = "p" + secrets.token_hex(12)
        never = 1 << 62
        await self._start(
            sandbox_id,
            f"{ROOT}/p/{process_id}",
            {**spec, "caps": [never, never], "remove": False},
            0,
        )
        return Process(id=process_id, sandbox_id=sandbox_id, started_at=datetime.now(UTC))

    async def process_output(
        self, sandbox_id: str, process_id: str, *, cursor: str | None = None, wait_ms: int = 0
    ) -> OutputPage:
        if not _PROCESS_ID.match(process_id):
            raise SpsError("not_found", f"No process {process_id}")
        if not 0 <= wait_ms <= MAX_WAIT_S * 1000:
            raise SpsError("invalid_request", f"wait_ms must be 0 to {MAX_WAIT_S * 1000}")
        offsets = (0, 0)
        if cursor:
            try:
                out_s, err_s = cursor.split(".")
                offsets = (int(out_s), int(err_s))
            except ValueError:
                raise SpsError(
                    "invalid_request", "The cursor is not one this adapter made"
                ) from None
            if min(offsets) < 0:
                raise SpsError("invalid_request", "The cursor is not one this adapter made")
        never = 1 << 62
        doc = await self._poll(
            sandbox_id, f"{ROOT}/p/{process_id}", wait_ms / 1000, offsets, (never, never), False
        )
        if doc.get("error"):
            raise SpsError("not_found", f"No process {process_id}")
        done = bool(doc["done"])
        chunks: list[OutputChunk] = []
        ends: list[int] = []
        closed: list[bool] = []
        for stream, key, start in (("stdout", "out", offsets[0]), ("stderr", "err", offsets[1])):
            raw = base64.b64decode(doc[key])
            at_end = start + len(raw) >= doc[f"{key}_size"]
            text, used = _Utf8.decode(raw, final=done and at_end)
            if text:
                chunks.append(OutputChunk(stream, text))
            ends.append(start + used)
            closed.append(done and at_end)
        return OutputPage(
            chunks=chunks,
            next_cursor=f"{ends[0]}.{ends[1]}",
            gap=False,
            stdout_closed=closed[0],
            stderr_closed=closed[1],
            state="exited" if done else "running",
            exit_code=int(doc["exit"]["code"]) if done else None,
        )

    async def cost(self, sandbox_id: str, *, created_at: datetime | None = None) -> Cost:
        # `GET .../cost` states the billed amounts per meter (what run.cloud invoices), final once
        # the destroyed sandbox is fully metered and readable for 30 days after. A deployment
        # without that endpoint answers 404 there, and the usage estimate below stands in.
        resp = await self._req("GET", self._path(sandbox_id, "/cost"), ok=(404,))
        if resp.status_code == 404:
            return await self._usage_cost(sandbox_id)
        doc = resp.json()
        final = doc["status"] == "final"
        start = _time(doc["startedAt"])
        end = _time(doc["endedAt"]) if final and doc.get("endedAt") else None
        lines = tuple(
            CostLine(
                "memory" if line["meter"] == "memory" else "cpu",
                Decimal(line["nanodollars"]) / NANODOLLARS_PER_USD,
                Decimal(line["quantity"]),
                line["unit"],
                start,
                end,
            )
            for line in doc["lines"]
        )
        return Cost(
            sandbox_id=sandbox_id,
            status="final" if final else "accruing",
            total_usd=Decimal(doc["totalUsd"]),
            source="reported",
            lines=lines,
            as_of=_time(doc["asOf"]),
        )

    async def _usage_cost(self, sandbox_id: str) -> Cost:
        # Billed seconds come from run.cloud's usage ledger; the reservation, the CPU counter and
        # the end time from the sandbox. `as_of` is run.cloud's clock (the response's Date).
        raw = await self._sandbox(sandbox_id)
        resp = await self._req("GET", self._path(sandbox_id, "/usage"))
        usage = resp.json()
        date = resp.headers.get("date")
        as_of = parsedate_to_datetime(date).astimezone(UTC) if date else datetime.now(UTC)
        seconds = Decimal(str(usage.get("billedSeconds") or 0))
        reserved_vcpu = Decimal(int(raw.get("milli_cpu") or 0)) / 1000
        used_vcpu_s = Decimal(int(raw.get("cpu_used_usec") or 0)) / 1_000_000
        cpu_q = max(reserved_vcpu * seconds, used_vcpu_s)
        mem_q = Decimal(int(raw.get("mem_mb") or 0)) / 1024 * seconds
        burst_mib_s = Decimal(str(usage.get("memoryBurstMibSeconds") or 0))
        start = _time(raw["created_at"])
        ended = _time(raw["destroyed_at"]) if raw.get("destroyed_at") else None
        final = ended is not None and as_of >= ended + timedelta(seconds=COST_SETTLE_S)
        end = ended if final else None
        lines = [
            CostLine("cpu", None, cpu_q, "vcpu_second", start, end),
            CostLine("memory", None, mem_q, "gib_second", start, end),
        ]
        if burst_mib_s > 0:
            lines.append(CostLine("memory", None, burst_mib_s / 1024, "gib_second", start, end))
        return Cost(
            sandbox_id=sandbox_id,
            status="final" if final else "accruing",
            total_usd=None,
            source="usage",
            lines=tuple(lines),
            as_of=as_of,
        )

    async def aclose(self) -> None:
        await self._http.aclose()


def _check_path(path: str) -> None:
    if not path.startswith("/") or "\x00" in path or len(path.encode()) > 4096:
        raise SpsError("invalid_request", "path must be an absolute path")
    if any(len(part.encode()) > 255 for part in path.split("/")):
        raise SpsError("invalid_request", "A path component is over 255 bytes")


__all__ = ["RuncloudProvider"]
