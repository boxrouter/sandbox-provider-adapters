# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Blaxel as a 1.0 `Provider`, over Blaxel's HTTP API with httpx.

Two hosts are called. The control plane (`https://api.blaxel.ai/v0`) creates, reads, lists and
deletes sandboxes and reports their cost. Each sandbox has its own runtime host (`*.bl.run`, from the
sandbox's `metadata.url`) that runs commands and processes and moves files. Both take the API key and
the workspace the key reaches (`X-Blaxel-Authorization`, `X-Blaxel-Workspace`).

A sandbox's id is its Blaxel name, a 32-hex digest of the `Idempotency-Key`; the request's
fingerprint is the display name, so a replayed create finds the first sandbox, and a replay with
another body is a conflict. The adapter keeps one memo between calls: each sandbox's runtime URL,
which never changes for a name.
"""

from __future__ import annotations

import asyncio
import builtins
import posixpath
import shlex
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from contextlib import AsyncExitStack
from datetime import datetime, timedelta
from decimal import Decimal
from math import ceil
from typing import Any
from urllib.parse import quote, urlsplit
from uuid import uuid4

import httpx
from sandbox_provider_spec.v1 import (
    AccountCost,
    Cost,
    CostLine,
    CreateRequest,
    DestroyResult,
    DirectoryEntry,
    DirectoryPage,
    Discovery,
    FileInfo,
    FileStream,
    ImageDeleteResult,
    ImagePage,
    ImageRequest,
    OutputChunk,
    OutputPage,
    PathInfo,
    PreparedImage,
    PreviewAccess,
    PreviewLink,
    Process,
    ProcessStartRequest,
    Provider,
    RunRequest,
    RunResult,
    Sandbox,
    SandboxPage,
    Size,
    SpsError,
    Terminal,
    TerminalRequest,
    connections,
    waiting,
)
from sandbox_provider_spec.v1.timing import lookup
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus, WebSocketException

from sps_provider_blaxel import directories, dockerfile, guest, images, previews, wire
from sps_provider_blaxel.create_spec import fingerprint, plan_create, sandbox_name
from sps_provider_blaxel.facts import (
    BASE_IMAGE,
    COST_FINAL_WITHIN_S,
    FILE_BYTES,
    IMAGE,
    LIMITS,
    LIST_LIMIT,
    MAX_WAIT_S,
    build_discovery,
    cpu_of,
    region_id,
)
from sps_provider_blaxel.terminal import BlaxelTerminal, launcher, socket_query
from sps_provider_blaxel.times import times_of

ACCOUNT_WINDOW_S = 7 * 24 * 3600  # one metrics request covers a week
API = "https://api.blaxel.ai/v0"
API_VERSION = "2026-04-28"  # Blaxel-Version: list replies are `{data, meta}` pages
RUNTIME_SUFFIX = ".bl.run"
_POLL_S = 0.5
# A process output read with a wait reads again after 0.1 s, doubling to 2 s.
_OUTPUT_POLL_FIRST_S = 0.1
_OUTPUT_POLL_MAX_S = 2.0
# Blaxel's gateway cuts a waited `POST /process` near 120 s (504) while the named command runs on
# (live 2026-11-22, cloud#9466). A command still running then is read again 0.05 s after it was
# last seen running, doubling to 2 s, and to 5 s once it has run 300 s: the read that finds it ended
# comes at most about 1.7% of its run time late, and a follow costs 0.5, then 0.2, reads a second.
_FOLLOW_FIRST_S = 0.05
_FOLLOW_CAP_S = 2.0
_FOLLOW_LONG_CAP_S = 5.0
_FOLLOW_LONG_AFTER_S = 300
# The marker Blaxel puts at the start of a stream it has cut short (seen in its logs replies).
_TRUNCATED = "[... truncated"
# Blaxel's own `timeout` kills a command at its deadline; a command followed after the cut that is
# `killed` more than this before the deadline was not ended by its timeout.
_EARLY_KILL_S = 2.0
_DEPLOY_WAIT_S = 120
# A pause: `POST /standby?force=true` is answered 202 before the sandbox is in standby (live
# 2026-11-23: 2.8 to 5.5 s later), so the record's `state` is read again after 0.5 s, doubling to
# 1 s. The host ignores a request that comes in the first seconds of a sandbox's life (2 of 4 live
# at ages 1, 4, 8 and 11 s); such a sandbox still goes to standby on its own about 15 s after its
# last request, which live took up to 22 s. So the wait is long enough for that, and no request
# is sent twice.
_RESUMED_KEEP_S = 60  # how long a resume is remembered: Blaxel's `state` lags it by 3 to 5 s
_STANDBY_POLL_FIRST_S = 0.5
_STANDBY_POLL_CAP_S = 1.0
_STANDBY_WAIT_S = 60
# The host ignores a standby request within 5 s of the sandbox's previous one, so a request is sent
# once more when none has taken effect this long after the first (a standby takes 2.8 to 6.9 s).
_RESEND_AFTER_S = 8
# Blaxel's hourly metrics: a sandbox's cost is read from the hour it was created to the hour it
# ended, in windows of at most seven days. A figure counts as final this long after the end
# (`facts.COST_FINAL_WITHIN_S`): live through Cloud on 2026-11-23, every bucket of 40 sandboxes
# (lives of 3 to 150 s, 30 of them at once) was published 0 to 7 s after the destroy and never
# changed; the 2026-10-20 evidence saw it settle 45 to 113 s after the end.
_METRICS_WINDOW = timedelta(days=7)
_COST_SETTLE = timedelta(seconds=COST_FINAL_WITHIN_S)
_RETRIES = (0.25, 0.5)  # the pause before each retry of a read that hit an upstream 502
_EXTEND_MARGIN_S = (
    5  # an extension counts from the read of the sandbox, so it adds what the call takes
)
_TERMINAL_OPEN_S = 30
_DIRECTORY_TIMEOUT_MS = 60_000
_CWD_TIMEOUT_MS = 5_000


def _plain(path: str) -> str:
    """A sandbox path as the runtime's URL path. Blaxel reads a path with no leading slash as
    relative to its working directory, so an absolute one keeps its slash (`/filesystem//tmp/x`)."""
    return "/".join(
        s.replace(".", "%2E") if s in (".", "..") else quote(s, safe="") for s in path.split("/")
    )


def _page_cursor(meta: Any) -> str | None:
    cursor = meta.get("nextCursor") if isinstance(meta, dict) else None
    has_more = isinstance(meta, dict) and meta.get("hasMore") is True
    return cursor if has_more and isinstance(cursor, str) and cursor else None


def _accepted(error: SpsError) -> SpsError:
    """A failure after Blaxel accepted a standby request: the standby may still happen, so the
    outcome is unknown. A code that is a caller's fault cannot be unknown (a key refused on a
    later read is Blaxel's answer, not the caller's request), so it becomes a provider error."""
    if error.code == "not_found" or error.outcome == "unknown":
        return error
    try:
        return SpsError(
            error.code,
            error.message,
            outcome="unknown",
            retry_after_seconds=error.retry_after_seconds,
        )
    except ValueError:
        return SpsError(
            "provider_error",
            f"Blaxel failed a read ({error.code}) after it accepted the standby",
            outcome="unknown",
        )


class BlaxelProvider(Provider):
    """A 1.0 provider over Blaxel's sandbox API."""

    def __init__(
        self,
        credentials: Mapping[str, str],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(credentials)
        self._base_headers = {
            "X-Blaxel-Authorization": f"Bearer {self.credentials['api_key']}",
            "Blaxel-Version": API_VERSION,
        }
        timeout = httpx.Timeout(60.0, connect=10.0)
        self._control = connections.provider_client(
            base_url=API, headers=self._base_headers, transport=transport, timeout=timeout
        )
        self._runtime = connections.provider_client(transport=transport, timeout=timeout)
        self._workspace: str | None = None
        self._account: str | None = None
        self._lock = asyncio.Lock()
        self._urls: dict[str, str] = {}
        self._cwds: dict[str, str] = {}
        self._resolved: dict[str, str] = {}
        self._image_users: dict[str, str] = {}
        self._users: dict[str, str] = {}
        # Blaxel's own time (its `Date`) at each `POST /resume` this object sent. The record's
        # `state` still says STANDBY for 3 to 5 s after a resume (live 2026-11-23), so a STANDBY
        # record whose `lastUsedAt` is older than this time is the earlier standby's, not a pause.
        # An entry is dropped as soon as a read no longer needs it (see `_woken`) and never kept
        # past `_RESUMED_KEEP_S`: the lag it covers is 3 to 5 s.
        self._resumed: dict[str, tuple[datetime, float]] = {}

    # -- transport ------------------------------------------------------------

    async def _send(
        self, client: httpx.AsyncClient, method: str, url: str, **kw: Any
    ) -> httpx.Response:
        try:
            return await client.request(method, url, **kw)
        except httpx.TimeoutException:
            raise SpsError("timeout", "Blaxel did not answer", outcome="unknown") from None
        except httpx.HTTPError:
            raise SpsError("unavailable", "Blaxel is unreachable", outcome="unknown") from None

    async def _identity(self) -> dict[str, str]:
        """The headers of every call: the key and the one workspace it reaches."""
        async with self._lock:
            if self._workspace is None:
                resp = await self._send(self._control, "GET", "/workspaces")
                if resp.status_code >= 400:
                    raise wire.failure(resp, "the workspace list")
                found = [w.get("name") for w in resp.json() if isinstance(w, dict)]
                names = [n for n in found if isinstance(n, str) and n]
                if len(names) != 1:
                    raise SpsError(
                        "forbidden",
                        "This Blaxel key must reach exactly one workspace; use a key limited to one",
                    )
                self._workspace = names[0]
            workspace = self._workspace
        return {**self._base_headers, "X-Blaxel-Workspace": workspace}

    async def ctl_raw(self, method: str, path: str, **kw: Any) -> httpx.Response:
        """A control-plane call whose reply is the caller's to read, whatever its status."""
        return await self._send(self._control, method, path, headers=await self._identity(), **kw)

    async def ctl(self, method: str, path: str, what: str, **kw: Any) -> httpx.Response:
        """A control-plane call; a reply of 400 or more is the error for `what`."""
        resp = await self.ctl_raw(method, path, **kw)
        if resp.status_code >= 400:
            raise wire.failure(resp, what)
        return resp

    _ctl = ctl

    def _runtime_url(self, native: dict[str, Any]) -> str:
        """The sandbox's runtime URL, refused unless it is an HTTPS Blaxel host for this sandbox:
        the API key is sent to it."""
        name = native["metadata"]["name"]
        url = native["metadata"].get("url")
        parts = urlsplit(url) if isinstance(url, str) else None
        if (
            parts is None
            or parts.scheme != "https"
            or not parts.netloc.endswith(RUNTIME_SUFFIX)
            or not parts.netloc.startswith(f"sbx-{name}-")
            or parts.path not in ("", "/")
        ):
            raise SpsError("provider_error", "Blaxel returned no usable sandbox endpoint")
        self._urls[name] = f"https://{parts.netloc}"
        return self._urls[name]

    async def _base(self, sandbox_id: str) -> str:
        if sandbox_id not in self._urls:
            with lookup():  # the record is read only to find the sandbox's runtime address
                native = await self._native(sandbox_id)
            self._runtime_url(native)
        return self._urls[sandbox_id]

    async def _rt(
        self,
        sandbox_id: str,
        method: str,
        path: str,
        what: str,
        *,
        retry: bool = False,
        raw: bool = False,
        **kw: Any,
    ) -> httpx.Response:
        """A call to the sandbox's runtime. A read that hits Blaxel's transient upstream 502 is
        repeated; a sandbox that has ended answers 404. With `raw` an error reply is returned."""
        headers = {**await self._identity(), **kw.pop("headers", {})}
        url = await self._base(sandbox_id) + path
        for pause in (*_RETRIES, None):
            resp = await self._send(self._runtime, method, url, headers=headers, **kw)
            if resp.status_code != 502 or not retry or pause is None:
                break
            await waiting.pause(pause)
        if resp.status_code >= 400 and not raw:
            raise wire.failure(resp, what)
        return resp

    # -- sandboxes --------------------------------------------------------------

    async def _native(self, sandbox_id: str) -> dict[str, Any]:
        if sandbox_id.startswith(images.PREFIX):  # an image's record is no sandbox of the caller's
            raise SpsError("not_found", "The sandbox: not found")
        resp = await self._ctl("GET", f"/sandboxes/{quote(sandbox_id, safe='')}", "the sandbox")
        native: dict[str, Any] = resp.json()
        return native

    @staticmethod
    def _to_sandbox(native: dict[str, Any]) -> Sandbox:
        meta, spec = native["metadata"], native.get("spec") or {}
        runtime = spec.get("runtime") or {}
        memory = int(runtime.get("memory") or 0)
        created = wire.stamp(meta["createdAt"])
        ttl = wire.ttl_seconds(runtime.get("ttl"))
        image = runtime.get("image")
        return Sandbox(
            id=meta["name"],
            sandbox_class="default",
            state=wire.sandbox_state(native["status"], native.get("state")),
            size=Size(cpu=cpu_of(memory), memory_mib=memory),
            region=region_id(spec["region"]) if spec.get("region") else None,
            image=IMAGE if image == BASE_IMAGE else None,
            labels=dict(meta.get("labels") or {}),
            created_at=created,
            ends_at=created + timedelta(seconds=ttl) if ttl is not None else None,
            at_end="destroyed" if ttl is not None else None,
            times=times_of(native),
        )

    async def _deployed(self, native: dict[str, Any]) -> dict[str, Any]:
        """Wait for a sandbox that Blaxel is still deploying; a failed deploy is an error."""
        name = native["metadata"]["name"]
        deadline = asyncio.get_running_loop().time() + _DEPLOY_WAIT_S
        while native["status"] in ("DEPLOYING", "UPDATING"):
            if asyncio.get_running_loop().time() > deadline:
                raise SpsError(
                    "timeout", "Blaxel did not finish deploying the sandbox", outcome="unknown"
                )
            await waiting.pause(_POLL_S)
            native = await self._native(name)
        if native["status"] == "FAILED":
            raise SpsError(
                "provider_error", "Blaxel could not deploy the sandbox", outcome="unknown"
            )
        self._runtime_url(native)
        return native

    @classmethod
    def image_recipe_revision(cls) -> str:
        """The digest of the tail that `image.prepare` adds to every image (`dockerfile.py`), so a
        copy prepared with another `sandbox-api` is another copy."""
        return dockerfile.recipe_revision()

    @classmethod
    def static_discovery(cls) -> Discovery:
        """What every account's `discovery()` narrows from: no I/O, no credentials."""
        return build_discovery(None)  # every region Blaxel offers

    async def discovery(self) -> Discovery:
        # GET /locations lists the regions that this account can use.
        resp = await self._ctl("GET", "/locations", "the region list")
        regions = [r["region"] for r in resp.json() if isinstance(r, dict) and r.get("region")]
        return build_discovery(regions)

    async def create(self, request: CreateRequest, *, idempotency_key: str) -> Sandbox:
        # POST /sandboxes. The name is the key's digest, so a replay is Blaxel's own
        # SANDBOX_ALREADY_EXISTS (409), after which the first sandbox is read.
        artifact = (
            request.image.get("provider_artifact") if isinstance(request.image, Mapping) else None
        )
        image_id = artifact if isinstance(artifact, str) else None
        if isinstance(image_id, str) and image_id.startswith(images.PREFIX):
            remembered = image_id in self._resolved
            artifact = await images.resolve(self, image_id)  # a prepared image's id, to its tag
        else:
            remembered = False
        user = self._image_user(image_id, artifact)
        if user is not None:
            self._users[sandbox_name(idempotency_key)] = user
        body = plan_create(request, idempotency_key, artifact)
        headers = await self._identity()
        resp = await self._send(self._control, "POST", "/sandboxes", headers=headers, json=body)
        if resp.status_code == 409 and wire.error_code(resp) == "SANDBOX_ALREADY_EXISTS":
            native = await self._native(sandbox_name(idempotency_key))
            if native["metadata"].get("displayName") != fingerprint(request):
                raise SpsError("conflict", "Idempotency-Key was used with a different body")
            return self._to_sandbox(await self._deployed(native))
        if resp.status_code >= 400:
            if remembered and image_id is not None:
                # A tag this object remembered may be stale: forget it and read the image
                # again, so an image that went answers its own error instead of the create's.
                self._resolved.pop(image_id, None)
                await images.resolve(self, image_id)
            raise wire.failure(resp, "the create")
        return self._to_sandbox(await self._deployed(resp.json()))

    def _image_user(self, image_id: str | None, artifact: object) -> str | None:
        """The default exec user of the image a create names, when this object knows it.

        `sandbox-api` starts privileged — the fs daemon is root whatever the image's USER —
        so the files endpoints are usable only when the default exec user is root too. The
        declared base image's is a fact; a prepared image's is what `image_prepare` built in
        (`_image_users`); any other artifact's is baked into the image and unknown here.
        """
        if image_id is not None and image_id.startswith(images.PREFIX):
            return self._image_users.get(image_id)
        if artifact is None or artifact == BASE_IMAGE:
            return "root"
        if isinstance(artifact, str) and artifact.startswith("sandbox/" + images.PREFIX):
            return self._image_users.get(artifact.split("/", 1)[1].split(":", 1)[0])
        return None

    async def get(self, sandbox_id: str) -> Sandbox:
        return self._to_sandbox(self._woken(await self._native(sandbox_id)))

    def _woken(self, native: dict[str, Any]) -> dict[str, Any]:
        """The record of a sandbox this object resumed, without the standby that the resume ended:
        Blaxel's `state` lags the resume by seconds. Only a STANDBY record that Blaxel dates
        before the resume is read as running; a later standby (its `lastUsedAt` is later) and a
        record with no date are left as they are. The resume is forgotten once a read no longer
        needs it (the record is not that old standby any more) or after `_RESUMED_KEEP_S`."""
        name = native["metadata"]["name"]
        self._forget_old_resumes()
        found = self._resumed.get(name)
        if found is None:
            return native
        resumed = found[0]
        if native.get("state") == "STANDBY":
            used = wire.last_used(native)
            if used is not None and used < resumed:
                return {**native, "state": "RUNNING"}
        del self._resumed[name]
        return native

    def _forget_old_resumes(self) -> None:
        now = asyncio.get_running_loop().time()
        for name in [n for n, (_, at) in self._resumed.items() if now - at > _RESUMED_KEEP_S]:
            del self._resumed[name]

    async def list(
        self,
        *,
        labels: Mapping[str, str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SandboxPage:
        # GET /sandboxes pages by cursor and has no label filter, so labels are matched here. A
        # Blaxel page with no match is skipped, so a page is empty only at the last one.
        wanted = dict(labels or {})
        params: dict[str, Any] = {"limit": min(limit, LIST_LIMIT)}
        while True:
            if cursor:
                params["cursor"] = cursor
            page = (await self._ctl("GET", "/sandboxes", "the sandbox list", params=params)).json()
            cursor = _page_cursor(page.get("meta"))
            found = [
                self._to_sandbox(self._woken(item))
                for item in page.get("data") or []
                if item["status"] not in wire.ENDED
                and not images.is_record(item)
                and all(
                    (item["metadata"].get("labels") or {}).get(k) == v for k, v in wanted.items()
                )
            ]
            if found or cursor is None:
                return SandboxPage(sandboxes=found, next_cursor=cursor)

    async def destroy(self, sandbox_id: str) -> DestroyResult:
        # DELETE /sandboxes/{name}: Blaxel answers at once with the sandbox in DELETING, which
        # `get` reports as destroyed, and answers a repeat the same way.
        if sandbox_id.startswith(images.PREFIX):
            raise SpsError("not_found", "The sandbox: not found")
        await self._ctl("DELETE", f"/sandboxes/{quote(sandbox_id, safe='')}", "the destroy")
        return DestroyResult(sandbox_id=sandbox_id, state="destroyed")

    async def labels_update(self, sandbox_id: str, labels: Mapping[str, str]) -> Sandbox:
        # PUT /sandboxes/{name} replaces the whole sandbox, so the update reads it, sets each label
        # (an empty value removes the key) and sends it back; `displayName` (the create's
        # fingerprint) and every other field stay as read. Blaxel keeps the sandbox running.
        native = await self._native(sandbox_id)
        if native["status"] in wire.ENDED:
            raise SpsError("not_found", "The sandbox has ended")
        merged = dict(native["metadata"].get("labels") or {})
        for key, value in labels.items():
            if value == "":
                merged.pop(key, None)
            else:
                merged[key] = value
        if len(merged) > 32:
            raise SpsError("invalid_request", "A sandbox has at most 32 labels", field="labels")
        body = {
            "metadata": {
                k: v
                for k, v in native["metadata"].items()
                if k in ("name", "displayName", "labels")
            },
            "spec": native["spec"],
        }
        body["metadata"]["labels"] = merged
        resp = await self._ctl(
            "PUT", f"/sandboxes/{quote(sandbox_id, safe='')}", "the label update", json=body
        )
        return self._to_sandbox(resp.json())

    # -- commands ------------------------------------------------------------------

    async def _check_cwd(self, sandbox_id: str, cwd: str | None) -> None:
        """Blaxel starts a command in a missing directory as if it were there: say not_found."""
        if cwd is None:
            return
        resp = await self._rt(
            sandbox_id, "GET", f"/filesystem/{_plain(cwd)}", "the working directory", retry=True
        )
        try:
            listing = resp.json()  # a directory is a JSON listing; a file answers its own bytes
        except ValueError:
            listing = None
        if not (isinstance(listing, dict) and "files" in listing):
            raise SpsError("not_found", f"No such directory: {cwd}")

    @staticmethod
    def _command(
        argv: builtins.list[str], env: Mapping[str, str], cwd: str | None
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "command": shlex.join(argv),
            "name": f"sps-{uuid4().hex}",
            "keepAlive": True,
            "maxRestarts": 0,
            "restartOnFailure": False,
        }
        if env:
            body["env"] = dict(env)
        if cwd is not None:
            body["workingDir"] = cwd
        return body

    @staticmethod
    def _exit_code(info: dict[str, Any]) -> int:
        code = info.get("exitCode")
        # A process that Blaxel killed reports -1: SIGKILL, so 128 + 9.
        return code if isinstance(code, int) and code >= 0 else 137

    async def _process(self, sandbox_id: str, ident: str, what: str) -> dict[str, Any]:
        resp = await self._rt(
            sandbox_id, "GET", f"/process/{quote(ident, safe='')}", what, retry=True
        )
        info: dict[str, Any] = resp.json()
        return info

    async def _logs(self, sandbox_id: str, ident: str) -> dict[str, Any]:
        resp = await self._rt(
            sandbox_id,
            "GET",
            f"/process/{quote(ident, safe='')}/logs",
            "the process output",
            retry=True,
        )
        logs: dict[str, Any] = resp.json()
        return logs

    async def _output(
        self, sandbox_id: str, ident: str, info: dict[str, Any], seen: tuple[int, int] = (0, 0)
    ) -> dict[str, Any]:
        """A process's output so far: `stdout` and `stderr`. The process record carries both,
        running or ended (live 2026-11-22, cloud#9466: equal to the logs read), so the logs are read
        only when the record may not hold all of it: a stream missing or not text, a stream that
        starts with Blaxel's truncation marker, a stream shorter than what `seen` already returned,
        or an ended process whose record has no output at all (one read to confirm the empty end).
        """
        out, err = info.get("stdout"), info.get("stderr")
        if isinstance(out, str) and isinstance(err, str):
            ended = info.get("status") in wire.TERMINAL_PROCESS
            trusted = (
                not out.startswith(_TRUNCATED)
                and not err.startswith(_TRUNCATED)
                and len(out) >= seen[0]
                and len(err) >= seen[1]
                and not (ended and not out and not err)
            )
            if trusted:
                return {"stdout": out, "stderr": err}
        return await self._logs(sandbox_id, ident)

    async def _kill(self, sandbox_id: str, ident: str) -> None:
        try:
            await self._rt(
                sandbox_id, "DELETE", f"/process/{quote(ident, safe='')}/kill", "the kill"
            )
        except SpsError:
            pass  # the command's own timeout still applies

    async def execute(
        self,
        sandbox_id: str,
        argv: builtins.list[str],
        env: Mapping[str, str],
        timeout_ms: int,
        cwd: str | None = None,
        *,
        after_timeout: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        """Run one command to its end: Blaxel's process record, its logs and whether it timed out.
        A caller that has no use for them after a timeout passes `after_timeout=False`: both come
        back empty then, and the two reads that fetch them are not made.

        One POST /process with waitForCompletion, whatever the timeout, as Blaxel's SDK sends it:
        it answers with the exit status and the output once the command ends, and its `timeout`
        makes Blaxel kill the command and every process it started and answer 422. Blaxel's
        gateway cuts a waited request near 120 s (504) while the named command runs on under its
        own `timeout`, so only a command still running then is followed by its name (`_follow`).
        """
        body = self._command(argv, env, cwd)
        seconds = ceil(timeout_ms / 1000)
        name = body["name"]
        body.update(waitForCompletion=True, timeout=seconds)
        started = asyncio.get_running_loop().time()
        resp = await self._waited_post(sandbox_id, body, seconds)
        if isinstance(resp, SpsError):  # the cut: the command may run on
            info, timed_out = await self._follow(sandbox_id, name, started, seconds, resp)
            if timed_out and not after_timeout:
                return info, {}, True
            return info, await self._output(sandbox_id, name, info), timed_out
        timed_out = resp.status_code == 422 and "timed out" in resp.text
        if resp.status_code >= 400 and not timed_out:
            raise wire.failure(resp, "the command")
        if not timed_out:
            return resp.json(), resp.json(), False
        if not after_timeout:
            return {}, {}, True
        info = await self._process(sandbox_id, name, "the command")
        return info, await self._output(sandbox_id, name, info), True

    async def quick(self, sandbox_id: str, argv: builtins.list[str]) -> tuple[int, str]:
        """A short command of the adapter's own: its exit status and stderr."""
        info, logs, timed_out = await self.execute(
            sandbox_id, argv, {}, 60_000, after_timeout=False
        )
        if timed_out:
            raise SpsError("timeout", "A command of the adapter timed out", outcome="unknown")
        return self._exit_code(info), str(logs.get("stderr") or "")

    async def require_user(self, sandbox_id: str, user: str) -> None:
        """`not_found` unless the sandbox has the user (a name, or a numeric uid)."""
        guest.check_user(user)
        code, _ = await self.quick(sandbox_id, ["getent", "passwd", user])
        if code == 2:
            raise SpsError("not_found", "The sandbox has no such user")
        if code != 0:
            raise SpsError("provider_error", "The sandbox could not look the user up")

    async def run(self, sandbox_id: str, request: RunRequest) -> RunResult:
        # A plain text command with no user is Blaxel's own waited process (see `execute`); one with
        # a user, or one that asks for exact bytes, runs inside the wrapper of `guest.py`.
        limit = request.output_limit_bytes
        if not 0 <= limit <= LIMITS.output_bytes:
            raise SpsError("invalid_request", f"output_limit_bytes over {LIMITS.output_bytes}")
        if not 1 <= request.timeout_ms <= LIMITS.run_timeout_ms:
            raise SpsError("invalid_request", f"timeout_ms must be 1 to {LIMITS.run_timeout_ms}")
        if request.user is not None or request.output != "text":
            return await guest.run(self, sandbox_id, request)
        await self._check_cwd(sandbox_id, request.cwd)
        info, logs, timed_out = await self.execute(
            sandbox_id, builtins.list(request.argv), request.env, request.timeout_ms, request.cwd
        )
        out, err = logs["stdout"].encode(), logs["stderr"].encode()
        return RunResult(
            exit_code=None if timed_out else self._exit_code(info),
            stdout=out[:limit].decode("utf-8", "replace"),
            stderr=err[:limit].decode("utf-8", "replace"),
            stdout_truncated=len(out) > limit,
            stderr_truncated=len(err) > limit,
            timed_out=timed_out,
        )

    async def _runtime_post(self, sandbox_id: str, body: dict[str, Any]) -> httpx.Response:
        headers = await self._identity()
        url = await self._base(sandbox_id) + "/process"
        return await self._send(self._runtime, "POST", url, headers=headers, json=body)

    async def _waited_post(
        self, sandbox_id: str, body: dict[str, Any], seconds: int
    ) -> httpx.Response | SpsError:
        """The waited POST /process: Blaxel's answer, or, when the request was sent and no answer
        came (the gateway's 504 near 120 s, or the connection dropped or timed out while waiting),
        the error to raise if Blaxel does not know the command. Nothing is sent twice."""
        headers = await self._identity()
        url = await self._base(sandbox_id) + "/process"
        try:
            resp = await self._runtime.request(
                "POST",
                url,
                headers=headers,
                json=body,
                timeout=httpx.Timeout(seconds + 30, connect=10.0),
            )
        except (httpx.ReadTimeout, httpx.ReadError, httpx.RemoteProtocolError):
            return SpsError("timeout", "Blaxel did not answer", outcome="unknown")
        except httpx.TimeoutException:
            raise SpsError("timeout", "Blaxel did not answer", outcome="unknown") from None
        except httpx.HTTPError:
            raise SpsError("unavailable", "Blaxel is unreachable", outcome="unknown") from None
        return wire.failure(resp, "the command") if resp.status_code == 504 else resp

    async def _follow(
        self, sandbox_id: str, name: str, started: float, seconds: int, cut: SpsError
    ) -> tuple[dict[str, Any], bool]:
        """Follow a command whose waited request was cut to its end; kill it at its deadline,
        `seconds` after `started` (the loop's clock). The first read is at once, then 0.05 s after
        a running answer, doubling to 2 s, and to 5 s once the command has run 300 s; a pause
        never passes the deadline. A command Blaxel does not know was never started: `cut` is
        raised."""
        loop = asyncio.get_running_loop()
        deadline = started + seconds
        pause = _FOLLOW_FIRST_S
        try:
            info = await self._process(sandbox_id, name, "the command")
        except SpsError as error:
            if error.code == "not_found":
                raise cut from None
            raise
        while True:
            if info["status"] in wire.TERMINAL_PROCESS:
                if info["status"] == "killed" and loop.time() < deadline - _EARLY_KILL_S:
                    raise SpsError(
                        "provider_error",
                        "Blaxel ended the command before its timeout",
                        outcome="unknown",
                    )
                # Blaxel's own `timeout` can end it as `killed` just before our deadline.
                return info, info["status"] == "killed"
            if loop.time() >= deadline:
                await self._kill(sandbox_id, name)
                return await self._process(sandbox_id, name, "the command"), True
            await waiting.pause(min(pause, max(deadline - loop.time(), _FOLLOW_FIRST_S)))
            long_run = loop.time() - started >= _FOLLOW_LONG_AFTER_S
            pause = min(pause * 2, _FOLLOW_LONG_CAP_S if long_run else _FOLLOW_CAP_S)
            info = await self._process(sandbox_id, name, "the command")

    # -- files ------------------------------------------------------------------

    async def files_read(self, sandbox_id: str, path: str) -> FileStream:
        # GET /filesystem/{path}?download=true streams the bytes of a file as an attachment; a
        # directory answers its listing as JSON, with no attachment header.
        headers = await self._identity()
        url = await self._base(sandbox_id) + f"/filesystem/{_plain(path)}"
        stack = AsyncExitStack()
        try:
            resp = await stack.enter_async_context(
                self._runtime.stream(
                    "GET",
                    url,
                    params={"download": "true"},
                    headers={
                        **headers,
                        "Accept": "application/octet-stream",
                        "Accept-Encoding": "identity",
                    },
                    timeout=httpx.Timeout(None, connect=10.0),
                )
            )
            if resp.status_code >= 400:
                await resp.aread()
                raise wire.failure(resp, "the file")
            if not resp.headers.get("content-disposition", "").startswith("attachment"):
                raise SpsError("invalid_request", f"{path} is a directory")
        except httpx.TimeoutException:
            await stack.aclose()
            raise SpsError("timeout", "Blaxel did not answer", outcome="unknown") from None
        except httpx.HTTPError:
            await stack.aclose()
            raise SpsError("unavailable", "Blaxel is unreachable", outcome="unknown") from None
        except BaseException:
            await stack.aclose()
            raise
        length = resp.headers.get("content-length")
        size = int(length) if length and length.isdecimal() else None

        async def chunks() -> AsyncIterator[bytes]:
            received = 0
            try:
                async for part in resp.aiter_bytes(1 << 16):
                    received += len(part)
                    yield part
                if size is not None and received != size:
                    raise SpsError("provider_error", "The file read ended early", outcome="unknown")
            except httpx.HTTPError:
                raise SpsError(
                    "provider_error", "The file read was cut off", outcome="unknown"
                ) from None
            finally:
                await stack.aclose()

        return FileStream(size=size, chunks=chunks())

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
            return await guest.write_as(self, sandbox_id, path, data, size, user)
        if not 0 <= size <= FILE_BYTES:
            raise SpsError("too_large", f"A file is at most {FILE_BYTES} bytes")
        await self._upload(sandbox_id, path, data, size, "0644")
        return FileInfo(path=path, size=size)

    async def write_raw(
        self,
        sandbox_id: str,
        path: str,
        data: AsyncIterable[bytes],
        *,
        size: int,
        mode: str = "0644",
    ) -> FileInfo:
        """A file written as Blaxel's own user, with the permissions `mode` (an octal string)."""
        await self._upload(sandbox_id, path, data, size, mode)
        return FileInfo(path=path, size=size)

    async def _upload(
        self, sandbox_id: str, path: str, data: AsyncIterable[bytes], size: int, permissions: str
    ) -> None:
        # PUT /filesystem/{path} takes a multipart form (permissions, path, file) and creates the
        # missing parent directories. The body is streamed with its exact length.
        boundary = uuid4().hex
        fields = "".join(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'
            for name, value in (("permissions", permissions), ("path", path))
        )
        head = (
            f'{fields}--{boundary}\r\nContent-Disposition: form-data; name="file"; '
            'filename="content"\r\nContent-Type: application/octet-stream\r\n\r\n'
        ).encode()
        tail = f"\r\n--{boundary}--\r\n".encode()

        async def body() -> AsyncIterator[bytes]:
            yield head
            sent = 0
            async for chunk in data:
                sent += len(chunk)
                if sent > size:
                    break
                yield chunk
            if sent != size:
                raise SpsError(
                    "invalid_request",
                    "The stream's length differs from its declared size",
                    outcome="unknown",
                )
            yield tail

        headers = await self._identity()
        url = await self._base(sandbox_id) + f"/filesystem/{_plain(path)}"
        resp = await self._send(
            self._runtime,
            "PUT",
            url,
            headers={
                **headers,
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Content-Length": str(len(head) + size + len(tail)),
            },
            content=body(),
            timeout=httpx.Timeout(None, connect=10.0),
        )
        if resp.status_code >= 400:
            if "no space left" in resp.text:
                raise SpsError("too_large", "The sandbox's disk is full", outcome="unknown")
            raise wire.failure(resp, "the file write")

    # -- processes -----------------------------------------------------------------

    async def process_start(self, sandbox_id: str, request: ProcessStartRequest) -> Process:
        # POST /process without waitForCompletion starts a named process that runs until it ends.
        # With `stdin` the process keeps a pipe open for `process_write_stdin`; without it the
        # process reads an immediate end of file.
        argv = builtins.list(request.argv)
        if request.user is not None:
            await self.require_user(sandbox_id, request.user)
            argv = guest.switched(request.user, argv)
        await self._check_cwd(sandbox_id, request.cwd)
        body = self._command(argv, request.env, request.cwd)
        body["timeout"] = 0  # no limit: the sandbox's own lifetime ends it
        if request.stdin:
            body["stdin"] = True
        resp = await self._runtime_post(sandbox_id, body)
        if resp.status_code >= 400:
            raise wire.failure(resp, "the process start")
        started = wire.http_date(resp.json().get("startedAt")) or wire.response_time(resp)
        return Process(id=body["name"], sandbox_id=sandbox_id, started_at=started)

    async def process_output(
        self, sandbox_id: str, process_id: str, *, cursor: str | None = None, wait_ms: int = 0
    ) -> OutputPage:
        # GET /process/{name} gives the state and the output so far, one exchange a pass; the logs
        # are read only when the record may not hold all of it (`_output`). The cursor is
        # "<stdout characters>.<stderr characters>" already returned; a call with a `wait_ms`
        # repeats the reads until there is output, an exit or the wait is over.
        try:
            seen_out, seen_err = (int(p) for p in cursor.split(".")) if cursor else (0, 0)
        except ValueError:
            raise SpsError("invalid_request", "The cursor is not one this adapter gave") from None
        # The pause between reads doubles up to a cap, so a long wait costs a few calls.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + min(wait_ms / 1000, MAX_WAIT_S)
        pause = _OUTPUT_POLL_FIRST_S
        while True:
            info = await self._process(sandbox_id, process_id, "the process")
            logs = await self._output(sandbox_id, process_id, info, (seen_out, seen_err))
            out, err = logs["stdout"], logs["stderr"]
            exited = info["status"] in wire.TERMINAL_PROCESS
            fresh = len(out) > seen_out or len(err) > seen_err
            if fresh or exited or loop.time() >= deadline:
                break
            await waiting.pause(min(pause, max(deadline - loop.time(), 0)))
            pause = min(pause * 2, _OUTPUT_POLL_MAX_S)
        # A log that is shorter than what was read has lost its start.
        gap = len(out) < seen_out or len(err) < seen_err
        chunks = []
        if len(out) > seen_out:
            chunks.append(OutputChunk("stdout", out[seen_out:]))
        if len(err) > seen_err:
            chunks.append(OutputChunk("stderr", err[seen_err:]))
        return OutputPage(
            chunks=chunks,
            next_cursor=f"{len(out)}.{len(err)}",
            gap=gap,
            stdout_closed=exited,
            stderr_closed=exited,
            state="exited" if exited else "running",
            exit_code=self._exit_code(info) if exited else None,
        )

    async def process_write_stdin(self, sandbox_id: str, process_id: str, data: bytes) -> None:
        # POST /process/{name}/stdin takes the bytes as they are.
        await self._rt(
            sandbox_id,
            "POST",
            f"/process/{quote(process_id, safe='')}/stdin",
            "the stdin write",
            headers={"Content-Type": "application/octet-stream"},
            content=data,
        )

    async def process_close_stdin(self, sandbox_id: str, process_id: str) -> None:
        # DELETE /process/{name}/stdin; a repeat answers the same.
        await self._rt(
            sandbox_id, "DELETE", f"/process/{quote(process_id, safe='')}/stdin", "the stdin close"
        )

    async def process_cancel(self, sandbox_id: str, process_id: str) -> None:
        # DELETE /process/{name}/kill. A process that has ended answers 404 "is not running", and
        # a cancel of one that has ended is done.
        resp = await self._rt(
            sandbox_id,
            "DELETE",
            f"/process/{quote(process_id, safe='')}/kill",
            "the cancel",
            raw=True,
        )
        if resp.status_code == 404 and "is not running" in resp.text:
            return
        if resp.status_code >= 400:
            raise wire.failure(resp, "the cancel")

    # -- pause, resume -----------------------------------------------------------------------

    async def pause(self, sandbox_id: str) -> Sandbox:
        """Blaxel's standby, asked for: `POST /standby?force=true` on the sandbox's host (`force`
        puts it in standby although a keep-alive process holds it awake; the hold is taken back at
        the resume). Blaxel answers 202 before the standby: a 202 is not proof, so the record is
        read until its `state` is STANDBY (`status` stays DEPLOYED), within `_STANDBY_WAIT_S`.
        A STANDBY record that predates the request (the standby of before a resume the record
        still shows) is not the answer: `lastUsedAt` is the time of the last request to the
        host (a real standby carries the second of the request that caused it), so one dated
        before the request's own `Date` belongs to an earlier standby. The host ignores a request in the first seconds
        of a sandbox's life and within 5 s of the sandbox's previous request, and a request to a
        sandbox already in standby wakes it first (live 2026-11-23: one that holds a keep-alive
        process then stayed running). So if no standby is seen `_RESEND_AFTER_S` after the
        request, it is sent once more, past the host's quiet window; a sandbox with no such
        process still stands by on its own about 15 s after its last request, which live took
        up to 22 s. Whatever fails after the 202 is `unknown`: the standby was accepted."""
        resp = await self._standby(sandbox_id)
        asked = wire.http_date(resp.headers.get("date"))
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline, delay, resent = started + _STANDBY_WAIT_S, _STANDBY_POLL_FIRST_S, False
        while True:
            await waiting.pause(delay)  # a standby is never seen before 2 s: no read at once
            delay = min(delay * 2, _STANDBY_POLL_CAP_S)
            try:
                native = await self._native(sandbox_id)
            except SpsError as error:
                raise _accepted(error) from error
            if native["status"] in wire.ENDED:
                raise SpsError("not_found", "The sandbox has ended")
            if native.get("state") == "STANDBY":
                used = wire.last_used(native)
                # `>=`, not `>`: both times are whole seconds, and a real standby carries the
                # second of the request that caused it (live 2026-11-23: POST `Date` :56 and
                # `lastUsedAt` :56; :22 and :22), so `>` would refuse genuine standbys and end
                # every such pause in the 60 s timeout. What `>=` can wrongly accept is a first
                # standby inside the resend's own second while the record read RUNNING at the
                # resend: rare, and the resend comes at least 8 s after the first request.
                if asked is None or used is None or used >= asked:
                    self._resumed.pop(sandbox_id, None)
                    return self._to_sandbox(native)
            now = loop.time()
            if now + delay > deadline:
                raise SpsError(
                    "timeout", "Blaxel did not put the sandbox in standby", outcome="unknown"
                )
            if not resent and now - started >= _RESEND_AFTER_S:
                resent = True
                try:
                    again = await self._standby(sandbox_id)
                except SpsError as error:
                    raise _accepted(error) from error
                # The resend wakes a sandbox that had already gone to standby (a record that lags
                # still said RUNNING): a STANDBY dated before it is the first request's, and the
                # guest runs. Only the standby that follows the resend counts from here on.
                asked = wire.http_date(again.headers.get("date")) or asked

    async def _standby(self, sandbox_id: str) -> httpx.Response:
        return await self._host_call(sandbox_id, "/standby?force=true", "the pause")

    async def _host_call(
        self, sandbox_id: str, path: str, what: str, *, wakes: bool = False
    ) -> httpx.Response:
        """`POST path` on the sandbox's host, for the two calls that old images lack. A sandbox
        whose image carries a `sandbox-api` older than 0.2.78 has neither `/standby` nor
        `/resume` and answers 404 to both (production 2026-11-23: every sandbox made from a
        prepared image built with the pin of 0.2.74 did). A 404 from the host does not say whether
        the sandbox or the call is missing, so the record is read: a sandbox that has ended is
        `not_found`. For a sandbox that is there:
        - `/standby` did nothing: `unsupported` (failed).
        - `/resume` (`wakes`) is answered success: Blaxel's host wakes a sandbox on any request
          before the sandbox-api sees it, and waking is all that `/resume` asks, so the sandbox
          runs although the call answered 404 (it would be wrong to report that as a failure)."""
        resp = await self._rt(sandbox_id, "POST", path, what, raw=True)
        if resp.status_code == 404:
            native = await self._native(sandbox_id)
            if native["status"] in wire.ENDED:
                raise SpsError("not_found", "The sandbox has ended")
            if wakes:
                return resp
            raise SpsError(
                "unsupported",
                f"This sandbox's image has a sandbox-api that predates {what} (it answered 404; "
                "the call is in sandbox-api 0.2.78 and later): prepare the image again",
            )
        if resp.status_code == 501:
            raise SpsError("unsupported", f"Blaxel answered HTTP 501 to {what}")
        if resp.status_code >= 400:
            raise wire.failure(resp, what)
        return resp

    async def resume(self, sandbox_id: str) -> None:
        """`POST /resume` on the sandbox's host, one call: Blaxel answers 200 once the sandbox runs
        (0.1 s live), and the next command runs 0.1 s later. The record is `get`'s, which does not
        wait for the control plane's own `state` to catch up (see `_woken`). The guest was frozen,
        not restarted: its processes carry on, a keep-alive hold is taken back by Blaxel."""
        resp = await self._host_call(sandbox_id, "/resume", "the resume", wakes=True)
        resumed = wire.http_date(resp.headers.get("date"))
        self._forget_old_resumes()
        if resumed is not None:
            self._resumed[sandbox_id] = (resumed, asyncio.get_running_loop().time())

    # -- lifetime, previews, terminals, directories ---------------------------------------

    async def lifetime_extend(self, sandbox_id: str, seconds: int) -> Sandbox:
        # Blaxel's ttl is a maximum age from creation, and the record says how many seconds are
        # left (`expiresIn`), so the ttl to write is the age so far plus `seconds`. PUT
        # /sandboxes/{name} changes it in place, with no restart. An extension never shortens the
        # lifetime, and a sandbox with no ttl has no lifetime to extend.
        native = await self._native(sandbox_id)
        if native["status"] in wire.ENDED:
            raise SpsError("conflict", "The sandbox has ended")
        runtime = (native.get("spec") or {}).get("runtime") or {}
        ttl, left = wire.ttl_seconds(runtime.get("ttl")), native.get("expiresIn")
        if ttl is None or not isinstance(left, int) or left >= seconds:
            return self._to_sandbox(self._woken(native))
        body = {
            "metadata": {
                k: v
                for k, v in native["metadata"].items()
                if k in ("name", "displayName", "labels")
            },
            "spec": {
                **native["spec"],
                "runtime": {**runtime, "ttl": f"{ttl - left + seconds + _EXTEND_MARGIN_S}s"},
            },
        }
        resp = await self._ctl(
            "PUT", f"/sandboxes/{quote(sandbox_id, safe='')}", "the lifetime update", json=body
        )
        return self._to_sandbox(self._woken(resp.json()))

    async def terminal_open(self, sandbox_id: str, request: TerminalRequest) -> Terminal:
        # The program runs inside a launcher script that records its exit status (see terminal.py),
        # because Blaxel's socket closes with "Shell exited" and no status.
        await self._check_cwd(sandbox_id, request.cwd)
        ident = uuid4().hex
        stem = f"/tmp/.sps-terminal-{ident}"
        script, status, pidfile = f"{stem}.sh", f"{stem}.status", f"{stem}.pid"
        text = launcher(request.argv, request.env, request.term, status, pidfile)

        async def once() -> AsyncIterator[bytes]:
            yield text

        async def read_status() -> int | None:
            try:
                resp = await self._rt(sandbox_id, "GET", f"/filesystem/{_plain(status)}", "status")
                return int(resp.json()["content"].strip())
            except (SpsError, KeyError, ValueError, TypeError):
                return None

        async def cleanup(end_program: bool = False) -> None:
            if end_program:
                try:
                    await self._end_group(sandbox_id, pidfile)
                except (SpsError, KeyError, ValueError):
                    pass  # no pid was recorded: the program never started
            for path in (script, status, pidfile):
                try:
                    await self._rt(sandbox_id, "DELETE", f"/filesystem/{_plain(path)}", "cleanup")
                except SpsError:
                    pass  # the files are small and go with the sandbox

        await self._upload(sandbox_id, script, once(), len(text), "0755")
        host = urlsplit(await self._base(sandbox_id)).netloc
        url = f"wss://{host}/terminal/ws?" + socket_query(
            request.columns, request.rows, script, request.cwd
        )
        try:
            socket = await connect(
                url,
                additional_headers=await self._identity(),
                open_timeout=_TERMINAL_OPEN_S,
                max_size=1 << 20,
            )
        except InvalidStatus as error:
            await cleanup()
            raise wire.failure(httpx.Response(error.response.status_code), "the terminal") from None
        except (WebSocketException, OSError, TimeoutError):
            await cleanup()
            raise SpsError(
                "unavailable", "Blaxel's terminal is unreachable", outcome="unknown"
            ) from None
        return BlaxelTerminal(socket, read_status, cleanup)

    async def _end_group(self, sandbox_id: str, pidfile: str) -> None:
        """SIGHUP to the processes of the terminal's session, then SIGKILL to those left five seconds
        later, by a detached process so that the caller does not wait. The launcher's pid is the
        session id (field 4 of `/proc/<pid>/stat` after the command's name)."""
        resp = await self._rt(sandbox_id, "GET", f"/filesystem/{_plain(pidfile)}", "the pid")
        session = int(resp.json()["content"].strip())
        script = (
            f"sid={session}; pids=\n"
            "for d in /proc/[0-9]*; do\n"
            "  s=$(sed 's/^.*) //' \"$d/stat\" 2>/dev/null | cut -d' ' -f4)\n"
            '  [ "$s" = "$sid" ] && pids="$pids ${d#/proc/}"\n'
            "done\n"
            '[ -n "$pids" ] || exit 0\n'
            "kill -HUP $pids 2>/dev/null; sleep 5; kill -KILL $pids 2>/dev/null; exit 0\n"
        )
        body = self._command(["sh", "-c", script], {}, None)
        body["timeout"] = 30
        await self._runtime_post(sandbox_id, body)

    async def _script(
        self, sandbox_id: str, script: str, path: str, *, flag: bool = False
    ) -> RunResult:
        directories.check_path(path)
        result = await self.run(
            sandbox_id,
            RunRequest(
                argv=directories.argv(script, path, flag),
                timeout_ms=_DIRECTORY_TIMEOUT_MS,
                output_limit_bytes=LIMITS.output_bytes,
            ),
        )
        if result.timed_out:
            raise SpsError("timeout", "The directory operation timed out", outcome="unknown")
        return result

    async def _cwd(self, sandbox_id: str) -> str:
        """The working directory a command and the filesystem daemon share. It depends on the
        image and the user, so it is learned from the sandbox once and kept for its life."""
        cwd = self._cwds.get(sandbox_id)
        if cwd is not None:
            return cwd
        info, logs, timed_out = await self.execute(sandbox_id, ["pwd"], {}, _CWD_TIMEOUT_MS)
        out = str(logs.get("stdout") or "").strip()
        if timed_out or self._exit_code(info) != 0 or not out.startswith("/"):
            raise SpsError("provider_error", "The sandbox's working directory could not be read")
        self._cwds[sandbox_id] = out
        return out

    async def files_list(
        self, sandbox_id: str, path: str, *, cursor: str | None = None, limit: int = 100
    ) -> DirectoryPage:
        # GET /filesystem/{dir} lists the entries with a file's size and a mode but no size
        # for a directory, which one `stat` of the page's directories adds (see directories.py).
        # Blaxel refuses a `..` segment (400): `.` and `..` inside the path are normalized away
        # here, and a plain relative path goes to the daemon untouched, which resolves it
        # against the sandbox's working directory. A `..` that climbs out of the path's own
        # frame is resolved against that same directory, which one `pwd` learns.
        directories.check_path(path)
        where = posixpath.normpath(path)
        if where.split("/")[0] == "..":
            where = posixpath.normpath(posixpath.join(await self._cwd(sandbox_id), where))
        resp = await self._rt(
            sandbox_id, "GET", f"/filesystem/{_plain(where)}", "the listing", retry=True, raw=True
        )
        if resp.status_code >= 400:
            if "ermission denied" in resp.text:
                raise SpsError("forbidden", "the listing: permission denied")
            raise wire.failure(resp, "the listing")
        try:
            body = resp.json()
        except ValueError:
            body = None  # a file answers its own bytes
        page = directories.page(directories.listing(body), cursor, limit, LIST_LIMIT)
        dirs = [e.name for e in page.entries if e.type == "directory"]
        if not dirs:
            return page
        info, logs, timed_out = await self.execute(
            sandbox_id,
            [*directories.DIR_SIZES, *(posixpath.join(where, name) for name in dirs)],
            {},
            _DIRECTORY_TIMEOUT_MS,
        )
        sizes = str(logs.get("stdout") or "").split()
        if (
            timed_out
            or self._exit_code(info) != 0
            or len(sizes) != len(dirs)
            or not all(n.isdecimal() for n in sizes)
        ):
            raise SpsError("provider_error", "The listing could not read a directory's size")
        by_name = dict(zip(dirs, (int(n) for n in sizes), strict=True))
        entries = [
            DirectoryEntry(name=e.name, type=e.type, size=by_name[e.name])
            if e.type == "directory"
            else e
            for e in page.entries
        ]
        return DirectoryPage(entries=entries, next_cursor=page.next_cursor)

    async def files_mkdir(self, sandbox_id: str, path: str, *, parents: bool = False) -> None:
        # The fs daemon is root: the endpoint makes root-owned directories a non-root default
        # user could not write into. Its privilege must match the exec's, so the endpoint is
        # used only when the sandbox's default user is root (or unknown, which keeps the exec).
        if parents and self._users.get(sandbox_id) == "root":
            # PUT /filesystem/{dir} with isDirectory makes the directory and its missing
            # ancestors, as `mkdir -p`, and answers an existing directory the same way. The
            # plain mkdir keeps the in-sandbox script: the endpoint parents a missing ancestor
            # that must be a `not_found`.
            resp = await self._rt(
                sandbox_id,
                "PUT",
                f"/filesystem/{_plain(path)}",
                "the mkdir",
                raw=True,
                json={"isDirectory": True, "permissions": "0755"},
            )
            if resp.status_code >= 400:
                if "is a directory" in resp.text:
                    # The endpoint's own idempotency is unreliable: a repeat PUT on an
                    # existing directory answers 200 once and 422 "open <path>: is a
                    # directory" the next. `mkdir -p` answers that case successfully.
                    return
                if "xist" in resp.text or "ot a director" in resp.text:
                    raise SpsError("conflict", f"An entry that is not a directory is at {path}")
                if "ermission denied" in resp.text:
                    raise SpsError("forbidden", "the mkdir: permission denied")
                raise wire.failure(resp, "the mkdir")
            return
        result = await self._script(sandbox_id, directories.MKDIR, path, flag=parents)
        if result.exit_code != 0:
            raise directories.failure(result, "the mkdir")

    async def files_remove(self, sandbox_id: str, path: str, *, recursive: bool = False) -> None:
        if path and path.strip("/") == "":
            raise SpsError("invalid_request", "The root cannot be removed")
        if recursive and self._users.get(sandbox_id) == "root":
            # DELETE /filesystem/{p}?recursive removes a file, link or tree in one call. The
            # daemon is root: on an image whose default user is not, it would delete what the
            # exec's `rm -rf` could not, so the exec keeps that case. A removal without
            # `recursive` keeps the in-sandbox script: the endpoint does not say it left a
            # non-empty directory alone (a `conflict` that removes nothing).
            resp = await self._rt(
                sandbox_id,
                "DELETE",
                f"/filesystem/{_plain(path)}",
                "the removal",
                raw=True,
                params={"recursive": "true"},
            )
            if resp.status_code >= 400:
                lowered = resp.text.lower()
                if resp.status_code == 404 or "no such" in lowered or "not found" in lowered:
                    raise SpsError("not_found", f"No such file or directory: {path}")
                if "ermission denied" in resp.text:
                    raise SpsError("forbidden", "the removal: permission denied")
                raise wire.failure(resp, "the removal")
            return
        result = await self._script(sandbox_id, directories.REMOVE, path, flag=recursive)
        if result.exit_code != 0:
            raise directories.failure(result, "the removal")

    async def files_exists(self, sandbox_id: str, path: str) -> PathInfo:
        result = await self._script(sandbox_id, directories.EXISTS, path)
        if result.exit_code == directories.MISSING:
            return PathInfo(exists=False, type=None)
        if result.exit_code != 0:
            raise directories.failure(result, "the check")
        return PathInfo(exists=True, type=directories.entry_type(result.stdout.strip()))

    # -- images ---------------------------------------------------------------------

    async def image_prepare(
        self,
        request: ImageRequest,
        *,
        context: AsyncIterable[bytes] | None = None,
        context_size: int | None = None,
        idempotency_key: str,
    ) -> PreparedImage:
        return await images.prepare(self, request, context, context_size, idempotency_key)

    async def image_get(self, image_id: str) -> PreparedImage:
        return await images.get(self, image_id)

    async def image_list(
        self,
        *,
        labels: Mapping[str, str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> ImagePage:
        return await images.list_images(self, labels, cursor, limit)

    async def image_delete(self, image_id: str) -> ImageDeleteResult:
        return await images.delete(self, image_id)

    async def upload(self, url: str | None, archive: Any) -> None:
        """Send a build's zip to the presigned address that Blaxel answered with. It goes through a
        client with no Blaxel headers, so the key never reaches the storage host."""
        target = images.upload_target(url, type(self).api_domains)
        size = archive.seek(0, 2)
        archive.seek(0)

        async def chunks() -> AsyncIterator[bytes]:
            while chunk := archive.read(1 << 16):
                yield chunk

        resp = await self._send(
            self._runtime,
            "PUT",
            target,
            content=chunks(),
            headers={"Content-Type": "application/zip", "Content-Length": str(size)},
            timeout=httpx.Timeout(None, connect=10.0),
        )
        if resp.status_code >= 400:
            raise SpsError(
                "provider_error",
                f"Blaxel's upload host answered HTTP {resp.status_code}",
                outcome="unknown",
            )

    # -- previews -------------------------------------------------------------------

    async def preview_link(
        self,
        sandbox_id: str,
        port: int,
        *,
        access: PreviewAccess = "public",
        lifetime_seconds: int | None = None,
    ) -> PreviewLink:
        return await previews.link(
            self, sandbox_id, port, access=access, lifetime_seconds=lifetime_seconds
        )

    async def preview_revoke(self, sandbox_id: str, preview_id: str) -> None:
        await previews.revoke(self, sandbox_id, preview_id)

    # -- cost ------------------------------------------------------------------------

    async def _account_id(self) -> str:
        if self._account is None:
            accounts = (await self._ctl("GET", "/accounts", "the account")).json()
            ids = [a.get("metadata", {}).get("id") for a in accounts if isinstance(a, dict)]
            if len(ids) != 1 or not isinstance(ids[0], str):
                raise SpsError("forbidden", "This Blaxel key must reach exactly one account")
            self._account = ids[0]
        return self._account

    async def _metrics(
        self, account: str, name: str, start: datetime, end: datetime
    ) -> tuple[builtins.list[dict[str, Any]], datetime]:
        """The hourly cost buckets of one sandbox from `start` to `end`, and Blaxel's clock."""
        buckets: builtins.list[dict[str, Any]] = []
        now = start
        window = start
        while window < end:
            stop = min(window + _METRICS_WINDOW, end)
            cursor: str | None = None
            while True:
                params = {
                    "startTime": window.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "endTime": stop.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "resolution": "hourly",
                    "groupBy": "resource_uuid",
                    "resourceType": "sandbox",
                    "resourceName": name,
                }
                if cursor:
                    params["cursor"] = cursor
                resp = await self._ctl(
                    "GET", f"/accounts/{quote(account, safe='')}/metrics", "the cost", params=params
                )
                now = wire.response_time(resp)
                page = resp.json()
                for row in page.get("data") or []:
                    if row.get("resourceType") == "sandbox" and row.get("resourceName") == name:
                        buckets.extend(row.get("timeseries") or [])
                cursor = _page_cursor(page.get("meta"))
                if cursor is None:
                    break
            window = stop
        return buckets, now

    async def cost(self, sandbox_id: str, *, created_at: datetime | None = None) -> Cost:
        # GET /accounts/{id}/metrics: the sandbox's cost in hourly buckets, from the hour it was
        # created to the hour it ended (or the hour now, while it runs). Blaxel's amounts are its
        # list prices (see sandbox-provider-pricing). When Blaxel no longer holds the sandbox's
        # record, the caller's `created_at` hint starts the window. A 404 here is read as: ended
        # and dropped (`_cost_of_gone`), so a caller gives the hint only for a sandbox that has
        # ended. Cloud does: its cost pass reads a sandbox from its own end plus pricing's first
        # read delay, and its image-copy pass after the prepare is recorded; a capture before a
        # destroy gives no hint, so a 404 there is `not_found`.
        headers = await self._identity()
        path = f"/sandboxes/{quote(sandbox_id, safe='')}"
        resp = await self._send(self._control, "GET", path, headers=headers)
        if resp.status_code == 404 and created_at is not None:
            return await self._cost_of_gone(sandbox_id, created_at, wire.response_time(resp))
        if resp.status_code >= 400:
            raise wire.failure(resp, "the sandbox")
        native, read_at = resp.json(), wire.response_time(resp)
        created = wire.stamp(native["metadata"]["createdAt"])
        ended = native["status"] in wire.ENDED
        end = wire.stamp(native["metadata"]["updatedAt"]) if ended else None
        return await self._cost_between(sandbox_id, created, end, read_at)

    async def _cost_of_gone(self, sandbox_id: str, created_at: datetime, read_at: datetime) -> Cost:
        """The cost of a sandbox whose record Blaxel has dropped: the sum of its buckets, final.

        Blaxel drops a deleted sandbox's record long after it has published the sandbox's buckets
        (live 2026-11-23: buckets 0 to 7 s after the destroy; of sixteen records two were dropped
        about 16 minutes after it, the others were still there at 15 to 52 minutes), so what the
        metrics hold then is all there will be. With no bucket that has
        a cost, Blaxel holds nothing for the sandbox: `not_found`.
        """
        account = await self._account_id()
        first = wire.hour_start(created_at)
        buckets, now = await self._metrics(
            account, sandbox_id, first, wire.hour_start(read_at) + wire.HOUR
        )
        if not any(Decimal(str(b["cost"])) for b in buckets):
            raise SpsError("not_found", "Blaxel holds no cost for this sandbox")
        return self._cost_from(sandbox_id, buckets, first, None, now, final=True)

    async def _cost_between(
        self, sandbox_id: str, created: datetime, end: datetime | None, read_at: datetime
    ) -> Cost:
        account = await self._account_id()
        first = wire.hour_start(created)
        last = wire.hour_start(end or read_at) + wire.HOUR
        buckets, now = await self._metrics(account, sandbox_id, first, last)
        settled = end is not None and now - end >= _COST_SETTLE
        return self._cost_from(sandbox_id, buckets, first, end, now, final=settled)

    @staticmethod
    def _cost_from(
        sandbox_id: str,
        buckets: builtins.list[dict[str, Any]],
        first: datetime,
        end: datetime | None,
        now: datetime,
        *,
        final: bool,
    ) -> Cost:
        """The sandbox's hourly buckets from `first` to the hour of `end` (all of them without an
        end). Blaxel answers every hour of the window, with a cost of 0 for the hours the sandbox
        did not run, so a figure is `final` only when `final` holds and a bucket has a cost."""
        last = None if end is None else wire.hour_start(end) + wire.HOUR
        lines: builtins.list[CostLine] = []
        for item in sorted(buckets, key=lambda b: b["timestamp"]):
            begins = wire.stamp(item["timestamp"])
            if begins >= first and (last is None or begins < last):
                lines.append(
                    CostLine(
                        "memory", Decimal(str(item["cost"])), None, None, begins, begins + wire.HOUR
                    )
                )
        costed = any(line.amount_usd for line in lines)
        total = sum((line.amount_usd or Decimal(0) for line in lines), start=Decimal(0))
        return Cost(
            sandbox_id=sandbox_id,
            status="final" if final and costed else "accruing",
            total_usd=total if lines else None,
            source="reported",
            lines=tuple(lines),
            as_of=now,
        )

    async def account_cost(self, start: datetime, end: datetime) -> AccountCost:
        # GET /accounts/{id}/metrics without a resource filter: every resource's hourly cost, added
        # up page by page. A bucket counts when its hour starts inside [start, end).
        if not start < end or end - start > timedelta(seconds=ACCOUNT_WINDOW_S):
            raise SpsError(
                "invalid_request", f"The window must be 1 second to {ACCOUNT_WINDOW_S} seconds"
            )
        account = await self._account_id()
        first, stop = wire.hour_start(start), wire.hour_start(end - timedelta(microseconds=1))
        params: dict[str, Any] = {
            "startTime": first.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": (stop + wire.HOUR).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "resolution": "hourly",
            "groupBy": "resource_uuid",
        }
        hours: dict[datetime, Decimal] = {}
        now = start
        cursor: str | None = None
        while True:
            if cursor:
                params["cursor"] = cursor
            resp = await self._ctl(
                "GET",
                f"/accounts/{quote(account, safe='')}/metrics",
                "the account cost",
                params=params,
            )
            now = wire.response_time(resp)
            page = resp.json()
            for row in page.get("data") or []:
                for item in row.get("timeseries") or []:
                    begins = wire.stamp(item["timestamp"])
                    if start <= begins < end:
                        hours[begins] = hours.get(begins, Decimal(0)) + Decimal(str(item["cost"]))
            cursor = _page_cursor(page.get("meta"))
            if cursor is None:
                break
        lines = tuple(
            CostLine("account", hours[h], None, None, h, h + wire.HOUR) for h in sorted(hours)
        )
        final = now - (stop + wire.HOUR) >= _COST_SETTLE
        return AccountCost(
            start=start,
            end=end,
            status="final" if final else "accruing",
            total_usd=sum((line.amount_usd or Decimal(0) for line in lines), start=Decimal(0))
            if lines
            else None,
            source="reported",
            lines=lines,
            as_of=now,
        )

    async def aclose(self) -> None:
        await self._control.aclose()
        await self._runtime.aclose()
