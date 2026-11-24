# SPDX-FileCopyrightText: Groundbox
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The Groundbox adapter: a 1.0 `Provider` over Groundbox's public API (https://api.groundbox.dev).

A sandbox is a Groundbox sandbox, a Linux microVM. Its labels are its `metadata`, its lifetime is
`timeout_minutes`, and a create's `Idempotency-Key` is Groundbox's own header, so the adapter
keeps nothing of its own. Commands run through `POST /exec`; a background process is a Groundbox
background command, whose output is read by position. Files larger than one request may carry
are moved in parts (`transfer.py`). The cost is priced from Groundbox's usage record of the
sandbox at the list price in discovery.
"""

from __future__ import annotations

import asyncio
import base64
import math
from collections.abc import AsyncIterable, Mapping
from datetime import datetime
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any

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

from sps_provider_groundbox import transfer
from sps_provider_groundbox.create_spec import plan_create
from sps_provider_groundbox.facts import (
    CLASS,
    CPU_PRICE,
    CPU_UNIT,
    IMAGE,
    LIMITS,
    LIST_LIMIT,
    MAX_PROCESS_S,
    MAX_RUN_S,
    MAX_WAIT_S,
    build_discovery,
)

API_URL = "https://api.groundbox.dev/v1"

# Groundbox's error codes, each as the standard's code for the same failure.
_ERRORS: dict[str, str] = {
    "bad_request": "invalid_request",
    "unauthorized": "unauthenticated",
    "key_pending_claim": "unauthenticated",
    "forbidden": "forbidden",
    "missing_scope": "forbidden",
    "github_org_restricted": "forbidden",
    "team_held": "forbidden",
    "team_suspended": "forbidden",
    "not_found": "not_found",
    "conflict": "conflict",
    "idempotency_key_reused": "conflict",
    "insufficient_balance": "quota_exceeded",
    "spend_cap_reached": "quota_exceeded",
    "paid_plan_required": "quota_exceeded",
    "trial_limit": "quota_exceeded",
    "trial_credit_exhausted": "quota_exceeded",
    "concurrency_limit": "quota_exceeded",
    "rate_limited": "rate_limited",
    "no_capacity": "unavailable",
    "not_available": "unavailable",
    "busy": "unavailable",
    "timeout": "timeout",
    "internal": "provider_error",
    "upgrade_required": "provider_error",
}
# For an answer with no Groundbox error body (its front end's own page).
_STATUSES: dict[int, str] = {
    401: "unauthenticated",
    403: "forbidden",
    404: "not_found",
    413: "too_large",
    429: "rate_limited",
    502: "unavailable",
    503: "unavailable",
    504: "timeout",
}
_PROVIDER_FAULT = frozenset({"unavailable", "timeout", "provider_error"})

# A Groundbox status as the standard's state. `stopped` there is ended for good (a deleted
# sandbox reads `stopped`); a sandbox shut down with its disk kept is `shut_down`.
_STATES: dict[str, str] = {
    "queued": "running",
    "starting": "running",
    "running": "running",
    "resuming": "running",
    "pausing": "paused",
    "paused": "paused",
    "shutting_down": "stopped",
    "shut_down": "stopped",
    "stopping": "destroyed",
    "stopped": "destroyed",
    "failed": "destroyed",
}
_STARTING = frozenset({"queued", "starting", "resuming"})
_ENDED = frozenset({"stopping", "stopped", "failed"})
_AT_END: dict[str, str] = {"delete": "destroyed", "pause": "paused"}
_TIMES = (
    ("created", "created_at"),
    ("started", "started_at"),
    ("deadline", "expires_at"),
    ("paused", "paused_at"),
    ("stopped", "shut_down_at"),
    ("ended", "stopped_at"),
)

CREATE_WAIT_S = 120  # how long `create` waits for a sandbox that is still starting
# `POST /exec` answers with the last 4 MB of a stream. A stream this long may have lost its
# start, so the adapter reads it again from the command's log.
EXEC_TAIL_BYTES = 3 * 1024 * 1024
_STORAGE_USD_PER_GB_HOUR = Decimal("0.05") / 720  # $0.05 per GB for a 30-day month
_HOUR = Decimal(3600)


def _stamp(text: str) -> datetime:
    return datetime.fromisoformat(text)


def _text(data: bytes) -> str:
    return data.decode("utf-8", "replace")


class GroundboxProvider(Provider):
    """A 1.0 provider over Groundbox's sandbox API."""

    def __init__(
        self,
        credentials: Mapping[str, str],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(credentials)
        self._http = httpx.AsyncClient(
            base_url=API_URL,
            headers={
                "Authorization": f"Bearer {self.credentials['api_key']}",
                "User-Agent": "sps-provider-groundbox",
            },
            transport=transport,
            timeout=httpx.Timeout(60.0, connect=10.0),
        )
        self._sleep = asyncio.sleep  # a test replaces it

    # -- transport ------------------------------------------------------------

    @staticmethod
    def _error(resp: httpx.Response) -> SpsError:
        """A Groundbox error answer as an `SpsError`. Groundbox says why in its own words; an
        answer without its error body (a front end's page) carries only the status."""
        retry: int | None = None
        try:
            retry = max(0, math.ceil(float(resp.headers.get("retry-after", ""))))
        except ValueError:
            pass
        try:
            body = resp.json()["error"]
            native, message = body["code"], str(body["message"])
            details = body.get("details")
        except (ValueError, KeyError, TypeError):
            code = _STATUSES.get(resp.status_code, "provider_error")
            return SpsError(
                code,  # type: ignore[arg-type]
                f"Groundbox answered HTTP {resp.status_code}",
                outcome="unknown" if code in _PROVIDER_FAULT else "failed",
                retry_after_seconds=retry,
            )
        code = _ERRORS.get(native, "provider_error")
        if (
            native == "conflict"
            and isinstance(details, Mapping)
            and details.get("status") in _ENDED
        ):
            code = "not_found"  # an operation on a sandbox that has ended
        return SpsError(
            code,  # type: ignore[arg-type]
            message[:1024],
            outcome="unknown" if code in _PROVIDER_FAULT else "failed",
            retry_after_seconds=retry,
        )

    async def _req(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            resp = await self._http.request(method, path, **kwargs)
        except httpx.TimeoutException:
            raise SpsError("timeout", "Groundbox did not answer", outcome="unknown") from None
        except httpx.HTTPError:
            raise SpsError("unavailable", "Groundbox is unreachable", outcome="unknown") from None
        if resp.status_code >= 400:
            raise self._error(resp)
        return resp

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        return (await self._req(method, path, **kwargs)).json()

    @staticmethod
    def _to_sandbox(row: Mapping[str, Any]) -> Sandbox:
        times = tuple(
            ProviderTime(kind, _stamp(row[field]), field)  # type: ignore[arg-type]
            for kind, field in _TIMES
            if row.get(field)
        )
        image = row.get("image")
        if isinstance(image, Mapping):
            image = image.get("reference")
        elif image is None and row.get("template") is None and row.get("snapshot_id") is None:
            image = IMAGE
        ends = _stamp(row["expires_at"]) if row.get("expires_at") else None
        return Sandbox(
            id=row["id"],
            sandbox_class=CLASS,
            state=_STATES.get(row["status"], "destroyed"),  # type: ignore[arg-type]
            size=Size(cpu=row["cpus"], memory_mib=row["memory_gb"] * 1024),
            region=None,
            image=image if isinstance(image, str) else None,
            labels=dict(row.get("metadata") or {}),
            created_at=_stamp(row["created_at"]),
            ends_at=ends,
            at_end=_AT_END.get(row.get("on_timeout", "")) if ends else None,  # type: ignore[arg-type]
            times=times,
        )

    async def _missing_cwd(self, sandbox_id: str, cwd: str | None, error: SpsError) -> SpsError:
        """Groundbox refuses a missing working directory as a bad request. When the path really
        is not a directory, that is the standard's `not_found`."""
        if error.code != "invalid_request" or cwd is None:
            return error
        try:
            entry = await self._json(
                "GET", f"/sandboxes/{sandbox_id}/files/stat", params={"path": cwd}
            )
        except SpsError as found:
            if found.code != "not_found":
                return error
            return SpsError("not_found", f"No such directory: {cwd}"[:1024])
        if entry.get("type") == "file":
            return SpsError("not_found", f"No such directory: {cwd}"[:1024])
        return error

    # -- operations ------------------------------------------------------------

    async def discovery(self) -> Discovery:
        # No Groundbox call returns these facts; listing sandboxes proves the key.
        await self._req("GET", "/sandboxes", params={"limit": 1})
        return build_discovery(self.provider_id, self.name)

    @classmethod
    def static_discovery(cls) -> Discovery:
        return build_discovery(cls.provider_id, cls.name)

    async def create(self, request: CreateRequest, *, idempotency_key: str) -> Sandbox:
        # POST /sandboxes with Groundbox's own `Idempotency-Key`: a repeat answers with the first
        # sandbox, and a repeat with another body is `idempotency_key_reused`. The call waits up
        # to 30 s for the sandbox to run and then answers with whatever status it has, so the
        # adapter reads the sandbox until it runs.
        body = plan_create(request)
        row = await self._json(
            "POST",
            "/sandboxes",
            json=body,
            headers={"Idempotency-Key": idempotency_key},
            timeout=httpx.Timeout(90.0, connect=10.0),
        )
        waited = 0.0
        while row["status"] in _STARTING:
            if waited >= CREATE_WAIT_S:
                raise SpsError(
                    "timeout", f"Sandbox {row['id']} did not start in time", outcome="unknown"
                )
            await self._sleep(0.5)
            waited += 0.5
            row = await self._json("GET", f"/sandboxes/{row['id']}")
        if row["status"] == "failed" or row.get("stop_reason") == "error":
            raise SpsError(
                "provider_error", f"Sandbox {row['id']} failed to start", outcome="unknown"
            )
        return self._to_sandbox(row)

    async def get(self, sandbox_id: str) -> Sandbox:
        # GET /sandboxes/{id}. Groundbox keeps the row of an ended sandbox, so it reads `destroyed`.
        return self._to_sandbox(await self._json("GET", f"/sandboxes/{sandbox_id}"))

    async def list(
        self,
        *,
        labels: Mapping[str, str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SandboxPage:
        # GET /sandboxes?live=true&metadata[key]=value: every status but the ended ones, and
        # every pair must be on the sandbox with exactly that value.
        if limit < 1:
            raise SpsError("invalid_request", "limit must be at least 1")
        params: list[tuple[str, str]] = [("live", "true"), ("limit", str(min(limit, LIST_LIMIT)))]
        params += [(f"metadata[{key}]", value) for key, value in dict(labels or {}).items()]
        if cursor:
            params.append(("cursor", cursor))
        page = await self._json("GET", "/sandboxes", params=params)
        boxes = [self._to_sandbox(row) for row in page["data"] if row["status"] not in _ENDED]
        return SandboxPage(sandboxes=boxes, next_cursor=page.get("next_cursor"))

    async def destroy(self, sandbox_id: str) -> DestroyResult:
        # DELETE /sandboxes/{id}: safe to repeat, 404 only for an id the account never had.
        await self._req("DELETE", f"/sandboxes/{sandbox_id}")
        return DestroyResult(sandbox_id=sandbox_id, state="destroyed")

    async def _log(self, sandbox_id: str, exec_id: str, limit: int) -> tuple[bytes, bytes, bool]:
        """A finished command's output from the start of its log: GET .../commands/{id}/output
        by position, 1 MB a call, until each stream is past `limit` or the log ends. The last
        value says that the log no longer has the start."""
        out, err = bytearray(), bytearray()
        seq, lost = 0, False
        while True:
            page = await self._json(
                "GET",
                f"/sandboxes/{sandbox_id}/commands/{exec_id}/output",
                params={"seq": seq, "encoding": "base64"},
            )
            lost = lost or bool(page["skipped_bytes"]) or bool(page["truncated"])
            out += base64.b64decode(page["stdout"])
            err += base64.b64decode(page["stderr"])
            if not page["more"] or page["seq"] == seq:
                break
            if len(out) > limit and len(err) > limit:
                break
            seq = page["seq"]
        return bytes(out), bytes(err), lost

    async def run(self, sandbox_id: str, request: RunRequest) -> RunResult:
        # POST /sandboxes/{id}/exec with `args` (no shell). At `timeout_s` Groundbox kills the
        # command's process group and answers `timed_out`. Whole seconds: a timeout between two
        # seconds ends at the later one.
        if request.user is not None:
            raise SpsError("unsupported", "Groundbox runs commands as the default user")
        limit = request.output_limit_bytes
        if not 0 <= limit <= LIMITS.output_bytes:
            raise SpsError("invalid_request", f"output_limit_bytes over {LIMITS.output_bytes}")
        if not 1 <= request.timeout_ms <= MAX_RUN_S * 1000:
            raise SpsError("invalid_request", f"timeout_ms must be 1 to {MAX_RUN_S * 1000}")
        timeout_s = math.ceil(request.timeout_ms / 1000)
        body: dict[str, Any] = {
            "args": list(request.argv),
            "env": dict(request.env),
            "timeout_s": timeout_s,
            "encoding": "base64",
        }
        if request.cwd is not None:
            body["cwd"] = request.cwd
        try:
            res = await self._json(
                "POST",
                f"/sandboxes/{sandbox_id}/exec",
                json=body,
                timeout=httpx.Timeout(timeout_s + 60.0, connect=10.0),
            )
        except SpsError as error:
            raise await self._missing_cwd(sandbox_id, request.cwd, error) from None
        out, err = base64.b64decode(res["stdout"]), base64.b64decode(res["stderr"])
        lost = False
        if max(len(out), len(err)) >= EXEC_TAIL_BYTES:
            if res.get("exec_id"):
                out, err, lost = await self._log(sandbox_id, res["exec_id"], limit)
            else:
                lost = True  # the command ran outside the supervisor, which keeps no log
        timed_out = bool(res["timed_out"])
        as_base64 = request.output == "base64"
        return RunResult(
            exit_code=None if timed_out else res["exit_code"],
            stdout="" if as_base64 else _text(out[:limit]),
            stderr="" if as_base64 else _text(err[:limit]),
            stdout_truncated=len(out) > limit or lost,
            stderr_truncated=len(err) > limit or lost,
            timed_out=timed_out,
            stdout_base64=base64.b64encode(out[:limit]).decode() if as_base64 else None,
            stderr_base64=base64.b64encode(err[:limit]).decode() if as_base64 else None,
        )

    async def files_read(self, sandbox_id: str, path: str) -> FileStream:
        # GET /sandboxes/{id}/files?path= streams a file of up to 100 MB; a larger one is read
        # in parts (transfer.py).
        return await transfer.read(self, sandbox_id, path)

    async def files_write(
        self,
        sandbox_id: str,
        path: str,
        data: AsyncIterable[bytes],
        *,
        size: int,
        user: str | None = None,
    ) -> FileInfo:
        # PUT /sandboxes/{id}/files?path= takes one request body; a larger file is written in
        # parts and put together in the sandbox (transfer.py). Groundbox makes missing parents.
        if user is not None:
            raise SpsError("unsupported", "Groundbox writes files as the default user")
        return await transfer.write(self, sandbox_id, path, data, size)

    async def process_start(self, sandbox_id: str, request: ProcessStartRequest) -> Process:
        # POST /sandboxes/{id}/commands starts a command in the background, under a supervisor
        # in the sandbox that keeps its output. Its default timeout is an hour, so the adapter
        # asks for the longest one.
        if request.stdin:
            raise SpsError("unsupported", "Groundbox does not claim process input")
        if request.user is not None:
            raise SpsError("unsupported", "Groundbox starts processes as the default user")
        body: dict[str, Any] = {
            "args": list(request.argv),
            "env": dict(request.env),
            "timeout_s": MAX_PROCESS_S,
        }
        if request.cwd is not None:
            body["cwd"] = request.cwd
        try:
            command = await self._json("POST", f"/sandboxes/{sandbox_id}/commands", json=body)
        except SpsError as error:
            raise await self._missing_cwd(sandbox_id, request.cwd, error) from None
        return Process(
            id=command["id"], sandbox_id=sandbox_id, started_at=_stamp(command["started_at"])
        )

    async def process_output(
        self, sandbox_id: str, process_id: str, *, cursor: str | None = None, wait_ms: int = 0
    ) -> OutputPage:
        # GET /sandboxes/{id}/commands/{exec_id}/output?seq= answers at once with at most 1 MB
        # from that position, so the adapter asks again until there is output, the command has
        # ended or `wait_ms` has passed.
        try:
            seq = int(cursor) if cursor else 0
        except ValueError:
            raise SpsError(
                "invalid_request", "cursor is not one that process_output gave"
            ) from None
        if seq < 0 or wait_ms < 0:
            raise SpsError("invalid_request", "cursor and wait_ms cannot be negative")
        remaining = min(wait_ms / 1000, MAX_WAIT_S)
        pause = 0.1
        while True:
            page = await self._json(
                "GET",
                f"/sandboxes/{sandbox_id}/commands/{process_id}/output",
                params={"seq": seq, "encoding": "base64"},
            )
            out, err = base64.b64decode(page["stdout"]), base64.b64decode(page["stderr"])
            if out or err or page["done"] or page["seq"] != seq or remaining <= 0:
                break
            await self._sleep(min(pause, remaining))
            remaining -= pause
            pause = min(pause * 2, 1.0)
        done = bool(page["done"]) and not page["more"]
        if done and page["exit_code"] is None:
            raise SpsError(
                "provider_error", "Groundbox has no exit status for the command", outcome="unknown"
            )
        chunks = [
            OutputChunk(stream, _text(data))  # type: ignore[arg-type]
            for stream, data in (("stdout", out), ("stderr", err))
            if data
        ]
        return OutputPage(
            chunks=chunks,
            next_cursor=str(page["seq"]),
            gap=bool(page["skipped_bytes"]) or bool(page["truncated"]),
            stdout_closed=done,
            stderr_closed=done,
            state="exited" if done else "running",
            exit_code=page["exit_code"] if done else None,
        )

    async def cost(self, sandbox_id: str, *, created_at: datetime | None = None) -> Cost:
        # GET /sandboxes/{id}/usage: Groundbox's own record of the seconds the sandbox ran (and
        # of what its kept disk took), readable after the sandbox ended. The adapter prices the
        # seconds at the list price in discovery, so the amounts are before any plan or credit.
        # `created_at` is only a lookup hint, and Groundbox needs none.
        del created_at
        row = await self._json("GET", f"/sandboxes/{sandbox_id}")
        resp = await self._req("GET", f"/sandboxes/{sandbox_id}/usage")
        usage = resp.json()
        final = row["status"] in ("stopped", "failed")
        start = _stamp(row.get("started_at") or row["created_at"])
        end = _stamp(row["stopped_at"]) if final and row.get("stopped_at") else None
        if end is not None:
            as_of = end
        else:  # Groundbox's clock at the read, never the adapter's
            try:
                as_of = parsedate_to_datetime(resp.headers.get("date", ""))
            except (TypeError, ValueError):
                as_of = start
        lines: list[CostLine] = []
        seconds = Decimal(str(usage["running_seconds"]))
        if seconds > 0 or final:
            quantity = (Decimal(row["cpus"]) * seconds / _HOUR).quantize(Decimal("1e-12"))
            amount = (quantity * CPU_PRICE).quantize(Decimal("1e-8"))
            lines.append(CostLine("cpu", amount, quantity, CPU_UNIT, start, end))
        stored = Decimal(str(usage["storage_gb_hours"]))
        if stored > 0:  # a sandbox that was paused or shut down with its disk kept
            quantity = stored.quantize(Decimal("1e-12"))
            amount = (quantity * _STORAGE_USD_PER_GB_HOUR).quantize(Decimal("1e-8"))
            lines.append(CostLine("disk", amount, quantity, "gb_hour", start, end))
        return Cost(
            sandbox_id=sandbox_id,
            status="final" if final else "accruing",
            total_usd=sum((line.amount_usd or Decimal(0) for line in lines), Decimal(0))
            if lines
            else None,
            source="calculated",
            lines=tuple(lines),
            as_of=as_of,
        )

    async def aclose(self) -> None:
        await self._http.aclose()
