# SPDX-FileCopyrightText: Groundbox
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Offline tests for the Groundbox adapter.

A small in-memory Groundbox sits behind `httpx.MockTransport` and answers the routes the adapter
calls with the shapes the real API answered on 2026-11-23, so the tests need no account.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator, Coroutine
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
from sandbox_provider_spec.v1 import (
    CreateRequest,
    ProcessStartRequest,
    RunRequest,
    SpsError,
    to_json,
)
from sandbox_provider_spec.v1.within import within

from sps_provider_groundbox import GroundboxProvider, transfer
from sps_provider_groundbox.facts import FILE_BYTES, MAX_PROCESS_S

KEY = "gbx_test_key"
T0 = "2026-11-23T10:00:00.000Z"


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return asyncio.run(coro)


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


class FakeGroundbox:
    """A recorded Groundbox: the same routes, answered from memory."""

    def __init__(self) -> None:
        self.boxes: dict[str, dict[str, Any]] = {}
        self.keys: dict[str, tuple[str, str]] = {}
        self.files: dict[tuple[str, str], bytes] = {}
        self.folders: set[tuple[str, str]] = {("*", "/tmp")}
        self.commands: dict[str, dict[str, Any]] = {}
        self.execs: list[dict[str, Any]] = []
        self.requests: list[httpx.Request] = []
        self.start_as = "running"
        self.next_exec: dict[str, Any] | None = None
        self.usage = {"running_seconds": 0.0, "storage_gb_hours": 0.0}

    # -- helpers --------------------------------------------------------------

    @staticmethod
    def _json(status: int, body: Any, headers: dict[str, str] | None = None) -> httpx.Response:
        return httpx.Response(status, json=body, headers=headers)

    def _error(self, status: int, code: str, message: str, **extra: Any) -> httpx.Response:
        headers = extra.pop("headers", None)
        return self._json(status, {"error": {"code": code, "message": message, **extra}}, headers)

    def _ended(self, box: dict[str, Any]) -> httpx.Response:
        return self._error(
            409,
            "conflict",
            f"Sandbox {box['id']} is stopped, not running",
            details={"status": box["status"], "stop_reason": box["stop_reason"]},
        )

    # -- routing --------------------------------------------------------------

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.headers.get("authorization") != f"Bearer {KEY}":
            return self._error(401, "unauthorized", "No such key")
        segments = [s for s in request.url.path.split("/") if s][1:]  # drop "v1"
        if segments == ["sandboxes"]:
            return self._create(request) if request.method == "POST" else self._list(request)
        box = self.boxes.get(segments[1]) if len(segments) > 1 else None
        if box is None:
            return self._error(404, "not_found", "No such sandbox")
        rest = segments[2:]
        if not rest:
            if request.method == "DELETE" and box["status"] != "stopped":
                box.update(status="stopped", stop_reason="deleted", stopped_at=T0, expires_at=None)
            return self._json(200, box)
        if rest == ["usage"]:
            usage = {"sandbox_id": box["id"], **self.usage}
            return self._json(200, usage, {"Date": "Mon, 23 Nov 2026 10:05:00 GMT"})
        if box["status"] != "running":
            return self._ended(box)
        if rest == ["exec"]:
            return self._exec(box, json.loads(request.content))
        if rest == ["commands"]:
            return self._start(box, json.loads(request.content))
        if len(rest) == 3 and rest[0] == "commands" and rest[2] == "output":
            return self._output(rest[1], request)
        if rest == ["files", "stat"]:
            return self._stat(box, request.url.params["path"])
        if rest == ["files"]:
            return self._file(box, request)
        return self._error(404, "not_found", f"No route for {request.method} {request.url.path}")

    def _create(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        key = request.headers["idempotency-key"]
        if key in self.keys:
            first, sent = self.keys[key]
            if sent != request.content.decode():
                return self._error(
                    409,
                    "idempotency_key_reused",
                    "This Idempotency-Key was used for a different request",
                )
            return self._json(200, self.boxes[first])
        if body["memory_gb"] > 4 * body["cpus"]:
            return self._error(400, "bad_request", "memory_gb can be at most 4 GB per core")
        box = {
            "id": f"sbx_{len(self.boxes):012d}",
            "status": self.start_as,
            "cpus": body["cpus"],
            "memory_gb": body["memory_gb"],
            "disk_gb": body.get("disk_gb", 20),
            "timeout_minutes": body["timeout_minutes"],
            "metadata": body.get("metadata", {}),
            "env": body.get("env", {}),
            "template": None,
            "image": None,
            "snapshot_id": None,
            "created_at": T0,
            "started_at": T0,
            "expires_at": "2026-11-23T10:05:00.000Z",
            "paused_at": None,
            "shut_down_at": None,
            "on_timeout": body["on_timeout"],
            "stopped_at": None,
            "stop_reason": None,
        }
        self.boxes[box["id"]] = box
        self.keys[key] = (box["id"], request.content.decode())
        return self._json(200, box)

    def _list(self, request: httpx.Request) -> httpx.Response:
        params = request.url.params
        wanted = {
            name[len("metadata[") : -1]: value
            for name, value in params.multi_items()
            if name.startswith("metadata[")
        }
        live = [
            box
            for box in self.boxes.values()
            if box["status"] not in ("stopped", "failed")
            and all(box["metadata"].get(k) == v for k, v in wanted.items())
        ]
        return self._json(200, {"data": live[: int(params["limit"])], "next_cursor": None})

    def _exec(self, box: dict[str, Any], body: dict[str, Any]) -> httpx.Response:
        self.execs.append(body)
        args = body["args"]
        sid = box["id"]
        if "cwd" in body and not self._is_folder(sid, body["cwd"]):
            return self._error(400, "bad_request", f"cwd {body['cwd']} is not a folder")
        result: dict[str, Any] = {"exit_code": 0, "stdout": "", "stderr": "", "timed_out": False}
        if self.next_exec is not None:
            result.update(self.next_exec)
            self.next_exec = None
        elif args[:3] == ["sudo", "-n", "sh"]:  # the join of a write in parts
            temporary, destination, *parts = args[6:]
            self.files[(sid, destination)] = b"".join(self.files.pop((sid, p)) for p in parts)
            assert temporary.endswith("-tmp")
        elif args[:3] == ["sudo", "-n", "dd"]:  # one slice of a read in parts
            named = dict(arg.split("=", 1) for arg in args[3:] if "=" in arg)
            start = int(named["skip"]) << 20
            data = self.files[(sid, named["if"])]
            self.files[(sid, named["of"])] = data[start : start + (int(named["count"]) << 20)]
        elif args[:3] == ["sudo", "-n", "stat"]:
            path = args[-1]
            if (sid, path) in self.files:
                result["stdout"] = f"{len(self.files[(sid, path)])} regular file\n"
            elif self._is_folder(sid, path):
                result["stdout"] = "4096 directory\n"
            else:
                result["exit_code"] = 1
        elif args[:3] == ["sudo", "-n", "rm"]:
            for path in args[5:]:
                self.files.pop((sid, path), None)
        if body.get("encoding") == "base64":
            result["stdout"] = (
                b64(result["stdout"].encode())
                if isinstance(result["stdout"], str)
                else b64(result["stdout"])
            )
            result["stderr"] = (
                b64(result["stderr"].encode())
                if isinstance(result["stderr"], str)
                else b64(result["stderr"])
            )
        return self._json(
            200,
            {"duration_ms": 1, "exec_id": result.pop("exec_id", "ex_0000000000000000"), **result},
        )

    def _is_folder(self, sid: str, path: str) -> bool:
        return (sid, path) in self.folders or ("*", path) in self.folders

    def _start(self, box: dict[str, Any], body: dict[str, Any]) -> httpx.Response:
        if "cwd" in body and not self._is_folder(box["id"], body["cwd"]):
            return self._error(400, "bad_request", f"cwd {body['cwd']} is not a folder")
        command = {
            "id": f"ex_{len(self.commands):016d}",
            "body": body,
            "pages": [],
            "started_at": T0,
        }
        self.commands[command["id"]] = command
        return self._json(200, {"id": command["id"], "state": "running", "started_at": T0})

    def _output(self, exec_id: str, request: httpx.Request) -> httpx.Response:
        command = self.commands.get(exec_id)
        if command is None:
            return self._error(404, "not_found", "No such command")
        seq = int(request.url.params["seq"])
        assert request.url.params["encoding"] == "base64"
        empty = {"stdout": "", "stderr": "", "seq": seq, "skipped_bytes": 0, "truncated": False,
                 "more": False, "done": False, "exit_code": None, "duration_ms": None, "timed_out": False}  # fmt: skip
        pages = command["pages"]
        if command.get("by_seq"):
            return self._json(200, {**empty, **command["by_seq"][seq]})
        return self._json(200, {**empty, **pages.pop(0)} if pages else empty)

    def _stat(self, box: dict[str, Any], path: str) -> httpx.Response:
        if (box["id"], path) in self.files:
            return self._json(200, {"name": path, "path": path, "type": "file", "size_bytes": 1})
        if self._is_folder(box["id"], path):
            return self._json(200, {"name": path, "path": path, "type": "dir", "size_bytes": 4096})
        return self._error(404, "not_found", f"No such file or folder: {path}")

    def _file(self, box: dict[str, Any], request: httpx.Request) -> httpx.Response:
        path = request.url.params["path"]
        where = (box["id"], path)
        if request.method == "PUT":
            if len(request.content) > 32 << 20:
                return httpx.Response(413, text="<html><h1>Request Entity Too Large</h1></html>")
            self.files[where] = request.content
            return self._json(200, {"path": path, "size_bytes": len(request.content)})
        if self._is_folder(box["id"], path):
            return self._error(400, "bad_request", f"{path} is a folder")
        if where not in self.files:
            return self._error(404, "not_found", f"No such file or folder: {path}")
        if len(self.files[where]) > transfer.READ_PART:
            return self._error(400, "bad_request", "The file is over the download limit")
        return httpx.Response(200, content=self.files[where])


async def no_sleep(_: float) -> None:
    return None


def make(fake: FakeGroundbox, key: str = KEY) -> GroundboxProvider:
    provider = GroundboxProvider({"api_key": key}, transport=httpx.MockTransport(fake.handler))
    provider._sleep = no_sleep  # pylint: disable=protected-access
    return provider


async def new_box(provider: GroundboxProvider, **fields: Any) -> str:
    return (await provider.create(CreateRequest(**fields), idempotency_key="key-00000001")).id


async def stream(data: bytes, step: int = 1 << 16) -> AsyncIterator[bytes]:
    for start in range(0, len(data), step):
        yield data[start : start + step]


async def drain(chunks: AsyncIterator[bytes]) -> bytes:
    return b"".join([chunk async for chunk in chunks])


def raises(code: str, coro: Coroutine[Any, Any, Any]) -> SpsError:
    with pytest.raises(SpsError) as caught:
        run(coro)
    assert caught.value.code == code, caught.value
    return caught.value


# -- discovery ----------------------------------------------------------------


def test_discovery_is_valid_and_within_the_static_one() -> None:
    provider = make(FakeGroundbox())
    found = run(provider.discovery())
    document = to_json(found)
    assert document["provider"] == {
        "id": "groundbox",
        "name": "Groundbox",
        "docs_url": "https://groundbox.dev/docs",
    }
    assert found.limits.file_bytes >= 1 << 30
    assert found.limits.cost_final_within_seconds == 60
    assert within(found, GroundboxProvider.static_discovery()) == []
    klass = found.classes[0]
    assert klass.resources["sizes"][15] == {"cpu": 16, "memory_mib": [1024, 65536]}
    assert klass.lifetime_timer["step_seconds"] == 60
    assert {(p.item, str(p.price_usd), p.unit) for p in klass.prices if not p.included} == {
        ("cpu", "0.04", "vcpu_hour")
    }


def test_discovery_proves_the_key() -> None:
    raises("unauthenticated", make(FakeGroundbox(), key="gbx_wrong").discovery())


def test_a_missing_key_is_unauthenticated() -> None:
    with pytest.raises(SpsError) as caught:
        GroundboxProvider({})
    assert caught.value.code == "unauthenticated"


def test_the_key_is_not_in_the_repr() -> None:
    assert KEY not in repr(make(FakeGroundbox()))


# -- create, get, list, destroy -----------------------------------------------


def test_create_sends_the_groundbox_body() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    request = CreateRequest(
        resources={"cpu_min": 3, "memory_min_mib": 5000, "disk_min_mib": 30000, "arch": "amd64"},
        lifetime_seconds=150,
        env={"A": "1"},
        labels={"sps.test/case": "x", "empty": ""},
    )
    box = run(provider.create(request, idempotency_key="key-00000001"))
    sent = next(r for r in fake.requests if r.method == "POST")
    assert sent.headers["idempotency-key"] == "key-00000001"
    assert json.loads(sent.content) == {
        "cpus": 3,
        "memory_gb": 5,
        "disk_gb": 30,
        "timeout_minutes": 2,  # 150 s ends at the earlier whole minute
        "on_timeout": "delete",
        "env": {"A": "1"},
        "metadata": {"sps.test/case": "x", "empty": ""},
    }
    assert (box.state, box.size.cpu, box.size.memory_mib) == ("running", 3, 5120)
    assert box.labels == {"sps.test/case": "x", "empty": ""}
    assert box.image == "ubuntu-24.04"
    assert box.at_end == "destroyed"
    assert box.ends_at == datetime(2026, 11, 23, 10, 5, tzinfo=UTC)
    assert {time.kind for time in box.times} == {"created", "started", "deadline"}


@pytest.mark.parametrize(
    ("resources", "size"),
    [
        ({}, (2, 4)),
        ({"cpu_min": 4}, (4, 8)),
        ({"cpu_min": 0.5}, (1, 2)),
        ({"memory_min_mib": 1024}, (1, 1)),
        ({"memory_min_mib": 9000}, (3, 9)),
        ({"cpu_min": 16, "memory_min_mib": 65536}, (16, 64)),
        ({"cpu_min": None, "root": "sudo"}, (2, 4)),
    ],
)
def test_create_picks_the_smallest_size(resources: dict[str, Any], size: tuple[int, int]) -> None:
    fake = FakeGroundbox()
    run(make(fake).create(CreateRequest(resources=resources), idempotency_key="key-00000001"))
    body = json.loads(fake.requests[0].content)
    assert (body["cpus"], body["memory_gb"]) == size


@pytest.mark.parametrize(
    "fields",
    [
        {"resources": {"cpu_min": 100000}},
        {"resources": {"memory_min_mib": 70000}},
        {"resources": {"disk_min_mib": 200000}},
        {"resources": {"region": "us"}},
        {"resources": {"arch": "arm64"}},
        {"resources": {"root": "by_default"}},
        {"image": "sps-test-image-that-no-class-lists"},
        {"sandbox_class": "gpu"},
        {"requirements": {"snapshots": {}}},
        {"snapshot": "snap_1"},
        {"at_end": "paused"},
        {"lifetime_seconds": None},
        {"lifetime_seconds": 30},
        {"lifetime_seconds": 43200 * 60 + 1},
    ],
)
def test_create_refuses_what_groundbox_does_not_offer(fields: dict[str, Any]) -> None:
    fake = FakeGroundbox()
    raises("unsupported", new_box(make(fake), **fields))
    assert fake.requests == []  # nothing was asked of Groundbox, so nothing was created


def test_create_refuses_a_lifetime_that_is_not_a_number() -> None:
    raises("invalid_request", new_box(make(FakeGroundbox()), lifetime_seconds=0))


def test_create_waits_for_a_sandbox_that_is_starting() -> None:
    fake = FakeGroundbox()
    fake.start_as = "starting"
    provider = make(fake)
    reads = 0

    async def start_later(_: float) -> None:
        nonlocal reads
        reads += 1
        if reads == 3:
            next(iter(fake.boxes.values()))["status"] = "running"

    provider._sleep = start_later  # pylint: disable=protected-access
    box = run(provider.create(CreateRequest(), idempotency_key="key-00000001"))
    assert (box.state, reads) == ("running", 3)


def test_create_reports_a_sandbox_that_failed_to_start() -> None:
    fake = FakeGroundbox()
    fake.start_as = "failed"
    error = raises("provider_error", new_box(make(fake)))
    assert error.outcome == "unknown"


def test_a_repeated_key_gives_the_first_sandbox_or_a_conflict() -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> tuple[str, str]:
        first = await provider.create(CreateRequest(env={"A": "1"}), idempotency_key="key-00000001")
        again = await provider.create(CreateRequest(env={"A": "1"}), idempotency_key="key-00000001")
        return first.id, again.id

    first, again = run(scenario())
    assert first == again and len(fake.boxes) == 1
    raises(
        "conflict", provider.create(CreateRequest(env={"A": "2"}), idempotency_key="key-00000001")
    )
    assert len(fake.boxes) == 1


@pytest.mark.parametrize(
    ("status", "state"),
    [
        ("running", "running"),
        ("paused", "paused"),
        ("shut_down", "stopped"),
        ("stopping", "destroyed"),
        ("stopped", "destroyed"),
        ("failed", "destroyed"),
    ],
)
def test_get_maps_the_status(status: str, state: str) -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> str:
        sandbox_id = await new_box(provider)
        fake.boxes[sandbox_id]["status"] = status
        return (await provider.get(sandbox_id)).state

    assert run(scenario()) == state


def test_list_filters_by_every_label_and_leaves_out_ended_sandboxes() -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> tuple[list[str], list[str], list[str]]:
        first = await provider.create(
            CreateRequest(labels={"case": "x", "which": "a"}), idempotency_key="key-0000000a"
        )
        second = await provider.create(
            CreateRequest(labels={"case": "x", "which": "b"}), idempotency_key="key-0000000b"
        )
        both = await provider.list(labels={"case": "x"})
        only = await provider.list(labels={"case": "x", "which": "a"})
        fake.boxes[second.id]["status"] = "stopping"  # ending: still listed there, not here
        await provider.destroy(first.id)
        after = await provider.list(labels={"case": "x"}, limit=1000)
        return (
            [b.id for b in both.sandboxes],
            [b.id for b in only.sandboxes],
            [b.id for b in after.sandboxes],
        )

    both, only, after = run(scenario())
    assert (len(both), only, after) == (2, [both[0]], [])
    listing = fake.requests[-1].url.params
    assert (listing["live"], listing["limit"], listing["metadata[case]"]) == ("true", "200", "x")


def test_destroy_repeats_and_an_unknown_id_is_not_found() -> None:
    provider = make(FakeGroundbox())

    async def scenario() -> tuple[str, str, str]:
        sandbox_id = await new_box(provider)
        first = await provider.destroy(sandbox_id)
        again = await provider.destroy(sandbox_id)
        return first.state, again.state, (await provider.get(sandbox_id)).state

    assert run(scenario()) == ("destroyed", "destroyed", "destroyed")
    raises("not_found", provider.destroy("sps-test-no-such-sandbox"))
    raises("not_found", provider.get("sps-test-no-such-sandbox"))


# -- run ----------------------------------------------------------------------


def test_run_sends_argv_without_a_shell_and_decodes_the_output() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    fake.next_exec = {"exit_code": 7, "stdout": b"out\xff", "stderr": b"err"}

    async def scenario() -> Any:
        sandbox_id = await new_box(provider)
        request = RunRequest(argv=["bash", "-c", "x"], cwd="/tmp", env={"A": "1"}, timeout_ms=1500)
        return await provider.run(sandbox_id, request)

    result = run(scenario())
    assert fake.execs[0] == {
        "args": ["bash", "-c", "x"],
        "env": {"A": "1"},
        "timeout_s": 2,
        "encoding": "base64",
        "cwd": "/tmp",
    }
    assert (result.exit_code, result.stdout, result.stderr) == (7, "out�", "err")
    assert not (result.timed_out or result.stdout_truncated or result.stderr_truncated)


def test_run_reports_a_timeout_as_a_result() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    fake.next_exec = {"exit_code": 137, "stdout": b"before\n", "stderr": b"", "timed_out": True}

    async def scenario() -> Any:
        return await provider.run(
            await new_box(provider), RunRequest(argv=["sleep", "9"], timeout_ms=3000)
        )

    result = run(scenario())
    assert (result.timed_out, result.exit_code, result.stdout) == (True, None, "before\n")


def test_run_gives_base64_when_asked() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    fake.next_exec = {"stdout": b"\x00\xff", "stderr": b"e"}

    async def scenario() -> Any:
        return await provider.run(await new_box(provider), RunRequest(argv=["x"], output="base64"))

    result = run(scenario())
    assert (result.stdout, result.stderr) == ("", "")
    assert (result.stdout_base64, result.stderr_base64) == (b64(b"\x00\xff"), b64(b"e"))


def test_run_cuts_each_stream_at_the_limit() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    fake.next_exec = {"stdout": b"abcdef", "stderr": b"xy"}

    async def scenario() -> Any:
        request = RunRequest(argv=["x"], output_limit_bytes=4)
        return await provider.run(await new_box(provider), request)

    result = run(scenario())
    assert (result.stdout, result.stdout_truncated) == ("abcd", True)
    assert (result.stderr, result.stderr_truncated) == ("xy", False)


def test_run_reads_a_long_stream_from_the_commands_log() -> None:
    """`POST /exec` keeps the end of a long stream; the log has it from the start."""
    fake = FakeGroundbox()
    provider = make(fake)
    whole = b"a" * (5 << 20)
    fake.next_exec = {
        "stdout": whole[-(4 << 20) :],
        "stderr": b"",
        "exec_id": "ex_0000000000000000",
    }

    async def scenario() -> Any:
        sandbox_id = await new_box(provider)
        fake.commands["ex_0000000000000000"] = {
            "by_seq": {
                0: {"stdout": b64(whole[: 3 << 20]), "seq": 10, "more": True},
                10: {"stdout": b64(whole[3 << 20 :]), "seq": 20, "done": True, "exit_code": 0},
            },
            "pages": [],
        }
        return await provider.run(sandbox_id, RunRequest(argv=["x"]))

    result = run(scenario())
    assert (len(result.stdout), result.stdout_truncated) == (5 << 20, False)


def test_run_marks_a_stream_whose_start_the_log_lost() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    fake.next_exec = {"stdout": b"a" * (4 << 20), "stderr": b""}

    async def scenario() -> Any:
        sandbox_id = await new_box(provider)
        fake.commands["ex_0000000000000000"] = {
            "by_seq": {
                0: {
                    "stdout": b64(b"tail"),
                    "seq": 9,
                    "skipped_bytes": 100,
                    "done": True,
                    "exit_code": 0,
                }
            },
            "pages": [],
        }
        return await provider.run(sandbox_id, RunRequest(argv=["x"]))

    result = run(scenario())
    assert (result.stdout, result.stdout_truncated) == ("tail", True)


def test_run_in_a_missing_directory_is_not_found() -> None:
    provider = make(FakeGroundbox())

    async def scenario() -> Any:
        request = RunRequest(argv=["true"], cwd="/sps-test-no-such-directory")
        return await provider.run(await new_box(provider), request)

    raises("not_found", scenario())


def test_run_checks_its_limits_before_it_calls() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    raises(
        "invalid_request", provider.run("sbx_1", RunRequest(argv=["x"], output_limit_bytes=1 << 30))
    )
    raises("invalid_request", provider.run("sbx_1", RunRequest(argv=["x"], timeout_ms=3_600_001)))
    raises("unsupported", provider.run("sbx_1", RunRequest(argv=["x"], user="root")))
    assert fake.requests == []


def test_an_operation_on_an_ended_sandbox_is_not_found() -> None:
    provider = make(FakeGroundbox())

    async def scenario() -> Any:
        sandbox_id = await new_box(provider)
        await provider.destroy(sandbox_id)
        return await provider.run(sandbox_id, RunRequest(argv=["true"]))

    raises("not_found", scenario())


# -- files --------------------------------------------------------------------


def test_a_small_file_is_one_call_each_way() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    data = b"\x00\xffhello" * 1000

    async def scenario() -> tuple[Any, bytes, int | None]:
        sandbox_id = await new_box(provider)
        info = await provider.files_write(sandbox_id, "/tmp/a/b", stream(data), size=len(data))
        found = await provider.files_read(sandbox_id, "/tmp/a/b")
        return info, await drain(found.chunks), found.size

    info, back, size = run(scenario())
    assert (info.path, info.size, back, size) == ("/tmp/a/b", len(data), data, None)
    assert [r.method for r in fake.requests[1:]] == ["PUT", "GET"]


@pytest.mark.parametrize("declared", [9, 11])
def test_a_body_of_another_size_is_refused(declared: int) -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> Any:
        sandbox_id = await new_box(provider)
        return await provider.files_write(
            sandbox_id, "/tmp/a", stream(b"0123456789"), size=declared
        )

    error = raises("invalid_request", scenario())
    assert error.outcome == "unknown"
    assert fake.files == {}


def test_a_large_file_is_written_in_parts_and_put_together(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transfer, "WRITE_PART", 1000)
    fake = FakeGroundbox()
    provider = make(fake)
    data = bytes(range(256)) * 17  # 4352 bytes: five parts

    async def scenario() -> tuple[str, Any]:
        sandbox_id = await new_box(provider)
        return sandbox_id, await provider.files_write(
            sandbox_id, "/tmp/big", stream(data, 700), size=len(data)
        )

    sandbox_id, info = run(scenario())
    assert info.size == len(data)
    assert fake.files == {(sandbox_id, "/tmp/big"): data}  # the whole file, and no part left
    puts = [r for r in fake.requests if r.method == "PUT"]
    assert len(puts) == 5 and all(len(r.content) <= 1000 for r in puts)
    assert all(r.url.params["path"].startswith("/tmp/big.sps-part-") for r in puts)


def test_a_part_that_fails_leaves_no_file_and_no_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transfer, "WRITE_PART", 1000)
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> Any:
        sandbox_id = await new_box(provider)
        return await provider.files_write(sandbox_id, "/tmp/big", stream(b"x" * 2500), size=3000)

    error = raises("invalid_request", scenario())  # the stream ended 500 bytes early
    assert error.outcome == "unknown"
    assert fake.files == {}


def test_a_large_file_is_read_in_slices(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transfer, "READ_PART_MIB", 1)
    monkeypatch.setattr(transfer, "READ_PART", 1 << 20)
    fake = FakeGroundbox()
    provider = make(fake)
    data = bytes(range(256)) * 10240  # 2.5 MiB: three slices

    async def scenario() -> tuple[str, int | None, bytes]:
        sandbox_id = await new_box(provider)
        fake.files[(sandbox_id, "/tmp/big")] = data
        found = await provider.files_read(sandbox_id, "/tmp/big")
        return sandbox_id, found.size, await drain(found.chunks)

    sandbox_id, size, back = run(scenario())
    assert (size, back == data) == (len(data), True)
    assert fake.files == {(sandbox_id, "/tmp/big"): data}  # the temporary slice is gone
    assert sum(1 for body in fake.execs if body["args"][2] == "dd") == 3


def test_read_errors() -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def read(path: str) -> Any:
        return await provider.files_read(await new_box(provider), path)

    raises("not_found", read("/sps-test-no-such-file"))
    raises("invalid_request", read("/tmp"))


def test_write_checks_its_limits_before_it_calls() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    raises("too_large", provider.files_write("sbx_1", "/a", stream(b""), size=FILE_BYTES + 1))
    raises("unsupported", provider.files_write("sbx_1", "/a", stream(b""), size=0, user="root"))
    assert fake.requests == []


# -- background processes -----------------------------------------------------


def test_a_process_is_a_background_command_with_the_longest_timeout() -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> Any:
        request = ProcessStartRequest(argv=["sleep", "1"], cwd="/tmp", env={"A": "1"})
        return await provider.process_start(await new_box(provider), request)

    process = run(scenario())
    assert process.started_at == datetime(2026, 11, 23, 10, 0, tzinfo=UTC)
    assert fake.commands[process.id]["body"] == {
        "args": ["sleep", "1"],
        "env": {"A": "1"},
        "timeout_s": MAX_PROCESS_S,
        "cwd": "/tmp",
    }


def test_process_start_refusals() -> None:
    provider = make(FakeGroundbox())

    async def start(**fields: Any) -> Any:
        return await provider.process_start(
            await new_box(provider), ProcessStartRequest(argv=["x"], **fields)
        )

    raises("unsupported", start(stdin=True))
    raises("unsupported", start(user="root"))
    raises("not_found", start(cwd="/sps-test-no-such-directory"))


def test_process_output_polls_until_there_is_output() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    naps: list[float] = []

    async def nap(seconds: float) -> None:
        naps.append(seconds)

    provider._sleep = nap  # pylint: disable=protected-access

    async def scenario() -> tuple[Any, Any, Any]:
        sandbox_id = await new_box(provider)
        process = await provider.process_start(sandbox_id, ProcessStartRequest(argv=["x"]))
        fake.commands[process.id]["pages"] = [
            {},
            {},
            {"stdout": b64(b"out"), "stderr": b64(b"err"), "seq": 16},
            {"seq": 16},
            {"stdout": b64(b"more"), "seq": 30, "skipped_bytes": 5, "done": True, "exit_code": 7},
        ]
        first = await provider.process_output(sandbox_id, process.id, wait_ms=30_000)
        idle = await provider.process_output(sandbox_id, process.id, cursor=first.next_cursor)
        last = await provider.process_output(
            sandbox_id, process.id, cursor=idle.next_cursor, wait_ms=5
        )
        return first, idle, last

    first, idle, last = run(scenario())
    assert [(c.stream, c.data) for c in first.chunks] == [("stdout", "out"), ("stderr", "err")]
    assert (first.next_cursor, first.state, first.exit_code, first.gap) == (
        "16",
        "running",
        None,
        False,
    )
    assert naps == [0.1, 0.2]
    assert (list(idle.chunks), idle.next_cursor, idle.state) == ([], "16", "running")
    assert (last.state, last.exit_code, last.gap, last.stdout_closed) == ("exited", 7, True, True)
    assert fake.requests[-1].url.params["seq"] == "16"


def test_process_output_errors() -> None:
    provider = make(FakeGroundbox())

    async def output(process_id: str, cursor: str | None = None) -> Any:
        return await provider.process_output(await new_box(provider), process_id, cursor=cursor)

    raises("not_found", output("ex_nope"))
    raises("invalid_request", output("ex_nope", cursor="not-a-cursor"))


# -- cost ---------------------------------------------------------------------


def test_cost_is_priced_from_the_usage_record() -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> tuple[Any, Any, Any]:
        sandbox_id = await new_box(provider, resources={"cpu_min": 4})
        empty = await provider.cost(sandbox_id)
        fake.usage = {"running_seconds": 90.5, "storage_gb_hours": 0}
        running = await provider.cost(sandbox_id)
        await provider.destroy(sandbox_id)
        return empty, running, await provider.cost(sandbox_id)

    empty, running, final = run(scenario())
    assert (empty.status, empty.total_usd, list(empty.lines)) == ("accruing", None, [])
    assert (running.status, running.source) == ("accruing", "calculated")
    assert running.as_of == datetime(2026, 11, 23, 10, 5, tzinfo=UTC)  # Groundbox's own clock
    assert running.lines[0].end is None
    line = final.lines[0]
    assert (final.status, line.item, line.unit) == ("final", "cpu", "vcpu_hour")
    assert line.quantity == Decimal("0.100555555556")  # 4 vCPUs for 90.5 s, in hours
    assert line.amount_usd == Decimal("0.00402222") == final.total_usd
    assert abs(line.amount_usd - line.quantity * Decimal("0.04")) <= Decimal("0.000000005")
    assert (line.start, line.end) == (datetime(2026, 11, 23, 10, 0, tzinfo=UTC),) * 2
    assert final.as_of == line.end


def test_cost_counts_a_kept_disk() -> None:
    fake = FakeGroundbox()
    provider = make(fake)
    fake.usage = {"running_seconds": 3600, "storage_gb_hours": 720}

    async def scenario() -> Any:
        sandbox_id = await new_box(provider)
        await provider.destroy(sandbox_id)
        return await provider.cost(sandbox_id)

    cost = run(scenario())
    assert [(l.item, l.amount_usd) for l in cost.lines] == [
        ("cpu", Decimal("0.08000000")),
        ("disk", Decimal("0.05000000")),
    ]
    assert cost.total_usd == Decimal("0.13000000")


# -- errors -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "native", "code", "outcome"),
    [
        (400, "bad_request", "invalid_request", "failed"),
        (401, "unauthorized", "unauthenticated", "failed"),
        (403, "missing_scope", "forbidden", "failed"),
        (402, "insufficient_balance", "quota_exceeded", "failed"),
        (402, "trial_limit", "quota_exceeded", "failed"),
        (429, "concurrency_limit", "quota_exceeded", "failed"),
        (429, "rate_limited", "rate_limited", "failed"),
        (503, "no_capacity", "unavailable", "unknown"),
        (503, "busy", "unavailable", "unknown"),
        (504, "timeout", "timeout", "unknown"),
        (500, "internal", "provider_error", "unknown"),
        (418, "a_code_from_the_future", "provider_error", "unknown"),
    ],
)
def test_error_codes(status: int, native: str, code: str, outcome: str) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        body = {"error": {"code": native, "message": "why", "request_id": "req_1"}}
        return httpx.Response(status, json=body, headers={"Retry-After": "7"})

    provider = GroundboxProvider({"api_key": KEY}, transport=httpx.MockTransport(handler))
    error = raises(code, provider.get("sbx_1"))
    assert (error.outcome, error.retry_after_seconds, error.message) == (outcome, 7, "why")


def test_an_answer_without_an_error_body_carries_only_the_status() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(413, text="<html>Request Entity Too Large " + KEY + "</html>")

    provider = GroundboxProvider({"api_key": KEY}, transport=httpx.MockTransport(handler))
    error = raises("too_large", provider.get("sbx_1"))
    assert error.message == "Groundbox answered HTTP 413"


def test_a_dropped_connection_has_an_unknown_outcome() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route")

    provider = GroundboxProvider({"api_key": KEY}, transport=httpx.MockTransport(handler))
    assert raises("unavailable", provider.get("sbx_1")).outcome == "unknown"


def test_every_call_goes_to_the_declared_host() -> None:
    fake = FakeGroundbox()
    provider = make(fake)

    async def scenario() -> None:
        sandbox_id = await new_box(provider)
        await provider.run(sandbox_id, RunRequest(argv=["true"]))
        await provider.files_write(sandbox_id, "/tmp/a", stream(b"x"), size=1)
        await provider.cost(sandbox_id)
        await provider.destroy(sandbox_id)
        await provider.aclose()

    run(scenario())
    assert {request.url.host for request in fake.requests} == set(GroundboxProvider.api_domains)
    assert {request.url.scheme for request in fake.requests} == {"https"}
