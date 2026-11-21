# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Offline tests: the adapter against a small in-memory stand-in for Blaxel's two hosts."""

from __future__ import annotations

import asyncio
import json
import shlex
import subprocess
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pytest
from sandbox_provider_spec.v1 import (
    CreateRequest,
    ProcessStartRequest,
    RunRequest,
    SpsError,
    TerminalRequest,
)

from sps_provider_blaxel import BlaxelProvider
from sps_provider_blaxel.create_spec import sandbox_name

DATE = "Sat, 21 Nov 2026 01:21:30 GMT"
RUNTIME = "https://sbx-{name}-empkgz.us-pdx-1.bl.run"


class FakeBlaxel:
    """Blaxel's control plane and sandbox runtime, in memory."""

    def __init__(self) -> None:
        self.sandboxes: dict[str, dict[str, Any]] = {}
        self.processes: dict[str, dict[str, Any]] = {}
        self.files: dict[str, bytes] = {}
        self.requests: list[httpx.Request] = []
        self.metrics: list[dict[str, Any]] = []
        self.runtime_url: str | None = None  # overrides the runtime URL of a new sandbox
        self.fail_next: dict[str, httpx.Response] = {}
        self.updates: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.stdin: dict[str, bytes] = {}
        self.preview_records: dict[str, dict[str, Any]] = {}
        self.files_dir: Path | None = None  # where a real `sh -c` command runs

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    @staticmethod
    def _json(body: Any, status: int = 200, **headers: str) -> httpx.Response:
        return httpx.Response(status, json=body, headers={"date": DATE, **headers})

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        key = f"{request.method} {request.url.path}"
        if key in self.fail_next:
            return self.fail_next.pop(key)
        if request.url.host == "api.blaxel.ai":
            return self.control(request)
        return self.runtime(request)

    # -- control plane --------------------------------------------------------

    def control(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path.removeprefix("/v0"), request.method
        if request.headers["x-blaxel-authorization"] != "Bearer key-1":
            return self._json({"code": 401, "error": "Unauthorized"}, 401)
        if path == "/workspaces":
            return self._json([{"name": "ws-1", "id": "EMPKGZ"}])
        assert request.headers["x-blaxel-workspace"] == "ws-1"
        if path == "/locations":
            return self._json([{"region": "us-pdx-1"}, {"region": "eu-lon-1"}])
        if path == "/accounts":
            return self._json([{"metadata": {"id": "acct-1"}}])
        if path == "/accounts/acct-1/metrics":
            return self._json({"data": self.metrics, "meta": {"hasMore": False}})
        if path == "/sandboxes" and method == "POST":
            body = json.loads(request.content)
            name = body["metadata"]["name"]
            if name in self.sandboxes:
                return self._json(
                    {"code": "SANDBOX_ALREADY_EXISTS", "message": "exists", "status_code": 409}, 409
                )
            url = self.runtime_url or RUNTIME.format(name=name)
            self.sandboxes[name] = {
                "status": "DEPLOYED",
                "metadata": {
                    **body["metadata"],
                    "createdAt": "2026-11-21T01:18:37.790101021Z",
                    "updatedAt": "2026-11-21T01:18:37.790101021Z",
                    "url": url,
                },
                "spec": {"region": "us-pdx-1", **body["spec"]},
            }
            ttl = body["spec"]["runtime"].get("ttl")
            if ttl:
                self.sandboxes[name]["expiresIn"] = int(ttl.removesuffix("s")) - 10
            return self._json(self.sandboxes[name])
        if path == "/sandboxes":
            data = list(self.sandboxes.values())
            return self._json({"data": data, "meta": {"hasMore": False}})
        name = path.removeprefix("/sandboxes/")
        if "/previews" in name:
            return self.previews(request, name.split("/previews")[0], method)
        record = self.sandboxes.get(name)
        if record is None:
            return self._json({"code": 404, "error": "Sandbox not found"}, 404)
        if method == "PUT":
            body = json.loads(request.content)
            self.updates.append(body)
            record["spec"] = body["spec"]
            record["expiresIn"] = int(body["spec"]["runtime"]["ttl"].removesuffix("s")) - 10
        if method == "DELETE":
            record["status"] = "DELETING"
            record["metadata"]["updatedAt"] = "2026-11-21T01:21:08.861739444Z"
        return self._json(record)

    def previews(self, request: httpx.Request, sandbox: str, method: str) -> httpx.Response:
        if sandbox not in self.sandboxes:
            return self._json({"code": 404, "error": "Sandbox not found"}, 404)
        if method == "POST":
            body = json.loads(request.content)
            name = body["metadata"]["name"]
            if name in self.preview_records:
                return self._json({"code": 409, "error": "Resource already exists"}, 409)
            self.preview_records[name] = {
                "metadata": {"name": name},
                "spec": {**body["spec"], "url": f"https://pv{body['spec']['port']}.preview.bl.run"},
            }
            return self._json(self.preview_records[name])
        name = request.url.path.rsplit("/", 1)[1]
        found = self.preview_records.get(name)
        if found is None:
            return self._json({"code": 404, "error": "Preview not found"}, 404)
        return self._json(found)

    # -- runtime ----------------------------------------------------------------

    def runtime(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if path == "/process" and method == "POST":
            body = json.loads(request.content)
            if "# sps-directory" in body["command"] and body.get("waitForCompletion"):
                done = subprocess.run(
                    shlex.split(body["command"]),
                    capture_output=True,
                    text=True,
                    cwd=self.files_dir,
                    check=False,
                )
                return self._json(
                    {
                        "name": body["name"],
                        "status": "completed" if done.returncode == 0 else "failed",
                        "exitCode": done.returncode,
                        "stdout": done.stdout,
                        "stderr": done.stderr,
                    }
                )
            info = {
                "pid": str(len(self.processes) + 1),
                "name": body["name"],
                "command": body["command"],
                "status": "completed",
                "exitCode": 0,
                "stdout": "hello\n",
                "stderr": "",
                "startedAt": "Sat, 21 Nov 2026 01:18:39 GMT",
            }
            if "exit 3" in body["command"]:
                info.update(status="failed", exitCode=3, stderr="err\n")
            if "sleep 99" in body["command"] and body.get("waitForCompletion"):
                self.processes[body["name"]] = {**info, "status": "killed", "exitCode": -1}
                return self._json({"error": "process timed out after 1 seconds"}, 422)
            if not body.get("waitForCompletion"):
                info.update(status="running", exitCode=0, stdout="", stderr="")
                if body.get("stdin"):
                    self.stdin[body["name"]] = b""
                else:
                    info["_then"] = {"status": "completed", "stdout": "late\n"}
            self.processes[body["name"]] = info
            return self._json(info)
        if path.startswith("/process/") and path.endswith(("/stdin", "/kill")):
            name, verb = path.removeprefix("/process/").rsplit("/", 1)
            info = self.processes.get(name)
            if info is None:
                return self._json({"error": f"process with identifier {name} not found"}, 404)
            if verb == "stdin" and method == "POST":
                self.stdin[name] += request.content
                info["stdout"] += request.content.decode()
            elif verb == "stdin":
                info.update(status="completed", stdin_closed=True)
            elif info["status"] != "running":
                return self._json({"error": f"process with Identifier {name} is not running"}, 404)
            else:
                info.update(status="killed", exitCode=-1)
            return self._json({"message": "ok"})
        if path.startswith("/process/"):
            name = path.removeprefix("/process/").removesuffix("/logs").removesuffix("/kill")
            info = self.processes.get(name)
            if info is None:
                return self._json({"error": "process not found"}, 404)
            if path.endswith("/logs"):
                return self._json({"stdout": info["stdout"], "stderr": info["stderr"]})
            if "_then" in info and method == "GET":
                if info.pop("_seen", False):
                    info.update(info.pop("_then"), exitCode=0)
                else:
                    info["_seen"] = True
            return self._json(info)
        if path.startswith("/filesystem/"):
            key = path.removeprefix("/filesystem/")
            if method == "PUT":
                boundary = request.headers["content-type"].split("boundary=")[1].encode()
                part = request.content.split(b'filename="content"\r\n')[1]
                content = part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n--" + boundary + b"--", 1)[0]
                assert int(request.headers["content-length"]) == len(request.content)
                self.files["/" + key.lstrip("/")] = content
                return self._json({"path": key, "message": "uploaded"})
            if method == "DELETE":
                self.deleted.append("/" + key.lstrip("/"))
                return self._json({"message": "deleted"})
            if key.lstrip("/") == "dir":
                return self._json({"files": [], "subdirectories": []})
            if "download" not in request.url.params and "/" + key.lstrip("/") in self.files:
                text = self.files["/" + key.lstrip("/")].decode()
                return self._json({"path": key, "content": text})
            content = self.files.get("/" + key.lstrip("/"))
            if content is None:
                return self._json({"error": "no such file"}, 404)
            return httpx.Response(
                200, content=content, headers={"content-disposition": 'attachment; filename="f"'}
            )
        return self._json({"error": "unknown"}, 404)


@pytest.fixture
def fake() -> FakeBlaxel:
    return FakeBlaxel()


@pytest.fixture
def provider(fake: FakeBlaxel) -> BlaxelProvider:
    return BlaxelProvider({"api_key": "key-1"}, transport=fake.transport())


def run(coro: Any) -> Any:
    return asyncio.run(coro)


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


def test_discovery_reports_unavailable_regions(provider: BlaxelProvider) -> None:
    found = run(provider.discovery())
    klass = found.classes[0]
    assert found.spec_version == "1.0"
    assert set(klass.unavailable_regions) == {"blaxel:us-was-1", "blaxel:eu-fra-1"}
    assert found.limits.cost_final_within_seconds is not None


def test_create_translates_the_request(provider: BlaxelProvider, fake: FakeBlaxel) -> None:
    request = CreateRequest(
        resources={"cpu_min": 3, "region": "blaxel:eu-lon-1"},
        requirements={"idle_shutdown": {"on_idle": "paused"}},
        lifetime_seconds=900,
        env={"A": "1"},
        labels={"team/x": "y"},
    )
    box = run(provider.create(request, idempotency_key="k1"))
    body = json.loads(next(r for r in fake.requests if r.method == "POST").content)
    assert body["metadata"]["name"] == sandbox_name("k1") == box.id
    assert body["metadata"]["labels"] == {"team/x": "y"}
    assert body["spec"]["region"] == "eu-lon-1"
    assert body["spec"]["runtime"] == {
        "image": "blaxel/base-image:latest",
        "memory": 8192,
        "ttl": "900s",
        "envs": [{"name": "A", "value": "1"}],
    }
    assert (box.size.cpu, box.size.memory_mib, box.image) == (4, 8192, "blaxel-base")
    assert box.labels == {"team/x": "y"}
    assert box.at_end == "destroyed" and box.ends_at is not None


def test_replayed_create_returns_the_first_sandbox(provider: BlaxelProvider) -> None:
    first = run(provider.create(CreateRequest(), idempotency_key="k"))
    again = run(provider.create(CreateRequest(), idempotency_key="k"))
    assert again == first
    with pytest.raises(SpsError) as error:
        run(provider.create(CreateRequest(labels={"a": "b"}), idempotency_key="k"))
    assert error.value.code == "conflict"


@pytest.mark.parametrize(
    "request_",
    [
        CreateRequest(resources={"cpu_min": 64}),
        CreateRequest(lifetime_seconds=60),
        CreateRequest(at_end="paused"),
        CreateRequest(requirements={"network_lockdown": {}}),
        CreateRequest(requirements={"idle_shutdown": {"on_idle": "destroyed"}}),
        CreateRequest(resources={"region": "blaxel:mars-1"}),
        CreateRequest(image="nope"),
        CreateRequest(snapshot="s1"),
    ],
)
def test_create_refuses_what_it_cannot_meet(
    provider: BlaxelProvider, fake: FakeBlaxel, request_: CreateRequest
) -> None:
    with pytest.raises(SpsError) as error:
        run(provider.create(request_, idempotency_key="k"))
    assert error.value.code == "unsupported"
    assert not any(r.method == "POST" for r in fake.requests)


def test_create_with_a_provider_artifact(provider: BlaxelProvider, fake: FakeBlaxel) -> None:
    request = CreateRequest(
        requirements={"custom_images": {}},
        image={"provider_artifact": "sandbox/mine:v1", "provider": "blaxel"},
    )
    box = run(provider.create(request, idempotency_key="k"))
    assert fake.sandboxes[box.id]["spec"]["runtime"]["image"] == "sandbox/mine:v1"
    assert box.image is None


def test_a_runtime_host_that_is_not_blaxel_never_gets_the_key(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    fake.runtime_url = "https://sbx-x-empkgz.us-pdx-1.evil.example"
    with pytest.raises(SpsError) as error:
        run(provider.create(CreateRequest(), idempotency_key="k"))
    assert error.value.code == "provider_error"
    assert not any(r.url.host.endswith("evil.example") for r in fake.requests)


def test_get_list_and_destroy(provider: BlaxelProvider) -> None:
    one = run(provider.create(CreateRequest(labels={"t": "1"}), idempotency_key="a"))
    run(provider.create(CreateRequest(labels={"t": "2"}), idempotency_key="b"))
    assert run(provider.get(one.id)) == one
    assert [b.id for b in run(provider.list(labels={"t": "1"})).sandboxes] == [one.id]
    assert run(provider.destroy(one.id)).state == "destroyed"
    assert run(provider.destroy(one.id)).state == "destroyed"
    assert run(provider.get(one.id)).state == "destroyed"
    assert [b.labels["t"] for b in run(provider.list()).sandboxes] == ["2"]
    with pytest.raises(SpsError) as error:
        run(provider.get("missing"))
    assert error.value.code == "not_found"


def test_errors_carry_codes_and_never_a_body(provider: BlaxelProvider, fake: FakeBlaxel) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    fake.fail_next[f"GET /v0/sandboxes/{box.id}"] = httpx.Response(
        429, text="secret body", headers={"retry-after": "7"}
    )
    with pytest.raises(SpsError) as error:
        run(provider.get(box.id))
    assert (error.value.code, error.value.retry_after_seconds) == ("rate_limited", 7)
    assert "secret" not in str(error.value)
    fake.fail_next[f"GET /v0/sandboxes/{box.id}"] = httpx.Response(500, text="boom")
    with pytest.raises(SpsError) as error:
        run(provider.get(box.id))
    assert (error.value.code, error.value.outcome) == ("provider_error", "unknown")
    with pytest.raises(SpsError) as error:
        run(BlaxelProvider({"api_key": "wrong"}, transport=fake.transport()).discovery())
    assert error.value.code == "unauthenticated"


def test_run_returns_output_and_the_exit_code(provider: BlaxelProvider, fake: FakeBlaxel) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    result = run(provider.run(box.id, RunRequest(argv=["echo", "hello world"], env={"X": "1"})))
    assert (result.exit_code, result.stdout, result.timed_out) == (0, "hello\n", False)
    post = next(r for r in fake.requests if r.url.path == "/process")
    sent = json.loads(post.content)
    assert sent["command"] == "echo 'hello world'" and sent["env"] == {"X": "1"}
    assert sent["waitForCompletion"] is True and sent["timeout"] == 60
    failed = run(provider.run(box.id, RunRequest(argv=["sh", "-c", "exit 3"])))
    assert (failed.exit_code, failed.stderr) == (3, "err\n")


def test_run_truncates_output_and_reports_a_timeout(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    cut = run(provider.run(box.id, RunRequest(argv=["echo"], output_limit_bytes=3)))
    assert (cut.stdout, cut.stdout_truncated) == ("hel", True)
    late = run(provider.run(box.id, RunRequest(argv=["sleep", "99"], timeout_ms=1000)))
    assert (late.timed_out, late.exit_code) == (True, None)


def test_run_in_a_missing_directory_is_not_found(provider: BlaxelProvider) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    with pytest.raises(SpsError) as error:
        run(provider.run(box.id, RunRequest(argv=["pwd"], cwd="/nope")))
    assert error.value.code == "not_found"
    assert run(provider.run(box.id, RunRequest(argv=["pwd"], cwd="/dir"))).exit_code == 0


def test_files_round_trip_and_a_directory_is_refused(provider: BlaxelProvider) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    info = run(provider.files_write(box.id, "/tmp/a.txt", _chunks(b"hel", b"lo"), size=5))
    assert (info.path, info.size) == ("/tmp/a.txt", 5)

    async def read(path: str) -> tuple[int | None, bytes]:
        stream = await provider.files_read(box.id, path)
        return stream.size, b"".join([c async for c in stream.chunks])

    assert run(read("/tmp/a.txt")) == (5, b"hello")
    with pytest.raises(SpsError) as error:
        run(read("/tmp/missing"))
    assert error.value.code == "not_found"
    with pytest.raises(SpsError) as error:
        run(provider.files_write(box.id, "/tmp/b", _chunks(b"ab"), size=5))
    assert (error.value.code, error.value.outcome) == ("invalid_request", "unknown")


def test_a_background_process_is_followed_to_its_exit(provider: BlaxelProvider) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    process = run(provider.process_start(box.id, ProcessStartRequest(argv=["sleep", "2"])))
    assert process.started_at.year == 2026
    first = run(provider.process_output(box.id, process.id))
    assert (first.state, first.exit_code, first.chunks) == ("running", None, [])
    done = run(provider.process_output(box.id, process.id, cursor=first.next_cursor))
    assert (done.state, done.exit_code, done.stdout_closed) == ("exited", 0, True)
    assert [(c.stream, c.data) for c in done.chunks] == [("stdout", "late\n")]
    again = run(provider.process_output(box.id, process.id, cursor=done.next_cursor))
    assert again.chunks == [] and again.gap is False


def _series(*costs: str) -> list[dict[str, Any]]:
    return [
        {
            "resourceType": "sandbox",
            "resourceName": sandbox_name("k"),
            "timeseries": [
                {"timestamp": f"2026-11-21T0{hour + 1}:00:00Z", "cost": cost}
                for hour, cost in enumerate(costs)
            ],
        }
    ]


def test_cost_is_accruing_while_it_runs_and_final_once_settled(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    nothing = run(provider.cost(box.id))
    assert (nothing.status, nothing.total_usd, nothing.lines) == ("accruing", None, ())
    fake.metrics = _series("0.000828")
    running = run(provider.cost(box.id))
    assert (running.status, running.source) == ("accruing", "reported")
    assert running.total_usd == Decimal("0.000828") and len(running.lines) == 1
    assert running.lines[0].item == "memory" and running.lines[0].start is not None
    run(provider.destroy(box.id))
    # Blaxel's clock reads 01:21:30 and the sandbox ended at 01:21:08: not yet settled.
    assert run(provider.cost(box.id)).status == "accruing"
    import sps_provider_blaxel as module

    module._COST_SETTLE = module._COST_SETTLE.__class__(seconds=10)
    done = run(provider.cost(box.id))
    assert (done.status, done.total_usd) == ("final", Decimal("0.000828"))


def test_what_it_does_not_claim_is_unsupported(provider: BlaxelProvider) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    for call in (provider.pause(box.id), provider.snapshot_get("x"), provider.tcp_link(box.id, 80)):
        with pytest.raises(SpsError) as error:
            run(call)
        assert error.value.code == "unsupported"


# -- the optional 1.0 sections ----------------------------------------------------------------


def test_discovery_claims_the_sections_it_serves(provider: BlaxelProvider) -> None:
    klass = run(provider.discovery()).classes[0]
    assert klass.gates is not None
    assert klass.gates["processes"] == [{"input": True, "cancel": True, "reattach": True}]
    assert klass.gates["previews"] == [
        {
            "access": "public",
            "revocable": True,
            "max_ports": {"at_least": 2},
            "reserved_ports": [8080],
        },
        {
            "access": "private",
            "credential": ["link", "header"],
            "revocable": True,
            "max_ports": {"at_least": 2},
            "reserved_ports": [8080],
        },
    ]
    assert klass.gates["user_selection"] == [
        {"commands": True, "files_write": True, "process_start": True}
    ]
    assert klass.gates["terminal"] == [{}]
    assert klass.gates["hidden_secrets"] == [
        {
            "rotates": "unknown",
            "scrubs_responses": False,
            "injection": "sets_header",
            "intercepts": "all_https",
            "transparent": False,
        }
    ]
    assert set(klass.operations) == {
        "files.list",
        "files.mkdir",
        "files.remove",
        "files.exists",
        "run.base64",
        "labels.update",
        "image.prepare",
        "image.get",
        "image.list",
        "image.delete",
    }
    assert klass.resources["extendable_lifetime"] is True


def test_lifetime_extend_writes_the_age_so_far_plus_the_time_asked(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(
        provider.create(CreateRequest(lifetime_seconds=300, labels={"a": "b"}), idempotency_key="k")
    )
    # The fake's record says 290 s are left, so the sandbox is 10 s old.
    longer = run(provider.lifetime_extend(box.id, 600))
    sent = fake.updates[0]
    assert sent["spec"]["runtime"]["ttl"] == "615s"  # 10 + 600, and the 5 s margin
    assert sent["metadata"]["labels"] == {"a": "b"} and sent["metadata"]["name"] == box.id
    assert sent["spec"]["runtime"]["image"] == "blaxel/base-image:latest"
    assert longer.ends_at is not None and box.ends_at is not None
    assert (longer.ends_at - box.ends_at).total_seconds() == 315
    # An extension never shortens: 605 s are left now, so a request for 100 s changes nothing.
    same = run(provider.lifetime_extend(box.id, 100))
    assert len(fake.updates) == 1 and same.ends_at == longer.ends_at


def test_lifetime_extend_leaves_a_sandbox_with_no_timer_alone(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(provider.create(CreateRequest(lifetime_seconds=None), idempotency_key="k"))
    assert run(provider.lifetime_extend(box.id, 600)).ends_at is None
    assert fake.updates == []
    run(provider.destroy(box.id))
    with pytest.raises(SpsError) as error:
        run(provider.lifetime_extend(box.id, 600))
    assert error.value.code == "conflict"


def test_stdin_is_written_closed_and_the_process_cancelled(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    process = run(provider.process_start(box.id, ProcessStartRequest(argv=["cat"], stdin=True)))
    start = next(r for r in fake.requests if r.method == "POST" and r.url.path == "/process")
    assert json.loads(start.content)["stdin"] is True
    run(provider.process_write_stdin(box.id, process.id, b"hello\n"))
    assert fake.stdin[process.id] == b"hello\n"
    posted = next(r for r in fake.requests if r.url.path.endswith("/stdin"))
    assert posted.headers["content-type"] == "application/octet-stream"
    run(provider.process_close_stdin(box.id, process.id))
    run(provider.process_close_stdin(box.id, process.id))  # closing twice succeeds
    assert fake.processes[process.id]["stdin_closed"] is True
    # A process without stdin starts without the flag.
    run(provider.process_start(box.id, ProcessStartRequest(argv=["sleep", "2"])))
    plain = [r for r in fake.requests if r.method == "POST" and r.url.path == "/process"][-1]
    assert "stdin" not in json.loads(plain.content)


def test_cancel_ends_a_running_process_and_is_done_for_one_that_ended(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    process = run(provider.process_start(box.id, ProcessStartRequest(argv=["sleep", "99"])))
    run(provider.process_cancel(box.id, process.id))
    assert fake.processes[process.id]["status"] == "killed"
    gone = run(provider.process_output(box.id, process.id))
    assert (gone.state, gone.exit_code) == ("exited", 137)
    run(provider.process_cancel(box.id, process.id))  # not running any more: done
    with pytest.raises(SpsError) as error:
        run(provider.process_cancel(box.id, "no-such-process"))
    assert error.value.code == "not_found"
    with pytest.raises(SpsError) as error:
        run(provider.process_write_stdin(box.id, "no-such-process", b"x"))
    assert error.value.code == "not_found"


def test_preview_link_is_public_and_made_once_per_port(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    link = run(provider.preview_link(box.id, 3000))
    assert (link.url, dict(link.headers), link.expires_at) == (
        "https://pv3000.preview.bl.run",
        {},
        None,
    )
    assert fake.preview_records["sps-3000-public"]["spec"] == {
        "port": 3000,
        "public": True,
        "url": "https://pv3000.preview.bl.run",
    }
    assert run(provider.preview_link(box.id, 3000)) == link
    posts = [r for r in fake.requests if r.method == "POST" and r.url.path.endswith("/previews")]
    assert len(posts) == 1
    with pytest.raises(SpsError) as error:
        run(provider.preview_link(box.id, 8080))
    assert error.value.code == "unsupported"
    with pytest.raises(SpsError) as error:
        run(provider.preview_link("missing", 3000))
    assert error.value.code == "not_found"


@pytest.fixture
def tree(fake: FakeBlaxel, tmp_path: Path) -> Path:
    """The directory operations run their real scripts, in a temporary directory."""
    fake.files_dir = tmp_path
    return tmp_path


def _sandbox(provider: BlaxelProvider) -> str:
    return run(provider.create(CreateRequest(), idempotency_key="k")).id


def test_files_list_gives_name_order_types_and_sizes_and_never_follows_a_link(
    provider: BlaxelProvider, tree: Path
) -> None:
    (tree / "a").mkdir()
    (tree / "b").write_text("12345")
    (tree / "c").symlink_to("/nonexistent")
    subprocess.run(["mkfifo", str(tree / "d")], check=True)
    (tree / "é").write_text("x")
    box = _sandbox(provider)
    page = run(provider.files_list(box, str(tree)))
    assert [(e.name, e.type) for e in page.entries] == [
        ("a", "directory"),
        ("b", "file"),
        ("c", "symlink"),
        ("d", "other"),
        ("é", "file"),
    ]
    assert next(e for e in page.entries if e.name == "b").size == 5
    assert next(e for e in page.entries if e.name == "c").size == len("/nonexistent")
    assert page.next_cursor is None


def test_files_list_pages_follow_the_last_name(provider: BlaxelProvider, tree: Path) -> None:
    for i in range(1, 8):
        (tree / f"f{i:03d}").write_text("")
    box = _sandbox(provider)
    first = run(provider.files_list(box, str(tree), limit=3))
    assert [e.name for e in first.entries] == ["f001", "f002", "f003"]
    second = run(provider.files_list(box, str(tree), limit=3, cursor=first.next_cursor))
    assert [e.name for e in second.entries] == ["f004", "f005", "f006"]
    last = run(provider.files_list(box, str(tree), limit=3, cursor=second.next_cursor))
    assert ([e.name for e in last.entries], last.next_cursor) == (["f007"], None)
    for bad in (0, 1000):
        with pytest.raises(SpsError) as error:
            run(provider.files_list(box, str(tree), limit=bad))
        assert error.value.code == "invalid_request"
    with pytest.raises(SpsError) as error:
        run(provider.files_list(box, str(tree), cursor="***"))
    assert error.value.code == "invalid_request"


def test_files_list_errors(provider: BlaxelProvider, tree: Path) -> None:
    (tree / "file").write_text("x")
    box = _sandbox(provider)
    with pytest.raises(SpsError) as error:
        run(provider.files_list(box, str(tree / "missing")))
    assert error.value.code == "not_found"
    with pytest.raises(SpsError) as error:
        run(provider.files_list(box, str(tree / "file")))
    assert error.value.code == "invalid_request"
    with pytest.raises(SpsError) as error:
        run(provider.files_list(box, ""))
    assert error.value.code == "invalid_request"


def test_files_mkdir(provider: BlaxelProvider, tree: Path) -> None:
    (tree / "file").write_text("x")
    box = _sandbox(provider)
    with pytest.raises(SpsError) as error:
        run(provider.files_mkdir(box, str(tree / "x/y")))
    assert error.value.code == "not_found"
    run(provider.files_mkdir(box, str(tree / "x/y"), parents=True))
    assert (tree / "x/y").is_dir()
    run(provider.files_mkdir(box, str(tree / "x/y"), parents=True))
    run(provider.files_mkdir(box, str(tree / "x/y")))  # an existing directory is success
    for path, parents in ((tree / "file", False), (tree / "file/z", True)):
        with pytest.raises(SpsError) as error:
            run(provider.files_mkdir(box, str(path), parents=parents))
        assert error.value.code == "conflict"


def test_files_remove(provider: BlaxelProvider, tree: Path) -> None:
    (tree / "full/sub").mkdir(parents=True)
    (tree / "full/sub/f").write_text("x")
    (tree / "target").mkdir()
    (tree / "target/keep").write_text("keep")
    (tree / "link").symlink_to(tree / "target")
    (tree / "file").write_text("x")
    box = _sandbox(provider)
    run(provider.files_remove(box, str(tree / "file")))
    assert not (tree / "file").exists()
    with pytest.raises(SpsError) as error:
        run(provider.files_remove(box, str(tree / "full")))
    assert error.value.code == "conflict" and (tree / "full/sub/f").exists()
    run(provider.files_remove(box, str(tree / "full"), recursive=True))
    assert not (tree / "full").exists()
    run(provider.files_remove(box, str(tree / "link")))
    assert not (tree / "link").is_symlink() and (tree / "target/keep").exists()
    with pytest.raises(SpsError) as error:
        run(provider.files_remove(box, str(tree / "nothing")))
    assert error.value.code == "not_found"
    with pytest.raises(SpsError) as error:
        run(provider.files_remove(box, "/", recursive=True))
    assert error.value.code == "invalid_request"


def test_files_exists_names_the_entry_and_does_not_follow_a_link(
    provider: BlaxelProvider, tree: Path
) -> None:
    (tree / "d").mkdir()
    (tree / "f").write_text("x")
    (tree / "l").symlink_to("/nonexistent")
    box = _sandbox(provider)
    for name, kind in (("f", "file"), ("d", "directory"), ("l", "symlink")):
        found = run(provider.files_exists(box, str(tree / name)))
        assert (found.exists, found.type) == (True, kind)
    for gone in (tree / "none", tree / "f" / "child", tree / "no/parent"):
        found = run(provider.files_exists(box, str(gone)))
        assert (found.exists, found.type) == (False, None)


def test_a_relative_path_is_never_taken_for_an_option(provider: BlaxelProvider, tree: Path) -> None:
    box = _sandbox(provider)
    run(provider.files_mkdir(box, "-rf", parents=True))
    assert (tree / "-rf").is_dir()
    assert run(provider.files_exists(box, "-rf")).type == "directory"


# -- terminals ------------------------------------------------------------------------------------


def test_the_launcher_sets_the_environment_runs_the_program_and_records_its_status() -> None:
    from sps_provider_blaxel.terminal import launcher

    text = launcher(["bash", "-c", "exit 7"], {"A": "x y"}, "vt100", "/tmp/s", "/tmp/p").decode()
    assert text.splitlines() == [
        "#!/bin/sh",
        "echo $$ > /tmp/p",
        "trap : HUP",
        "export TERM=vt100",
        "export A='x y'",
        "bash -c 'exit 7'",
        "code=$?",
        "echo $code > /tmp/s",
        'exit "$code"',
    ]
    assert "bash -l" in launcher(None, {}, "xterm-256color", "/tmp/s", "/tmp/p").decode()
    with pytest.raises(SpsError) as error:
        launcher(None, {"A B": "x"}, "xterm", "/tmp/s", "/tmp/p")
    assert error.value.code == "invalid_request"
    # The script really runs as written, and leaves the status that the program ended with.
    done = subprocess.run(
        [
            "sh",
            "-c",
            launcher(["sh", "-c", "exit 7"], {}, "x", "/dev/stdout", "/dev/null").decode(),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (done.returncode, done.stdout) == (7, "7\n")


class FakeShell:
    """A Blaxel terminal socket on localhost: it echoes input as output, and says how it ends."""

    def __init__(self, ends: tuple[int, str] | None = (1000, "Shell exited")) -> None:
        self.ends = ends
        self.seen: list[dict[str, Any]] = []
        self.port = 0

    async def handler(self, socket: Any) -> None:
        async for message in socket:
            found = json.loads(message)
            self.seen.append(found)
            if found["type"] == "input" and found["data"] == "exit\n":
                break
            if found["type"] == "input":
                await socket.send(json.dumps({"type": "output", "data": found["data"].upper()}))
        if self.ends is not None:
            await socket.close(*self.ends)


async def _serve(shell: FakeShell) -> Any:
    from websockets.asyncio.server import serve

    server = await serve(shell.handler, "127.0.0.1", 0)
    shell.port = next(iter(server.sockets)).getsockname()[1]
    return server


@pytest.mark.integration
@pytest.mark.loopback
def test_a_terminal_carries_input_resize_output_and_the_exit_status() -> None:
    from websockets.asyncio.client import connect

    from sps_provider_blaxel.terminal import BlaxelTerminal

    async def go() -> tuple[list[bytes], int | None, list[dict[str, Any]], list[bool]]:
        shell = FakeShell()
        server = await _serve(shell)
        socket = await connect(f"ws://127.0.0.1:{shell.port}")
        reads, cleaned = [], []

        async def status() -> int | None:
            return 7

        async def cleanup(end_program: bool) -> None:
            cleaned.append(end_program)

        terminal = BlaxelTerminal(socket, status, cleanup)
        await terminal.resize(100, 30)
        await terminal.write("é".encode()[:1])  # half of a character waits for the other half
        await terminal.write("é".encode()[1:] + b"abc\n")
        await terminal.write(b"exit\n")
        reads = [chunk async for chunk in terminal.output()]
        await terminal.close()
        await terminal.close()  # closing twice succeeds
        server.close()
        return reads, terminal.exit_code, shell.seen, cleaned

    reads, exit_code, seen, cleaned = run(go())
    assert (
        b"".join(reads).decode() == "ÉABC\n" and exit_code == 7 and cleaned == [False]
    )  # it exited, so there is nothing to end
    assert seen[0] == {"type": "resize", "cols": 100, "rows": 30}
    assert [m["data"] for m in seen if m["type"] == "input"] == ["éabc\n", "exit\n"]


@pytest.mark.integration
@pytest.mark.loopback
def test_a_terminal_that_is_closed_by_the_sandbox_has_no_exit_status() -> None:
    from websockets.asyncio.client import connect

    from sps_provider_blaxel.terminal import BlaxelTerminal

    async def go() -> int | None:
        shell = FakeShell(ends=(1001, "going away"))
        server = await _serve(shell)
        socket = await connect(f"ws://127.0.0.1:{shell.port}")

        async def status() -> int | None:
            raise AssertionError("the status is read only after 'Shell exited'")

        async def cleanup(end_program: bool) -> None:
            return None

        terminal = BlaxelTerminal(socket, status, cleanup)
        await terminal.write(b"exit\n")
        _ = [chunk async for chunk in terminal.output()]
        await terminal.close()
        server.close()
        return terminal.exit_code

    assert run(go()) is None


@pytest.mark.integration
@pytest.mark.loopback
def test_terminal_open_uploads_the_launcher_and_connects_to_the_runtime_host(
    provider: BlaxelProvider, fake: FakeBlaxel, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sps_provider_blaxel as module

    connected: dict[str, Any] = {}

    async def connect(url: str, **kw: Any) -> Any:
        from websockets.asyncio.client import connect as real

        connected.update(url=url, **kw)
        shell = FakeShell()
        connected["server"] = await _serve(shell)
        return await real(f"ws://127.0.0.1:{shell.port}")

    monkeypatch.setattr(module, "connect", connect)

    async def go() -> str:
        box = await provider.create(CreateRequest(), idempotency_key="k")
        request = TerminalRequest(
            argv=["printf", "hi"], cwd="/dir", env={"K": "v"}, columns=90, rows=20, term="vt100"
        )
        terminal = await provider.terminal_open(box.id, request)
        script = next(p for p in fake.files if p.startswith("/tmp/.sps-terminal-"))
        fake.files[script.replace(".sh", ".pid")] = b"4242\n"  # what the launcher records
        await terminal.close()  # before the program exited: its group is ended
        connected["server"].close()
        return box.id

    box_id = run(go())
    url = connected["url"]
    assert url.startswith(f"wss://sbx-{box_id}-empkgz.us-pdx-1.bl.run/terminal/ws?")
    assert "cols=90" in url and "rows=20" in url and "workingDir=%2Fdir" in url
    assert "key-1" not in url  # the key travels in a header, never in the URL
    assert connected["additional_headers"]["X-Blaxel-Authorization"] == "Bearer key-1"
    assert connected["additional_headers"]["X-Blaxel-Workspace"] == "ws-1"
    script = next(
        p for p in fake.files if p.startswith("/tmp/.sps-terminal-") and p.endswith(".sh")
    )
    assert f"shell={quote(script, safe='')}" in url
    text = fake.files[script].decode()
    assert "export TERM=vt100" in text and "export K=v" in text and "printf hi" in text
    put = next(r for r in fake.requests if r.method == "PUT" and "sps-terminal" in r.url.path)
    assert b"0755" in put.content
    assert script in fake.deleted and script.replace(".sh", ".status") in fake.deleted
    ended = [r for r in fake.requests if r.method == "POST" and r.url.path == "/process"][-1]
    command = json.loads(ended.content)["command"]
    assert "sid=4242;" in command and "kill -HUP $pids" in command and "kill -KILL $pids" in command


def test_terminal_open_refuses_a_missing_directory_before_it_uploads_anything(
    provider: BlaxelProvider, fake: FakeBlaxel
) -> None:
    box = run(provider.create(CreateRequest(), idempotency_key="k"))
    with pytest.raises(SpsError) as error:
        run(provider.terminal_open(box.id, TerminalRequest(cwd="/nope")))
    assert error.value.code == "not_found"
    assert not any(r.method == "PUT" for r in fake.requests)


@pytest.mark.parametrize(
    "requirements",
    [
        {"terminal": {}},
        {"processes": {"input": True, "cancel": True}},
        {"previews": {"access": "public", "ports": [3000]}},
        {"previews": {"access": ["public", "private"], "ports": [3000, 4000]}},
    ],
)
def test_create_accepts_the_sections_it_serves(
    provider: BlaxelProvider, requirements: dict[str, Any]
) -> None:
    assert run(provider.create(CreateRequest(requirements=requirements), idempotency_key="k"))


@pytest.mark.parametrize(
    "requirements",
    [
        {"terminal": {"reattach": True}},
        {"processes": {"reattach": False}},
        {"processes": {"input": False}},
        {"previews": {"credential": "cookie", "ports": [3000]}},
        {"previews": {"access": "public", "ports": [8080]}},
        {"previews": {"access": "public", "websocket": True}},
    ],
)
def test_create_refuses_what_those_sections_do_not_offer(
    provider: BlaxelProvider, requirements: dict[str, Any]
) -> None:
    with pytest.raises(SpsError) as error:
        run(provider.create(CreateRequest(requirements=requirements), idempotency_key="k"))
    assert error.value.code == "unsupported"
