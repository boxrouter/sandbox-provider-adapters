# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Offline tests for the run.cloud adapter.

A small in-memory run.cloud answers the `/run-cloud/sandboxes` routes through
`httpx.MockTransport`. Its exec route runs the adapter's real supervisor script in a local Python
process, with `/tmp/.sps` moved to a temporary directory, so `run` and the process operations are
tested end to end without a sandbox.
"""

from __future__ import annotations

import asyncio
import base64
import json
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from email.utils import format_datetime
from pathlib import Path
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

from sps_provider_runcloud import RuncloudProvider
from sps_provider_runcloud.guest import ROOT

TOKEN = "rc_test_token"


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


class FakeRunCloud:
    """The run.cloud sandbox routes the adapter calls, answered from memory."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.sandboxes: dict[str, dict[str, Any]] = {}
        self.files: dict[tuple[str, str], bytearray] = {}
        self.keys: dict[str, str] = {}
        self.creates: list[dict[str, Any]] = []
        self.now = datetime(2026, 11, 22, 10, 0, tzinfo=UTC)
        self.billed_seconds = 100
        self.costs: dict[str, dict[str, Any]] | None = None  # None: a deployment without /cost

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.headers.get("authorization") != f"Bearer {TOKEN}":
            return httpx.Response(401, json={"error": "Unauthorized"})
        parts = [p for p in request.url.path.split("/") if p]
        if parts[:2] != ["run-cloud", "sandboxes"]:
            return httpx.Response(404, json={"error": "Not found"})
        if len(parts) == 2:
            if request.method == "POST":
                return self._create(request)
            return self._list(request)
        if parts[3:] == ["cost"]:
            if self.costs is not None and parts[2] in self.costs:
                return httpx.Response(200, json=self.costs[parts[2]])
            return httpx.Response(404, json={"error": "Not found"})
        box = self.sandboxes.get(parts[2])
        if box is None:
            return httpx.Response(404, json={"error": "Not found"})
        if len(parts) == 3:
            if request.method == "DELETE":
                return self._destroy(box)
            return httpx.Response(200, json=box)
        action = parts[3]
        if action == "usage":
            body = {"billedSeconds": self.billed_seconds, "estimatedCostUsd": 0.001}
            return httpx.Response(200, json=body, headers={"date": format_datetime(self.now, True)})
        if box["state"] != "running":
            return httpx.Response(409, json={"error": f"sandbox is {box['state']}, not running"})
        body = json.loads(request.content)
        if action == "exec":
            return self._exec(body)
        if action == "fs":
            return self._fs(box["id"], body)
        return httpx.Response(404, json={"error": "Not found"})

    def _create(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.creates.append(body)
        key = request.headers.get("idempotency-key")
        if key and key in self.keys:
            return httpx.Response(200, json=self.sandboxes[self.keys[key]])
        sandbox_id = "sbx_" + uuid.uuid4().hex[:20]
        box = {
            "id": sandbox_id,
            "image": body.get("image", "runcloud/agent-base"),
            "milli_cpu": int(body.get("cpu", 0.125) * 1000),
            "mem_mb": body.get("memory", 128),
            "disk_mb": 40960,
            "state": "running",
            "timeout_seconds": body.get("timeoutSeconds", 300),
            "created_at": _iso(self.now),
            "cpu_used_usec": 4_000_000,
            "tags": body.get("tags", {}),
        }
        if body.get("name"):
            box["name"] = body["name"]
        if body.get("onTimeout"):
            box["on_timeout"] = body["onTimeout"]
        self.sandboxes[sandbox_id] = box
        if key:
            self.keys[key] = sandbox_id
        return httpx.Response(202, json={**box, "state": "pending", "tags": None})

    def _list(self, request: httpx.Request) -> httpx.Response:
        wanted = [tuple(t.split(":", 1)) for t in request.url.params.get_list("tag")]
        items = [
            box
            for box in self.sandboxes.values()
            if all(box["tags"].get(k) == v for k, v in wanted)
        ]
        return httpx.Response(200, json={"items": items, "nextCursor": None})

    def _destroy(self, box: dict[str, Any]) -> httpx.Response:
        if box["state"] == "destroyed":
            return httpx.Response(204)
        box["state"] = "destroyed"
        box["destroyed_at"] = _iso(self.now)
        return httpx.Response(202, json={**box, "state": "destroying"})

    def _exec(self, body: dict[str, Any]) -> httpx.Response:
        argv = [a.replace(ROOT, str(self.root)) for a in body["cmd"]]
        assert argv[:2] == ["python3", "-c"]
        done = subprocess.run(
            [sys.executable, *argv[1:]], capture_output=True, text=True, timeout=120, check=False
        )
        return httpx.Response(
            200, json={"exit_code": done.returncode, "stdout": done.stdout, "stderr": done.stderr}
        )

    def _fs(self, sandbox_id: str, body: dict[str, Any]) -> httpx.Response:
        key = (sandbox_id, body["path"])
        offset = body.get("offset", 0)
        if body["op"] == "write":
            content = base64.b64decode(body["content"])
            data = bytearray() if body.get("truncate") else self.files.setdefault(key, bytearray())
            if len(data) < offset:
                data.extend(b"\0" * (offset - len(data)))
            data[offset : offset + len(content)] = content
            self.files[key] = data
            return httpx.Response(200, json={"next_offset": offset + len(content)})
        if key not in self.files:
            return httpx.Response(
                200, json={"error": f"open {body['path']}: no such file or directory"}
            )
        data = self.files[key]
        piece = bytes(data[offset : offset + body.get("length", 524288)])
        doc = {
            "content": base64.b64encode(piece).decode(),
            "next_offset": offset + len(piece),
            "size": len(data),
        }
        return httpx.Response(200, json=doc)


@pytest.fixture
def cloud(tmp_path: Path) -> FakeRunCloud:
    return FakeRunCloud(tmp_path)


def provider(cloud: FakeRunCloud, token: str = TOKEN) -> RuncloudProvider:
    return RuncloudProvider({"api_key": token}, transport=httpx.MockTransport(cloud.handler))


def go(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


async def _created(cloud: FakeRunCloud, **kwargs: Any) -> tuple[RuncloudProvider, str]:
    p = provider(cloud)
    box = await p.create(CreateRequest(**kwargs), idempotency_key=uuid.uuid4().hex)
    return p, box.id


def test_static_discovery_is_valid_json() -> None:
    doc = to_json(RuncloudProvider.static_discovery())
    assert doc["classes"][0]["id"] == "default"
    assert doc["limits"]["file_bytes"] >= 1 << 30


def test_endpoint_must_be_a_runcloud_host() -> None:
    with pytest.raises(SpsError) as error:
        RuncloudProvider({"api_key": TOKEN, "endpoint": "https://evil.example"})
    assert error.value.code == "invalid_request"


def test_wrong_token_is_unauthenticated(cloud: FakeRunCloud) -> None:
    with pytest.raises(SpsError) as error:
        go(provider(cloud, "nope").discovery())
    assert error.value.code == "unauthenticated"


def test_create_maps_the_request(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p = provider(cloud)
        box = await p.create(
            CreateRequest(
                resources={"cpu_min": 2},
                lifetime_seconds=600,
                labels={"team": "a", "k/x": ""},
                env={"FOO": "bar"},
            ),
            idempotency_key="key-0001",
        )
        assert box.state == "running"
        assert box.size.cpu == 2 and box.size.memory_mib == 4096
        assert dict(box.labels) == {"team": "a", "k/x": ""}
        assert box.at_end == "destroyed"
        assert box.ends_at == box.created_at + timedelta(seconds=600)

    go(main())
    sent = cloud.creates[0]
    assert sent["timeoutSeconds"] == 600 and sent["onTimeout"] == "destroy"
    assert sent["idlePauseSeconds"] == 0 and sent["env"] == {"FOO": "bar"}
    assert sent["name"].startswith("sps-")


def test_repeated_key_returns_the_first_sandbox_or_conflict(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p = provider(cloud)
        first = await p.create(CreateRequest(labels={"a": "1"}), idempotency_key="key-0002")
        again = await p.create(CreateRequest(labels={"a": "1"}), idempotency_key="key-0002")
        assert again.id == first.id
        with pytest.raises(SpsError) as error:
            await p.create(CreateRequest(labels={"a": "2"}), idempotency_key="key-0002")
        assert error.value.code == "conflict"

    go(main())


@pytest.mark.parametrize(
    "request_",
    [
        CreateRequest(sandbox_class="gpu"),
        CreateRequest(at_end="paused"),
        CreateRequest(image="ubuntu:24.04"),
        CreateRequest(lifetime_seconds=86401),
        CreateRequest(resources={"cpu_min": 64}),
        CreateRequest(resources={"region": "eu"}),
        CreateRequest(requirements={"snapshots": {}}),
    ],
)
def test_unmet_requests_are_unsupported(cloud: FakeRunCloud, request_: CreateRequest) -> None:
    with pytest.raises(SpsError) as error:
        go(provider(cloud).create(request_, idempotency_key="key-0003"))
    assert error.value.code == "unsupported"
    assert not cloud.creates


def test_list_filters_labels_and_leaves_out_ended(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p, kept = await _created(cloud, labels={"run": "x"})
        _, gone = await _created(cloud, labels={"run": "x"})
        await _created(cloud, labels={"run": "y"})
        await p.destroy(gone)
        page = await p.list(labels={"run": "x"})
        assert [b.id for b in page.sandboxes] == [kept]

    go(main())


def test_destroy_get_and_not_found(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p, sandbox_id = await _created(cloud)
        assert (await p.destroy(sandbox_id)).state == "destroyed"
        assert (await p.destroy(sandbox_id)).state == "destroyed"
        assert (await p.get(sandbox_id)).state == "destroyed"
        with pytest.raises(SpsError) as error:
            await p.run(sandbox_id, RunRequest(argv=["true"]))
        assert error.value.code == "not_found"
        with pytest.raises(SpsError) as error:
            await p.get("sbx_missing")
        assert error.value.code == "not_found"

    go(main())


def test_run_results(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p, sid = await _created(cloud, env={})
        res = await p.run(
            sid, RunRequest(argv=["sh", "-c", "echo $X; echo e >&2; exit 3"], env={"X": "1"})
        )
        assert (res.exit_code, res.stdout, res.stderr) == (3, "1\n", "e\n")
        res = await p.run(sid, RunRequest(argv=["pwd"], cwd="/"))
        assert res.stdout == "/\n"
        with pytest.raises(SpsError) as error:
            await p.run(sid, RunRequest(argv=["pwd"], cwd="/no/such/dir"))
        assert error.value.code == "not_found"
        res = await p.run(sid, RunRequest(argv=["no-such-program-sps"]))
        assert res.exit_code == 127
        res = await p.run(sid, RunRequest(argv=["sh", "-c", "printf abcdef"], output_limit_bytes=3))
        assert res.stdout == "abc" and res.stdout_truncated
        res = await p.run(
            sid, RunRequest(argv=["sh", "-c", "echo before; sleep 30"], timeout_ms=500)
        )
        assert res.timed_out and res.exit_code is None and res.stdout == "before\n"

    go(main())
    assert not list((cloud.root / "r").iterdir())  # every run removed its record


def test_process_output_streams_and_exits(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p, sid = await _created(cloud)
        proc = await p.process_start(
            sid,
            ProcessStartRequest(
                argv=[
                    "sh",
                    "-c",
                    "printf 'a\\303'; sleep 0.3; printf '\\251b'; echo err >&2; exit 4",
                ]
            ),
        )
        text, err, cursor = "", "", None
        for _ in range(50):
            page = await p.process_output(sid, proc.id, cursor=cursor, wait_ms=2000)
            cursor = page.next_cursor
            text += "".join(c.data for c in page.chunks if c.stream == "stdout")
            err += "".join(c.data for c in page.chunks if c.stream == "stderr")
            if page.state == "exited" and page.stdout_closed and page.stderr_closed:
                break
        assert (text, err, page.exit_code) == ("aéb", "err\n", 4)
        with pytest.raises(SpsError) as error:
            await p.process_output(sid, "p" + "0" * 24)
        assert error.value.code == "not_found"

    go(main())


def test_files_round_trip(cloud: FakeRunCloud) -> None:
    data = bytes(range(256)) * 9000  # several 512 KiB chunks

    async def body() -> Any:
        for i in range(0, len(data), 100_000):
            yield data[i : i + 100_000]

    async def main() -> None:
        p, sid = await _created(cloud)
        info = await p.files_write(sid, "/tmp/a/b.bin", body(), size=len(data))
        assert info.size == len(data)
        stream = await p.files_read(sid, "/tmp/a/b.bin")
        assert stream.size == len(data)
        assert b"".join([c async for c in stream.chunks]) == data
        await p.files_write(sid, "/tmp/empty", body_of(b""), size=0)
        stream = await p.files_read(sid, "/tmp/empty")
        assert stream.size == 0 and [c async for c in stream.chunks] == []
        with pytest.raises(SpsError) as error:
            await p.files_read(sid, "/tmp/missing")
        assert error.value.code == "not_found"
        with pytest.raises(SpsError) as error:
            await p.files_write(sid, "/tmp/short", body_of(b"abc"), size=4)
        assert error.value.code == "invalid_request"

    go(main())


async def body_of(data: bytes) -> Any:
    if data:
        yield data


def test_cost_reports_usage_and_turns_final(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p, sid = await _created(cloud, resources={"cpu_min": 1})
        cost = await p.cost(sid)
        assert cost.status == "accruing" and cost.source == "usage" and cost.total_usd is None
        cpu, memory = cost.lines
        assert (cpu.quantity, cpu.unit) == (100, "vcpu_second")
        assert (memory.quantity, memory.unit) == (200, "gib_second")
        await p.destroy(sid)
        cloud.now += timedelta(seconds=30)
        assert (await p.cost(sid)).status == "accruing"
        cloud.now += timedelta(seconds=300)
        final = await p.cost(sid)
        assert final.status == "final" and all(line.end is not None for line in final.lines)

    go(main())


def test_cost_reads_the_billed_record_and_falls_back_without_it(cloud: FakeRunCloud) -> None:
    async def main() -> None:
        p, sid = await _created(cloud, resources={"cpu_min": 1})
        cloud.costs = {}
        assert (await p.cost(sid)).source == "usage"  # no record: the usage estimate
        cloud.costs[sid] = {
            "sandboxId": sid,
            "status": "final",
            "currency": "USD",
            "totalNanodollars": "1234567",
            "totalUsd": "0.001234567",
            "lines": [
                {"meter": "cpu", "nanodollars": "246000", "quantity": "100", "unit": "vcpu_second"},
                {
                    "meter": "memory",
                    "nanodollars": "988567",
                    "quantity": "200.5",
                    "unit": "gib_second",
                },
                {"meter": "cpu_burst", "nanodollars": "0", "quantity": "0", "unit": "vcpu_second"},
            ],
            "startedAt": "2026-11-22T10:00:00.000Z",
            "endedAt": "2026-11-22T10:05:00.000Z",
            "meteredThrough": "2026-11-22T10:05:00.000Z",
            "asOf": "2026-11-22T10:07:00.000Z",
            "availableUntil": "2026-12-22T10:07:00.000Z",
        }
        del cloud.sandboxes[sid]  # pruned: only the retained record answers
        cost = await p.cost(sid)
        assert (cost.status, cost.source, cost.total_usd) == (
            "final",
            "reported",
            Decimal("0.001234567"),
        )
        assert [line.item for line in cost.lines] == ["cpu", "memory", "cpu"]
        assert sum(line.amount_usd for line in cost.lines) == cost.total_usd
        assert cost.lines[1].quantity == Decimal("200.5") and cost.lines[1].unit == "gib_second"
        assert all(line.end == datetime(2026, 11, 22, 10, 5, tzinfo=UTC) for line in cost.lines)

    go(main())
