# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The HTTP exchanges each operation makes, listed at the transport (`FakeBlaxel`'s `MockTransport`
sees every request to both Blaxel hosts, which a recording provider cannot).

Blaxel limits an account's requests, and a benchmark's published provider time for an operation
includes every sequential exchange in it. An operation that is several exchanges spends that limit
several times over and takes several round trips, so each list is exact: a call added anywhere
fails the test with its name. Where the provider's API shape needs a poll, the test asserts the
bound as a function of the duration, and the interval where it backs off.

Steady state: the workspace, runtime URL and account are already known to the provider object.
`# removable:` marks an exchange that a later change could drop; `# API shape:` one that Blaxel's
API needs."""

from __future__ import annotations

import asyncio
import base64
import getpass
import json
import math
import re
import shlex
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from sandbox_provider_spec.v1 import (
    CreateRequest,
    DockerfileSource,
    ImageRequest,
    ProcessStartRequest,
    RunRequest,
    SpsError,
    waiting,
)
from sandbox_provider_spec.v1.timing import in_lookup
from test_additions import SANDBOX
from test_additions import Fake as ImageFake
from test_offline import FakeBlaxel

from sps_provider_blaxel import BlaxelProvider, guest
from sps_provider_blaxel.create_spec import sandbox_name
from sps_provider_blaxel.guest import COPY_AS, SWITCH

#: Exchanges per operation, exactly, in steady state, from `wire-budget.json` beside the package
#: (the one list the benchmark lane reads too). A variant is its own key with its total.
#: `run[exact]` is a small byte-exact answer; the others add what the request asks for.
WIRE_BUDGET = json.loads((Path(__file__).parents[1] / "wire-budget.json").read_text())
BUDGET = {name: op["exchanges"] for name, op in WIRE_BUDGET["operations"].items()}


def poll_pauses(name: str, count: int) -> list[float]:
    """The first `count` pauses of the operation's poll schedule in the budget."""
    poll = WIRE_BUDGET["operations"][name]["poll"]
    if "interval_s" in poll:
        return [poll["interval_s"]] * count
    return [min(poll["first_s"] * poll["factor"] ** i, poll["cap_s"]) for i in range(count)]


# A provider object that has not seen the account or the sandbox: its lookups come first. These
# totals (the warm operation plus the lookups) are the test's own; `wire-budget.json` has the warm
# count and, per marked operation, `lookup`: how many of a cold call's exchanges are lookups.
BUDGET.update(
    {
        "create[cold]": 2,
        "get[cold]": 2,
        "run[cold]": 3,
        "files_read[cold]": 3,
        "files_write[cold]": 3,
        "process_output[cold]": 3,
        "cost[cold]": 4,
    }
)

#: How many of an operation's exchanges are marked `lookup()`: only the one whose sole purpose is to
#: find a sandbox's runtime address (its record read in `_base`, once per sandbox per provider
#: object). The workspace list and the account read are account open (Core's), not marked here.
#: Warm operations mark none (not listed).
LOOKUPS = {
    "run[cold]": 1,
    "files_read[cold]": 1,
    "files_write[cold]": 1,
    "process_output[cold]": 1,
    # `create[image]` resolves the artifact: its record and its tag are handle reads, marked
    "create[image]": 2,
}

#: The only exchange that may be marked. A marked exchange that is not here fails the test, so a
#: package cannot hide real work as lookup.
PINNED_LOOKUPS = {
    "GET /sandboxes/<box>",
    # `create[image]` resolves the artifact first: the image's record and its tag are handle
    # reads, marked `lookup()` in `images.resolve`
    "GET /sandboxes/<image>",
    "GET /images/sandbox/<image>",
}

STAGE = re.compile(r"/tmp/\.sps-run-[0-9a-f]{32}")


class Recorded(FakeBlaxel):
    """`FakeBlaxel` with the answers these tests steer: what the byte-exact wrapper prints, the
    files a timed-out wrapper left, a deploy that takes a few reads, a process still running."""

    def __init__(self) -> None:
        super().__init__()
        self.wrapper_answer: str | None = None  # stdout of the byte-exact wrapper
        self.staged: tuple[bytes, bytes] | None = None  # the wrapper's `o` and `e` files
        self.copy_exit: int | None = None  # exit status of the copy as a user
        self.real_copy = False  # run the copy as a user in the local shell, on real files
        self.deploying = 0  # the next N create answers and reads say DEPLOYING
        self.running = 0  # the next N reads of a followed command say running
        # How the gateway cuts a waited `sleep 150`: "504" (the fake's own, the command runs on),
        # "read" (the connection times out instead), "unknown" (504, and Blaxel never started
        # it) or "killed" (504, and the command ended with the request).
        self.cut = "504"
        # Fields laid over every process record read (`None` removes the field): a record that
        # lacks or cuts its output, to steer `_output`'s logs read.
        self.record: dict[str, Any] = {}
        self.marked: list[bool] = []  # per request, in order: was it inside `lookup()`

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.marked.append(in_lookup())  # whether the package marked this exchange as a lookup
        return super().handle(request)

    def control(self, request: httpx.Request) -> httpx.Response:
        resp = super().control(request)
        path = request.url.path.removeprefix("/v0")
        reads = path == "/sandboxes" or path.startswith("/sandboxes/")
        if reads and request.method in ("POST", "GET") and self.deploying > 0:
            self.deploying -= 1
            return self._json({**json.loads(resp.content), "status": "DEPLOYING"})
        return resp

    def runtime(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/process" and request.method == "POST":
            body = json.loads(request.content)
            command = body["command"]
            stage = STAGE.search(command)
            if stage and self.staged:
                self.files[f"{stage.group()}.o"], self.files[f"{stage.group()}.e"] = self.staged
            if "sleep 150" in command and body.get("waitForCompletion") and self.cut != "504":
                if self.cut == "unknown":
                    return self._json({"error": "upstream request timeout"}, 504)
                resp = super().runtime(request)  # the command is known, and running
                if self.cut == "read":
                    raise httpx.ReadTimeout("no answer", request=request)
                self.processes[body["name"]].update(status="killed", exitCode=-1)
                self.processes[body["name"]].pop("_then")
                return resp
            answered = "sleep 99" not in command and body.get("waitForCompletion")
            if answered and self.wrapper_answer is not None and "sps-run" in command:
                return self._ok(body, stdout=self.wrapper_answer)
            if answered and self.real_copy and "sps-write" in command:
                return self._real_copy(body)
            if answered and self.copy_exit is not None and "sps-write" in command:
                return self._ok(body, code=self.copy_exit)
        record = (
            request.method == "GET"
            and path.startswith("/process/")
            and not path.endswith(("/logs", "/kill"))
        )
        if record and self.running > 0:
            self.running -= 1
            info = self.processes[path.removeprefix("/process/")]
            return self._overlaid({**info, "status": "running"})
        resp = super().runtime(request)
        return self._overlaid(json.loads(resp.content)) if record and resp.is_success else resp

    def _overlaid(self, info: dict[str, Any]) -> httpx.Response:
        merged = {**info, **self.record}
        return self._json({k: v for k, v in merged.items() if v is not None})

    def _real_copy(self, body: dict[str, Any]) -> httpx.Response:
        """The copy command as the sandbox's shell would run it, on the staged file."""
        argv = shlex.split(body["command"])
        stage = Path(argv[5])  # /bin/sh -c COPY_AS sps-write <user> <stage> <switch> <path>
        stage.write_bytes(self.files[str(stage)])
        done = subprocess.run(argv, capture_output=True, text=True, check=False)
        resp = self._ok(body, code=done.returncode)
        return httpx.Response(
            200, json={**json.loads(resp.content), "stderr": done.stderr}, headers=resp.headers
        )

    def _ok(self, body: dict[str, Any], *, stdout: str = "", code: int = 0) -> httpx.Response:
        return self._json(
            {
                "name": body["name"],
                "status": "completed" if code == 0 else "failed",
                "exitCode": code,
                "stdout": stdout,
                "stderr": "",
            }
        )


class Clock:
    """Time that moves only when the provider pauses, so a 100 s wait costs no time to test."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.pauses: list[float] = []
        self.skipped = 0.0
        loop = asyncio.get_running_loop()
        real = loop.time
        monkeypatch.setattr(loop, "time", lambda: real() + self.skipped)
        monkeypatch.setattr(waiting, "pause", self.pause)

    async def pause(self, seconds: float) -> None:
        self.pauses.append(round(seconds, 3))
        self.skipped += seconds


def exchanges(fake: Recorded, box: str, since: int) -> list[str]:
    """`METHOD path` of every exchange after the first `since`, with the ids made generic."""
    out = []
    for request in fake.requests[since:]:
        path = request.url.path.removeprefix("/v0")
        path = path.replace(box, "<box>")
        path = re.sub(r"sps-[0-9a-f]{32}", "<process>", path)
        out.append(f"{request.method} {STAGE.sub('<stage>', path)}")
    return out


class World:
    """One provider, one sandbox it created, and the list of exchanges an operation makes."""

    def __init__(self) -> None:
        self.fake = Recorded()
        self.provider = BlaxelProvider({"api_key": "key-1"}, transport=self.fake.transport())
        self.box = asyncio.run(self.provider.create(CreateRequest(), idempotency_key="k0")).id

    def cold(self) -> BlaxelProvider:
        """A provider object that knows nothing of the account or the sandbox."""
        return BlaxelProvider({"api_key": "key-1"}, transport=self.fake.transport())

    def calls(self, key: str, coro: Any) -> list[str]:
        """The exchanges of one operation, after checking their number against `BUDGET[key]` and
        which of them are lookups against `LOOKUPS` and `PINNED_LOOKUPS`."""
        since = len(self.fake.requests)
        asyncio.run(coro)
        listed = exchanges(self.fake, self.box, since)
        assert len(listed) == BUDGET[key], (key, listed)
        marked = [e for e, m in zip(listed, self.fake.marked[since:], strict=True) if m]
        assert len(marked) == LOOKUPS.get(key, 0), (key, "lookups", marked)
        assert set(marked) <= PINNED_LOOKUPS, (key, "marked, not pinned", marked)
        return listed


@pytest.fixture()
def world() -> World:
    return World()


async def _body(data: bytes) -> AsyncIterator[bytes]:
    yield data


def _run(world: World, key: str, **kw: Any) -> list[str]:
    return world.calls(key, world.provider.run(world.box, RunRequest(**kw)))


# -- create, get, destroy --------------------------------------------------------------


def test_create_get_and_destroy() -> None:
    world = World()
    # (the fixture's own create did the workspace read; steady state starts here)
    assert world.calls("create", world.provider.create(CreateRequest(), idempotency_key="k1")) == [
        "POST /sandboxes"
    ]
    assert world.calls("get", world.provider.get(world.box)) == ["GET /sandboxes/<box>"]
    assert world.calls("destroy", world.provider.destroy(world.box)) == ["DELETE /sandboxes/<box>"]


def test_a_replayed_create_reads_the_first_sandbox() -> None:
    world = World()
    assert world.calls(
        "create[replay]", world.provider.create(CreateRequest(), idempotency_key="k0")
    ) == [
        "POST /sandboxes",
        "GET /sandboxes/<box>",  # API shape: the 409 names no sandbox; the replay fetches it
    ]


def test_a_create_that_is_still_deploying_polls_at_a_fixed_half_second(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = World()
    world.fake.deploying = 4

    async def scenario() -> Clock:
        clock = Clock(monkeypatch)
        await world.provider.create(CreateRequest(), idempotency_key="k1")
        return clock

    since = len(world.fake.requests)
    clock = asyncio.run(scenario())
    # API shape: Blaxel answers the create while it is DEPLOYING; the sandbox is read until it is
    # not. The interval is fixed (0.5 s, in `check_no_sleeps.FIXED_POLLS` with this reason), so the
    # calls are 1 + duration / 0.5, up to 1 + 120 / 0.5 before the create gives up.
    assert exchanges(world.fake, sandbox_name("k1"), since) == [
        "POST /sandboxes",
        *["GET /sandboxes/<box>"] * 4,
    ]
    interval = WIRE_BUDGET["operations"]["create"]["poll"]["interval_s"]
    assert clock.pauses == poll_pauses("create", 4)
    assert 1 + len(clock.pauses) <= BUDGET["create"] + math.ceil(sum(clock.pauses) / interval)


# -- pause and resume --------------------------------------------------------------------------


def test_a_pause_is_the_standby_post_and_a_read_of_the_record_per_poll(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`pause`: one POST to the sandbox's host, then the record until its `state` is STANDBY (a
    202 is not proof). The budget's 1 is the POST; each pause of the poll schedule (0.5 s,
    doubling to 1 s) is followed by a read, so a standby is never looked for at once. A cold
    object adds the one lookup."""
    world.fake.standby_reads = 3

    async def scenario() -> Clock:
        clock = Clock(monkeypatch)
        await world.provider.pause(world.box)
        return clock

    since = len(world.fake.requests)
    clock = asyncio.run(scenario())
    listed = exchanges(world.fake, world.box, since)
    assert listed == ["POST /standby", *["GET /sandboxes/<box>"] * 4]  # API shape: 202 is not done
    assert clock.pauses == poll_pauses("pause", 4) == [0.5, 1.0, 1.0, 1.0]
    assert len(listed) == BUDGET["pause"] + len(clock.pauses)
    assert not any(world.fake.marked[since:])  # none of it is a lookup on a warm object


def test_a_pause_the_host_ignored_sends_the_standby_once_more_and_is_its_own_budget_key(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    world.fake.ignored_standbys = 1
    world.fake.standby_reads = 0

    async def scenario() -> Clock:
        clock = Clock(monkeypatch)
        await world.provider.pause(world.box)
        return clock

    since = len(world.fake.requests)
    clock = asyncio.run(scenario())
    listed = exchanges(world.fake, world.box, since)
    assert listed.count("POST /standby") == 2
    assert len(listed) == BUDGET["pause[resend]"] + len(clock.pauses)
    assert clock.pauses == poll_pauses("pause[resend]", len(clock.pauses))
    assert sum(clock.pauses) >= 8  # the second request is past the host's quiet window


def test_a_standby_that_is_seen_at_the_first_read_is_the_post_and_one_read(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    world.fake.standby_reads = 0

    async def scenario() -> Clock:
        clock = Clock(monkeypatch)
        await world.provider.pause(world.box)
        return clock

    since = len(world.fake.requests)
    clock = asyncio.run(scenario())
    assert exchanges(world.fake, world.box, since) == ["POST /standby", "GET /sandboxes/<box>"]
    assert clock.pauses == [0.5]


def test_a_resume_is_one_exchange_and_the_pause_after_it_reads_the_new_standby(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    world.fake.standby_reads = 0

    async def cycle() -> None:
        Clock(monkeypatch)
        await world.provider.pause(world.box)

    asyncio.run(cycle())
    assert world.calls("resume", world.provider.resume(world.box)) == ["POST /resume"]
    since = len(world.fake.requests)
    asyncio.run(cycle())  # the second cycle costs what the first did
    assert exchanges(world.fake, world.box, since) == ["POST /standby", "GET /sandboxes/<box>"]
    assert world.calls("resume", world.provider.resume(world.box)) == ["POST /resume"]


def test_a_cold_pause_and_resume_add_the_one_lookup_that_finds_the_address(world: World) -> None:
    world.fake.standby_reads = 0
    lookups = ["GET /workspaces", "GET /sandboxes/<box>"]
    since = len(world.fake.requests)
    asyncio.run(world.cold().pause(world.box))
    listed = exchanges(world.fake, world.box, since)
    assert listed == [*lookups, "POST /standby", "GET /sandboxes/<box>"]
    marked = [e for e, m in zip(listed, world.fake.marked[since:], strict=True) if m]
    assert (
        marked == ["GET /sandboxes/<box>"]
        and len(marked) == WIRE_BUDGET["operations"]["pause"]["lookup"]
    )
    since = len(world.fake.requests)
    asyncio.run(world.cold().resume(world.box))
    assert exchanges(world.fake, world.box, since) == [*lookups, "POST /resume"]


def test_image_get_and_preview_link() -> None:
    fake = ImageFake()  # the fake with a build API; `World`'s has none
    provider = fake.provider()
    request = ImageRequest(DockerfileSource("FROM a\n"))
    made = asyncio.run(provider.image_prepare(request, idempotency_key="img-1"))

    def calls(key: str, work: Any) -> list[str]:
        since = len(fake.requests)
        asyncio.run(work)
        listed = [
            re.sub(
                r"sps-img-[0-9a-f]{32}",
                "<image>",
                f"{r.method} {r.url.path.removeprefix('/v0')}",
            )
            for r in fake.requests[since:]
        ]
        assert len(listed) == BUDGET[key], (key, listed)
        marked = [e for e, m in zip(listed, fake.marked[since:], strict=True) if m]
        assert len(marked) == LOOKUPS.get(key, 0), (key, "lookups", marked)
        assert set(marked) <= PINNED_LOOKUPS, (key, "marked, not pinned", marked)
        return listed

    assert calls("image_get", provider.image_get(made.id)) == [
        "GET /sandboxes/<image>",  # API shape: the label record holds labels, the image its tag
        "GET /images/sandbox/<image>",
    ]
    assert calls("preview_link", provider.preview_link(SANDBOX, 3000)) == [
        f"GET /sandboxes/{SANDBOX}/previews/sps-3000-public",  # removable: the lookup by name
        f"POST /sandboxes/{SANDBOX}/previews",
    ]
    # a create from a prepared image resolves its tag first (`images.resolve`): the label record
    # and the image, both immutable once built, read on the first create and remembered after it
    from_image = CreateRequest(image={"provider_artifact": made.id})
    assert calls("create[image]", provider.create(from_image, idempotency_key="img-create-0")) == [
        "GET /sandboxes/<image>",  # the image's record, remembered on the provider once resolved
        "GET /images/sandbox/<image>",  # its tag, the same
        "POST /sandboxes",
    ]
    assert calls(
        "create[image,resolved]", provider.create(from_image, idempotency_key="img-create-1")
    ) == [
        "POST /sandboxes",
    ]
    # one preview per port and mode: a second link is the lookup alone
    assert calls("preview_link[repeat]", provider.preview_link(SANDBOX, 3000)) == [
        f"GET /sandboxes/{SANDBOX}/previews/sps-3000-public"
    ]
    # deleting the image forgets the remembered tag: the next create resolves it again and finds
    # it gone
    asyncio.run(provider.image_delete(made.id))
    with pytest.raises(SpsError, match="no such image"):
        asyncio.run(provider.create(from_image, idempotency_key="img-create-2"))


# -- commands --------------------------------------------------------------------------


def test_run_text(world: World) -> None:
    assert _run(world, "run", argv=["true"]) == ["POST /process"]


def test_run_text_in_a_directory_checks_it_first(world: World) -> None:
    assert _run(world, "run[cwd]", argv=["true"], cwd="/dir") == [
        "GET /filesystem//dir",  # removable: Blaxel runs in a missing directory as if it were there
        "POST /process",
    ]


def test_a_run_text_that_times_out_reads_the_killed_process(world: World) -> None:
    assert _run(world, "run[timeout]", argv=["sleep", "99"], timeout_ms=1000) == [
        "POST /process",
        "GET /process/<process>",  # API shape: the 422 carries no output; the record does
    ]


@pytest.mark.parametrize("timeout_ms", [100_000, 610_000, 3_600_000])
def test_a_run_text_with_a_long_timeout_is_one_waited_request(
    world: World, timeout_ms: int
) -> None:
    # Core's 600 s command (610 s with its kill margin) included: one request, as Blaxel's SDK
    # sends it, not a detached start and polls.
    assert _run(world, "run", argv=["true"], timeout_ms=timeout_ms) == ["POST /process"]


def _cut_run(
    world: World, monkeypatch: pytest.MonkeyPatch, seconds: int
) -> tuple[list[str], Clock, Any]:
    """A `sleep 150` run whose waited request the gateway cuts: its exchanges, the pauses, and
    the result or the error."""

    async def scenario() -> tuple[Clock, Any]:
        clock = Clock(monkeypatch)
        request = RunRequest(argv=["sleep", "150"], timeout_ms=seconds * 1000)
        try:
            return clock, await world.provider.run(world.box, request)
        except SpsError as error:
            return clock, error

    since = len(world.fake.requests)
    clock, outcome = asyncio.run(scenario())
    return exchanges(world.fake, world.box, since), clock, outcome


@pytest.mark.parametrize("cut", ["504", "read"])
def test_a_run_cut_by_the_gateway_is_followed_with_backoff(
    world: World, monkeypatch: pytest.MonkeyPatch, cut: str
) -> None:
    world.fake.cut = cut
    world.fake.running = 6  # the fake's own `_then` adds one more running read
    seconds = 300
    listed, clock, result = _cut_run(world, monkeypatch, seconds)
    assert (result.exit_code, result.stdout, result.timed_out) == (0, "late\n", False)
    # API shape: Blaxel's gateway cuts a waited request near 120 s while the command runs on, so a
    # command still running then is read by its name: at once, then after 0.05 s doubling to 2 s.
    # The last record carries the output: no logs read. The POST is never sent again.
    assert clock.pauses == poll_pauses("run[cut]", 7)
    assert listed == ["POST /process", *["GET /process/<process>"] * 8]
    assert listed.count("GET /process/<process>") <= max_follow_reads(sum(clock.pauses))
    # the POST, one read at once, one more per pause
    assert len(listed) == BUDGET["run[cut]"] + len(clock.pauses)


def max_follow_reads(seconds: float) -> int:
    """The reads of a command followed for `seconds`: one at once, the pause 0.05 s doubling to
    2 s (six pauses, 3.15 s, to get there), every 2 s to 300 s of run time, then every 5 s (after
    one 4 s step)."""
    ramp = 0.05 * (2**6 - 1)
    to_long = max(0.0, min(seconds, 300) - ramp)
    long = max(0.0, seconds - 300)
    return 1 + 6 + math.ceil(to_long / 2) + (1 + math.ceil(long / 5) if long else 0)


def test_a_long_follow_backs_off_to_two_then_five_seconds(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A command that runs ~1,000 s more after the cut: about 0.5 reads a second, 0.2 after 300 s
    # (the 0.5 s cap made 2 a second: ~2,000 reads for the same follow).
    world.fake.running = 400
    listed, clock, result = _cut_run(world, monkeypatch, 3600)
    assert (result.exit_code, result.timed_out) == (0, False)
    followed = sum(clock.pauses)
    # 2 s from the end of the ramp to 300 s of run time, then one doubling (4 s) to 5 s
    assert all(p == 2.0 for at, p in _pause_times(clock.pauses) if 3.15 < at < 300)
    later = [p for at, p in _pause_times(clock.pauses) if at >= 300]
    assert later[0] == 4.0 and set(later[1:]) == {5.0}
    assert listed.count("GET /process/<process>") <= max_follow_reads(followed)
    assert len(listed) == BUDGET["run[cut]"] + len(clock.pauses)


def _pause_times(pauses: list[float]) -> list[tuple[float, float]]:
    """Each pause with the run time at which it started."""
    at, out = 0.0, []
    for pause in pauses:
        out.append((at, pause))
        at += pause
    return out


def test_a_follow_never_pauses_past_the_deadline(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    world.fake.running = 100  # still running at the deadline: killed there, a timeout
    listed, clock, result = _cut_run(world, monkeypatch, 10)
    assert (result.timed_out, result.exit_code) == (True, None)
    assert 10 - 0.01 <= sum(clock.pauses) <= 10 + 0.06  # the clock rounds each pause to 1 ms
    assert listed[-2:] == ["DELETE /process/<process>/kill", "GET /process/<process>"]


def test_a_cut_run_that_blaxel_never_started_is_an_unknown_timeout(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    world.fake.cut = "unknown"
    listed, _, error = _cut_run(world, monkeypatch, 300)
    assert (error.code, error.outcome) == ("timeout", "unknown")
    assert listed == ["POST /process", "GET /process/<process>"]


def test_a_cut_run_killed_before_its_timeout_is_not_called_a_timeout(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    world.fake.cut = "killed"
    listed, _, error = _cut_run(world, monkeypatch, 300)
    assert (error.code, error.outcome) == ("provider_error", "unknown")
    assert listed == ["POST /process", "GET /process/<process>"]
    # killed at its deadline (Blaxel's own `timeout`): a timeout, with the output so far
    listed, _, result = _cut_run(world, monkeypatch, 1)
    assert (result.timed_out, result.exit_code) == (True, None)
    # its record has no output at all: one logs read confirms the empty end (`_output`)
    assert listed == ["POST /process", "GET /process/<process>", "GET /process/<process>/logs"]


def test_run_exact_with_small_output(world: World) -> None:
    world.fake.wrapper_answer = "0 2 0 -\naGk=\n"
    assert _run(world, "run[exact]", argv=["true"], output="base64") == ["POST /process"]
    assert _run(world, "run[user]", argv=["true"], user="root") == [
        "POST /process"
    ]  # the wrapper does getent


def test_run_exact_with_a_large_stream_reads_its_file_and_removes_it(world: World) -> None:
    world.fake.wrapper_answer = "0 200000 0 KEEP\n" + base64.b64encode(b"x" * 32768).decode()
    world.fake.staged = (b"x" * 200_000, b"")
    assert _run(
        world, "run[exact,large stream]", argv=["true"], output="base64", output_limit_bytes=100_000
    ) == [
        "POST /process",
        "GET /filesystem/<stage>.o",  # API shape: the waited answer is JSON text
        "POST /process",  # removable: best-effort `rm` of the staging files
    ]


def test_run_exact_that_times_out(world: World) -> None:
    world.fake.staged = (b"out", b"err")
    result = asyncio.run(
        world.provider.run(
            world.box, RunRequest(argv=["sleep", "99"], output="base64", timeout_ms=1000)
        )
    )
    assert result.timed_out and base64.b64decode(result.stdout_base64 or "") == b"out"
    world.fake.staged = (b"out", b"err")
    assert _run(
        world, "run[exact,timeout]", argv=["sleep", "99"], output="base64", timeout_ms=1000
    ) == [
        "POST /process",
        "GET /filesystem/<stage>.o",  # API shape
        "GET /filesystem/<stage>.e",  # API shape
        "POST /process",  # removable: best-effort `rm`
    ]


# -- files -----------------------------------------------------------------------------


def test_files_read_write_and_list(world: World, tmp_path: Path) -> None:
    provider, box = world.provider, world.box
    assert world.calls(
        "files_write", provider.files_write(box, "/tmp/f", _body(b"abc"), size=3)
    ) == ["PUT /filesystem//tmp/f"]

    async def read() -> bytes:
        stream = await provider.files_read(box, "/tmp/f")
        return b"".join([c async for c in stream.chunks])

    assert world.calls("files_read", read()) == ["GET /filesystem//tmp/f"]
    world.fake.files_dir = tmp_path
    (tmp_path / "file").write_text("x")
    assert world.calls("files_list", provider.files_list(box, str(tmp_path))) == [
        f"GET /filesystem/{tmp_path}"  # API shape: Blaxel's own listing, no process
    ]
    (tmp_path / "sub").mkdir()
    assert world.calls("files_list[dirs]", provider.files_list(box, str(tmp_path))) == [
        f"GET /filesystem/{tmp_path}",
        "POST /process",  # a directory's size is not in the listing: one `stat` for the page's dirs
    ]


def test_files_list_normalizes_dot_segments(world: World, tmp_path: Path) -> None:
    # The runtime answers 400 to a `..` segment, so the path that reaches the wire is the
    # normalized one: `..` inside a path folds; a `..` that escapes the path's own frame is
    # anchored at the sandbox's working directory, which one `pwd` command learns once (the
    # fake runs it in files_dir, so `..` climbs out of it); a plain relative path is the
    # daemon's to resolve.
    world.fake.files_dir = tmp_path
    provider, box = world.provider, world.box

    def listed(path: str) -> list[str]:
        since = len(world.fake.requests)
        with pytest.raises(SpsError):
            asyncio.run(provider.files_list(box, path))
        return exchanges(world.fake, box, since)

    assert listed("/a/../b") == ["GET /filesystem//b"]
    assert listed("../tmp/lc") == [
        "POST /process",  # the one command that learns the working directory
        f"GET /filesystem/{tmp_path.parent}/tmp/lc",
    ]
    assert listed("../../tmp/lc") == [  # the learned directory is kept for the sandbox
        f"GET /filesystem/{tmp_path.parent.parent}/tmp/lc"
    ]
    assert listed("lc") == ["GET /filesystem/lc"]


def test_files_write_as_a_user(world: World) -> None:
    assert world.calls(
        "files_write[user]",
        world.provider.files_write(world.box, "/tmp/f", _body(b"abc"), size=3, user="root"),
    ) == [
        "PUT /filesystem/<stage>",  # API shape: a file API write cannot set the owner
        "POST /process",  # API shape: one command: user check, chown, switch, copy, rm staging
    ]


# -- processes -------------------------------------------------------------------------


def test_process_start(world: World) -> None:
    def start(key: str, **kw: Any) -> list[str]:
        return world.calls(
            key,
            world.provider.process_start(world.box, ProcessStartRequest(argv=["sleep", "1"], **kw)),
        )

    assert start("process_start") == ["POST /process"]
    assert start("process_start[cwd]", cwd="/dir") == [
        "GET /filesystem//dir",  # removable: the same pre-check as run
        "POST /process",
    ]
    assert start("process_start[user]", user="root") == [
        "POST /process",  # a synchronous not_found for a missing user needs this read
        "POST /process",
    ]


def test_process_output_without_a_wait_is_one_exchange(world: World) -> None:
    started = asyncio.run(
        world.provider.process_start(world.box, ProcessStartRequest(argv=["cat"], stdin=True))
    )
    assert world.calls("process_output", world.provider.process_output(world.box, started.id)) == [
        "GET /process/<process>",  # API shape: the record holds the state and the output so far
    ]


@pytest.mark.parametrize(
    ("record", "cursor", "why"),
    [
        ({"stdout": None}, None, "a stream missing from the record"),
        ({"stdout": "[... truncated, see the log file ...]\nend\n"}, None, "Blaxel's marker"),
        ({"stdout": "ab"}, "5.0", "shorter than what was already returned"),
        ({"stdout": "", "stderr": "", "status": "completed"}, None, "an ended process, no output"),
    ],
)
def test_process_output_reads_the_logs_only_when_the_record_may_not_hold_it(
    world: World, record: dict[str, Any], cursor: str | None, why: str
) -> None:
    started = asyncio.run(
        world.provider.process_start(world.box, ProcessStartRequest(argv=["cat"], stdin=True))
    )
    asyncio.run(world.provider.process_write_stdin(world.box, started.id, b"hello"))
    world.fake.record = record
    since = len(world.fake.requests)
    page = asyncio.run(world.provider.process_output(world.box, started.id, cursor=cursor))
    assert exchanges(world.fake, world.box, since) == [
        "GET /process/<process>",
        "GET /process/<process>/logs",
    ], why
    seen = int((cursor or "0.0").split(".")[0])
    assert "".join(c.data for c in page.chunks if c.stream == "stdout") == "hello"[seen:]


def max_output_calls(seconds: float) -> int:
    """A waiting output read: one exchange a pass, the pause 0.1 s doubling to 2 s (five pauses,
    3.1 s, to get there), then every 2 s, up to the wait."""
    ramp = 0.1 * (2**5 - 1)
    return 1 + 5 + math.ceil(max(0.0, seconds - ramp) / 2)


@pytest.mark.parametrize("wait_ms", [3200, 30_000])
def test_a_waiting_output_read_is_bounded_by_the_wait(
    world: World, monkeypatch: pytest.MonkeyPatch, wait_ms: int
) -> None:
    started = asyncio.run(
        world.provider.process_start(world.box, ProcessStartRequest(argv=["cat"], stdin=True))
    )

    async def scenario() -> Clock:
        clock = Clock(monkeypatch)
        page = await world.provider.process_output(world.box, started.id, wait_ms=wait_ms)
        assert page.state == "running" and page.chunks == []  # a quiet process: it waits it out
        return clock

    since = len(world.fake.requests)
    clock = asyncio.run(scenario())
    listed = exchanges(world.fake, world.box, since)
    assert clock.pauses[:5] == poll_pauses("process_output", 5)
    assert listed == ["GET /process/<process>"] * (len(clock.pauses) + 1)
    assert len(listed) <= max_output_calls(wait_ms / 1000)
    # a pass is one exchange: the record carries the state and the output
    assert len(listed) == BUDGET["process_output"] + len(clock.pauses)


# -- lifetime --------------------------------------------------------------------------


def test_lifetime_extend_writes_the_ttl_only_when_it_must_grow(world: World) -> None:
    """`lifetime_extend` is in `wire-budget.json` at its larger branch: Cloud sends it after every
    resume on a class that offers `lifetime.extend` (cloud#9580), and on `POST /lifetime`; with no
    key the benchmark's exchange audit withholds those steps (`no-exchange-budget`)."""
    world.box = asyncio.run(
        world.provider.create(CreateRequest(lifetime_seconds=300), idempotency_key="k-life")
    ).id
    grows = world.calls("lifetime_extend", world.provider.lifetime_extend(world.box, 600))
    assert (
        grows
        == [
            "GET /sandboxes/<box>",  # API shape: the ttl is an age from creation; `expiresIn` says the rest
            "PUT /sandboxes/<box>",  # the new ttl, in place, with no restart
        ]
    )
    # A ttl that already covers the seconds asked is the read alone, within the budget.
    since = len(world.fake.requests)
    asyncio.run(world.provider.lifetime_extend(world.box, 100))
    covered = exchanges(world.fake, world.box, since)
    assert covered == ["GET /sandboxes/<box>"]
    assert len(covered) < BUDGET["lifetime_extend"]


# -- cost ------------------------------------------------------------------------------


def test_cost_is_the_record_and_the_metrics(world: World) -> None:
    first = world.calls("cost[first use]", world.provider.cost(world.box))
    assert first == [
        "GET /sandboxes/<box>",  # API shape: the cost window starts at createdAt
        "GET /accounts",  # first use only: the account is remembered
        "GET /accounts/acct-1/metrics",  # one page; one more per cursor page or 7-day window
    ]
    assert world.calls("cost", world.provider.cost(world.box)) == [first[0], first[2]]


def _copy_as(user: str, stage: Path, path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/sh", "-c", COPY_AS, "sps-write", user, str(stage), SWITCH, str(path)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_copy_as_a_user_copies_and_removes_the_staging_file(tmp_path: Path) -> None:
    stage, target = tmp_path / "stage", tmp_path / "sub" / "file"
    stage.write_bytes(b"abc")
    done = _copy_as(getpass.getuser(), stage, target)
    assert done.returncode == 0 and target.read_bytes() == b"abc" and not stage.exists()


def test_the_copy_as_a_missing_user_ends_126_and_removes_the_staging_file(tmp_path: Path) -> None:
    stage = tmp_path / "stage"
    stage.write_bytes(b"abc")
    done = _copy_as("no-such-user-xyz", stage, tmp_path / "file")
    assert done.returncode == 126 and not stage.exists() and not (tmp_path / "file").exists()


def test_a_write_as_a_user_the_sandbox_lacks_is_not_found(world: World) -> None:
    world.fake.copy_exit = 126
    with pytest.raises(SpsError) as error:
        asyncio.run(
            world.provider.files_write(world.box, "/tmp/f", _body(b"abc"), size=3, user="nobody")
        )
    assert error.value.code == "not_found"


def test_a_write_as_an_unknown_user_is_not_found_and_leaves_no_staging_file(
    world: World, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The error is what the separate `getent` call gave on main: `require_user` raised
    `SpsError("not_found", "The sandbox has no such user")` (outcome "failed"), and the staging
    file that the upload made is gone from the sandbox when the command ends."""
    monkeypatch.setattr(guest, "STAGE", f"{tmp_path}/.sps-run-")
    world.fake.real_copy = True
    since = len(world.fake.requests)
    with pytest.raises(SpsError) as error:
        asyncio.run(
            world.provider.files_write(
                world.box, str(tmp_path / "out"), _body(b"abc"), size=3, user="no-such-user-xyz"
            )
        )
    assert (str(error.value), error.value.outcome) == (
        "not_found: The sandbox has no such user",
        "failed",
    )
    assert [r.method for r in world.fake.requests[since:]] == ["PUT", "POST"]
    assert list(tmp_path.iterdir()) == []  # no staging file, and nothing copied to the path


def test_a_write_as_a_user_through_the_real_copy_leaves_no_staging_file(
    world: World, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(guest, "STAGE", f"{tmp_path}/.sps-run-")
    world.fake.real_copy = True
    target = tmp_path / "sub" / "out"
    asyncio.run(
        world.provider.files_write(
            world.box, str(target), _body(b"abc"), size=3, user=getpass.getuser()
        )
    )
    assert target.read_bytes() == b"abc"
    assert [p.name for p in tmp_path.iterdir()] == ["sub"]


def test_a_timed_out_command_of_the_adapter_reads_nothing_after_the_timeout(world: World) -> None:
    since = len(world.fake.requests)
    with pytest.raises(SpsError) as error:
        asyncio.run(world.provider.quick(world.box, ["sleep", "99"]))
    assert error.value.code == "timeout"
    assert exchanges(world.fake, world.box, since) == ["POST /process"]


def test_a_cold_provider_object_marks_only_its_lookups(world: World) -> None:
    box = world.box
    started = asyncio.run(
        world.provider.process_start(box, ProcessStartRequest(argv=["cat"], stdin=True))
    )
    lookups = ["GET /workspaces", "GET /sandboxes/<box>"]

    def _read(provider: BlaxelProvider) -> Any:
        async def read() -> bytes:
            stream = await provider.files_read(box, "/tmp/f")
            return b"".join([c async for c in stream.chunks])

        return read()

    asyncio.run(world.provider.files_write(box, "/tmp/f", _body(b"abc"), size=3))
    assert world.calls(
        "create[cold]", world.cold().create(CreateRequest(), idempotency_key="k9")
    ) == [
        lookups[0],
        "POST /sandboxes",
    ]
    assert world.calls("get[cold]", world.cold().get(box)) == [lookups[0], "GET /sandboxes/<box>"]
    assert world.calls("run[cold]", world.cold().run(box, RunRequest(argv=["true"]))) == [
        *lookups,
        "POST /process",
    ]
    assert world.calls("files_read[cold]", _read(world.cold())) == [
        *lookups,
        "GET /filesystem//tmp/f",
    ]
    assert world.calls(
        "files_write[cold]", world.cold().files_write(box, "/tmp/g", _body(b"abc"), size=3)
    ) == [*lookups, "PUT /filesystem//tmp/g"]
    assert world.calls("process_output[cold]", world.cold().process_output(box, started.id)) == [
        *lookups,
        "GET /process/<process>",
    ]
    # cost reads the record as its work, not to find an address: nothing is marked
    assert world.calls("cost[cold]", world.cold().cost(box)) == [
        lookups[0],
        "GET /sandboxes/<box>",
        "GET /accounts",
        "GET /accounts/acct-1/metrics",
    ]


def test_the_budget_declares_a_lookup_for_every_marked_operation() -> None:
    """The benchmark withholds a step that made more lookups than `wire-budget.json` declares."""
    declared = {k: op["lookup"] for k, op in WIRE_BUDGET["operations"].items() if "lookup" in op}
    marked = {key.removesuffix("[cold]"): count for key, count in LOOKUPS.items()}
    assert {key: declared.get(key) for key in marked} == marked


def test_the_budget_declares_the_lookups_a_cold_call_adds(world: World, tmp_path: Path) -> None:
    """`wire-budget.json` says per operation how many lookup exchanges a cold call adds (the
    benchmark withholds a step that made more): the one record read that finds the runtime
    address, for every operation that goes to the sandbox's runtime."""
    declared = {k: op["lookup"] for k, op in WIRE_BUDGET["operations"].items() if "lookup" in op}
    cold_keys = {key.removesuffix("[cold]") for key in LOOKUPS}
    assert declared == {
        **dict.fromkeys(cold_keys, 1),
        **dict.fromkeys(
            [
                "files_list",
                "files_write[user]",
                "pause",
                "process_start",
                "process_start[cwd]",
                "process_start[user]",
                "resume",
                "run[cwd]",
                "run[exact]",
                "run[user]",
            ],
            1,
        ),
        # a cold create-from-image resolves the record and the tag before the POST
        "create[image]": 2,
    }
    started = {
        "files_list": lambda p: p.files_list(world.box, str(tmp_path)),
        "run[cwd]": lambda p: p.run(world.box, RunRequest(argv=["true"], cwd="/dir")),
        "files_write[user]": lambda p: p.files_write(
            world.box, "/tmp/f", _body(b"abc"), size=3, user="root"
        ),
        "process_start": lambda p: p.process_start(world.box, ProcessStartRequest(argv=["x"])),
        "pause": lambda p: p.pause(world.box),
        "resume": lambda p: p.resume(world.box),
        "process_start[cwd]": lambda p: p.process_start(
            world.box, ProcessStartRequest(argv=["x"], cwd="/dir")
        ),
        "process_start[user]": lambda p: p.process_start(
            world.box, ProcessStartRequest(argv=["x"], user="root")
        ),
    }
    world.fake.files_dir = tmp_path
    world.fake.standby_reads = 0
    for key, operation in started.items():
        since = len(world.fake.requests)
        asyncio.run(operation(world.cold()))
        marked = [m for m in world.fake.marked[since:] if m]
        assert len(marked) == declared[key], key
