# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Pause and resume: Blaxel's standby, asked for.

`POST /standby?force=true` is answered 202 before the sandbox is in standby, and the control
plane's `state` lags a resume by seconds, so these tests drive the fake's record through both:
a pause waits for STANDBY (and not for a STANDBY it already showed), a resume is one call, and a
second pause and resume work as the first did."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sandbox_provider_spec.v1 import CreateRequest, SpsError, waiting
from test_offline import EARLIER, NOW, FakeBlaxel

from sps_provider_blaxel import BlaxelProvider


class Sandbox:
    def __init__(self) -> None:
        self.fake = FakeBlaxel()
        self.provider = BlaxelProvider({"api_key": "key-1"}, transport=self.fake.transport())
        self.box = asyncio.run(self.provider.create(CreateRequest(), idempotency_key="k0")).id

    def sent(self, since: int) -> list[str]:
        return [
            f"{r.method} {r.url.path.removeprefix('/v0').replace(self.box, '<box>')}"
            for r in self.fake.requests[since:]
        ]


@pytest.fixture()
def sandbox(monkeypatch: pytest.MonkeyPatch) -> Sandbox:
    async def instant(seconds: float) -> None:  # the polls cost no time here
        return None

    monkeypatch.setattr(waiting, "pause", instant)
    return Sandbox()


def test_a_pause_returns_once_the_record_says_standby(sandbox: Sandbox) -> None:
    paused = asyncio.run(sandbox.provider.pause(sandbox.box))
    assert paused.state == "paused"
    assert asyncio.run(sandbox.provider.get(sandbox.box)).state == "paused"


def test_a_resume_is_one_call_and_get_reads_running_before_the_control_plane_catches_up(
    sandbox: Sandbox,
) -> None:
    asyncio.run(sandbox.provider.pause(sandbox.box))
    since = len(sandbox.fake.requests)
    assert asyncio.run(sandbox.provider.resume(sandbox.box)) is None
    assert sandbox.sent(since) == ["POST /resume"]
    # The record still says STANDBY (Blaxel's `state` lags the resume) with the standby's time,
    # which predates the resume: it is the earlier standby's, and the sandbox runs.
    sandbox.fake.sandboxes[sandbox.box]["lastUsedAt"] = EARLIER
    assert sandbox.fake.sandboxes[sandbox.box]["state"] == "STANDBY"
    assert asyncio.run(sandbox.provider.get(sandbox.box)).state == "running"
    page = asyncio.run(sandbox.provider.list())
    assert [s.state for s in page.sandboxes] == ["running"]
    # a later read finds the control plane caught up
    for _ in range(sandbox.fake.resume_reads):
        asyncio.run(sandbox.provider.get(sandbox.box))
    assert sandbox.fake.sandboxes[sandbox.box]["state"] == "RUNNING"
    assert asyncio.run(sandbox.provider.get(sandbox.box)).state == "running"


def test_a_remembered_resume_is_dropped_once_a_read_no_longer_needs_it(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    asyncio.run(sandbox.provider.pause(sandbox.box))
    asyncio.run(sandbox.provider.resume(sandbox.box))
    assert len(sandbox.provider._resumed) == 1
    for _ in range(sandbox.fake.resume_reads + 1):  # the lag ends: the record says RUNNING
        asyncio.run(sandbox.provider.get(sandbox.box))
    assert sandbox.provider._resumed == {}


def test_a_remembered_resume_is_dropped_after_a_minute_and_by_a_pause(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        provider = sandbox.provider
        await provider.pause(sandbox.box)
        await provider.resume(sandbox.box)
        assert len(provider._resumed) == 1
        loop = asyncio.get_running_loop()
        real = loop.time
        monkeypatch.setattr(loop, "time", lambda: real() + 61)
        other = (await provider.create(CreateRequest(), idempotency_key="k-other")).id
        await provider.resume(other)  # any resume forgets what is older than a minute
        assert set(provider._resumed) == {other}
        await provider.pause(sandbox.box)
        await provider.resume(sandbox.box)
        await provider.pause(sandbox.box)  # a pause ends the need as well
        assert sandbox.box not in provider._resumed

    asyncio.run(scenario())


def test_a_record_of_another_object_is_read_as_it_is(sandbox: Sandbox) -> None:
    """Only the object that sent the resume knows its time: another one reports STANDBY."""
    asyncio.run(sandbox.provider.pause(sandbox.box))
    asyncio.run(sandbox.provider.resume(sandbox.box))
    other = BlaxelProvider({"api_key": "key-1"}, transport=sandbox.fake.transport())
    assert asyncio.run(other.get(sandbox.box)).state == "paused"


def test_a_second_pause_is_not_answered_by_the_standby_of_the_first(sandbox: Sandbox) -> None:
    asyncio.run(sandbox.provider.pause(sandbox.box))
    asyncio.run(sandbox.provider.resume(sandbox.box))
    # The control plane has not caught up: it still shows the first standby, dated before now.
    sandbox.fake.sandboxes[sandbox.box]["lastUsedAt"] = EARLIER
    since = len(sandbox.fake.requests)
    again = asyncio.run(sandbox.provider.pause(sandbox.box))
    assert again.state == "paused"
    assert sandbox.fake.sandboxes[sandbox.box]["lastUsedAt"] == NOW
    listed = sandbox.sent(since)
    # one POST, then reads until the new standby (the stale ones do not end the wait)
    assert listed[0] == "POST /standby"
    assert len(listed) - 1 > sandbox.fake.standby_reads
    assert set(listed[1:]) == {"GET /sandboxes/<box>"}


def test_two_cycles_keep_a_running_process_and_its_files(sandbox: Sandbox) -> None:
    """The counter's lifetime is the guest's, not the adapter's: the adapter's part of a cycle is
    that both calls succeed in turn and the sandbox reads paused, then running, each time."""
    states = []
    for _ in range(2):
        states.append(asyncio.run(sandbox.provider.pause(sandbox.box)).state)
        asyncio.run(sandbox.provider.resume(sandbox.box))
        sandbox.fake.sandboxes[sandbox.box]["lastUsedAt"] = EARLIER
        states.append(asyncio.run(sandbox.provider.get(sandbox.box)).state)
    assert states == ["paused", "running", "paused", "running"]


class Virtual:
    """Time that moves only when the provider pauses (a 60 s wait costs no time to test)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.pauses: list[float] = []
        self.skipped = 0.0
        loop = asyncio.get_running_loop()
        real = loop.time
        monkeypatch.setattr(loop, "time", lambda: real() + self.skipped)
        monkeypatch.setattr(waiting, "pause", self.pause)

    async def pause(self, seconds: float) -> None:
        self.pauses.append(seconds)
        self.skipped += seconds


def test_a_pause_the_host_ignores_is_sent_once_more_after_the_quiet_window(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    sandbox.fake.ignored_standbys = 1

    async def scenario() -> Virtual:
        clock = Virtual(monkeypatch)
        assert (await sandbox.provider.pause(sandbox.box)).state == "paused"
        return clock

    since = len(sandbox.fake.requests)
    clock = asyncio.run(scenario())
    listed = sandbox.sent(since)
    assert listed.count("POST /standby") == 2  # the second one stood
    assert listed[0] == "POST /standby"
    resend_at = listed.index("POST /standby", 1)
    assert sum(clock.pauses[: resend_at - 1]) >= 8  # past the host's quiet window


def test_a_pause_of_a_sandbox_already_in_standby_that_the_host_wakes_and_does_not_put_back(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live 2026-11-23: a standby request to a sandbox in standby woke it; one holding a
    keep-alive process then stayed running. The record read STANDBY (the old one) and then
    RUNNING; the second request puts it back."""
    asyncio.run(sandbox.provider.pause(sandbox.box))
    sandbox.fake.sandboxes[sandbox.box]["lastUsedAt"] = EARLIER  # an idle standby of before
    sandbox.fake.ignored_standbys = 1

    async def scenario() -> Virtual:
        clock = Virtual(monkeypatch)
        assert (await sandbox.provider.pause(sandbox.box)).state == "paused"
        return clock

    since = len(sandbox.fake.requests)
    asyncio.run(scenario())
    assert sandbox.sent(since).count("POST /standby") == 2
    assert sandbox.fake.sandboxes[sandbox.box]["lastUsedAt"] == NOW


def test_a_standby_the_resend_woke_is_not_the_answer_a_late_first_standby_gives(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first standby happened but the record said RUNNING for 8 s: the resend then WAKES the
    sandbox, and the record then shows the FIRST standby (dated before the resend). That is not a
    pause: the guest runs. Only a STANDBY dated at or after the resend is."""
    first, second = "Sat, 21 Nov 2026 01:21:30 GMT", "Sat, 21 Nov 2026 01:21:50 GMT"
    sandbox.fake.standby_dates = [first, second]
    sandbox.fake.standby_reads = 99  # the fake's own standby never shows: the script does
    running = {"state": "RUNNING", "lastUsedAt": "2026-11-21T01:21:00.000Z"}
    late_first = {"state": "STANDBY", "lastUsedAt": "2026-11-21T01:21:31.000Z"}
    after_resend = {"state": "STANDBY", "lastUsedAt": "2026-11-21T01:21:50.000Z"}
    sandbox.fake.scripted = [running] * 10 + [late_first, late_first, after_resend]

    async def scenario() -> None:
        Virtual(monkeypatch)
        assert (await sandbox.provider.pause(sandbox.box)).state == "paused"

    since = len(sandbox.fake.requests)
    asyncio.run(scenario())
    listed = sandbox.sent(since)
    assert listed.count("POST /standby") == 2
    assert listed.count("GET /sandboxes/<box>") == 13  # the two stale STANDBY reads were refused
    assert sandbox.fake.scripted == []


def test_a_pause_that_is_never_seen_ends_with_an_unknown_timeout(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    sandbox.fake.ignored_standbys = 99

    async def scenario() -> Virtual:
        clock = Virtual(monkeypatch)
        with pytest.raises(SpsError) as raised:
            await sandbox.provider.pause(sandbox.box)
        assert raised.value.code == "timeout"
        assert raised.value.outcome == "unknown"
        return clock

    since = len(sandbox.fake.requests)
    clock = asyncio.run(scenario())
    assert sandbox.sent(since).count("POST /standby") == 2  # sent again once, never more
    assert 55 <= sum(clock.pauses) <= 60
    assert clock.pauses[:3] == [0.5, 1.0, 1.0]


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (401, "provider_error"),
        (403, "provider_error"),
        (429, "provider_error"),
        (503, "unavailable"),
    ],
)
def test_a_failed_read_after_the_standby_was_accepted_is_unknown(
    sandbox: Sandbox, status: int, code: str
) -> None:
    import httpx

    sandbox.fake.fail_next[f"GET /v0/sandboxes/{sandbox.box}"] = httpx.Response(
        status, json={"error": "no"}, headers={"date": "Sat, 21 Nov 2026 01:21:30 GMT"}
    )
    with pytest.raises(SpsError) as raised:
        asyncio.run(sandbox.provider.pause(sandbox.box))
    assert (raised.value.code, raised.value.outcome) == (code, "unknown")


def test_a_409_on_the_resend_is_unknown_not_failed(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first standby was accepted: a refusal of the resend cannot say that nothing happened."""
    sandbox.fake.ignored_standbys = 99
    real = sandbox.provider._standby
    sent = [0]

    async def refusing(sandbox_id: str) -> Any:
        sent[0] += 1
        if sent[0] == 2:
            raise SpsError("conflict", "held awake")
        return await real(sandbox_id)

    monkeypatch.setattr(sandbox.provider, "_standby", refusing)

    async def scenario() -> SpsError:
        Virtual(monkeypatch)
        with pytest.raises(SpsError) as raised:
            await sandbox.provider.pause(sandbox.box)
        return raised.value

    error = asyncio.run(scenario())
    assert (error.code, error.outcome) == ("provider_error", "unknown")


def test_a_resume_of_a_running_sandbox_is_one_call_and_succeeds(sandbox: Sandbox) -> None:
    since = len(sandbox.fake.requests)
    assert asyncio.run(sandbox.provider.resume(sandbox.box)) is None
    assert sandbox.sent(since) == ["POST /resume"]


def test_lifetime_extend_after_a_resume_answers_running(sandbox: Sandbox) -> None:
    asyncio.run(sandbox.provider.pause(sandbox.box))
    asyncio.run(sandbox.provider.resume(sandbox.box))
    sandbox.fake.sandboxes[sandbox.box]["lastUsedAt"] = EARLIER
    assert asyncio.run(sandbox.provider.lifetime_extend(sandbox.box, 1)).state == "running"


@pytest.mark.parametrize(("status", "code"), [(409, "conflict"), (501, "unsupported")])
def test_a_refused_standby_request_is_one_error_and_no_polling(
    sandbox: Sandbox, status: int, code: str
) -> None:
    import httpx

    sandbox.fake.fail_next["POST /standby"] = httpx.Response(
        status, json={"error": "refused"}, headers={"date": "Sat, 21 Nov 2026 01:21:30 GMT"}
    )
    since = len(sandbox.fake.requests)
    with pytest.raises(SpsError) as raised:
        asyncio.run(sandbox.provider.pause(sandbox.box))
    assert raised.value.code == code
    assert sandbox.sent(since) == ["POST /standby"]


def test_a_sandbox_api_without_standby_makes_a_pause_unsupported_not_a_provider_error(
    sandbox: Sandbox,
) -> None:
    """Production 2026-11-23: prepared images built with sandbox-api 0.2.74 answer 404 to
    `/standby`. The sandbox is there and nothing was done: `unsupported`, never a provider error
    with an unknown outcome."""
    sandbox.fake.old_sandbox_api = True
    since = len(sandbox.fake.requests)
    with pytest.raises(SpsError) as raised:
        asyncio.run(sandbox.provider.pause(sandbox.box))
    assert (raised.value.code, raised.value.outcome) == ("unsupported", "failed")
    assert "0.2.78" in raised.value.message and "prepare the image again" in raised.value.message
    # the call, then the record that tells a missing call from a missing sandbox
    assert sandbox.sent(since) == ["POST /standby", "GET /sandboxes/<box>"]


def test_a_sandbox_api_without_resume_still_woke_the_sandbox_so_a_resume_succeeds(
    sandbox: Sandbox,
) -> None:
    """Blaxel's host wakes a sandbox on the request itself and the old sandbox-api then answers
    404: the sandbox runs, so the resume is a success (not `unsupported`, not `not_found`), and
    get reads it as running."""
    sandbox.fake.old_sandbox_api = True
    since = len(sandbox.fake.requests)
    assert asyncio.run(sandbox.provider.resume(sandbox.box)) is None
    assert sandbox.sent(since) == ["POST /resume", "GET /sandboxes/<box>"]


def test_a_404_for_a_sandbox_that_has_ended_is_not_found(sandbox: Sandbox) -> None:
    sandbox.fake.old_sandbox_api = True
    sandbox.fake.sandboxes[sandbox.box]["status"] = "TERMINATED"
    for call in (sandbox.provider.pause, sandbox.provider.resume):
        with pytest.raises(SpsError) as raised:
            asyncio.run(call(sandbox.box))
        assert raised.value.code == "not_found"


def test_a_sandbox_that_ends_during_the_wait_is_not_found(sandbox: Sandbox) -> None:
    sandbox.fake.standby_reads = 5
    original: Any = sandbox.fake._advance
    reads = [0]

    def ending(name: str, record: dict[str, Any]) -> None:
        reads[0] += 1
        if reads[0] == 2:
            record["status"] = "DELETING"
        original(name, record)

    sandbox.fake._advance = ending  # type: ignore[method-assign]
    with pytest.raises(SpsError) as raised:
        asyncio.run(sandbox.provider.pause(sandbox.box))
    assert raised.value.code == "not_found"


def test_the_pause_that_keeps_memory_is_not_claimed() -> None:
    """Withdrawn 2026-11-23: images prepared with sandbox-api 0.2.74 have no `/standby`."""
    klass = BlaxelProvider.static_discovery().classes[0]
    assert "pause_resume" not in klass.gates
    assert klass.lifetime_timer["counts_paused_time"] is True
