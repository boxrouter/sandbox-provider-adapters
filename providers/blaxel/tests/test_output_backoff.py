# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""A process output read that waits reads again after a pause that doubles to a cap, so its calls
grow with the logarithm of the wait: it paused a fixed 0.2 s, two calls a pass, which made a 60 s
wait about 600 calls against the provider's request limit. A pass is now one call (the record)."""

from __future__ import annotations

import asyncio

import pytest
from sandbox_provider_spec.v1 import CreateRequest, ProcessStartRequest, waiting
from test_offline import FakeBlaxel

from sps_provider_blaxel import BlaxelProvider


def test_a_waiting_output_read_backs_off(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeBlaxel()
    provider = BlaxelProvider({"api_key": "key-1"}, transport=fake.transport())
    pauses: list[float] = []
    real = waiting.pause

    async def recorded(seconds: float) -> None:
        pauses.append(round(seconds, 1))
        await real(seconds)

    monkeypatch.setattr(waiting, "pause", recorded)

    async def scenario() -> int:
        box = await provider.create(CreateRequest(), idempotency_key="k")
        # A process that reads its input prints nothing until it gets some: it stays quiet.
        quiet = await provider.process_start(box.id, ProcessStartRequest(argv=["cat"], stdin=True))
        before = len(fake.requests)
        page = await provider.process_output(box.id, quiet.id, wait_ms=3200)
        assert page.state == "running" and page.chunks == []
        return len(fake.requests) - before

    calls = asyncio.run(scenario())
    assert pauses[:5] == [0.1, 0.2, 0.4, 0.8, 1.6]
    assert len(pauses) <= 6  # the last pause is cut to the wait's end
    assert calls == len(pauses) + 1  # each read is the process record, which carries the output
