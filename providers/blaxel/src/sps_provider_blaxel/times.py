# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""A sandbox's provider-reported times: Blaxel's own event times, from `events[]` on the record."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from sandbox_provider_spec.v1 import ProviderTime
from sandbox_provider_spec.v1.types import TimeKind

from sps_provider_blaxel import wire

_STATUS_KIND: dict[str, TimeKind] = {
    "CREATED": "created",
    "DEPLOYED": "ready",
    "PAUSED": "paused",
    "RESUMED": "resumed",
    "STOPPED": "stopped",
    "TERMINATED": "ended",
}
# Accepting a delete is a stop request, not the end: Blaxel bills a tail after it.
_DELETE_EVENT = "ai.blaxel.controlplane.api.delete"


def _stamp(value: object) -> Any:
    if not isinstance(value, str) or not value:
        return None
    try:
        at = wire.stamp(value)
    except ValueError:
        return None
    return at if at.utcoffset() == timedelta(0) else None


def times_of(native: dict[str, Any]) -> tuple[ProviderTime, ...]:
    """Every time Blaxel states on the record: each event's `time`, the deadline (`createdAt` plus
    `spec.runtime.ttl`, because the ttl counts from creation) and `lastUsedAt`. A time that Blaxel
    gives without an event that names it is `updated`; none is a clock of ours."""
    found: list[ProviderTime] = []
    for event in native.get("events") or []:
        at = _stamp(event.get("time")) if isinstance(event, dict) else None
        if at is None:
            continue
        label = str(event.get("type") or event.get("status") or "event")
        kind: TimeKind = (
            "stop_requested"
            if event.get("type") == _DELETE_EVENT
            else _STATUS_KIND.get(str(event.get("status")).upper(), "updated")
        )
        found.append(ProviderTime(kind, at, f"events[].time ({label})"))
    created = _stamp((native.get("metadata") or {}).get("createdAt"))
    ttl = wire.ttl_seconds(((native.get("spec") or {}).get("runtime") or {}).get("ttl"))
    if created is not None and ttl is not None:
        found.append(
            ProviderTime(
                "deadline",
                created + timedelta(seconds=ttl),
                "metadata.createdAt + spec.runtime.ttl",
            )
        )
    last_used = _stamp(native.get("lastUsedAt"))
    if last_used is not None:
        found.append(ProviderTime("ready", last_used, "lastUsedAt"))
    return tuple(found)
