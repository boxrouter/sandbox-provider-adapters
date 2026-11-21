# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Blaxel's wire formats: its timestamps, its status words and its error replies."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import httpx
from sandbox_provider_spec.v1 import SandboxState, SpsError

_FRACTION = re.compile(r"(\.\d{6})\d+")
_TTL = re.compile(r"(\d+)([smhd])")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

# A sandbox's `status`. DEPLOYED covers STANDBY too: Blaxel's own idle sleep wakes on use.
ENDED = frozenset({"DELETING", "TERMINATED", "FAILED"})
TERMINAL_PROCESS = frozenset({"completed", "failed", "killed", "stopped"})


def stamp(text: str) -> datetime:
    """A Blaxel RFC 3339 time, which has nanoseconds; Python keeps microseconds."""
    return datetime.fromisoformat(_FRACTION.sub(r"\1", text))


def http_date(value: str | None) -> datetime | None:
    """An HTTP date such as a `Date` header or a process's `startedAt`, in UTC."""
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).astimezone(UTC)
    except (TypeError, ValueError):
        return None


def response_time(response: httpx.Response) -> datetime:
    """Blaxel's own clock at the reply: its `Date` header."""
    found = http_date(response.headers.get("date"))
    if found is None:
        raise SpsError("provider_error", "Blaxel sent no Date header", outcome="unknown")
    return found


def ttl_seconds(ttl: object) -> int | None:
    """A Blaxel lifetime such as "600s", "15m" or "24h" in seconds."""
    matched = _TTL.fullmatch(ttl) if isinstance(ttl, str) else None
    return int(matched[1]) * _UNIT_SECONDS[matched[2]] if matched else None


def sandbox_state(status: str, state: str | None) -> SandboxState:
    if status in ENDED:
        return "destroyed"
    if status in ("DEPLOYED", "DEPLOYING", "UPDATING"):
        # Blaxel's idle sleep keeps memory and wakes on the next call; SPS calls it paused.
        return "paused" if state == "STANDBY" else "running"
    return "stopped"  # ARCHIVED, ARCHIVING and the like


def retry_after(response: httpx.Response) -> int | None:
    value = response.headers.get("retry-after", "")
    return int(value) if value.isdecimal() else None


def failure(response: httpx.Response, what: str) -> SpsError:
    """The `SpsError` for a Blaxel error reply. The message never carries the reply's body."""
    status = response.status_code
    where = f"Blaxel answered HTTP {status} to {what}"
    if status == 401:
        return SpsError("unauthenticated", "Blaxel rejected the API key")
    if status == 403:
        return SpsError("forbidden", where)
    if status == 402:
        return SpsError("quota_exceeded", where)
    if status == 404:
        return SpsError("not_found", f"{what}: not found")
    if status == 409:
        return SpsError("conflict", where)
    if status == 413:
        return SpsError("too_large", where)
    if status == 429:
        return SpsError("rate_limited", where, retry_after_seconds=retry_after(response))
    if status in (400, 422):
        return SpsError("invalid_request", where)
    if status == 504:
        return SpsError("timeout", where, outcome="unknown")
    if status == 503:
        return SpsError("unavailable", where, outcome="unknown")
    return SpsError("provider_error", where, outcome="unknown")


def error_code(response: httpx.Response) -> str | None:
    """Blaxel's own `code` string of an error reply, such as `SANDBOX_ALREADY_EXISTS`."""
    try:
        body = response.json()
    except ValueError:
        return None
    code = body.get("code") if isinstance(body, dict) else None
    return code if isinstance(code, str) else None


def hour_start(moment: datetime) -> datetime:
    return moment.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


HOUR = timedelta(hours=1)
