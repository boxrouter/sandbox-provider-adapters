# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Previews over Blaxel's named previews and their expiring tokens.

A sandbox's port is reached through a preview, one per port and access mode, named by both
(`sps-<port>-public`, `sps-<port>-private`):

- a public preview is the preview's URL, which needs no credential;
- a private preview is the same URL plus a token that Blaxel mints for it, which expires; the
  link carries it in the query (`bl_preview_token`), and `X-Blaxel-Preview-Token` takes it too;
- a link's `id` is `<preview>` for a public one and `<preview>/<token>` for a private one. Revoking a
  private link deletes its token, so other links of the port stay; revoking a public one deletes
  the preview, which ends every public link of that port.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import quote
from uuid import uuid4

from sandbox_provider_spec.v1 import PreviewLink, SpsError

from sps_provider_blaxel import wire
from sps_provider_blaxel.facts import RESERVED_PORT

if TYPE_CHECKING:
    from sps_provider_blaxel import BlaxelProvider

HEADER = "X-Blaxel-Preview-Token"
QUERY = "bl_preview_token"
DEFAULT_LINK_SECONDS = 3600
_GONE_WAIT_S = 30


def _where(sandbox_id: str) -> str:
    return f"/sandboxes/{quote(sandbox_id, safe='')}/previews"


async def _preview(p: BlaxelProvider, sandbox_id: str, port: int, public: bool) -> str:
    """The URL of the port's preview of this mode, made if it is not there: a GET that finds one
    made earlier, else a POST (and a GET again when another call made it first)."""
    name = f"sps-{port}-{'public' if public else 'private'}"
    where = _where(sandbox_id)
    resp = await p.ctl_raw("GET", f"{where}/{name}")
    if resp.status_code == 404:
        body = {"metadata": {"name": name}, "spec": {"port": port, "public": public}}
        resp = await p.ctl_raw("POST", where, json=body)
        if resp.status_code == 409:
            resp = await p.ctl_raw("GET", f"{where}/{name}")
    if resp.status_code >= 400:
        raise wire.failure(resp, "the preview")
    spec = resp.json().get("spec") or {}
    url = spec.get("url")
    if (
        spec.get("public") is not public
        or not isinstance(url, str)
        or not url.startswith("https://")
    ):
        raise SpsError(
            "provider_error", "Blaxel returned a preview of another mode", outcome="unknown"
        )
    return url


async def link(
    p: BlaxelProvider,
    sandbox_id: str,
    port: int,
    *,
    access: str,
    lifetime_seconds: int | None,
) -> PreviewLink:
    if port == RESERVED_PORT:
        raise SpsError("unsupported", f"Blaxel keeps port {RESERVED_PORT} for its sandbox API")
    if access not in ("public", "private"):
        raise SpsError("invalid_request", "access is public or private")
    if access == "public":
        url = await _preview(p, sandbox_id, port, True)
        return PreviewLink(
            url=url, headers={}, expires_at=None, id=f"sps-{port}-public", access="public"
        )
    seconds = DEFAULT_LINK_SECONDS if lifetime_seconds is None else lifetime_seconds
    if seconds < 1:
        raise SpsError("invalid_request", "lifetime_seconds must be at least 1")
    url = await _preview(p, sandbox_id, port, False)
    name = f"sps-{port}-private"
    expires = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=seconds)
    body = {
        "metadata": {"name": f"sps-{uuid4().hex}"},
        "spec": {"expiresAt": expires.strftime("%Y-%m-%dT%H:%M:%SZ")},
    }
    resp = await p.ctl(
        "POST", f"{_where(sandbox_id)}/{name}/tokens", "the preview token", json=body
    )
    made = resp.json()
    token = (made.get("spec") or {}).get("token")
    token_name = (made.get("metadata") or {}).get("name")
    if not isinstance(token, str) or not token or not isinstance(token_name, str):
        raise SpsError("provider_error", "Blaxel returned no preview token", outcome="unknown")
    # Blaxel takes the token in the query (`bl_preview_token`, which it then keeps in a cookie) or
    # in `X-Blaxel-Preview-Token`. The link carries it in the query; the header is the same
    # credential sent the other way, so a caller may use either.
    return PreviewLink(
        url=f"{url}?{QUERY}={quote(token, safe='')}",
        headers={HEADER: token},
        expires_at=expires,
        id=f"{name}/{token_name}",
        access="private",
        credential="link",
    )


async def revoke(p: BlaxelProvider, sandbox_id: str, preview_id: str) -> None:
    """End the link. Blaxel answers 404 for a preview or token that is already gone, and that
    succeeds."""
    name, _, token = preview_id.partition("/")
    if not name.startswith("sps-"):
        raise SpsError("not_found", "Blaxel holds no such preview")
    where = f"{_where(sandbox_id)}/{quote(name, safe='')}"
    if token:
        resp = await p.ctl_raw("DELETE", f"{where}/tokens/{quote(token, safe='')}")
        if resp.status_code not in (200, 202, 204, 404):
            raise wire.failure(resp, "the preview token")
        return
    resp = await p.ctl_raw("DELETE", where)
    if resp.status_code == 404:
        return
    if resp.status_code >= 400:
        raise wire.failure(resp, "the preview")
    # The preview is deleted asynchronously (status DELETING): the revoke returns once it is gone.
    deadline = asyncio.get_running_loop().time() + _GONE_WAIT_S
    while asyncio.get_running_loop().time() < deadline:
        found = await p.ctl_raw("GET", where)
        if found.status_code == 404:
            return
        await asyncio.sleep(0.2)
    raise SpsError("timeout", "Blaxel did not finish deleting the preview", outcome="unknown")
