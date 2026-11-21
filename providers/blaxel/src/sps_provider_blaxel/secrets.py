# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Write-only secrets on create, as Blaxel's outbound proxy routes.

Blaxel adds a secret to requests that the sandbox sends to named hosts: a route lists the
destination hosts, the headers to set (with `{{SECRET:name}}` where the value goes) and the
secrets themselves. The firewall's `proxy` ruleset sends all of the sandbox's HTTPS through that
proxy. Blaxel sets the header by itself, so the `placeholder` is never substituted: code that
sends it sends a harmless string, and the value reaches only the destination. The value goes to
Blaxel in the create body and is in no message of this adapter.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from sandbox_provider_spec.v1 import Secret, SpsError

_HEADER = "X-SPS-Secret"
_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")


def _exact_host(host: str) -> bool:
    """One host name, not a URL or a pattern: Blaxel routes by exact destination."""
    if not host or any(c in host for c in ":/*?#\\@") or any(c.isspace() for c in host):
        return False
    name = host.removesuffix(".")
    return len(name) <= 253 and all(_LABEL.fullmatch(label) for label in name.split("."))


def routes(secrets: Sequence[Secret]) -> list[dict[str, Any]]:
    """One proxy route for each secret. The message of a refusal never holds a value."""
    result = []
    for index, secret in enumerate(secrets):
        if not secret.value:
            raise SpsError("invalid_request", f"Secret {index} has no value", field="secrets")
        hosts = list(secret.hosts)
        if not hosts or len(set(hosts)) != len(hosts):
            raise SpsError(
                "invalid_request", f"Secret {index} needs distinct hosts", field="secrets"
            )
        name = f"sps-secret-{index}"
        headers = {
            h.name: f"{h.prefix}{{{{SECRET:{name}}}}}{h.suffix}" for h in secret.headers
        } or {_HEADER: f"{{{{SECRET:{name}}}}}"}
        if secret.headers and not all(_exact_host(h) for h in hosts):
            raise SpsError(
                "unsupported",
                f"Blaxel sets a header only for an exact host name (secret {index})",
            )
        result.append({"destinations": hosts, "headers": headers, "secrets": {name: secret.value}})
    return result


def network(secrets: Sequence[Secret]) -> dict[str, Any]:
    """The `spec.network` that routes the secrets and sends HTTPS through the proxy."""
    return {"proxy": {"routing": routes(secrets)}, "firewall": {"rulesets": ["proxy"]}}
