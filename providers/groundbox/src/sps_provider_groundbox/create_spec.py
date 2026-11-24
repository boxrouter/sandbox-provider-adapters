# SPDX-FileCopyrightText: Groundbox
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Translate a 1.0 `CreateRequest` into the body of Groundbox's `POST /v1/sandboxes`."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from sandbox_provider_spec.v1 import CreateRequest, SpsError

from sps_provider_groundbox.facts import (
    CLASS,
    DEFAULT_SIZE,
    IMAGE,
    LIFETIME_STEP_S,
    MAX_CPUS,
    MAX_DISK_GB,
    MAX_LIFETIME_S,
    MEMORY_GB_PER_CPU,
    MIN_LIFETIME_S,
)

_MIB_PER_GB = 1024


def _number(key: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise SpsError("invalid_request", f"resources.{key} must be a positive number")
    return float(value)


def _offers(value: object, wanted: str) -> bool:
    """Whether a resource ask (one value or a list of them) accepts `wanted`."""
    return wanted in value if isinstance(value, (list, tuple)) else value == wanted


def plan_size(resources: Mapping[str, object]) -> dict[str, int]:
    """The smallest Groundbox size that meets the floors: `cpus`, `memory_gb` and, when a disk
    floor is given, `disk_gb`. Groundbox boots the size asked for exactly, or refuses."""
    cpu_min = memory_min = disk_min = None
    for key, value in resources.items():
        if value is None:
            continue
        if key == "cpu_min":
            cpu_min = _number(key, value)
        elif key == "memory_min_mib":
            memory_min = _number(key, value)
        elif key == "disk_min_mib":
            disk_min = _number(key, value)
        elif key == "arch":
            if not _offers(value, "amd64"):
                raise SpsError("unsupported", "Groundbox sandboxes are amd64")
        elif key == "root":
            if not _offers(value, "sudo"):
                raise SpsError("unsupported", "Groundbox's default user has root through sudo")
        else:
            raise SpsError("unsupported", f"Groundbox cannot meet resources.{key}")
    if cpu_min is None and memory_min is None:
        cpus = int(DEFAULT_SIZE.cpu)
        memory_gb = DEFAULT_SIZE.memory_mib // _MIB_PER_GB
    else:
        memory_gb = 0 if memory_min is None else max(1, math.ceil(memory_min / _MIB_PER_GB))
        cpus = max(
            1 if cpu_min is None else math.ceil(cpu_min),
            math.ceil(memory_gb / MEMORY_GB_PER_CPU),
        )
        if memory_min is None:  # the default ratio: 2 GB per vCPU
            memory_gb = cpus * (DEFAULT_SIZE.memory_mib // _MIB_PER_GB) // int(DEFAULT_SIZE.cpu)
    if cpus > MAX_CPUS:
        raise SpsError("unsupported", f"Groundbox's largest sandbox has {MAX_CPUS} vCPUs")
    size = {"cpus": cpus, "memory_gb": memory_gb}
    if disk_min is not None:
        disk_gb = max(1, math.ceil(disk_min / _MIB_PER_GB))
        if disk_gb > MAX_DISK_GB:
            raise SpsError("unsupported", f"Groundbox's largest disk is {MAX_DISK_GB} GB")
        size["disk_gb"] = disk_gb
    return size


def plan_lifetime(lifetime: object) -> int:
    """`timeout_minutes` for a lifetime in seconds. Groundbox counts whole minutes, so a lifetime
    between two minutes ends at the earlier one: never later than was asked."""
    if lifetime is None:
        raise SpsError("unsupported", "A Groundbox sandbox always has a lifetime")
    if isinstance(lifetime, bool) or not isinstance(lifetime, int) or lifetime < 1:
        raise SpsError("invalid_request", "lifetime_seconds must be an integer >= 1")
    if lifetime < MIN_LIFETIME_S:
        raise SpsError("unsupported", f"Groundbox's shortest lifetime is {MIN_LIFETIME_S} seconds")
    if lifetime > MAX_LIFETIME_S:
        raise SpsError("unsupported", f"Groundbox's longest lifetime is {MAX_LIFETIME_S} seconds")
    return lifetime // LIFETIME_STEP_S


def plan_create(request: CreateRequest) -> dict[str, Any]:
    """The `POST /v1/sandboxes` body for a `CreateRequest`. The same request always gives the
    same body, which is what Groundbox compares a repeated `Idempotency-Key` against."""
    if request.sandbox_class != CLASS:
        raise SpsError("unsupported", f"Groundbox has no class {request.sandbox_class!r}")
    if request.secrets and "hidden_secrets" not in request.requirements:
        raise SpsError("invalid_request", "secrets need the hidden_secrets section")
    for gate in request.requirements:
        raise SpsError("unsupported", f"Groundbox does not claim the section {gate!r}")
    if request.snapshot is not None:
        raise SpsError("unsupported", "Groundbox does not claim the section 'snapshots'")
    if request.at_end != "destroyed":
        raise SpsError("unsupported", "A Groundbox sandbox is destroyed at the end of its lifetime")
    if request.image not in (None, IMAGE):
        raise SpsError("unsupported", f"Groundbox offers only the {IMAGE!r} image")
    body: dict[str, Any] = plan_size(request.resources)
    body["timeout_minutes"] = plan_lifetime(request.lifetime_seconds)
    body["on_timeout"] = "delete"
    if request.env:
        body["env"] = dict(request.env)
    if request.labels:
        body["metadata"] = dict(request.labels)
    return body
