# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Translate a 1.0 `CreateRequest` into the body of Blaxel's `POST /sandboxes`."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from sandbox_provider_spec.v1 import CreateRequest, Size, SpsError

from sps_provider_blaxel import secrets
from sps_provider_blaxel.facts import (
    BASE_IMAGE,
    CLASS,
    CUSTOM_IMAGES,
    DEFAULT_SIZE,
    IMAGE,
    MIN_LIFETIME_S,
    REGIONS,
    RESERVED_PORT,
    SIZES,
    native_region,
)


def sandbox_name(idempotency_key: str) -> str:
    """A Blaxel sandbox name that stands for one `Idempotency-Key`: 32 hex characters."""
    return hashlib.sha256(f"sps-blaxel:{idempotency_key}".encode()).hexdigest()[:32]


def fingerprint(request: CreateRequest) -> str:
    """A stable digest of the request, kept as the sandbox's display name (at most 63 characters),
    so that a replayed key with another body is found."""
    doc = {
        "sandbox_class": request.sandbox_class,
        "resources": dict(request.resources),
        "requirements": dict(request.requirements),
        "image": request.image,
        "lifetime_seconds": request.lifetime_seconds,
        "at_end": request.at_end,
        "env": dict(request.env),
        "labels": dict(request.labels),
        "snapshot": request.snapshot,
    }
    raw = json.dumps(doc, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _offers(value: object, wanted: str) -> bool:
    """Whether a request value (one value or a list of them) accepts `wanted`."""
    return wanted in value if isinstance(value, list) else value == wanted


def _refuse(message: str) -> SpsError:
    return SpsError("unsupported", message)


def _check_idle_shutdown(wanted: Mapping[str, object]) -> None:
    """Blaxel puts a sandbox into standby by itself and wakes it on use: `on_idle: paused`, with
    no threshold to set and no wake switch to turn."""
    for knob, value in wanted.items():
        if knob == "on_idle" and _offers(value, "paused"):
            continue
        raise _refuse(f"Blaxel cannot meet idle_shutdown.{knob}")


def _check_knobs(gate: str, wanted: Mapping[str, object], offered: Mapping[str, object]) -> None:
    """Each knob of a section must be one that Blaxel offers, with a value that it offers."""
    for knob, value in wanted.items():
        have = offered.get(knob)
        if isinstance(have, list):
            fits = any(_offers(value, str(h)) for h in have)
        else:
            fits = value is True if have is True else have is not None and _offers(value, str(have))
        if not fits:
            raise _refuse(f"Blaxel cannot meet {gate}.{knob}")


def _check_previews(wanted: Mapping[str, object]) -> None:
    ports = wanted.get("ports")
    if isinstance(ports, list) and RESERVED_PORT in ports:
        raise _refuse(f"Blaxel keeps port {RESERVED_PORT} for its sandbox API")
    _check_knobs(
        "previews",
        {k: v for k, v in wanted.items() if k != "ports"},
        {"access": ["public", "private"], "credential": ["link", "header"], "revocable": True},
    )


# What Blaxel's proxy does (declaration `hidden_secrets`): it sets a header for each destination
# host, on every HTTPS connection of the sandbox, and does not scrub responses or reach clients that
# ignore proxy settings. A knob that asks for more is refused.
_SECRET_KNOBS: dict[str, object] = {
    "injection": "sets_header",
    "intercepts": "all_https",
    "scrubs_responses": False,
    "transparent": False,
    "header_templates": False,
}


def _check_hidden_secrets(wanted: Mapping[str, object]) -> None:
    for knob, value in wanted.items():
        if knob in ("inject_into", "headers"):
            continue  # settings: the secrets themselves name the hosts and headers
        have = _SECRET_KNOBS.get(knob)
        accepted = (
            _offers(value, "injected_hosts") or _offers(value, "all_https")
            if knob == "intercepts"
            else have is not None
            and (_offers(value, have) if isinstance(have, str) else value is have)
        )
        if not accepted:  # `rotates` is unknown, so no request for it can be met
            raise _refuse(f"Blaxel cannot meet hidden_secrets.{knob}")


def _check_custom_images(wanted: Mapping[str, object]) -> None:
    """A request's knobs must all be met by one variant of the `custom_images` claim."""

    def meets(variant: Mapping[str, object]) -> bool:
        return all(
            knob in variant
            and (value is True if variant[knob] is True else _offers(value, str(variant[knob])))
            for knob, value in wanted.items()
        )

    if not any(meets(variant) for variant in CUSTOM_IMAGES):
        raise _refuse("Blaxel cannot meet that custom_images request")


def _check_requirements(requirements: Mapping[str, object], image: object) -> str | None:
    """Check each section the request names; return the Blaxel image of a `provider_artifact`."""
    artifact: str | None = None
    for gate, wanted in requirements.items():
        if not isinstance(wanted, Mapping):
            raise SpsError("invalid_request", f"The requirement {gate!r} is a mapping of knobs")
        if gate == "idle_shutdown":
            _check_idle_shutdown(wanted)
        elif gate == "hidden_secrets":
            _check_hidden_secrets(wanted)
        elif gate == "custom_images":
            _check_custom_images(wanted)
            if not (isinstance(image, Mapping) and "provider_artifact" in image):
                raise SpsError(
                    "invalid_request", "custom_images needs an image that names a provider_artifact"
                )
        elif gate == "processes":
            _check_knobs(gate, wanted, {"input": True, "cancel": True, "reattach": True})
        elif gate == "previews":
            _check_previews(wanted)
        elif gate == "terminal":
            _check_knobs(gate, wanted, {})
        else:
            raise _refuse(f"Blaxel does not claim the section {gate!r}")
    return artifact


def _pick_size(floors: Mapping[str, float]) -> Size:
    size = next(
        (
            s
            for s in SIZES
            if s.cpu >= floors.get("cpu_min", 0) and s.memory_mib >= floors.get("memory_min_mib", 0)
        ),
        None,
    )
    if size is None:
        raise _refuse("No Blaxel size meets the request")
    return size


def _check_resources(resources: Mapping[str, object]) -> tuple[Size, str | None]:
    """The size to create and the native region, from the request's floors and pins."""
    floors: dict[str, float] = {}
    region: str | None = None
    disk = 0
    for key, value in resources.items():
        if value is None:
            continue
        if key in ("cpu_min", "memory_min_mib"):
            floors[key] = float(str(value))
        elif key == "disk_min_mib":
            disk = int(str(value))  # Blaxel's disk is in its memory (`disk_in_memory`)
        elif key == "region":
            region = native_region(str(value))
            if region not in REGIONS:
                raise _refuse(f"Blaxel has no region {value!r}")
        elif (
            key == "kernel"
            and _offers(value, "dedicated")
            or key in ("size_is_ceiling", "extendable_lifetime")
            and value is True
            or key == "root"
            and _offers(value, "by_default")
        ):
            continue
        else:
            raise _refuse(f"Blaxel cannot meet resources.{key}")
    if disk:
        floors["memory_min_mib"] = floors.get("memory_min_mib", 0) + disk
    return (_pick_size(floors) if floors else DEFAULT_SIZE), region


def _check_image(image: object, requirements: Mapping[str, object]) -> None:
    """A request's `image` is Blaxel's base image by its discovery id, or a provider_artifact."""
    if image is None or isinstance(image, Mapping):
        return
    if image != IMAGE:
        raise _refuse(f"Blaxel offers no image {image!r}")
    if "custom_images" in requirements:
        raise SpsError("invalid_request", "custom_images names a provider_artifact image")


def _artifact(image: object) -> str | None:
    """The Blaxel image that `image` names as a `provider_artifact`, whether or not the request
    also names the `custom_images` section."""
    if not (isinstance(image, Mapping) and "provider_artifact" in image):
        return None
    if image.get("provider", "blaxel") != "blaxel":
        raise SpsError("invalid_request", "The provider_artifact is not a Blaxel image")
    return str(image["provider_artifact"])


def plan_create(
    request: CreateRequest, idempotency_key: str, resolved: str | None = None
) -> dict[str, Any]:
    """The `POST /sandboxes` body for a request."""
    if request.snapshot is not None:
        raise _refuse("Blaxel does not claim the section 'snapshots'")
    if request.sandbox_class != CLASS:
        raise _refuse(f"Blaxel has no class {request.sandbox_class!r}")
    if request.at_end != "destroyed":
        raise _refuse("A Blaxel sandbox cannot end paused")
    _check_image(request.image, request.requirements)
    _check_requirements(request.requirements, request.image)
    artifact = _artifact(request.image)
    if artifact is not None and resolved is not None:
        artifact = resolved  # a prepared image's id, resolved by the caller to its reference
    if request.secrets and "hidden_secrets" not in request.requirements:
        raise SpsError(
            "invalid_request", "secrets need the hidden_secrets requirement", field="secrets"
        )
    size, region = _check_resources(request.resources)
    lifetime = request.lifetime_seconds
    runtime: dict[str, Any] = {"image": artifact or BASE_IMAGE, "memory": size.memory_mib}
    if lifetime is not None:
        if not isinstance(lifetime, int) or isinstance(lifetime, bool) or lifetime < 1:
            raise SpsError("invalid_request", "lifetime_seconds must be an integer >= 1")
        if lifetime < MIN_LIFETIME_S:
            raise _refuse(f"Blaxel's lifetime is at least {MIN_LIFETIME_S} seconds")
        runtime["ttl"] = f"{lifetime}s"
    if request.env:
        runtime["envs"] = [{"name": k, "value": v} for k, v in request.env.items()]
    spec: dict[str, Any] = {"runtime": runtime}
    if region is not None:
        spec["region"] = region
    if request.secrets:
        spec["network"] = secrets.network(request.secrets)
    metadata: dict[str, Any] = {
        "name": sandbox_name(idempotency_key),
        "displayName": fingerprint(request),
    }
    if request.labels:
        metadata["labels"] = dict(request.labels)
    return {"metadata": metadata, "spec": spec}
