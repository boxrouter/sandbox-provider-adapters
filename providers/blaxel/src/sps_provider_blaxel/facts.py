# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""What Blaxel offers: the facts behind discovery.

Blaxel's API returns its regions (`GET /locations`) but not its sizes, so the sizes and the
limits are written here from Blaxel's public pages, with the date they were read.
"""

from __future__ import annotations

from collections.abc import Iterable

from sandbox_provider_spec.v1 import (
    Discovery,
    Image,
    Limits,
    ProviderInfo,
    SandboxClass,
    Size,
)

CLASS = "default"
REGION_PREFIX = "blaxel:"
REGIONS = (
    "us-pdx-1",
    "us-was-1",
    "eu-lon-1",
    "eu-fra-1",
)  # https://docs.blaxel.ai/Infrastructure/Regions
# https://docs.blaxel.ai/Get-started: memory is in MB and sets the CPU. The 16 GiB row is 6 CPUs
# on the sandbox overview page (read 2026-11-07), not memory / 2048.
SIZES = (Size(cpu=2, memory_mib=4096), Size(cpu=4, memory_mib=8192), Size(cpu=6, memory_mib=16384))
DEFAULT_SIZE = SIZES[0]
IMAGE = "blaxel-base"  # the id discovery gives Blaxel's own `blaxel/base-image:latest`
BASE_IMAGE = "blaxel/base-image:latest"
MIN_LIFETIME_S = (
    300  # https://docs.blaxel.ai/Sandboxes/Expiration: "The minimum value for `ttl` is `5m`"
)
# The longest `run`: one waited request, followed by its name after Blaxel's gateway cuts it near
# 120 s. An hour, as the other spec packages: with 600 s, a caller's 600 s command plus its kill
# margin went past it, so Core started a process and polled its output (two reads a pass, pauses
# up to 2 s) instead of one waited request.
MAX_RUN_S = 3600
MAX_WAIT_S = 30  # one process_output call waits at most this long for new output
LIST_LIMIT = 100
FILE_BYTES = 1 << 30
# How long after a sandbox's end Blaxel's metrics may still lack or change its cost. Live through
# Cloud on 2026-11-23: buckets 0 to 7 s after the destroy, unchanged afterwards (40 sandboxes, 30
# at once); 2026-10-20: final 45 to 113 s after the destroy. The one read at this time plus the
# spec's 30 s grace (pricing's `final_after_s`) is final.
COST_FINAL_WITHIN_S = 120
# Blaxel's own label on every sandbox this adapter creates: the create's idempotency marker.
LABEL_BODY = "sps-body"
# The directory a default command and the filesystem daemon resolve a relative path against on
# the provider's default image (the spec tester's `default_cwd` observation). It depends on the
# image and the user — Cloud's image starts in `/home/user` — so a relative path that needs an
# anchor asks the sandbox for it (`files_list`'s `..` case); this constant is the declaration
# only and is not used to resolve a path.
CWD = "/blaxel"
RESERVED_PORT = 8080  # Blaxel keeps it for its sandbox API: a preview of it never reaches a program

LIMITS = Limits(
    file_bytes=FILE_BYTES,
    run_timeout_ms=MAX_RUN_S * 1000,
    output_bytes=16 * 1024 * 1024,
    wait_ms=MAX_WAIT_S * 1000,
    process_output_bytes=1024 * 1024,
    process_retention_seconds=600,
    list_limit=LIST_LIMIT,
    cost_final_within_seconds=COST_FINAL_WITHIN_S,
    image_context_bytes=512 * 1024 * 1024,
    account_cost_window_seconds=7 * 24 * 3600,
)


CUSTOM_IMAGES: tuple[dict[str, object], ...] = (
    {"source": "oci", "prepared": True},
    {"source": "dockerfile", "prepared": True, "build_context": True},
    {"source": "provider_artifact"},
)

# The optional operations that no gate covers, as discovery claims them (each one passes the tester).
OPERATIONS = (
    "files.list",
    "files.mkdir",
    "files.remove",
    "files.exists",
    "run.base64",
    "labels.update",
    "image.prepare",
    "image.get",
    "image.list",
    "image.delete",
)


def region_id(native: str) -> str:
    return f"{REGION_PREFIX}{native}"


def native_region(region: str) -> str | None:
    return region[len(REGION_PREFIX) :] if region.startswith(REGION_PREFIX) else None


def cpu_of(memory_mib: int) -> float:
    for size in SIZES:
        if size.memory_mib == memory_mib:
            return size.cpu
    return memory_mib / 2048  # https://docs.blaxel.ai/api-reference/compute/create-sandbox


def build_discovery(offered_regions: Iterable[str] | None = None) -> Discovery:
    """Discovery for one account. `offered_regions` are the native region ids that the account's
    `GET /locations` lists; a declared region that it omits is reported as unavailable."""
    offered = set(REGIONS if offered_regions is None else offered_regions)
    klass = SandboxClass(
        id=CLASS,
        name="Blaxel microVM",
        ready=True,
        resources={
            "kernel": "dedicated",
            "disk_in_memory": True,
            "sizes": [{"cpu": s.cpu, "memory_mib": s.memory_mib} for s in SIZES],
            "default_size": {"cpu": DEFAULT_SIZE.cpu, "memory_mib": DEFAULT_SIZE.memory_mib},
            "default_user": "root",
            "regions": [region_id(r) for r in REGIONS],
            "max_lifetime_seconds": "unlimited",
            "extendable_lifetime": True,
            "size_is_ceiling": True,
            "paused_retention_seconds": "unlimited",
            "needs": {
                "regions": ["region_enabled_for_workspace"],
                "max_lifetime_seconds": ["tier_2_or_higher"],
                "paused_retention_seconds": ["tier_2_or_higher"],
            },
            "unmet_limits": {"max_lifetime_seconds": {"tier_2_or_higher": 604800}},
            "default_cwd": CWD,
        },
        lifetime_timer={
            "enforced_by": "provider",
            "counts_paused_time": True,
            "at_end": ["destroyed"],
            "adjustable": True,
            "step_seconds": "unknown",
            "counts_from": "create",
            "min_seconds": MIN_LIFETIME_S,
        },
        images=(Image(IMAGE, True, "Blaxel's base image, with bash and python3"),),
        prices=(),
        # A section is claimed only once `create` and the live tester serve it.
        gates={
            "custom_images": list(CUSTOM_IMAGES),
            "idle_shutdown": [{"on_idle": "paused"}],
            # `pause_resume` is NOT claimed (withdrawn 2026-11-23): `POST /standby` and `POST
            # /resume` are served by the sandbox-api inside the sandbox from 0.2.78 on, and the
            # images this adapter prepares carried 0.2.74, so every sandbox made from one answered
            # 404. `pause` and `resume` stay in the package and answer `unsupported` there.
            "processes": [{"input": True, "cancel": True, "reattach": True}],
            "previews": [
                {
                    "access": "public",
                    "revocable": True,
                    "max_ports": {"at_least": 2},
                    "reserved_ports": [RESERVED_PORT],
                },
                {
                    "access": "private",
                    "credential": ["link", "header"],
                    "revocable": True,
                    "max_ports": {"at_least": 2},
                    "reserved_ports": [RESERVED_PORT],
                },
            ],
            "terminal": [{}],
            "hidden_secrets": [
                {
                    "rotates": "unknown",
                    "scrubs_responses": False,
                    "injection": "sets_header",
                    "intercepts": "all_https",
                    "transparent": False,
                }
            ],
            "user_selection": [{"commands": True, "files_write": True, "process_start": True}],
        },
        operations=OPERATIONS,
        unavailable_regions=tuple(region_id(r) for r in REGIONS if r not in offered),
    )
    return Discovery(
        spec_version="1.0",
        provider=ProviderInfo("blaxel", "Blaxel", "https://docs.blaxel.ai"),
        limits=LIMITS,
        classes=(klass,),
    )
