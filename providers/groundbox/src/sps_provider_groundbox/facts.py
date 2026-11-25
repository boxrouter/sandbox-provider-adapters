# SPDX-FileCopyrightText: Groundbox
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""What Groundbox offers: the facts behind discovery. No Groundbox call returns them, so they
are written here from Groundbox's API reference (https://api.groundbox.dev/openapi.json), its
pricing page (https://groundbox.dev/pricing) and live runs, with the date of each measurement."""

from __future__ import annotations

from decimal import Decimal

from sandbox_provider_spec.v1 import (
    Discovery,
    Image,
    Limits,
    Price,
    ProviderInfo,
    SandboxClass,
    Size,
)

CLASS = "default"
# The id discovery gives Groundbox's own image. A create names it by sending no image.
IMAGE = "ubuntu-24.04"
DOCS_URL = "https://groundbox.dev/docs"

# Sizes: 1 to 16 vCPUs, whole GB of memory from 1 to 4 per vCPU. A size is exact or refused.
MIN_CPUS, MAX_CPUS = 1, 16
MEMORY_GB_PER_CPU = 4
DEFAULT_SIZE = Size(cpu=2, memory_mib=4096)
# `disk_gb` is the least the root disk has (the image's own disk may be larger): 1 to 100 GB.
MAX_DISK_GB = 100

# The lifetime is `timeout_minutes`: whole minutes, 1 to 43200 (30 days), counted from the create.
LIFETIME_STEP_S = 60
MIN_LIFETIME_S = 60
MAX_LIFETIME_S = 43200 * 60
# Before a card is on file a sandbox has at most 2 vCPUs, 4 GB and one hour.
NEED_CARD = "card_on_file"
TRIAL_MAX_LIFETIME_S = 3600

MAX_RUN_S = 3600  # the longest `POST /exec`
MAX_PROCESS_S = 2592000  # the longest background command
MAX_WAIT_S = 30  # one process_output call polls at most this long for new output
LIST_LIMIT = 200
# Groundbox takes at most 100 MB per file call, and its front end refuses a request body over
# 32 MiB (measured 2026-11-23: 31 MiB accepted, 33 MiB refused with 413). The adapter moves a
# larger file in parts, so the limit here is the adapter's own.
FILE_BYTES = 4 * (1 << 30)
# `POST /exec` answers with the last 4 MB of each stream; a longer one is read from the command's
# log, which keeps the newest 32 MiB of both streams together.
OUTPUT_BYTES = 16 * 1024 * 1024
PROCESS_OUTPUT_BYTES = 1024 * 1024
PROCESS_RETENTION_S = 3600  # an hour after a command ends its output is cut to the last 64 KB
# Measured 2026-11-23: `GET /usage` stated the closing figure 1 s after a delete and after a
# timeout, and it did not change in the 100 s that followed.
COST_FINAL_WITHIN_S = 60

CPU_UNIT = "vcpu_hour"
CPU_PRICE = Decimal("0.04")  # https://groundbox.dev/pricing, read 2026-11-23

LIMITS = Limits(
    file_bytes=FILE_BYTES,
    run_timeout_ms=MAX_RUN_S * 1000,
    output_bytes=OUTPUT_BYTES,
    wait_ms=MAX_WAIT_S * 1000,
    process_output_bytes=PROCESS_OUTPUT_BYTES,
    process_retention_seconds=PROCESS_RETENTION_S,
    list_limit=LIST_LIMIT,
    cost_final_within_seconds=COST_FINAL_WITHIN_S,
)


def build_discovery(provider_id: str, provider_name: str) -> Discovery:
    klass = SandboxClass(
        id=CLASS,
        name="Linux microVM",
        ready=True,
        resources={
            "kernel": "dedicated",
            "arch": ["amd64"],
            # One entry per vCPU count: memory is any whole GB from 1 to 4 per vCPU.
            "sizes": [
                {"cpu": cpus, "memory_mib": [1024, cpus * MEMORY_GB_PER_CPU * 1024]}
                for cpus in range(MIN_CPUS, MAX_CPUS + 1)
            ],
            "default_size": {"cpu": DEFAULT_SIZE.cpu, "memory_mib": DEFAULT_SIZE.memory_mib},
            "max_lifetime_seconds": MAX_LIFETIME_S,
            "max_run_seconds": MAX_RUN_S,
            "max_process_seconds": MAX_PROCESS_S,
            "default_user": "runner",
            "default_cwd": "/home/runner",
            "root": ["sudo"],
            "package_manager": "apt",
            "libc": "glibc",
            "needs": {"sizes": [NEED_CARD], "max_lifetime_seconds": [NEED_CARD]},
            "unmet_limits": {"max_lifetime_seconds": {NEED_CARD: TRIAL_MAX_LIFETIME_S}},
        },
        lifetime_timer={
            "enforced_by": "provider",
            "counts_paused_time": False,
            "at_end": ["destroyed"],
            "step_seconds": LIFETIME_STEP_S,
            "min_seconds": MIN_LIFETIME_S,
            "counts_from": "create",
        },
        images=(Image(IMAGE, True, "Ubuntu 24.04 with bash, python3, Node.js, git and Docker"),),
        prices=(
            # Published list prices, billed by the second. Memory (up to 4 GB per vCPU), the
            # disk of a running sandbox and egress are in the vCPU price.
            Price("cpu", CPU_PRICE, CPU_UNIT, "allocated", increment_seconds=1),
            Price("memory", Decimal(0), "gib_hour", "allocated", included=True),
            Price("disk", Decimal(0), "gib_hour", "allocated", included=True),
            Price("egress", Decimal(0), "gb", "used", included=True),
        ),
        gates={},
        # The optional operations that no gate covers; each one passes the tester.
        operations=("run.base64",),
    )
    return Discovery(
        spec_version="1.0",
        provider=ProviderInfo(provider_id, provider_name, DOCS_URL),
        limits=LIMITS,
        classes=(klass,),
    )
