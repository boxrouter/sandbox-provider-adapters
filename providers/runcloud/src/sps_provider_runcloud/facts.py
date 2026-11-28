# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""What run.cloud offers: the facts behind discovery. No run.cloud call returns them.

Prices are the published list prices at https://run.cloud/pricing: CPU is $0.00000492 per
physical core-second, and one core is two vCPUs, so $0.00000246 per vCPU-second, charged on the
larger of the reservation and the CPU used (bursting); memory is $0.00000083175 per GiB-second of
the reservation. A paused sandbox is not charged.
"""

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

DEFAULT_ENDPOINT = "https://api.run.cloud"
CLASS = "default"
IMAGE = "runcloud/agent-base"
SIZES = (
    Size(cpu=0.25, memory_mib=512),
    Size(cpu=0.5, memory_mib=1024),
    Size(cpu=1, memory_mib=2048),
    Size(cpu=2, memory_mib=4096),
    Size(cpu=4, memory_mib=8192),
    Size(cpu=8, memory_mib=16384),
)
DEFAULT_SIZE = SIZES[2]
DISK_MIB = 40960  # every sandbox gets a 40 GiB disk unless asked otherwise
MAX_LIFETIME_S = 86400  # run.cloud's timeoutSeconds ceiling
MAX_RUN_S = 3600
MAX_WAIT_S = 30
POLL_S = 20  # longest one exec call waits inside the sandbox; the edge closes requests near 100 s
LIST_LIMIT = 100
FS_CHUNK = 512 * 1024  # run.cloud's file transfer chunk ceiling
FS_WINDOW = 16  # file chunks in flight at once
IO_CHUNK = 1024 * 1024  # most output bytes per stream one supervisor call returns
COST_FINAL_WITHIN_S = 600
COST_SETTLE_S = 120  # the usage ledger trails the fleet by one reconcile tick
CPU_PRICE = Decimal("0.00000246")
MEMORY_PRICE = Decimal("0.00000083175")
LIMITS = Limits(
    file_bytes=2 * 1024**3,
    run_timeout_ms=MAX_RUN_S * 1000,
    output_bytes=16 * 1024**2,
    wait_ms=MAX_WAIT_S * 1000,
    process_output_bytes=16 * 1024**2,
    process_retention_seconds=3600,
    list_limit=LIST_LIMIT,
    cost_final_within_seconds=COST_FINAL_WITHIN_S,
)


def build_discovery(provider_id: str, provider_name: str) -> Discovery:
    klass = SandboxClass(
        id=CLASS,
        name="Firecracker microVM",
        ready=True,
        resources={
            "regions": [],
            "located_in": "unknown",
            "sizes": [{"cpu": s.cpu, "memory_mib": s.memory_mib} for s in SIZES],
            "default_size": {"cpu": DEFAULT_SIZE.cpu, "memory_mib": DEFAULT_SIZE.memory_mib},
            "disk_mib": DISK_MIB,
            "arch": ["amd64"],
            "max_lifetime_seconds": MAX_LIFETIME_S,
            "long_running": True,
            "default_cwd": "/",
            "default_user": "root",
            "root": ["by_default"],
            "outbound_traffic": ["http", "any_tcp", "udp"],
        },
        lifetime_timer={
            "enforced_by": "provider",
            "at_end": ["destroyed"],
            "counts_paused_time": False,
        },
        images=(Image(IMAGE, True, "Ubuntu 24.04 with bash, python3, node and curl"),),
        prices=(
            Price("cpu", CPU_PRICE, "vcpu_second", "max_allocated_used"),
            Price("memory", MEMORY_PRICE, "gib_second", "allocated"),
        ),
        gates={},
    )
    return Discovery(
        spec_version="1.1",
        provider=ProviderInfo(provider_id, provider_name, "https://docs.run.cloud"),
        limits=LIMITS,
        classes=(klass,),
    )
