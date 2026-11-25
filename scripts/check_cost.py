"""Check that a provider package offers `cost`, in the provider's own environment.

Provider API 1.1 requires `cost` (`limits.cost_final_within_seconds` in discovery). A provider that
states its static discovery is read here, without credentials or I/O: the discovery must state the
manifest's `spec_version` and the cost limit. A provider that states no static discovery is
checked by the tester, which fails a 1.1 discovery without cost.

Usage: uv run --project providers/<id> python scripts/check_cost.py <id>
"""

import sys
from importlib.metadata import entry_points

GROUP = "sandbox_provider_spec.providers"


def check(provider: str) -> list[str]:
    found = [point for point in entry_points(group=GROUP) if point.name == provider]
    if not found:
        return [f"{provider}: no {GROUP} entry point named {provider}"]
    provider_class = found[0].load()
    try:
        discovery = provider_class.static_discovery()
    except NotImplementedError:
        return []
    failures = []
    if discovery.spec_version != provider_class.spec_version:
        failures.append(
            f"{provider}: static discovery states spec_version {discovery.spec_version!r}, "
            f"the manifest {provider_class.spec_version!r}"
        )
    if discovery.limits.cost_final_within_seconds is None:
        failures.append(
            f"{provider}: static discovery offers no cost (limits.cost_final_within_seconds); "
            "Provider API 1.1 requires it"
        )
    return failures


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    failures = check(sys.argv[1])
    for failure in failures:
        print(failure, file=sys.stderr)
    if failures:
        return 1
    print(f"{sys.argv[1]}: offers cost")
    return 0


if __name__ == "__main__":
    sys.exit(main())
