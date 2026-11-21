# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""`static_discovery()`: the ceiling every account's `discovery()` narrows from."""

from __future__ import annotations

from sandbox_provider_spec.v1 import within

from sps_provider_blaxel import BlaxelProvider


def test_static_discovery_names_the_routed_classes() -> None:
    # A classmethod: no instance, no credentials, no network.
    discovery = BlaxelProvider.static_discovery()
    assert [k.id for k in discovery.classes] == ["default"]
    assert all(k.ready for k in discovery.classes)


def test_static_discovery_offers_every_region() -> None:
    from sps_provider_blaxel.facts import REGIONS, region_id

    klass = BlaxelProvider.static_discovery().classes[0]
    assert klass.resources["regions"] == [region_id(r) for r in REGIONS]


def test_static_discovery_is_within_itself() -> None:
    assert within(BlaxelProvider.static_discovery(), BlaxelProvider.static_discovery()) == []


def test_one_region_account_is_within_static() -> None:
    from sps_provider_blaxel.facts import REGIONS, build_discovery

    assert within(build_discovery(list(REGIONS)[:1]), BlaxelProvider.static_discovery()) == []
