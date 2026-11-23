# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The image recipe revision: what the adapter adds to a prepared image, as one derived value.

A prepared copy is identified by its image and by what the adapter put into it. The `sandbox-api`
pinned in the tail decides whether a sandbox has `/standby` and `/resume` (0.2.78 and later), so a
copy made with another pin is another copy: the revision changes when the tail does, and only then."""

from __future__ import annotations

import re

import pytest
from sandbox_provider_spec.v1 import Provider

from sps_provider_blaxel import BlaxelProvider, dockerfile, images


def test_the_default_of_a_package_is_no_revision() -> None:
    assert Provider.image_recipe_revision() == ""


def test_blaxel_states_twelve_hex_characters_read_from_the_class() -> None:
    revision = BlaxelProvider.image_recipe_revision()
    assert re.fullmatch(r"[0-9a-f]{12}", revision)
    assert revision == dockerfile.recipe_revision() == BlaxelProvider.image_recipe_revision()


def test_prepared_images_carry_a_sandbox_api_that_has_standby_and_resume() -> None:
    major, minor, patch = (int(n) for n in dockerfile.SANDBOX_API_VERSION.split("."))
    assert (major, minor, patch) >= (0, 2, 78)  # the first with POST /standby and /resume


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SANDBOX_API_VERSION", "0.2.99"),
        ("SANDBOX_API_IMAGE", "ghcr.io/example/sandbox"),
        ("SANDBOX_API_PATH", "/opt/sandbox-api"),
    ],
)
def test_the_revision_changes_with_the_pin_or_the_path_in_the_tail(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    before = dockerfile.recipe_revision()
    monkeypatch.setattr(dockerfile, name, value)
    assert dockerfile.recipe_revision() != before


def test_the_revision_changes_with_the_tail_template(monkeypatch: pytest.MonkeyPatch) -> None:
    before = dockerfile.recipe_revision()
    real = dockerfile.tail

    def changed(user: str, command: list[str] | None, mark: str = "") -> list[str]:
        return [*real(user, command, mark), "ENV SPS_TAIL_CHANGED=1"]

    monkeypatch.setattr(dockerfile, "tail", changed)
    assert dockerfile.recipe_revision() != before


def test_the_revision_does_not_change_with_anything_else(monkeypatch: pytest.MonkeyPatch) -> None:
    """Not with the customer's recipe, the build memory, the image prefix or the card of a call:
    a copy is re-prepared only when what the adapter adds changes."""
    before = dockerfile.recipe_revision()
    monkeypatch.setattr(images, "BUILD_MEMORY_MIB", images.BUILD_MEMORY_MIB + 1)
    monkeypatch.setattr(images, "PREFIX", images.PREFIX + "x-")
    assert dockerfile.recipe_revision() == before
    assert dockerfile.from_oci(
        "ubuntu:24.04", dockerfile.ImageConfig(), "a"
    ) != dockerfile.from_oci("alpine:3", dockerfile.ImageConfig(), "a")
    assert dockerfile.recipe_revision() == before  # rendering a customer's image changes nothing
