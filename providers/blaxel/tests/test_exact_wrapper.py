# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""The byte-exact wrapper (`guest.WRAPPER`) under the two shells a sandbox may have: dash and
BusyBox `sh` (Blaxel's Alpine image). The commands run for real, through the stand-in of
`test_additions.py`, which hands Blaxel's command string to the local shell."""

from __future__ import annotations

import asyncio
import base64
import os
import shutil
from pathlib import Path
from typing import Any

import pytest
from sandbox_provider_spec.v1 import RunRequest
from test_additions import SANDBOX, Fake

from sps_provider_blaxel import guest

INLINE = guest.INLINE
BUSYBOX = shutil.which("busybox")
APPLETS = ("sh", "base64", "head", "wc", "rm", "cat", "tr", "getent")


@pytest.fixture
def box() -> Fake:
    fake = Fake()
    spec = {"metadata": {"name": SANDBOX}, "spec": {"runtime": {"ttl": "300s"}}}
    fake.sandboxes[SANDBOX] = fake._add(spec, False)
    return fake


@pytest.fixture(params=["dash", "busybox"])
def shell(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, box: Fake
) -> Fake:
    """Runs the test with `sh` and the tools the wrapper uses resolved to that shell's own."""
    if request.param == "busybox":
        if BUSYBOX is None:
            pytest.skip("no busybox")
        tools = tmp_path / "bin"
        tools.mkdir()
        for name in APPLETS:
            (tools / name).symlink_to(BUSYBOX)
        monkeypatch.setenv("PATH", f"{tools}:{os.environ['PATH']}")
    monkeypatch.setattr(guest, "stage_dir", lambda: f"{tmp_path}/stage")
    return box


def run(box: Fake, request: RunRequest) -> Any:
    return asyncio.run(box.provider().run(SANDBOX, request))


def left(tmp_path: Path) -> list[str]:
    return sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("stage"))


def exact(box: Fake, tmp_path: Path, out: bytes, err: bytes, *, code: int = 0, **kw: Any) -> Any:
    (tmp_path / "out").write_bytes(out)
    (tmp_path / "err").write_bytes(err)
    script = f"cat '{tmp_path}/out'; cat '{tmp_path}/err' >&2; exit {code}"
    return run(box, RunRequest(argv=["sh", "-c", script], output="base64", **kw))


def noise(size: int) -> bytes:
    return bytes((i * 7 + 255 - i // 3) % 256 for i in range(size))  # every byte value, no text


SIZES = [0, 1, INLINE - 1, INLINE, INLINE + 1, 100_000]


@pytest.mark.parametrize("size", SIZES)
def test_binary_output_is_exact_at_each_size(shell: Fake, tmp_path: Path, size: int) -> None:
    out, err = noise(size), noise(size)[::-1]
    result = exact(shell, tmp_path, out, err, code=3)
    assert result.exit_code == 3
    assert base64.b64decode(result.stdout_base64) == out
    assert base64.b64decode(result.stderr_base64) == err
    assert not (result.stdout_truncated or result.stderr_truncated)
    assert left(tmp_path) == []  # the staging files are gone, spilled or not


def test_one_stream_empty_the_other_over_the_inline_size(shell: Fake, tmp_path: Path) -> None:
    big = noise(INLINE + 5000)
    for out, err in ((big, b""), (b"", big), (b"x", big), (big, b"y")):
        result = exact(shell, tmp_path, out, err)
        assert base64.b64decode(result.stdout_base64) == out
        assert base64.b64decode(result.stderr_base64) == err
    assert left(tmp_path) == []


@pytest.mark.parametrize("limit", [0, 10, 40_000])
def test_a_stream_over_the_limit_is_cut_to_it(shell: Fake, tmp_path: Path, limit: int) -> None:
    out, err = noise(100_000), noise(20)
    result = exact(shell, tmp_path, out, err, output_limit_bytes=limit)
    assert base64.b64decode(result.stdout_base64) == out[:limit]
    assert base64.b64decode(result.stderr_base64) == err[:limit]
    assert (result.stdout_truncated, result.stderr_truncated) == (
        len(out) > limit,
        len(err) > limit,
    )
    assert left(tmp_path) == []


def test_stderr_only_and_a_failing_exit(shell: Fake, tmp_path: Path) -> None:
    result = exact(shell, tmp_path, b"", b"oops\n", code=7)
    assert (result.exit_code, base64.b64decode(result.stdout_base64)) == (7, b"")
    assert base64.b64decode(result.stderr_base64) == b"oops\n"


def test_a_command_that_is_not_found_is_127_with_its_message(shell: Fake, tmp_path: Path) -> None:
    result = run(shell, RunRequest(argv=["no-such-command-xyz"], output="base64"))
    assert result.exit_code == 127 and result.stdout_base64 == ""
    assert b"no-such-command-xyz" in base64.b64decode(result.stderr_base64)
    assert left(tmp_path) == []


def test_text_output_of_the_wrapper_replaces_bad_utf8(shell: Fake, tmp_path: Path) -> None:
    result = exact(shell, tmp_path, b"\xffz", b"")
    assert base64.b64decode(result.stdout_base64) == b"\xffz"


def test_the_working_directory_applies(shell: Fake, tmp_path: Path) -> None:
    result = run(shell, RunRequest(argv=["pwd"], output="base64", cwd="/usr"))
    assert base64.b64decode(result.stdout_base64) == b"/usr\n"
