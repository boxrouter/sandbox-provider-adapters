"""Tests for scripts/release.py: reproducible builds, a lock that matches its files, next-tag."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "release.py"


def release(*args: str, root: Path = ROOT) -> str:
    done = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree(directory: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(directory.iterdir())}


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("release") / "out"
    release("build", "--out", str(out))
    return out


def test_two_builds_of_one_commit_are_identical(built: Path, tmp_path: Path) -> None:
    second = tmp_path / "second"
    release("build", "--out", str(second))
    assert tree(built) == tree(second)


def test_lock_lines_match_the_files(built: Path) -> None:
    lines = (built / "providers.lock").read_text().splitlines()
    declarations = json.loads((built / "declarations.json").read_text())
    providers = declarations["providers"]
    assert declarations["schema"] == 1
    assert len(lines) == len(providers) + 1
    for line, provider in zip(lines, sorted(providers), strict=False):
        name, version, wheel, wheel_hash, requirements_hash = line.split(" ")
        assert name == provider
        assert version == providers[provider]["version"]
        assert wheel_hash == sha256(built / wheel)
        assert requirements_hash == sha256(built / f"requirements-{provider}.txt")
        requirements = (built / f"requirements-{provider}.txt").read_text()
        assert "sandbox-provider-spec" not in requirements
        assert "vendor" not in requirements
        assert "--hash=sha256:" in requirements
    assert lines[-1] == f"declarations.json {sha256(built / 'declarations.json')}"


def git_repo(root: Path, tags: list[str]) -> Path:
    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@example.test", *args],
            check=True,
            capture_output=True,
        )

    root.mkdir()
    git("init", "-q", "-b", "main")
    git("commit", "-q", "--allow-empty", "-m", "base")
    for tag in tags:
        git("tag", tag)
    return root


def test_next_tag_without_tags_is_v0_1_0(tmp_path: Path) -> None:
    root = git_repo(tmp_path / "repo", ["not-a-release"])
    assert release("next-tag", root=root).strip() == "v0.1.0"


def test_next_tag_raises_the_patch_of_the_newest_tag(tmp_path: Path) -> None:
    root = git_repo(tmp_path / "repo", ["v0.2.9", "v0.10.3", "v0.9.12", "other"])
    assert release("next-tag", root=root).strip() == "v0.10.4"
