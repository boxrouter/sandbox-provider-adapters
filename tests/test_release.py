"""Tests for scripts/release.py: reproducible builds, a lock that matches its files, next-tag."""

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "release.py"
IGNORE = shutil.ignore_patterns(".venv", "__pycache__", ".pytest_cache", ".ruff_cache")


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


ENDPOINT_YAML = (
    "provider_id: pingo\nname: Pingo\nspec_version: '1.0'\nbase_url: https://api.pingo.test/box\n"
)
PINGO_DECLARATION = {
    "credential_fields": [
        {"description": "API key for Pingo", "name": "api_key", "secret": True}
    ],
    "endpoint": {"base_url": "https://api.pingo.test/box"},
    "name": "Pingo",
    "provider_id": "pingo",
    "spec_version": "1.0",
}


def endpoint_folder(root: Path) -> None:
    folder = root / "providers" / "pingo"
    (folder / "brand").mkdir(parents=True)
    (folder / "endpoint.yaml").write_text(ENDPOINT_YAML)
    (folder / "OWNERS").write_text("@pingo-dev\n")
    (folder / "brand" / "mark.svg").write_text("<svg/>\n")


def test_an_endpoint_folder_is_declared_and_builds_no_package(tmp_path: Path) -> None:
    root = git_repo(tmp_path / "repo", [])
    endpoint_folder(root)
    out = tmp_path / "out"
    release("build", "--out", str(out), root=root)
    declarations = json.loads((out / "declarations.json").read_text())
    assert declarations == {"schema": 1, "providers": {"pingo": PINGO_DECLARATION}}
    assert sorted(p.name for p in out.iterdir()) == ["declarations.json", "providers.lock"]
    assert (out / "providers.lock").read_text() == (
        f"declarations.json {sha256(out / 'declarations.json')}\n"
    )


def test_an_endpoint_folder_beside_an_adapter_adds_only_its_declaration(
    built: Path, tmp_path: Path
) -> None:
    root = git_repo(tmp_path / "repo", [])
    shutil.copytree(ROOT / "providers" / "example", root / "providers" / "example", ignore=IGNORE)
    endpoint_folder(root)
    out = tmp_path / "out"
    release("build", "--out", str(out), root=root)
    declarations = json.loads((out / "declarations.json").read_text())
    assert sorted(declarations["providers"]) == ["example", "pingo"]
    assert declarations["providers"]["pingo"] == PINGO_DECLARATION
    lines = (out / "providers.lock").read_text().splitlines()
    assert [line.split(" ")[0] for line in lines] == ["example", "declarations.json"]
    assert sorted(p.name for p in out.iterdir() if p.suffix == ".whl") == sorted(
        p.name for p in built.iterdir() if p.suffix == ".whl"
    )


def test_an_endpoint_folder_with_another_provider_id_fails_the_build(tmp_path: Path) -> None:
    root = git_repo(tmp_path / "repo", [])
    endpoint_folder(root)
    (root / "providers" / "pingo" / "endpoint.yaml").write_text(
        ENDPOINT_YAML.replace("provider_id: pingo", "provider_id: other")
    )
    done = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), "build", "--out", str(tmp_path / "out")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 1
    assert "pingo: endpoint.yaml provider_id is 'other'" in done.stderr
