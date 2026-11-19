"""Build the release of the provider repo: wheels, locked requirements, declarations and the lock.

`release.py build --out <dir>` writes, for every `providers/<p>/` (folders starting with `_`
are templates and are skipped): `<wheel>`, `requirements-<p>.txt`, `declarations.json` and
`providers.lock`. `release.py next-tag` prints the next `vX.Y.Z`. Neither runs provider code:
declarations are read from `pyproject.toml` and `sps-provider.yaml`.

A folder holding `endpoint.yaml` is a native endpoint: the provider runs its own server, so the
folder has no package. It is listed in `declarations.json` (with `endpoint: {base_url}`) and gets
no wheel, no requirements file and no `providers.lock` line.

Two builds of one commit are byte-identical: wheels are built with `SOURCE_DATE_EPOCH` set to
the commit time, and every JSON and lock file has a fixed order.

The interface package (`sandbox-provider-spec`) is never part of a provider's requirements or
wheel list. A provider names it by version in `pyproject.toml`; the declaration records that
requirement under `sps_requirement`, and the installer (cloud-deploy) supplies the package.
Any other dependency that is not a hashed index release (a path or URL source) fails the build,
so a release cannot ship a vendored dependency.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import tomllib
import yaml
from check_provider import ENDPOINT, LICENSE_EXPRESSION, PROVIDER_ID, normalize, provider_dirs

SPS_PACKAGE = "sandbox-provider-spec"
DECLARATIONS = "declarations.json"
LOCK = "providers.lock"
SCHEMA = 1
TAG = re.compile(r"v(\d+)\.(\d+)\.(\d+)")


def run(args: list[str], env: dict[str, str] | None = None, cwd: Path | None = None) -> str:
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, **(env or {})},
        cwd=cwd,
    )
    if result.returncode != 0:
        sys.exit(f"{' '.join(args)} failed:\n{result.stderr}{result.stdout}")
    return result.stdout


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sps_requirement(project: dict) -> str:
    """The provider's own requirement string for the interface package, as the specifier text."""
    for dependency in project.get("dependencies", []):
        match = re.match(r"([A-Za-z0-9][A-Za-z0-9._-]*)\s*(.*)", dependency)
        if match and normalize(match.group(1)) == SPS_PACKAGE:
            return match.group(2).strip()
    sys.exit(f"{project['name']}: pyproject.toml does not depend on {SPS_PACKAGE}")


def read_declaration(root: Path, provider: str) -> dict:
    folder = root / "providers" / provider
    project = tomllib.loads((folder / "pyproject.toml").read_text())["project"]
    if project.get("license") != LICENSE_EXPRESSION:
        sys.exit(f"{provider}: pyproject.toml license must be {LICENSE_EXPRESSION!r}")
    manifests = list(folder.glob("src/*/sps-provider.yaml")) + list(folder.glob("sps-provider.yaml"))
    if len(manifests) != 1:
        sys.exit(f"{provider}: expected one sps-provider.yaml, found {len(manifests)}")
    declaration = yaml.safe_load(manifests[0].read_text())
    if declaration.get("provider_id") != provider:
        sys.exit(f"{provider}: sps-provider.yaml provider_id is {declaration.get('provider_id')!r}")
    return {
        **declaration,
        "version": project["version"],
        "sps_requirement": sps_requirement(project),
    }


def read_endpoint(root: Path, provider: str) -> dict:
    """A native endpoint's declaration: its `endpoint.yaml` and the one bearer token it takes."""
    folder = root / "providers" / provider
    endpoint = yaml.safe_load((folder / ENDPOINT).read_text())
    if endpoint.get("provider_id") != provider:
        sys.exit(f"{provider}: {ENDPOINT} provider_id is {endpoint.get('provider_id')!r}")
    return {
        "provider_id": provider,
        "name": endpoint["name"],
        "spec_version": endpoint["spec_version"],
        "credential_fields": [
            {
                "name": "api_key",
                "secret": True,
                "description": f"API key for {endpoint['name']}",
            }
        ],
        "endpoint": {"base_url": endpoint["base_url"]},
    }


def check_requirements(provider: str, text: str) -> None:
    """Every requirement line is `name==version` with hashes: no path, URL or VCS source."""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "--hash")):
            continue
        if not re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*(\[[^\]]*\])?==[^\s;]+", stripped):
            sys.exit(f"{provider}: requirement is not a pinned index release: {stripped}")


def commit_time(root: Path) -> str:
    return run(["git", "-C", str(root), "log", "-1", "--format=%ct"]).strip()


def build_wheel(root: Path, provider: str, out: Path, epoch: str) -> Path:
    with tempfile.TemporaryDirectory() as scratch:
        run(
            ["uv", "build", "--wheel", "--out-dir", scratch, str(root / "providers" / provider)],
            env={"SOURCE_DATE_EPOCH": epoch},
        )
        wheels = list(Path(scratch).glob("*.whl"))
        if len(wheels) != 1:
            sys.exit(f"{provider}: expected one wheel, built {len(wheels)}")
        target = out / wheels[0].name
        shutil.copyfile(wheels[0], target)
        return target


def build(root: Path, out: Path) -> None:
    providers = provider_dirs(root)
    out.mkdir(parents=True, exist_ok=True)
    epoch = commit_time(root)
    declarations: dict[str, dict] = {}
    lock_lines: list[str] = []
    for provider in providers:
        if not PROVIDER_ID.fullmatch(provider):
            sys.exit(f"{provider}: the folder name does not match ^[a-z0-9-]{{2,32}}$")
        if (root / "providers" / provider / ENDPOINT).is_file():
            declarations[provider] = read_endpoint(root, provider)
            continue
        declaration = read_declaration(root, provider)
        wheel = build_wheel(root, provider, out, epoch)
        requirements = run(
            [
                "uv",
                "export",
                "--project",
                str(root / "providers" / provider),
                "--frozen",
                "--no-dev",
                "--no-emit-project",
                "--no-header",
                "--no-annotate",
                "--no-emit-package",
                SPS_PACKAGE,
            ]
        )
        check_requirements(provider, requirements)
        requirements_file = out / f"requirements-{provider}.txt"
        requirements_file.write_text(requirements)
        declarations[provider] = declaration
        lock_lines.append(
            f"{provider} {declaration['version']} {wheel.name} {sha256(wheel)} "
            f"{sha256(requirements_file)}"
        )
    declarations_file = out / DECLARATIONS
    declarations_file.write_text(
        json.dumps({"schema": SCHEMA, "providers": declarations}, indent=2, sort_keys=True) + "\n"
    )
    lock_lines.append(f"{DECLARATIONS} {sha256(declarations_file)}")
    (out / LOCK).write_text("\n".join(lock_lines) + "\n")


def next_tag(root: Path) -> str:
    tags = run(["git", "-C", str(root), "tag", "--list", "v*"]).split()
    versions = [tuple(map(int, m.groups())) for t in tags if (m := TAG.fullmatch(t))]
    if not versions:
        return "v0.1.0"
    major, minor, patch = max(versions)
    return f"v{major}.{minor}.{patch + 1}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    commands = parser.add_subparsers(dest="command", required=True)
    build_command = commands.add_parser("build")
    build_command.add_argument("--out", type=Path, required=True)
    commands.add_parser("next-tag")
    args = parser.parse_args()
    if args.command == "build":
        build(args.root, args.out)
    else:
        print(next_tag(args.root))


if __name__ == "__main__":
    main()
