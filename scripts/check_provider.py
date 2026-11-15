"""Static checks for a pull request to the provider repo.

`check_provider.py --base <ref>` compares HEAD with the merge base of `<ref>` and prints one
line per failure on stderr. `check_provider.py --print-codeowners` prints the CODEOWNERS file
that the OWNERS files imply. Neither runs provider code.
"""

import argparse
import ipaddress
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

import tomllib
import yaml

MAX_DECLARED = 8
CODEOWNERS = ".github/CODEOWNERS"
VERSION = re.compile(r"\d+(\.\d+)*")
REQUIREMENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, text=True, check=True
    )
    return result.stdout


def normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def provider_dirs(root: Path) -> list[str]:
    """Provider folder names at HEAD; folders starting with `_` (the template) are not providers."""
    providers = root / "providers"
    if not providers.is_dir():
        return []
    return sorted(p.name for p in providers.iterdir() if p.is_dir() and not p.name.startswith("_"))


def read_owners(root: Path, provider: str) -> list[str]:
    path = root / "providers" / provider / "OWNERS"
    if not path.is_file():
        return []
    lines = (line.split("#")[0].strip() for line in path.read_text().splitlines())
    return [handle for line in lines for handle in line.split() if handle.startswith("@")]


MAINTAINERS = "@boxrouter/maintainers"


def codeowners(root: Path) -> str:
    """Maintainers own every path; a provider's own handles own its folder (the last match wins)."""
    lines = [f"* {MAINTAINERS}\n"]
    for provider in provider_dirs(root):
        owners = read_owners(root, provider)
        if owners:
            lines.append(f"/providers/{provider}/ {' '.join(owners)}\n")
    return "".join(lines)


def changed_paths(root: Path, base: str) -> list[str]:
    return git(root, "diff", "--name-only", f"{base}...HEAD").splitlines()


def check_scope(paths: list[str]) -> tuple[list[str], list[str]]:
    """Return the failures and the changed provider folders.

    The regenerated CODEOWNERS file may change together with a provider folder.
    """
    folders = sorted({p.split("/")[1] for p in paths if p.startswith("providers/")})
    if not folders:
        return [], []
    spans = folders + sorted(p for p in paths if not p.startswith("providers/") and p != CODEOWNERS)
    if len(spans) > 1:
        return [f"changes span more than one provider folder: {', '.join(spans)}"], folders
    return [], folders


def direct_dependencies(pyproject: dict) -> set[str]:
    project = pyproject.get("project", {})
    requirements = list(project.get("dependencies", []))
    for extra in project.get("optional-dependencies", {}).values():
        requirements.extend(extra)
    return {normalize(REQUIREMENT_NAME.match(r.strip()).group()) for r in requirements}


def check_dependencies(provider: str, pyproject: dict, policy: dict) -> list[str]:
    allowed = {normalize(n) for n in policy.get("global", [])}
    allowed |= {normalize(n) for n in policy.get("providers", {}).get(provider, [])}
    return [
        f"{provider}: dependency not on the allowlist: {name}"
        for name in sorted(direct_dependencies(pyproject) - allowed)
    ]


def check_wheels(provider: str, lock: dict) -> list[str]:
    failures = []
    for package in lock.get("package", []):
        if set(package.get("source", {})) & {"editable", "virtual"}:
            continue  # the provider's own project is built by the release, not locked as a wheel
        if not package.get("wheels"):
            failures.append(f"{provider}: no wheel for {package['name']} {package['version']}")
    return failures


def version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def check_version(root: Path, base: str, provider: str, new: str) -> list[str]:
    old_text = subprocess.run(
        ["git", "-C", str(root), "show", f"{base}:providers/{provider}/pyproject.toml"],
        capture_output=True,
        text=True,
        check=False,
    )
    if old_text.returncode != 0:
        return []  # a new folder
    old = tomllib.loads(old_text.stdout).get("project", {}).get("version", "")
    if not (VERSION.fullmatch(old) and VERSION.fullmatch(new)):
        return [f"{provider}: version is not a plain number ({old} -> {new})"]
    if version_key(new) <= version_key(old):
        return [f"{provider}: version not raised ({old} -> {new})"]
    return []


MANIFEST_FIELDS = ("provider_id", "name", "spec_version", "credential_fields", "api_domains")


def check_manifest(provider: str, manifest: dict, shared: list[str]) -> list[str]:
    """Check `sps-provider.yaml`: an `api_domains` entry is an exact host or a `.suffix`."""
    failures = [
        f"{provider}: sps-provider.yaml is missing {field}"
        for field in MANIFEST_FIELDS
        if field not in manifest
    ]
    domains = manifest.get("api_domains") or []
    suffixes = [d for d in domains if d.startswith(".")]
    hosts = [d for d in domains if not d.startswith(".")]
    if len(hosts) > MAX_DECLARED:
        failures.append(f"{provider}: more than {MAX_DECLARED} hosts ({len(hosts)})")
    if len(suffixes) > MAX_DECLARED:
        failures.append(f"{provider}: more than {MAX_DECLARED} suffixes ({len(suffixes)})")
    failures += [
        f"{provider}: shared-hosting suffix refused: {suffix}"
        for suffix in suffixes
        if any(suffix[1:] == banned or suffix.endswith("." + banned) for banned in shared)
    ]
    return failures


ENDPOINT = "endpoint.yaml"
ENDPOINT_FIELDS = ("provider_id", "name", "spec_version", "base_url")
#: The SPS 1.0 provider id (`sps-provider.schema.json`), the one rule for every folder and manifest.
#: Where a Python identifier is needed, replace `-` with `_` (`sps_provider_<id>`, the secret name
#: `BOXROUTER_CONTRIBUTED_<ID upper>_<FIELD>`).
PROVIDER_ID = re.compile(r"[a-z0-9-]{2,32}")
PACKAGE_FILES = ("pyproject.toml", "uv.lock", "src")


def check_base_url(provider: str, base_url: object, shared: list[str]) -> list[str]:
    """A native endpoint's base URL: https, a DNS name off the shared-hosting suffixes, no extras.

    Discovery is `<base_url>/v1/discovery`, so the URL has no trailing slash, query or fragment.
    """
    if not isinstance(base_url, str):
        return [f"{provider}: base_url is not text"]
    parts = urlsplit(base_url)
    if parts.scheme != "https":
        return [f"{provider}: base_url must be https: {base_url}"]
    host = (parts.hostname or "").lower()
    if not host:
        return [f"{provider}: base_url has no host: {base_url}"]
    if parts.username is not None or parts.password is not None or parts.query or parts.fragment:
        return [f"{provider}: base_url has credentials, a query or a fragment: {base_url}"]
    if base_url.endswith("/"):
        return [f"{provider}: base_url must not end with /: {base_url}"]
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return [f"{provider}: base_url host is an IP address: {host}"]
    if "." not in host:
        return [f"{provider}: base_url host is not a public name: {host}"]
    if any(host == banned or host.endswith("." + banned) for banned in shared):
        return [f"{provider}: base_url host is on a shared-hosting suffix: {host}"]
    return []


def check_endpoint(root: Path, provider: str, shared: list[str]) -> list[str]:
    """Check an endpoint-only folder: `endpoint.yaml` and no package files."""
    folder = root / "providers" / provider
    failures = [
        f"{provider}: an endpoint folder must not hold {name} (it has no package)"
        for name in PACKAGE_FILES
        if (folder / name).exists()
    ]
    endpoint = yaml.safe_load((folder / ENDPOINT).read_text())
    if not isinstance(endpoint, dict):
        return [*failures, f"{provider}: {ENDPOINT} is not a mapping"]
    failures += [
        f"{provider}: {ENDPOINT} is missing {field}"
        for field in ENDPOINT_FIELDS
        if field not in endpoint
    ]
    failures += [
        f"{provider}: {ENDPOINT} has an unknown field {field}"
        for field in sorted(set(endpoint) - set(ENDPOINT_FIELDS))
    ]
    if "provider_id" in endpoint and endpoint["provider_id"] != provider:
        failures.append(f"{provider}: {ENDPOINT} provider_id is {endpoint['provider_id']!r}")
    if "name" in endpoint and not (isinstance(endpoint["name"], str) and endpoint["name"].strip()):
        failures.append(f"{provider}: {ENDPOINT} name is empty")
    if "spec_version" in endpoint and endpoint["spec_version"] != "1.0":
        failures.append(f"{provider}: {ENDPOINT} spec_version must be '1.0'")
    if "base_url" in endpoint:
        failures += check_base_url(provider, endpoint["base_url"], shared)
    return failures + check_owners(root, provider)


def check_owners(root: Path, provider: str) -> list[str]:
    if read_owners(root, provider):
        return []
    return [f"{provider}: OWNERS is missing or lists no handle"]


def check_codeowners(root: Path) -> list[str]:
    path = root / CODEOWNERS
    current = path.read_text() if path.is_file() else ""
    if current == codeowners(root):
        return []
    return [
        "CODEOWNERS is out of date; "
        f"run scripts/check_provider.py --print-codeowners > {CODEOWNERS}"
    ]


def check_folder(
    root: Path, base: str, provider: str, policy: dict, shared: list[str]
) -> list[str]:
    folder = root / "providers" / provider
    if not PROVIDER_ID.fullmatch(provider):
        return [f"{provider}: the folder name does not match the SPS provider id ^[a-z0-9-]{{2,32}}$"]
    if (folder / ENDPOINT).is_file():
        return check_endpoint(root, provider, shared)
    missing = [n for n in ("pyproject.toml", "uv.lock") if not (folder / n).is_file()]
    if missing:
        return [f"{provider}: {name} is missing" for name in missing]
    pyproject = tomllib.loads((folder / "pyproject.toml").read_text())
    failures = check_dependencies(provider, pyproject, policy)
    failures += check_wheels(provider, tomllib.loads((folder / "uv.lock").read_text()))
    failures += check_version(root, base, provider, pyproject.get("project", {}).get("version", ""))
    manifests = sorted(folder.glob("src/*/sps-provider.yaml"))
    if manifests:
        manifest = yaml.safe_load(manifests[0].read_text()) or {}
        failures += check_manifest(provider, manifest, shared)
        if manifest.get("provider_id") != provider:
            failures.append(
                f"{provider}: sps-provider.yaml provider_id is {manifest.get('provider_id')!r}"
            )
    else:
        failures.append(f"{provider}: sps-provider.yaml is missing")
    return failures + check_owners(root, provider)


def run_checks(root: Path, base: str) -> tuple[list[str], int]:
    failures, folders = check_scope(changed_paths(root, base))
    if failures:
        return failures, len(folders)  # the other checks say nothing useful about a rejected PR
    folders = [f for f in folders if f in provider_dirs(root)]  # a deleted folder has no checks
    policy = yaml.safe_load((root / "policy/allowed-dependencies.yaml").read_text()) or {}
    shared = (root / "policy/shared-hosting-suffixes.txt").read_text().split()
    for provider in folders:
        failures += check_folder(root, base, provider, policy, shared)
    return failures + check_codeowners(root), len(folders)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="git ref to compare HEAD with")
    parser.add_argument("--print-codeowners", action="store_true")
    parser.add_argument("--root", type=Path, default=Path("."), help="repository root")
    args = parser.parse_args()
    if args.print_codeowners:
        sys.stdout.write(codeowners(args.root))
        return 0
    if not args.base:
        parser.error("--base is required")
    failures, count = run_checks(args.root, args.base)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"provider checks ok: {count} folder(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
