"""One passing and one failing case for each check in scripts/check_provider.py."""

import subprocess
import sys
from collections.abc import Callable

from conftest import SCRIPT, Outcome, Repo

PYPROJECT = "providers/acme/pyproject.toml"
LOCK = "providers/acme/uv.lock"
MANIFEST = "providers/acme/src/sps_provider_acme/sps-provider.yaml"
OWNERS = "providers/acme/OWNERS"
DEPENDENCIES = '"httpx>=0.27", "sandbox-provider-spec"]'
OK = "provider checks ok: 1 folder(s)\n"


def bump_version(repo: Repo) -> None:
    repo.edit(PYPROJECT, 'version = "0.1.0"', 'version = "0.1.1"')


def test_changes_inside_one_provider_folder_pass(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write("providers/acme/CHANGELOG.md", "0.1.1: fix\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout, outcome.stderr) == (0, OK, "")


def test_changes_in_two_folders_fail(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write("providers/other/README.md", "x\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "changes span more than one provider folder: acme, other\n",
    )


def test_dependency_on_the_allowlist_passes(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.edit(PYPROJECT, DEPENDENCIES, '"httpx>=0.27", "sandbox-provider-spec", "acme_sdk"]')

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout) == (0, OK)


def test_dependency_off_the_allowlist_fails(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.edit(PYPROJECT, DEPENDENCIES, '"httpx>=0.27", "sandbox-provider-spec", "requests>=2"]')

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "acme: dependency not on the allowlist: requests\n",
    )


def test_adapter_licence_other_than_the_repository_licence_fails(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.edit(PYPROJECT, 'license = "LicenseRef-PolyForm-Shield-1.0.0"', 'license = "MIT"')

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "acme: pyproject.toml license must be 'LicenseRef-PolyForm-Shield-1.0.0', found 'MIT'\n",
    )


def test_locked_package_with_a_wheel_passes(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(
            LOCK,
            (r.root / LOCK).read_text()
            + '\n[[package]]\nname = "idna"\nversion = "3.10"\n'
            + 'source = { registry = "https://pypi.org/simple" }\n'
            + 'wheels = [\n    { url = "https://f.example.test/i.whl", hash = "sha256:2" },\n]\n',
        )

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout) == (0, OK)


def test_locked_package_without_a_wheel_fails(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(
            LOCK,
            (r.root / LOCK).read_text()
            + '\n[[package]]\nname = "sdist-only"\nversion = "2.0"\n'
            + 'source = { registry = "https://pypi.org/simple" }\n'
            + 'sdist = { url = "https://f.example.test/sdist-only.tar.gz", hash = "sha256:03" }\n',
        )

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (1, "acme: no wheel for sdist-only 2.0\n")


def test_raised_version_passes(repo: Repo) -> None:
    outcome = repo.check(bump_version)
    assert (outcome.code, outcome.stdout) == (0, OK)


def test_unchanged_version_fails(repo: Repo) -> None:
    outcome = repo.check(lambda r: r.write("providers/acme/CHANGELOG.md", "note\n"))
    assert (outcome.code, outcome.stderr) == (1, "acme: version not raised (0.1.0 -> 0.1.0)\n")


def test_manifest_with_hosts_and_suffixes_passes(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.edit(
            MANIFEST,
            "api_domains: [api.acme.test]",
            "api_domains: [api.acme.test, .acme.test, files.acme.s3.amazonaws.com]",
        )

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout) == (0, OK)


def test_manifest_with_missing_field_and_shared_suffix_fails(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(
            MANIFEST,
            "provider_id: acme\nname: Acme\nspec_version: '1.0'\n"
            "api_domains: [api.acme.test, .s3.amazonaws.com]\n",
        )

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "acme: sps-provider.yaml is missing credential_fields\n"
        "acme: shared-hosting suffix refused: .s3.amazonaws.com\n",
    )


def test_owners_and_matching_codeowners_pass(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(OWNERS, "# maintainers\n@acme-dev @acme-ops\n")
        r.write(".github/CODEOWNERS", "* @boxrouter/maintainers\n/providers/acme/ @acme-dev @acme-ops\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout) == (0, OK)


def test_stale_codeowners_fails(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(OWNERS, "@acme-dev @acme-ops\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "CODEOWNERS is out of date; "
        "run scripts/check_provider.py --print-codeowners > .github/CODEOWNERS\n",
    )


ENDPOINT = "providers/pingo/endpoint.yaml"
ENDPOINT_TEXT = (
    "provider_id: pingo\nname: Pingo\nspec_version: '1.0'\nbase_url: https://api.pingo.test/box\n"
)
TWO_OWNERS = "* @boxrouter/maintainers\n/providers/acme/ @acme-dev\n/providers/pingo/ @pingo-dev\n"


def add_endpoint(repo: Repo, text: str = ENDPOINT_TEXT) -> None:
    repo.write(ENDPOINT, text)
    repo.write("providers/pingo/OWNERS", "@pingo-dev\n")
    repo.write(".github/CODEOWNERS", TWO_OWNERS)


def test_endpoint_folder_without_a_package_passes(repo: Repo) -> None:
    outcome = repo.check(add_endpoint)
    assert (outcome.code, outcome.stdout, outcome.stderr) == (0, OK, "")


def test_endpoint_folder_with_brand_files_passes(repo: Repo) -> None:
    def change(r: Repo) -> None:
        add_endpoint(r)
        r.write("providers/pingo/brand/mark.svg", "<svg/>\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout) == (0, OK)


def test_endpoint_url_problems_fail(repo: Repo) -> None:
    cases = {
        "https://api.pingo.test/box/": "pingo: base_url must not end with /: https://api.pingo.test/box/\n",
        "http://api.pingo.test": "pingo: base_url must be https: http://api.pingo.test\n",
        "https://10.0.0.5/box": "pingo: base_url host is an IP address: 10.0.0.5\n",
        "https://localhost/box": "pingo: base_url host is not a public name: localhost\n",
        "https://u:p@api.pingo.test": (
            "pingo: base_url has credentials, a query or a fragment: https://u:p@api.pingo.test\n"
        ),
        "https://api.pingo.test/box?x=1": (
            "pingo: base_url has credentials, a query or a fragment: https://api.pingo.test/box?x=1\n"
        ),
        "https://abc.execute.amazonaws.com": (
            "pingo: base_url host is on a shared-hosting suffix: abc.execute.amazonaws.com\n"
        ),
        "https://amazonaws.com": "pingo: base_url host is on a shared-hosting suffix: amazonaws.com\n",
    }
    for url, message in cases.items():
        fresh = repo.fresh()
        outcome = fresh.check(
            lambda r, url=url: add_endpoint(r, ENDPOINT_TEXT.replace("https://api.pingo.test/box", url))
        )
        assert (outcome.code, outcome.stderr) == (1, message), url


def test_endpoint_field_problems_fail(repo: Repo) -> None:
    def change(r: Repo) -> None:
        add_endpoint(r, "provider_id: other\nname: ' '\nspec_version: 1.0\nbase_url: https://a.test\nx: 1\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "pingo: endpoint.yaml has an unknown field x\n"
        "pingo: endpoint.yaml provider_id is 'other'\n"
        "pingo: endpoint.yaml name is empty\n"
        "pingo: endpoint.yaml spec_version must be '1.0'\n",
    )


def test_endpoint_missing_fields_and_owners_fail(repo: Repo) -> None:
    def change(r: Repo) -> None:
        r.write(ENDPOINT, "provider_id: pingo\nname: Pingo\n")
        r.write(".github/CODEOWNERS", "* @boxrouter/maintainers\n/providers/acme/ @acme-dev\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "pingo: endpoint.yaml is missing spec_version\n"
        "pingo: endpoint.yaml is missing base_url\n"
        "pingo: OWNERS is missing or lists no handle\n",
    )


def test_endpoint_folder_with_package_files_fails(repo: Repo) -> None:
    def change(r: Repo) -> None:
        add_endpoint(r)
        r.write("providers/pingo/pyproject.toml", "[project]\nname = 'x'\n")
        r.write("providers/pingo/src/x.py", "\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "pingo: an endpoint folder must not hold pyproject.toml (it has no package)\n"
        "pingo: an endpoint folder must not hold src (it has no package)\n",
    )


def add_hyphenated_adapter(repo: Repo) -> None:
    """The acme fixture copied to a hyphenated id: folder, manifest id and module name."""
    for source in sorted((repo.root / "providers" / "acme").rglob("*")):
        if source.is_file():
            relative = source.relative_to(repo.root / "providers" / "acme")
            target = str(relative).replace("sps_provider_acme", "sps_provider_zzsynthetic_adapter")
            text = source.read_text()
            if source.name != "OWNERS":
                text = text.replace("acme", "zzsynthetic-adapter")
            repo.write(f"providers/zzsynthetic-adapter/{target}", text)
    repo.write(".github/CODEOWNERS", "* @boxrouter/maintainers\n/providers/acme/ @acme-dev\n/providers/zzsynthetic-adapter/ @acme-dev\n")


def test_hyphenated_adapter_folder_passes(repo: Repo) -> None:
    outcome = repo.check(add_hyphenated_adapter)
    assert (outcome.code, outcome.stdout, outcome.stderr) == (0, OK, "")


def test_hyphenated_endpoint_folder_passes(repo: Repo) -> None:
    def change(r: Repo) -> None:
        folder = "providers/pingo-box"
        r.write(f"{folder}/endpoint.yaml", ENDPOINT_TEXT.replace("provider_id: pingo", "provider_id: pingo-box"))
        r.write(f"{folder}/OWNERS", "@pingo-dev\n")
        r.write(".github/CODEOWNERS", "* @boxrouter/maintainers\n/providers/acme/ @acme-dev\n/providers/pingo-box/ @pingo-dev\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout, outcome.stderr) == (0, OK, "")


def test_adapter_manifest_id_must_equal_the_folder(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.edit(MANIFEST, "provider_id: acme", "provider_id: acme-other")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "acme: sps-provider.yaml provider_id is 'acme-other'\n",
    )


def test_folder_names_outside_the_sps_rule_fail(repo: Repo) -> None:
    for name in ("Upper", "under_score", "a", "x" * 33):
        outcome = repo.fresh().check(
            lambda r, name=name: (
                r.write(f"providers/{name}/endpoint.yaml", ENDPOINT_TEXT),
                r.write(f"providers/{name}/OWNERS", "@pingo-dev\n"),
            )
        )
        assert outcome.code == 1, name
        assert f"{name}: the folder name does not match the SPS provider id" in outcome.stderr


def owned_check(repo: Repo, author: str | None, change: Callable[[Repo], None]) -> Outcome:
    repo.write("policy/cloud-owned-providers.txt", "# note\nacme\n")
    repo.git("add", "-A")
    repo.git("commit", "-q", "-m", "list")
    args = ["--pr-author", author] if author else []
    repo.git("switch", "-q", "-c", "pr")
    change(repo)
    repo.git("add", "-A")
    repo.git("commit", "-q", "-m", "change")
    done = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(repo.root), "--base", "main", *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return Outcome(done.returncode, done.stdout, done.stderr)


def change_acme(r: Repo) -> None:
    bump_version(r)
    r.write("providers/acme/CHANGELOG.md", "0.1.1: fix\n")


def test_a_pull_request_to_a_folder_boxrouter_maintains_fails(repo: Repo) -> None:
    outcome = owned_check(repo, "someone", change_acme)
    assert outcome.code == 1
    assert "providers/acme/ is published from BoxRouter's own source" in outcome.stderr


def test_the_export_app_may_change_a_folder_boxrouter_maintains(repo: Repo) -> None:
    assert owned_check(repo, "boxrouter-export[bot]", change_acme).code == 0


def test_without_a_pr_author_nothing_is_refused_for_ownership(repo: Repo) -> None:
    assert owned_check(repo, None, change_acme).code == 0
