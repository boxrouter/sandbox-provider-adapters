"""One passing and one failing case for each check in scripts/check_provider.py."""

from conftest import Repo

PYPROJECT = "providers/acme/pyproject.toml"
LOCK = "providers/acme/uv.lock"
DECLARATION = "providers/acme/declaration.yaml"
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


def test_declaration_with_hosts_under_suffixes_passes(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(DECLARATION, "hosts: [api.acme.test, files.acme.test]\nsuffixes: [acme.test]\n")

    outcome = repo.check(change)
    assert (outcome.code, outcome.stdout) == (0, OK)


def test_declaration_with_stray_host_and_shared_suffix_fails(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(
            DECLARATION,
            "hosts: [api.acme.test, api.other.test]\nsuffixes: [acme.test, s3.amazonaws.com]\n",
        )

    outcome = repo.check(change)
    assert (outcome.code, outcome.stderr) == (
        1,
        "acme: host api.other.test is not under a declared suffix\n"
        "acme: shared-hosting suffix refused: s3.amazonaws.com\n",
    )


def test_owners_and_matching_codeowners_pass(repo: Repo) -> None:
    def change(r: Repo) -> None:
        bump_version(r)
        r.write(OWNERS, "# maintainers\n@acme-dev @acme-ops\n")
        r.write(".github/CODEOWNERS", "/providers/acme/ @acme-dev @acme-ops\n")

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
