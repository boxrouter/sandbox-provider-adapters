# Security

## Reporting a vulnerability

Report a vulnerability privately through GitHub: on this repository's **Security** tab,
choose **Report a vulnerability**. If GitHub's private reporting form is unavailable, email
[security@boxrouter.ai](mailto:security@boxrouter.ai). Please do not open a public issue or
pull request for it, and do not include a live credential in a report.

The maintainers acknowledge a report within 7 days, keep you informed while it is fixed, and
credit you when it is disclosed, unless you prefer otherwise.

## Scope

In scope is what this repository holds and runs: the static checks (`scripts/check_provider.py`),
the release tooling (`scripts/release.py`), the workflows in `.github/workflows/`, and a
registered adapter that mishandles a credential. Examples are an adapter that sends a token to a
host other than its provider's, a check that accepts a file it should refuse, and a workflow
change that lets a pull request reach a secret or move a release tag.

A vulnerability in a provider's service or SDK belongs to that provider: report it to them. If it
also makes an adapter here wrong, report that here privately as well.

## Supported versions

Fixes go into the latest release.
