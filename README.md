# Sandbox provider adapters

This repository holds contributed adapters for the Sandbox Provider Spec (SPS). The layout
below describes the repository as it is developed; later tasks add the template, provider
projects, checks and workflows.

This repository is private until launch.

## Repository layout

```text
sandbox-providers/
├── README.md
├── CONTRIBUTING.md
├── REVIEW.md
├── LICENSE
├── NOTICE
├── policy/
│   ├── allowed-dependencies.yaml
│   └── shared-hosting-suffixes.txt
├── scripts/
│   └── check_provider.py
├── providers/
│   ├── _template/
│   └── <provider>/
│       ├── pyproject.toml
│       ├── uv.lock
│       ├── src/sps_provider_<provider>/   # includes sps-provider.yaml
│       ├── tests/
│       ├── brand/
│       ├── OWNERS
│       └── CHANGELOG.md
└── .github/
    ├── CODEOWNERS
    └── workflows/
        ├── checks.yml
        ├── live.yml
        └── release.yml
```

Each provider folder is its own Python project with its own lock, not a uv workspace member.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution path and [REVIEW.md](REVIEW.md)
for the first-pass review checklist. A dedicated release App and a `v*` tag ruleset are a
hardening item for the public launch (P5); until then `release.yml` tags with its own token.
