# Sandbox provider adapters

This repository holds contributed adapters for the
[Sandbox Provider Spec](https://github.com/boxrouter/sandbox-provider-spec) (SPS). A provider is
either an adapter, a Python package that implements the SPS 1.0 operations over the provider's own
API, or an endpoint, a pointer to an SPS 1.0 server the provider runs itself. Each provider has one
folder under `providers/`. Every merge that changes a provider cuts a release with the wheels, the
locked requirements and a `declarations.json` for all providers.

To add a provider, read [CONTRIBUTING.md](CONTRIBUTING.md). Maintainers review pull requests with
[REVIEW.md](REVIEW.md). Report a vulnerability as [SECURITY.md](SECURITY.md) says. The code is
licensed under Apache-2.0 ([LICENSE](LICENSE)); provider brand files are the owners' trademarks
([NOTICE](NOTICE)).

## Repository layout

```text
sandbox-providers/
├── README.md
├── CONTRIBUTING.md
├── REVIEW.md
├── SECURITY.md
├── LICENSE
├── NOTICE
├── policy/
│   ├── allowed-dependencies.yaml
│   └── shared-hosting-suffixes.txt
├── scripts/
│   ├── check_provider.py
│   ├── live_proxy.py
│   └── release.py
├── providers/
│   ├── example/               # the template adapter, over a fake provider API
│   └── <provider>/
│       ├── pyproject.toml
│       ├── uv.lock
│       ├── src/sps_provider_<provider>/   # includes sps-provider.yaml
│       ├── tests/
│       ├── brand/
│       ├── OWNERS
│       └── CHANGELOG.md
│   └── <endpoint-provider>/   # a provider's own server: endpoint.yaml, OWNERS, brand/ (no package)
└── .github/
    ├── CODEOWNERS
    └── workflows/
        ├── checks.yml
        ├── dco.yml
        ├── live.yml
        └── release.yml
```

Each provider folder is its own Python project with its own lock, not a uv workspace member. An endpoint folder (`endpoint.yaml`, no package) registers a server the provider runs itself.
