# Sandbox provider adapters

> [!NOTE]
> **Sandbox providers are invited to contribute.** If you run a sandbox service, apply at <https://boxrouter.ai/providers>. We can write the adapter over your public API, or you can add one, or register your own SPS 1.0 server endpoint. [CONTRIBUTING.md](CONTRIBUTING.md) lists the steps from application to going live.

This repository holds contributed provider integrations for the [Sandbox Provider Spec](https://github.com/boxrouter/sandbox-provider-spec) (SPS). SPS defines the standard 1.0 interface and capability declarations for code execution sandboxes; this repository packages the provider implementations and releases them for tools and consumers that speak SPS.

Each provider has one folder under `providers/` in one of two forms:

- **An adapter:** a Python package that implements the SPS 1.0 operations over the provider's own API.
- **An endpoint:** an `endpoint.yaml` file that points to an SPS 1.0 HTTP server that the provider runs itself.

Every merge that changes a provider folder cuts a release containing the built Python wheels, the locked dependency requirements, and a combined `declarations.json` file for all providers.

To add or update a provider, read [CONTRIBUTING.md](CONTRIBUTING.md). Maintainers review pull requests against [REVIEW.md](REVIEW.md). Report a security vulnerability as [SECURITY.md](SECURITY.md) describes. The code is source-available under the PolyForm Shield License 1.0.0 ([LICENSE](LICENSE)), which is not an OSI-approved license: you may use, modify, and share the code for any purpose except building a product that competes with BoxRouter. Releases published before this license keep the license that they carry, and provider brand files are the trademarks of their respective owners ([NOTICE](NOTICE)).

## Repository layout

```text
sandbox-providers/
├── README.md
├── CONTRIBUTING.md
├── REVIEW.md
├── SECURITY.md
├── LICENSE
├── LICENSES/
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
