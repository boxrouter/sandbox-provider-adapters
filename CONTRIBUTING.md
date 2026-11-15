# Contributing

## Add a provider

An adapter implements the [Sandbox Provider Spec](https://github.com/boxrouter/sandbox-provider-spec)
(SPS) 1.0 operations over the provider's own API. The spec repository's
[provider kit](https://github.com/boxrouter/sandbox-provider-spec/blob/main/docs/how-to/provider-kit.md)
and [declaration guide](https://github.com/boxrouter/sandbox-provider-spec/blob/main/spec/declarations.md)
describe the contract and the evidence it asks for; this page describes this repository's side.

1. Copy `providers/example/` to `providers/<provider>/`. It is the working template, with its own
   README that shows how to run it. Rename the package and the provider id in `pyproject.toml`
   and `src/<package>/sps-provider.yaml`, and replace each Example Cloud call with your API's.
2. Complete the provider project: the `sps-provider.yaml` manifest, offline tests, `OWNERS`, brand
   files and `CHANGELOG.md`.
3. Run the SPS tester against the adapter, naming each credential field and the environment
   variable that holds it (as the template's README shows):
   `sandbox-provider-spec test <provider> --credential <field>=<ENV_VAR>`.
4. Open a pull request. Keep the change inside that provider's folder and regenerate
   `.github/CODEOWNERS` as `scripts/check_provider.py` says.

Test-account credentials never go in a pull request, commit, source file or log. Keep them out of
all review text. A maintainer arranges the provider's test account for the live run.

## Provider ids

A folder name, `provider_id` and entry-point name match `^[a-z0-9-]{2,32}$`, the SPS 1.0 rule
(`docker-engine` is valid). Where Python needs an identifier, replace `-` with `_`: the package is
`sps_provider_docker_engine`, and the test-account secret name is `BOXROUTER_CONTRIBUTED_DOCKER_ENGINE_<FIELD>`.

## Add a native endpoint

A provider that already runs its own SPS 1.0 server registers it with a folder that has no
package:

```text
providers/<provider>/
├── endpoint.yaml    # provider_id, name, spec_version: "1.0", base_url
├── OWNERS
└── brand/           # optional
```

`base_url` is the HTTPS base of the server; discovery is `<base_url>/v1/discovery`. The host must
be a public DNS name that is not on a shared-hosting suffix (`policy/shared-hosting-suffixes.txt`),
and the URL has no trailing slash, query, fragment or user info. There is no `pyproject.toml`,
wheel, requirements file or `providers.lock` line: the release lists the folder in
`declarations.json` as `endpoint: {base_url}` with one secret `api_key` field, and Cloud opens
it at that URL with each workspace's own key. Run the SPS tester against the URL
(`sandbox-provider-spec test <url> --credential token=<ENV_VAR>`) before opening the pull request. Contact
addresses never go in this repository.

## Updates by the owning provider

An owning provider's update must:

1. Change only that provider's folder, raise its `version`, and add a line to its `CHANGELOG.md`.
2. Pass `checks.yml` and the provider's offline tests.
3. Pass `live.yml` on the final pull-request head SHA.
4. Complete the agent review, then receive a maintainer merge.

## Developer Certificate of Origin

Every commit carries a `Signed-off-by:` line; see <https://developercertificate.org/>.
