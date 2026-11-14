# Contributing

## Add a provider

1. Copy the adapter template from `cloud/packages/sandbox-provider-spec/template/` into
   `providers/<provider>/`.
2. Complete the provider project, `sps-provider.yaml` manifest, offline tests, `OWNERS`, brand files and
   changelog for that provider.
3. Run the SPS tester against the adapter or its served URL, using the provider's test-account
   environment variable. The interface assumed by this design is
   `sandbox-provider-test <adapter-or-url> --account-env <VAR>`.
4. Open a pull request for review. Keep the change inside that provider's folder.

Test-account credentials never go in a pull request, commit, source file or log. Keep them out of
all review text and use the approved secure credential flow for test runs.

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
(`sandbox-provider-test <url> --account-env <VAR>`) before opening the pull request. Contact
addresses never go in this repository.

## Updates by the owning provider

An owning provider's update must:

1. Change only that provider's folder, raise its `version`, and add a line to its `CHANGELOG.md`.
2. Pass `checks.yml` and the provider's offline tests.
3. Pass `live.yml` on the final pull-request head SHA.
4. Complete the agent review, then receive a maintainer merge.

## Developer Certificate of Origin

Every commit carries a `Signed-off-by:` line; see <https://developercertificate.org/>.
