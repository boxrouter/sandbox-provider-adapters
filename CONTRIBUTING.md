# Contributing

## How a provider goes live

1. **Apply.** Send the form at <https://boxrouter.ai/providers>. Say whether you or we build the adapter, and give the work email of the person who saves your test account. We reply by email.
2. **Build the adapter.** If your API is public, we can write the adapter for you. Otherwise you write it, either as an adapter in this repository or as your own API that speaks SPS, and run the tester until it passes.
3. **Register.** A pull request to this repository adds `providers/<provider_id>/`: your adapter (or, for your own API, its base URL) and your brand files. You open it, or we do when we wrote the adapter. BoxRouter reviews the code and its safety, then merges it.
4. **Save the test account.** After the merge, we add the work email from your application as a contact. Sign in with that email at <https://boxrouter.ai/providers/test-account> and save your test account's credentials there. They never go in the form, a pull request, or an email.
5. **Go live.** BoxRouter runs the tester against your test account. When the required operations pass, you go live automatically for customers who connect their own account with you and pay you directly.

The [implement guide](https://boxrouter.ai/docs/providers/implement) takes a provider to a passing tester run; the rest of this page is step 3 in detail.

## Add a provider

An adapter implements the [Sandbox Provider Spec](https://github.com/boxrouter/sandbox-provider-spec)
(SPS) 1.0 operations over the provider's own API.

1. Copy `providers/example/` to `providers/<provider>/`. It is the working template, with its own
   README that shows how to run it. Rename the package and the provider id in `pyproject.toml`
   and `src/<package>/sps-provider.yaml`, and replace each Example Cloud call with your API's.
2. Complete the provider project: the `sps-provider.yaml` manifest, offline tests, `OWNERS`, brand
   files and `CHANGELOG.md`.
3. Run the SPS tester from `providers/<provider>/`, naming each credential field and the environment
   variable that holds it (as the template's README shows):
   `uv run --group tools sandbox-provider-spec test <provider> --credential <field>=<ENV_VAR>`.
4. Open a pull request. Keep the change inside that provider's folder and regenerate
   `.github/CODEOWNERS` as `scripts/check_provider.py` says.

A provider's `vendor/` wheel is the SPS release its adapter was last tested with, for local tester
runs only. The release never ships it: BoxRouter runs your adapter on the SPS release it deploys.
`providers/example/` carries the current release, so a copy starts current; replace the wheel with the
newest release when you next update your adapter.

Never put test-account credentials in a pull request, commit, source file, log or review text.

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
`declarations.json` as `endpoint: {base_url}` with one secret `api_key` field, and BoxRouter calls
that URL with each workspace's own key. Run the SPS tester against the URL
(`sandbox-provider-spec test <url> --credential token=<ENV_VAR>`) before opening the pull request.
Contact addresses never go in this repository.

## Updates by the owning provider

An owning provider's update must:

1. Change only that provider's folder, raise its `version`, and add a line to its `CHANGELOG.md`.
2. Pass `checks.yml` and the provider's offline tests.
3. Pass `live.yml` on the final pull-request head SHA. It runs with the test account from step 4 of [How a provider goes live](#how-a-provider-goes-live).
4. Complete the agent review, then receive a maintainer merge.

## Licence Of Your Contribution

You keep the copyright in your contribution, and your own use of your adapter elsewhere is not
limited by this licence. The sign-off below stays. By submitting it you confirm that you have the
right to, and you license it to BoxRouter, Inc. under a perpetual, worldwide, non-exclusive,
royalty-free, irrevocable licence to use, modify, distribute and sublicense it. You also license it
to everyone under the PolyForm Shield License 1.0.0 as part of this repository (see
[LICENSE](LICENSE)). If your employer owns the work, the employer must be the contributor.

A provider that serves SPS 1.0 from its own API registers only its base URL, as
[Add a native endpoint](#add-a-native-endpoint) describes, and contributes no code.

## Developer Certificate of Origin

Every commit carries a `Signed-off-by:` line; see <https://developercertificate.org/>.
