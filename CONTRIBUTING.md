# Contributing

## Add a provider

1. Copy the adapter template from `cloud/packages/sandbox-provider-spec/template/` into
   `providers/<provider>/`.
2. Complete the provider project, static declaration, offline tests, `OWNERS`, brand files and
   changelog for that provider.
3. Run the SPS tester against the adapter or its served URL, using the provider's test-account
   environment variable. The interface assumed by this design is
   `sandbox-provider-test <adapter-or-url> --account-env <VAR>`.
4. Open a pull request for review. Keep the change inside that provider's folder.

Test-account credentials never go in a pull request, commit, source file or log. Keep them out of
all review text and use the approved secure credential flow for test runs.

## Updates by the owning provider

An owning provider's update must:

1. Change only that provider's folder, raise its `version`, and add a line to its `CHANGELOG.md`.
2. Pass `checks.yml` and the provider's offline tests.
3. Pass `live.yml` on the final pull-request head SHA.
4. Complete the agent review, then receive a maintainer merge.

## Developer Certificate of Origin

Every commit carries a `Signed-off-by:` line; see <https://developercertificate.org/>.
