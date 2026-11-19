## Change

Provider folder and what changes:

## Checklist

- [ ] The change stays inside one `providers/<provider>/` folder.
- [ ] The folder's `version` is raised and `CHANGELOG.md` has a line for it (an endpoint folder has neither).
- [ ] `sandbox-provider-spec test` passes against the adapter or its URL (see CONTRIBUTING.md).
- [ ] Every host the adapter calls is in `api_domains`.
- [ ] No credential, token or account identifier is in the diff, a test or a log.
- [ ] Every commit has a `Signed-off-by:` line.
- [ ] I have read [Licence Of Your Contribution](https://github.com/boxrouter/sandbox-providers/blob/main/CONTRIBUTING.md#licence-of-your-contribution) and agree to it.
