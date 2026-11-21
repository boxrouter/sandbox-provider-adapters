# sps-provider-blaxel

Blaxel as a [Sandbox Provider Spec](../sandbox-provider-spec/README.md) 1.0 provider: one
`BlaxelProvider` class over Blaxel's HTTP API, using only `httpx`. It is laid out as a
`sandbox-provider-adapters` provider folder, so it can be published as `providers/blaxel/`.

This version serves the eleven required operations: `discovery`, `create`, `get`, `list`,
`destroy`, `run`, `files.read`, `files.write`, `process.start`, `process.output` and `cost`. It
claims two optional sections that `create` meets: `idle_shutdown` (`on_idle: paused`, Blaxel's own
standby) and `custom_images` with `source: provider_artifact` (an image the workspace already
holds). It also serves processes with input and cancel (`process.write_stdin`,
`process.close_stdin`, `process.cancel`), public previews (`preview.link`), terminals
(`terminal.open`), the four directory operations (`files.list`, `files.mkdir`, `files.remove`,
`files.exists`) and `lifetime.extend`. It also serves the 1.0 additions: commands, process starts
and file writes as a named user (`user_selection`), exact output bytes (`run.base64`), prepared images
(`image.*`), private and revocable previews (`preview.revoke`), `labels.update`, provider times on
`get`, secrets on create (`hidden_secrets`), `account.cost` and the `created_at` hint of `cost`.

## Credentials

| Field | Secret | Description |
| --- | --- | --- |
| `api_key` | yes | API key from the Blaxel console |

The adapter finds the workspace the key reaches with `GET /workspaces`; a key that reaches none
or several is refused, so use a key limited to one workspace. The adapter connects only to
`api.blaxel.ai` and to the `*.bl.run` host that Blaxel names in a sandbox's `metadata.url`, and
refuses any other host before it sends the key.

## How it maps

| Operation | Blaxel call |
| --- | --- |
| `discovery` | `GET /locations`; sizes and limits are in `facts.py` |
| `create` | `POST /sandboxes`; the name is a digest of the `Idempotency-Key`, so a replay is Blaxel's `409 SANDBOX_ALREADY_EXISTS`, and the request's fingerprint is the display name |
| `get`, `list` | `GET /sandboxes/{name}`, `GET /sandboxes` (labels are matched in the adapter) |
| `destroy` | `DELETE /sandboxes/{name}` |
| `run` | `POST /process` with `waitForCompletion` and `timeout` on the sandbox's runtime host; a command over 90 s starts detached and is polled |
| `files.read`, `files.write` | `GET` / `PUT /filesystem/{path}` on the runtime host, streamed |
| `process.start`, `process.output` | `POST /process`; `GET /process/{name}` and `/logs` |
| `cost` | `GET /accounts/{id}/metrics`, hourly buckets of the sandbox, `source: reported` |
| `process.write_stdin`, `process.close_stdin` | `POST` / `DELETE /process/{name}/stdin` (the process starts with `stdin: true`) |
| `process.cancel` | `DELETE /process/{name}/kill`; a process that has ended counts as cancelled |
| `run` with `user` or `output: base64` | the same `POST /process`, with the command inside a wrapper (`guest.py`) that switches user and returns each stream's exact bytes |
| `files.write` with `user` | a staging upload, then a copy made by the user |
| `image.prepare` | `PUT` or `POST /sandboxes/{name}?upload=true`, then the build's zip to the presigned address it answers with; the sandbox stays as the image's record (labels, fingerprint) |
| `image.get`, `image.list`, `image.delete` | the record `GET /sandboxes/{name}` and the image `GET /images/sandbox/{name}`; delete removes both |
| `preview.link` with `access` | `POST /sandboxes/{name}/previews` (public or private), and for a private one a token (`.../tokens`, sent as `X-Blaxel-Preview-Token`) |
| `preview.revoke` | `DELETE` of the token, or of the public preview |
| `labels.update` | `GET`, then `PUT /sandboxes/{name}` with the merged labels |
| `account.cost` | `GET /accounts/{id}/metrics` with no resource filter, summed by hour |
| `preview.link` | `POST /sandboxes/{name}/previews`, one public preview per port (`sps-<port>-public`); port 8080 is Blaxel's own and is refused |
| `terminal.open` | `wss://<runtime host>/terminal/ws`, with the key in headers; see below |
| `files.list`, `files.mkdir`, `files.remove`, `files.exists` | one short command in the sandbox each, see below |
| `lifetime.extend` | `PUT /sandboxes/{name}` with a new `ttl` (the age so far plus the time asked, plus 5 s for the call); a sandbox with no ttl is returned unchanged |

**Terminals.** Blaxel's socket takes no program, environment or `TERM`, and closes with "Shell exited"
and no status. So `terminal.open` uploads a small launcher script (mode 0755, under `/tmp`) that sets
`TERM` and the environment, runs the argv (or `bash -l`) and writes its exit status to a file, and
names it as the socket's `shell`. It handles SIGHUP so that it reaps its program. Blaxel keeps the
shell after a client leaves, so closing a terminal whose program has not exited signals the
terminal's session: SIGHUP at once and SIGKILL five seconds later. Terminal input and output pass
through Blaxel as JSON text, so bytes that are not UTF-8 are replaced.

**Directory operations.** Blaxel's file API cannot give the contract's results: its `HEAD` follows a
link and answers 200 for a path that is missing, and its listing gives no size for a directory. So
each operation runs `sh`, or for the listing a few lines of `python3`, as the default user (the base
image has both; BusyBox's `find` has no `-printf`). A custom image that lacks them answers
`unsupported`. A listing is read whole, sorted by name bytes and paged by cursor (the last name), and
a directory whose listing exceeds the output limit is a `provider_error`.

A command's timeout has a granularity of one second, because Blaxel's is in whole seconds.
`cost` is `accruing` until Blaxel's metrics hold the hour that the sandbox ended in and 150 s have
passed since it ended (Blaxel's cost was final 45 to 113 s after the end in our measurements),
then `final`.

## Test it

```sh
BL_API_KEY=... uv run sandbox-provider-spec test blaxel --credential api_key=BL_API_KEY
```

The tests create billable sandboxes in the workspace and delete them. Pass
`--reachable-url https://www.iana.org/` for the outbound-network test. `reports/tester-report.json`
is the last report (46 tests passed; `contract.lifetime_ends` is `unsupported`, because it asks for a
60 s lifetime and Blaxel's least is 300 s; `previews.credential_scope` is `unsupported`, because
private previews are not claimed yet; the two section tests need fixtures that the tester does not
carry, so they are `not_run`, and so is the HTTP-only `terminal.v1.link_single_use`).
`reports/tester-report-lifetime-extend.json` is the slow `facts.extendable_lifetime` test, which the
default set leaves out and `reports/discovery.json` is the discovery snapshot that Cloud
reads.

## Layout

- `src/sps_provider_blaxel/sps-provider.yaml`: the manifest.
- `facts.py`: sizes, regions, limits and the discovery builder.
- `create_spec.py`: a `CreateRequest` into Blaxel's create body.
- `wire.py`: Blaxel's timestamps, statuses and error replies.
- `terminal.py`: the launcher script and the `Terminal` over Blaxel's socket.
- `directories.py`: the four directory operations' programs, and the listing's paging.
- `tests/`: offline tests over `httpx.MockTransport`.
