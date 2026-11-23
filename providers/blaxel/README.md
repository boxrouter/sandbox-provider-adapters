# sps-provider-blaxel

Blaxel as a [Sandbox Provider Spec](../sandbox-provider-spec/README.md) 1.0 provider: one
`BlaxelProvider` class over Blaxel's HTTP API, using only `httpx`. It is laid out as a
`sandbox-provider-adapters` provider folder, so it can be published as `providers/blaxel/`.

This version serves the eleven required operations: `discovery`, `create`, `get`, `list`,
`destroy`, `run`, `files.read`, `files.write`, `process.start`, `process.output` and `cost`. It
claims two optional sections: `idle_shutdown` (`on_idle: paused`, Blaxel's own standby that `create`
meets) and `custom_images` with `source: provider_artifact` (an image the workspace already holds). It also serves processes with input and cancel (`process.write_stdin`,
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
| `run` | One `POST /process` with `waitForCompletion` and `timeout` on the sandbox's runtime host, for any timeout up to an hour; a command still running when Blaxel's gateway cuts the waited request (504, near 120 s) is read by its name to its end |
| `files.read`, `files.write` | `GET` / `PUT /filesystem/{path}` on the runtime host, streamed |
| `process.start`, `process.output` | `POST /process`; `GET /process/{name}`, whose record carries the state and the output (`/logs` only when the record may not hold all of it) |
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
| `pause` | `POST /standby?force=true` on the runtime host, then `GET /sandboxes/{name}` until `state` is `STANDBY` (a read per pause of 0.5 s doubling to 1 s, at most 60 s; the POST is sent once more after 8 s if no standby is seen); see below |
| `resume` | `POST /resume` on the runtime host, one call |
| `lifetime.extend` | `PUT /sandboxes/{name}` with a new `ttl` (the age so far plus the time asked, plus 5 s for the call); a sandbox with no ttl is returned unchanged |

**Terminals.** Blaxel's socket takes no program, environment or `TERM`, and closes with "Shell exited"
and no status. So `terminal.open` uploads a small launcher script (mode 0755, under `/tmp`) that sets
`TERM` and the environment, runs the argv (or `bash -l`) and writes its exit status to a file, and
names it as the socket's `shell`. It handles SIGHUP so that it reaps its program. Blaxel keeps the
shell after a client leaves, so closing a terminal whose program has not exited signals the
terminal's session: SIGHUP at once and SIGKILL five seconds later. Terminal input and output pass
through Blaxel as JSON text, so bytes that are not UTF-8 are replaced.

**Pause and resume (not claimed).** The package serves `pause` and `resume`, but `pause_resume` is not claimed since 2026-11-23: the calls are in the `sandbox-api` of 0.2.78 and later, and images prepared by this adapter carried 0.2.74 (every sandbox made from one answers 404 to `/standby`). Blaxel's standby freezes the guest (memory, processes and files stay; the clock
catches up on resume). `POST /standby` answers 202 before the standby, so `pause` reads the record's
`state` (not `status`, which stays `DEPLOYED`) until it is `STANDBY`. The host ignores a standby request
in the first seconds of a sandbox's life; such a sandbox still stands by on its own about 15 s after its
last request. A request to a sandbox already in standby wakes it, and the host does not put it back within its quiet window, so the request is sent once more if no standby is seen 8 s after it (only a STANDBY dated at or after that second request counts then; the record's `lastUsedAt` is the time of the last request to the host, in whole seconds, and a real standby carries its request's own second, so the comparison is `>=`. A record that already showed the resend's second while its state still said STANDBY from before the wake would be taken for the pause: narrow, never seen live): a pause of a young or already-paused sandbox can take 7 to 16 s (up to 22 s without the resend). The
record's `state` lags a resume by 3 to 5 s, so an object that sent a resume reads an earlier standby's
record as running (it forgets the resume once the record has caught up, and after a minute); a read from
another object reports Blaxel's `state` as it is. **Limit:** the ttl counts the standby
(`counts_paused_time` stays true), so a pause longer than the lifetime left ends with Blaxel destroying the
sandbox while Cloud still shows it paused.

**Why `pause_resume` is not claimed (measured 2026-11-23).** Pause and resume exist in `sandbox-api` from
0.2.78 and work through BoxRouter on the prepared image (sandbox-api 0.2.81: guest frozen, same PID, counter
stopped, two cycles; evidence in `sandbox-provider-spec/providers/blaxel/pause-20261123/`). The claim is
withheld because in 5 to 8 percent of pauses Blaxel's control-plane record never shows the standby that the
sandbox performed (or never shows a `state` at all), so a pause cannot be confirmed: the shipped code ended
unknown in 3 of about 65 pauses through Cloud (and 1 of 5 in the first proof run), a variant that sent one
request and waited for the sandbox's own idle standby in 2 of 25. The record differs between RUNNING and
STANDBY only in `state`, `lastUsedAt` and `expiresIn`, its `events` hold only create and deploy events, so
there is no second signal. Measured through Cloud: a pause takes 3 to 16 s (p50 about 11 s), a resume p50 0.2 s
and p95 3 to 7 s. The host ignores a standby request that comes less than 5 s after the sandbox's previous
request (a standard-image sandbox, one request, then `POST /standby` after the wait, took effect in: 0 of 4 at
0.5 s, 1 of 4 at 2 s, 2 of 4 at 3.5 s, 16 of 16 from 5 s).

**Which images have the calls.** `/standby` and `/resume` are served by the `sandbox-api` inside the
sandbox, from version 0.2.78 on. Blaxel's own base image has them. An image this adapter prepares
(`image.prepare`) copies `sandbox-api` in at the version of `dockerfile.py` (0.2.81; it was 0.2.74 until
0.3.6), and that binary stays in the image. A pause of a sandbox made from an image prepared before is
`unsupported` ("prepare the image again"; nothing is done to the sandbox) and a resume succeeds (Blaxel's
host wakes the sandbox on the request itself, then the old `sandbox-api` answers 404). A prepared copy is
identified by what the adapter adds to it: `BlaxelProvider.image_recipe_revision()` is a digest of the
Dockerfile tail (the pinned `sandbox-api` image and version and the template), so raising the pin makes the
copies that Cloud holds other copies and Cloud prepares new ones by itself. A customer's image, prepared by
`image.prepare` with an idempotency key, is prepared again with a new key (the same key returns the image
that exists).

**Directory operations.** Blaxel's file API cannot give the contract's results: its `HEAD` follows a
link and answers 200 for a path that is missing, and its listing gives no size for a directory. So
each operation runs `sh`, or for the listing a few lines of `python3`, as the default user (the base
image has both; BusyBox's `find` has no `-printf`). A custom image that lacks them answers
`unsupported`. A listing is read whole, sorted by name bytes and paged by cursor (the last name), and
a directory whose listing exceeds the output limit is a `provider_error`.

A command's timeout has a granularity of one second, because Blaxel's is in whole seconds.
`cost` of an ended sandbox is `final` 120 s after its end (`limits.cost_final_within_seconds`) once a
bucket has a cost: live through Cloud, every bucket was in the metrics 0 to 7 s after the destroy and
never changed. Once Blaxel has dropped the sandbox's record (it keeps it 16 minutes at least, often more than 50), the sum of
its buckets is `final` at once, and a sandbox with no bucket that has a cost is `not_found`. A 404
for a `cost` with the `created_at` hint is read as: ended and dropped, so the hint is given only for
a sandbox that has ended (Cloud reads costs from a sandbox's own end plus pricing's first read delay).
Blaxel does not report every sandbox: live through Cloud on 2026-11-23, 1 of 70 sandboxes (a 30 s life)
had no metrics row 13 minutes after its end, while the other 69, 3 s lives included, did; its cost
stays Unknown.

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
