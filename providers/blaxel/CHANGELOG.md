# Changelog

## 0.3.0

- Claimed after the live tester passed them (SPS 0.5.43): `user_selection` (commands, files_write,
  process_start), private previews (credential `link` or `header`, revocable) beside public ones,
  `processes` `reattach`, and `previews` `max_ports`. A prepared image is ready only once Blaxel
  lists its tag, each build carries a label with its own name (Blaxel reuses an identical build and
  then names no tag), and deleting an image that a sandbox still uses is accepted and swept by the
  next `image.prepare`. `hidden_secrets` (Blaxel's proxy routes: sets a header, all HTTPS) is claimed too, after `hidden_secrets.injected` and `.unreadable` passed against the receiver fixtures.
  No call that changes something is sent twice inside the adapter.

- The 1.0 additions that Blaxel serves, each claimed in discovery after the live tester passes it:
  - `user` on `run`, `process.start` and `files.write` (`user_selection`): a command runs through a
    `setpriv`-or-`su` switch after the adapter has found that the user exists; a file is copied
    into place by the user, so they own it and are refused where they may not write.
  - `run` with `output: base64` (`run.base64`): exact bytes through a wrapper that keeps each
    stream in a file and reads it back as base64 (or from the file above 32 KiB).
  - Prepared images (`image.prepare`, `image.get`, `image.list`, `image.delete`, `custom_images`
    `prepared` for `oci` and `dockerfile` with a build context): a Blaxel upload build whose
    sandbox is kept as the image's record, which holds its labels and request fingerprint. A failed
    build carries `failed_step` when a command of the customer's own Dockerfile failed.
  - Private, expiring and revocable previews (`preview.revoke`, `previews` `access`, `credential:
    header`, `revocable`).
  - `labels.update`, `Sandbox.times` (from the record's `events[]`), `secrets` on create
    (`hidden_secrets`, Blaxel's proxy routes), `account.cost` and the `created_at` hint of `cost`.

## 0.2.0

- The optional 1.0 sections that Blaxel serves: `process.write_stdin`, `process.close_stdin` and
  `process.cancel` (`processes` with `input` and `cancel`), `preview.link` (`previews`, public
  only, port 8080 reserved), `terminal.open` (`terminal`, through a launcher script that records
  the exit status), `files.list`, `files.mkdir`, `files.remove` and `files.exists`, and
  `lifetime.extend`. Each is claimed in discovery only after the live tester passes it.
- `create` accepts the `processes`, `previews` and `terminal` requirements.
- Depends on `websockets`.
- Not ported: Blaxel's archive (pause) and its snapshots, by decision.

## 0.1.0

- First version: the eleven required operations over raw httpx, with `idle_shutdown` and
  `custom_images` (`provider_artifact`). Ported from Cloud's closed Blaxel adapter, without
  BoxRouter's machinery (no Core or Kit import, no SDK, no environment reads).
