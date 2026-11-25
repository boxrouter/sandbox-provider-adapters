<!-- SPDX-FileCopyrightText: Groundbox -->
<!-- SPDX-License-Identifier: PolyForm-Shield-1.0.0 -->

# Groundbox: an SPS 1.0 provider adapter

`sps_provider_groundbox` implements the ten required Sandbox Provider Spec 1.0 operations and
`cost` over [Groundbox](https://groundbox.dev)'s public API (`https://api.groundbox.dev/v1`,
[reference](https://api.groundbox.dev/openapi.json)). A sandbox is a Groundbox sandbox: a Linux
microVM with its own kernel, running Groundbox's Ubuntu 24.04 image as the user `runner`, who has
`sudo` without a password.

Its one credential field is `api_key`, a Groundbox API key with the scopes `sandboxes:read` and
`sandboxes:write`. The adapter calls `api.groundbox.dev` and nothing else, and depends on
`sandbox-provider-spec` and `httpx` only.

## How each operation maps

| Operation | Groundbox call |
| --- | --- |
| `discovery` | `GET /sandboxes?limit=1` proves the key; the facts are in `facts.py` |
| `create` | `POST /sandboxes` with Groundbox's own `Idempotency-Key`, then `GET /sandboxes/{id}` until it runs |
| `get` | `GET /sandboxes/{id}`. Groundbox keeps the row of an ended sandbox, so it reads `destroyed` |
| `list` | `GET /sandboxes?live=true&metadata[key]=value` |
| `destroy` | `DELETE /sandboxes/{id}` |
| `run` | `POST /sandboxes/{id}/exec` with `args`; a long stream is read again from `GET .../commands/{exec_id}/output` |
| `files.read` | `GET /sandboxes/{id}/files?path=`; a file over 64 MiB in slices |
| `files.write` | `PUT /sandboxes/{id}/files?path=`; a file over 16 MiB in parts |
| `process.start` | `POST /sandboxes/{id}/commands` |
| `process.output` | `GET /sandboxes/{id}/commands/{exec_id}/output?seq=` |
| `cost` | `GET /sandboxes/{id}/usage`, priced at the list price in discovery |

Labels are the sandbox's `metadata`, returned and filtered exactly. The adapter keeps no state and
writes nothing of its own on a sandbox.

## What to know

- **Sizes.** 1 to 16 vCPUs and any whole number of GB of memory from 1 to 4 per vCPU. Groundbox
  boots exactly the size asked for or refuses, so a create takes the smallest size that meets
  `cpu_min` and `memory_min_mib`; with no memory floor it gives 2 GB per vCPU. The default is
  2 vCPUs and 4 GB. `disk_min_mib` goes up to 100 GB.
- **Lifetime.** Groundbox counts whole minutes from the create. A lifetime between two minutes
  ends at the earlier one, so never later than was asked: 150 seconds is two minutes. The
  shortest is 60 seconds and the longest 30 days; discovery states both (`min_seconds`,
  `step_seconds`). A lifetime under 60 seconds, or none, is `unsupported`.
- **Before a card is on file** an account's sandboxes have at most 2 vCPUs, 4 GB and one hour, and
  two run at once. Groundbox answers a create past that with `quota_exceeded`.
- **Timeouts** of `run` are whole seconds: one between two seconds ends at the later one.
- **Large files.** Groundbox takes at most 32 MiB in one upload and serves at most 100 MB in one
  download, so a larger file moves in parts (`transfer.py`). A write uploads 16 MiB parts beside
  the destination, four at a time, and one command in the sandbox appends them to a temporary
  file and renames it over the destination: the destination is the whole file or is left as it
  was. A read copies one 64 MiB slice at a time to `/tmp` with `dd` and downloads it. Both need
  free disk for the file a second time (a write) or for one slice (a read), and both run their
  command through `sudo -n`. The adapter's own limit is 4 GiB.
- **Output.** `run` returns the first `output_limit_bytes` of each stream. The command's log keeps
  the newest 32 MiB of both streams together, so when a command writes more than that in all, the
  start is gone and the stream is marked truncated. A background process's output has the same
  window, and is cut to its last 64 KB an hour after the process ends; `process.output` reports
  what was dropped with `gap`.
- **Cost.** `GET /usage` is Groundbox's own record of the seconds a sandbox ran, readable after
  the sandbox has ended. The adapter multiplies them by the sandbox's vCPUs and the list price
  ($0.04 per vCPU-hour, by the second), so the amounts are before any plan, credit or discount.
  Memory, the disk of a running sandbox and egress are in that price. Measured on 2026-11-23, the
  record had its closing figure one second after a delete and after a timeout.
- **Errors.** Groundbox answers `{"error": {"code", "message"}}`; `_ERRORS` in `__init__.py` maps
  each code. An operation on an ended sandbox is `conflict` there and `not_found` here. A missing
  working directory is a bad request there; the adapter asks what the path is and answers
  `not_found`.

## Not claimed

No optional section is claimed yet. Groundbox has pause and resume, snapshots, forks, preview
URLs, a terminal over WebSocket, volumes, custom images from a registry or a Dockerfile, an
egress allowlist and hidden secrets, and the adapter can grow into them. Groundbox also runs
macOS and Windows machines for CI, which SPS 1.0 has no class for.

## Run the tester

Every command runs from this folder.

```console
$ export GROUNDBOX_API_KEY=...   # a key of a test account
$ uv run --group tools sandbox-provider-spec test groundbox --credential api_key=GROUNDBOX_API_KEY
```

A full run moves a 1 GiB file each way, waits for a final cost and waits for a 60-second lifetime
to end. Add `--quick` to skip those while you iterate.

## Offline tests

```console
$ uv run --locked pytest tests -q
```

They answer from an in-memory Groundbox behind `httpx.MockTransport` and need no account.
