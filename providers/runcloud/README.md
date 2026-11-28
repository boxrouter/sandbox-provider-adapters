# run.cloud

An SPS 1.0 adapter for [run.cloud](https://run.cloud) sandboxes: Firecracker microVMs with a
40 GiB disk, root by default, and full outbound network. The adapter calls the run.cloud REST API
(`https://api.run.cloud/run-cloud/sandboxes`, documented at <https://docs.run.cloud/sandboxes>)
with the account's API key, and nothing else.

## How it maps

| SPS | run.cloud |
| --- | --- |
| `create` | `POST /run-cloud/sandboxes` with `Idempotency-Key`, then `GET` until `running`. The lifetime is `timeoutSeconds` with `onTimeout: "destroy"`; idle pausing is off. A digest of the request is the sandbox `name`, so a repeated key with another body is `conflict`. |
| labels, `list` | run.cloud tags, filtered server-side with `?tag=key:value` |
| `get`, `destroy` | `GET` and `DELETE /run-cloud/sandboxes/{id}` |
| `run`, `process.start`, `process.output` | `POST .../exec` of a small supervisor (`guest.py`) that starts the argv in its own session, writes its output and exit status under `/tmp/.sps/`, and returns bytes from given offsets. A long command or process therefore never holds one HTTP request open, and output need not be UTF-8. At a `run` timeout the supervisor kills the command's whole session. |
| `files.read`, `files.write` | `POST .../fs` in 512 KiB chunks; a write creates missing parent directories |
| `cost` | `GET .../usage` (billed seconds) and the sandbox's CPU counter, reported as `usage` quantities (`vcpu_second`, `gib_second`) for the list prices in discovery |

Prices are run.cloud's list prices: $0.00000246 per vCPU-second, charged on the larger of the
reservation and the CPU used, and $0.00000083175 per GiB-second of reserved memory. Paused time is
free.

Not claimed in this version: pause and resume, snapshots, previews (run.cloud tunnels), terminals,
secrets, a named user, and stdin for processes. Each answers `unsupported`.

## Run the tester

```console
$ export RUNCLOUD_API_KEY=...            # an API key from the run.cloud dashboard
$ uv run --group tools sandbox-provider-spec test runcloud --credential api_key=RUNCLOUD_API_KEY
```

Add `--credential endpoint=RUNCLOUD_ENDPOINT` to test another run.cloud API host listed in
`api_domains`.

## Offline tests

```console
$ uv run --locked pytest tests -q
```

They answer the run.cloud routes from memory through `httpx.MockTransport`, and run the
supervisor script itself in a local Python process.
