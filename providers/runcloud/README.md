# run.cloud

An SPS Provider API 1.1 adapter for [run.cloud](https://run.cloud) sandboxes: Firecracker microVMs with a
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
| `cost` | `GET .../cost`: run.cloud's billed record, the nanodollar amounts it reports to Stripe, per meter (`cpu`, `cpu_burst`, `memory`), as `reported` costs. It is `accruing` while the sandbox runs and `final` within 10 minutes of its end, and stays readable for 30 days. On a 404 from `/cost` the adapter falls back to `GET .../usage` (see Cost below). |

Prices are run.cloud's list prices: $0.00000246 per vCPU-second, charged on the larger of the
reservation and the CPU used, and $0.00000083175 per GiB-second of reserved memory. Paused time is
free.

Not claimed in this version: pause and resume, snapshots, previews (run.cloud tunnels), terminals,
secrets, a named user, and stdin for processes. Each answers `unsupported`.

## Cost

`cost` reads `GET /run-cloud/sandboxes/{id}/cost`. Its amounts are the ones run.cloud invoices,
split per meter, so `source` is `reported`.

The adapter falls back to `GET .../usage` only when `/cost` answers 404, which an API host
without the route does. Its billed seconds come from the same run.cloud usage counter that the
invoice is metered from. The fallback reports `usage` quantities, not amounts:

- vCPU-seconds: the larger of the reservation times those seconds and the sandbox's CPU counter
- GiB-seconds: reserved memory times those seconds, plus any memory used above the reservation

At the list prices in discovery, these approximate the invoice. The exact amounts are the ones
`/cost` gives.

## What runs inside the sandbox

`run` and the process operations exec `python3 -c SUPERVISOR` (`guest.py`). The script uses only
the Python standard library and the `python3` of the `runcloud/agent-base` image. It writes only
under `/tmp/.sps/`, reads `/proc` to find the processes of the session it started, and signals
only that session. It makes no network calls. Nothing outside the sandbox is touched.

## Run the tester

```console
$ export RUNCLOUD_API_KEY=...            # an API key from the run.cloud dashboard
$ uv run --group tools sandbox-provider-spec test runcloud --credential api_key=RUNCLOUD_API_KEY
```

## Offline tests

```console
$ uv run --locked pytest tests -q
```

They answer the run.cloud routes from memory through `httpx.MockTransport`, and run the
supervisor script itself in a local Python process.
