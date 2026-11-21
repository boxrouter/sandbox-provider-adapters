# Changelog

## 0.3.1

- Vendor the SPS 0.5.62 wheel (the current release; adds `public_prefix` and the other optional parts since 0.5.57); `uv.lock` follows. The README now carries the template's sentence on `lifetime_enforced_by` and `ordered_sessions`. The adapter, fake backend, tests and smoke script are unchanged from the SPS template.

## 0.3.0

- Vendor the SPS 0.5.57 wheel (the current release); `uv.lock` follows. Refresh the adapter, `fake_cloud/`, tests and `scripts/smoke.py` from the SPS template: `cost` and `snapshot_cost` report usage (no prices in the adapter; discovery states the list prices as data), `cost` takes the `created_at` hint, and the READMEs name the optional additions the example does not claim (run as a user, exact output bytes, prepared images) and the class facts `lifetime_enforced_by` and `ordered_sessions`.
- The two files that stay under Apache-2.0 are unchanged from the template (see NOTICE).

## 0.2.0

- Vendor the SPS 0.5.33 wheel (the current release); `uv.lock` follows. Refresh the adapter, `fake_cloud/`, tests and `scripts/smoke.py` from the SPS template: snapshots, directory operations, a terminal, a final cost with calculated lines priced from discovery, and the `websockets` dependency. READMEs say how to run the tester (`--credential endpoint=…`, the `sps1.` token for a served adapter).
- The two files that stay under Apache-2.0 are unchanged from the template (see NOTICE).

## 0.1.3

- Licence is PolyForm Shield 1.0.0 (`LicenseRef-PolyForm-Shield-1.0.0` in the wheel metadata); source headers say `PolyForm-Shield-1.0.0`. Two files copied unchanged from the SPS template keep their template licence (see NOTICE).

## 0.1.2

- Refresh `fake_cloud/` from the SPS template: Example Cloud answers 404, not 500, for a container whose removal has begun.

## 0.1.1

- Vendor the SPS 0.3.206 wheel, which adds `sandbox-provider-spec test` for the live suite.

## 0.1.0

- First version: the template adapter over Example Cloud.
