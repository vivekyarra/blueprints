# PostgreSQL CDC cutover parity audit: verification

Source: `flows/postgres-cdc-cutover-parity-audit.yaml`, contribution commit
`059ab9effa49bd1de20d817646abd58f7dea0000`.

- Published YAML SHA-256 (LF): `a51da0b6dfc9ff6c95aeebfb83d1dfffbeb78576ea0a956f0bd7921d1bc69c54`
- Tested Windows checkout SHA-256 (CRLF): `9e2eb05942279277383c909577fe6424e4f3584fca3302d862c25433fd120439`
- Embedded Python SHA-256: `5e81f32ccbba0466ca89e51a27fbca5a90d638c2eeb04bf8ec828c61f872f93f`

Test date: October 17, 2026. The upstream PR contains only the blueprint; this
QA branch carries the executable checks and fixture evidence.

## Environment

- Kestra 2.0.6, image `kestra/kestra@sha256:82f7c84f00fb883a6ff93c6d07e31f30eaabd0c84d229fb56a80edeeecd8dbba`.
- PostgreSQL 16.15 on two separate Docker servers, image `postgres@sha256:ca0bd484cb98bf4b24eb1010e73fb3fcbd6714d240fbc1a10eea5b7dbecb641d`.
- PostgreSQL JDBC 2.4.0, Python Script 1.17.0; Debezium PostgreSQL 3.0.1 for the CDC producer fixture.
- Python Docker image pinned in the blueprint to `python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`.
- Dedicated SELECT-only audit users. Both rejected an UPDATE even after an explicit `BEGIN READ WRITE`; see `readonly-results.json`.

The source and replica tables use matching integer keys and column types.
Fixtures contain nulls, Unicode, timestamps and TOAST-sized text. None of the
customer data comes from a real business database.

## What was exercised

`test_comparator.py` extracts the submitted `rows` and `compare` functions via
Python AST. Fifteen unittest methods include 500 seeded randomized comparisons
against an independent dictionary oracle, strict key ordering/shape checks,
bounded samples, and a lazy generator that must stop at the row limit.

`run_runtime.py` deploys the submitted flow after removing **only** catalog
`extend` metadata, which is not part of Kestra's runtime schema. It asserts the
terminal execution state, exact failing task, downloaded JSON counts, report
sample limit and absence of fixture business values. It checks parity, missing,
extra and changed rows, tombstones, empty tables with/without permission, invalid
metadata, row/input limits, 10,000 changed rows with a three-example cap, duplicate
view keys, permission denial, and recovery. See `verified-results.json` and the
corresponding execution, output and report JSON files.

`run_identity.py` deliberately connects Python's replica session to the source
database. The embedded script is unchanged. It must reject the identical database
identity; the correct flow configuration is restored afterward.

`run_cdc.py` uses the merged Debezium replica blueprint as a producer fixture.
Only fixture wiring changes: test credentials, database/topic name, trigger ID,
poll interval, and omission of the external Slack error handler. Its capture,
staging and merge tasks process a real PostgreSQL snapshot and subsequent INSERT,
UPDATE and DELETE. The audit then compares that CDC-produced replica using its
SELECT-only credentials. This is separate from the deliberately seeded drift
matrix. See `cdc-fixture.yaml`, `cdc-executions.json`, `cdc-applied-events.json`, and
`debezium-*-report.json`.

## Reproduce

Use a clean Docker context with sufficient memory and no containers named
`parity-source`, `parity-replica` or `parity-kestra`. The scripts reset only these
disposable fixtures. Run from a checkout of this QA branch:

```powershell
python -m pip install PyYAML
$qa = '.qa/postgres-cdc-cutover-parity-audit'
python "$qa/test_comparator.py"
python "$qa/validate_flow.py"
& "$qa/start_runtime.ps1"
# Wait for the Kestra API at localhost:18080 to become available.
python "$qa/run_runtime.py"
python "$qa/run_identity.py"
python "$qa/run_cdc.py"
python "$qa/capture_cdc_evidence.py"
python "$qa/verify_readonly.py"
python "$qa/record_verified.py"
```

The bootstrap generates local `runtime.env` and `application.yaml` files. Their
credentials are disposable fixture values, not external service credentials.
The Docker socket is mounted only to this local QA Kestra so its Python task can
launch the configured Docker runner. Tests use the real database, task runner,
Kestra outputs protocol, artifact capture and fail tasks.

## Limits

The repository's exact `Validate Blueprint Tags` shell step also passed locally
in Linux for all 1,961 blueprints. See `validate-tags-ci.sh` and
`catalog-tags-ci.log`; `run-tags-ci.sh` installs the pinned YAML parser and runs
that unchanged validator from the repository root. The GitHub workflow itself
still needs maintainer authorization; a local run does not approve it.

Independent PostgreSQL snapshots are not atomic. The operator must actually pause
source writes and drain CDC; `cutover_ready` records acknowledgement and cannot
prove either condition. Keep writes paused until cutover completes.

This verification uses local plaintext connections. It does not validate a
production TLS/CA setup, cloud network, failover, or the five-million-row default
capacity. Memory scales with the cursor batch and row width, rather than total
table size. Query errors fail the execution without emitting a PASS report.

Docker Desktop restarted during an earlier attempt. Those interrupted executions
are excluded from `verified-results.json`; the complete fault matrix was rerun
on a fresh isolated Kestra instance. The old instance was retained locally for
diagnosis. No existing Debezium deployment or business database was modified.
