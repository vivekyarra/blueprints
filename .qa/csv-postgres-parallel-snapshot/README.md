# Bounded CSV snapshot loading: reproducible QA

The contribution branch adds one catalog YAML file. This QA branch contains the
same file plus local fixtures and recorded runtime evidence.

## Reproduce

Use a clean Docker context with the fixture names free. All passwords in these
scripts are disposable local fixture passwords.

```powershell
python -m pip install pyyaml requests
& ./.qa/csv-postgres-parallel-snapshot/start_runtime.ps1
# Wait for the Kestra API on http://localhost:18082.
python ./.qa/csv-postgres-parallel-snapshot/run_runtime.py
python ./.qa/csv-postgres-parallel-snapshot/verify_fidelity.py
python ./.qa/csv-postgres-parallel-snapshot/verify_readers.py
```

Services: Kestra 2.0.6, PostgreSQL 16, PostgreSQL plugin 2.4.0, CSV Serdes 2.2.1,
and an HTTP server serving actual CSV files to the Kestra worker. The flow itself
has no scripts or custom subflows. `application.yaml` sets the local test login.

## Runtime assertions

- CSV quoted commas, multiline fields and Unicode survive parsing, binary ION
  splitting, bounded Loop iterations and database publication.
- Eleven rows split into four chunks at three rows per chunk.
- Repeating a source publishes the same dataset, without appended duplicates.
- An injected constraint failure during publication rolls back the DELETE and
  preserves the previous live dataset hash. This tests a failure after staging,
  not just an early input check.
- Duplicate order IDs fail validation; invalid integer input and negative
  quantities fail staging. Each leaves the prior live snapshot unchanged.
- A header-only source is blocked by default; explicit empty approval clears the
  live snapshot. A later normal export restores it.
- 20,003 rows are loaded in five chunks with the source's fixed concurrency limit
  of three. Full-payload checks verify every name, price, quantity and total, plus
  the exact unique ID range.
- The unmodified public sample URL in the blueprint loads 100 rows in five chunks.

The main harness asserts 11 runtime scenarios. Two further runs check actual
stored field values and the large-load payload. Two more test a concurrent reader:
a fixture-only trigger pauses the INSERT for three seconds after the DELETE.
Fourteen reader samples observe only the old 11 rows or the new 20,003 rows.
Expected failure executions are
tests that pass only if the saved live dataset hash stays unchanged.

`*-execution.json`, `*-logs.json`, loop child logs and `*-outputs.json` retain the
runtime evidence. The deployed YAML omits only catalog `extend` metadata.

## Limits and recovery

This tests a local PostgreSQL server and HTTP source, not production TLS,
warehouse-size exports, or remote network faults. The default 5,000-row chunk is
tested; the 50,000-row input ceiling is not a capacity claim.

Use immutable full exports. This is a full replacement of a flow-owned table,
not incremental file ingestion. Typed constraints and row-count agreement do not
prove upstream business completeness. Add checks appropriate to your source.

Staging is retained by execution ID so failed chunks can be investigated and
reloaded. Database publication rechecks the row count inside its transaction.
Do not resume an older snapshot after a newer one has been published. Archive or
delete old staging rows after the required recovery window.
