# Kafka keyed state: reproducible QA

This directory tests the exact script in `flows/kafka-compacted-postgres-state.yaml`.
The contribution branch adds only that catalog file. This QA branch also contains
the fixtures, commands, execution records and downloaded output reports.

## Services

- Kestra 2.0.6, PostgreSQL plugin 2.4.0 and Python plugin 1.17.0.
- A real Confluent Platform 8.2.2 Kafka broker and PostgreSQL 16.
- Python 3.12, confluent-kafka 2.12.2, psycopg 3.2.12 and kestra 1.0.0.
- All database credentials are disposable local fixture credentials.

## Reproduce on Windows with Docker Desktop

From this repository checkout:

```powershell
python -m pip install pyyaml requests
& ./.qa/kafka-compacted-postgres-state/start_runtime.ps1
# Wait for Kafka and the Kestra API on localhost:18081.
docker exec kafka-state-client python /qa/test_materializer.py
python ./.qa/kafka-compacted-postgres-state/run_runtime.py
```

The startup script requires unused fixture container/network names. It does not
delete existing containers. `application.yaml` configures the local test login.
The broker has one node and plaintext listeners; this is a local test fixture.

## Assertions

The 16 integration methods use actual Kafka records and PostgreSQL transactions:

- Latest value, Kafka tombstones, re-creation after deletion, JSON null, Unicode,
  and an empty (but non-null) key.
- One-record batches with exact next offsets and no skips.
- Invalid JSON, non-finite numbers, a null key, and a PostgreSQL constraint error:
  valid earlier rows in the failed batch and read positions both roll back.
- A repeated failed batch still leaves the same durable state.
- A transaction lock excludes a second writer.
- Moved keys, changed partition count, wrong cluster ID, recreated topic ID,
  out-of-range offsets and missing positions all fail without changing values.
- Aborted Kafka transactions are excluded by read_committed.
- Kill the Python process while its second write waits on a real row lock.
  Its earlier write rolls back; the next run reads both records again.
- 3,500 deterministic random records over three partitions and 201 keys, loaded
  in batches of at most 333, match an independent latest-value oracle and exact
  per-partition offsets.

The Kestra harness deploys the flow with only `extend` removed. It asserts initial
load, idle rerun, update/delete, poison rollback, poison retry, three one-record
batches and the final idle run. It downloads execution records, logs and reports.
Expected failure executions are successful tests of the failure path.

## Evidence limits

These tests cover local plaintext Kafka/PostgreSQL, not TLS/SASL, remote network
failure or a production broker fleet. The 100,000-record input ceiling is a bound,
not a measured capacity result. They do not force the broker's log cleaner to
expire tombstones. Consumers must stay within Kafka's tombstone retention window.

Topic/cluster identity checks stop reuse after recreation; a changed stream needs
an empty destination. Restore rows and positions together. A database transaction
protects destination consistency; this does not claim a Kafka/PostgreSQL distributed
transaction or production-wide exactly-once delivery.
