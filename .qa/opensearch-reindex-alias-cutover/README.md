# OpenSearch mapping migration: reproducible QA

The contribution branch adds one flow. This QA branch includes the same submitted
flow, local setup, real service fixtures, execution records and API observations.

## Reproduce

Use a clean Docker context with fixture names and ports free:

```powershell
python -m pip install pyyaml requests
& ./.qa/opensearch-reindex-alias-cutover/start_runtime.ps1
# Wait for localhost:19200 and localhost:18083.
python ./.qa/opensearch-reindex-alias-cutover/run_runtime.py
python ./.qa/opensearch-reindex-alias-cutover/verify_behavior.py
```

The services are OpenSearch 2.19.3, Kestra 2.0.6 and the OpenSearch plugin 1.6.2.
Source indexes have two primary shards; candidates have one. Fixtures use zero
replicas on one node. The blueprint defaults to one replica. Security is disabled
only in this local OpenSearch fixture; this does not test TLS or authentication.

## Checks

The harness creates real indexes, indexes JSON documents, blocks writes, and
deploys the catalog YAML with only `extend` removed. All flow tasks are native
OpenSearch or core tasks; the fixture scripts are outside the contribution diff.

- Reindex with the desired mappings, wait for the actual asynchronous task,
  check item results and count, run a search canary, then verify the serving alias.
- Compare every document ID and complete `_source` value in source and candidate
  with an independent expected dictionary, using paginated scroll reads.
- Confirm the candidate is write-blocked and rejects a real PUT with HTTP 403.
- Use the retained source for an actual atomic alias rollback and check all data.
- Reject an unblocked source and an empty source before candidate creation.
- Reject filtered and multi-target aliases, preserving their exact definitions.
- Reject a canary with no matches, a too-high minimum, and a strict mapping that
  cannot accept source documents. The old alias and source data stay unchanged.
- Copy 10,003 documents across a two-shard to one-shard mapping migration and
  compare full data, not only the count. Retain the expected-corpus hash.
- Reject wildcard index selectors and alias paths before any OpenSearch call.
- Move the alias from a second thread after candidate creation. The flow detects
  the changed alias and preserves that external change instead of promoting.
- Exercise an atomic aliases request with a missing required alias removal; the
  failed operation does not apply its accompanying add action.
- Trigger the actual keyed webhook with the default inputs and replica count,
  verify full document contents, and reject a replay that still names the old
  source. JSON defaults are tested through both manual and webhook execution.

`*-execution.json`, `*-logs.json`, `*-outputs.json` and `runtime-results.jsonl`
record the observed executions. Expected failure cases pass only when the serving
alias definition and all source documents match their pre-execution values.

## Limits

This covers a local single-node cluster and plaintext requests. It does not
benchmark production reindex throughput, a large cluster, TLS, or authenticated
authorization failures. A source write block and exclusive control of its alias
are prerequisites. The workflow does not freeze writers itself or guarantee
consistency if another operator changes the alias after the final preflight.

Candidate and source indexes remain for inspection. A ten-minute timeout stops
the flow before promotion but does not cancel the cluster's reindex task. Inspect
the saved task ID before cancelling or restarting. An uncertain alias-API network
outcome also needs inspection before a new run.
