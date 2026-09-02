# distributed-statistics

A multi-node, map-reduce style distributed application executed by the Loom v2
runtime. It splits an input dataset into partitions, fans the partitions out to
compute nodes running on **both** WorkspaceReplica Slaves (`slave-a` and
`slave-b`), and reduces the per-partition summaries into one aggregate result.

## Node roles

| Node | Role in this app | Hosted by |
| --- | --- | --- |
| `loom://orchestrate` | Parent node: reads the input, emits one summarize child per partition, awaits them, then emits a single merge child | Driver (Docker `orchestrator_python_v1`) |
| `summarize` | Map node: computes `count/sum/min/max/sumsq` for one partition | `slave-a` / `slave-b` (`subprocess_json_v1`) |
| `merge-summaries` | Reduce node: combines all summaries into `mean/stddev` | `slave-a` / `slave-b` (`subprocess_json_v1`) |

The Observer is the state authority, the Driver owns the orchestration, and
each Slave keeps its own PostgreSQL ledger. Content (schemas, programs, input,
results) lives only in MinIO/S3 and is referenced by immutable
`content://sha256/<digest>` references.

## Layout

- `io/*.schema.json` — JSON Schema 2020-12 subset documents for every node input/output.
- `programs/summarize.py` — map program (reads JSON from stdin, prints JSON).
- `programs/merge.py` — reduce program.
- `programs/orchestrate.py.tpl` — orchestration program template; the E2E harness
  substitutes the materialized package `ResourceRef`s before upload.
- `data/partitions/p1.json`…`p4.json` — 4 content-addressed partitions; the E2E harness
  uploads each, then binds a parent input `{"partitions": [ref…]}`. Expected aggregate:
  `count=12`, `sum=57`, `min=0`, `max=10`, `mean=4.75`, `stddev≈3.139`.
- `closure.json` — the high-level `ClosureContract` used to open the Run.

## Execution flow (end-to-end)

1. Upload io-contract documents and schemas via `loom_put_content`.
2. Materialize `summarize` and `merge-summaries` capability packages
   (`subprocess_json_v1`, operation `run_code`).
3. Materialize the `orchestrate` capability package
   (`orchestrator_python_v1`, `allowed_node_package_refs=[summarize, merge]`,
   `max_nodes=6`, `max_live_nodes=2`).
4. Open the Run, bind the execution payload, commit, start.
5. The Driver runs the orchestration program in Docker; it emits summarize nodes
   round-robin across `slave-a` and `slave-b` (2 live nodes max), then a merge
   node over the collected summaries.
6. Observer persists the terminal result as a content reference.

Run the live end-to-end test with:

```bash
.venv/bin/pytest -q tests/e2e/test_live_distributed_statistics.py -s
```

Requires the Compose stack to be up and the Observer reachable at
`http://localhost:18080` (see `scripts/test-e2e.sh`).
