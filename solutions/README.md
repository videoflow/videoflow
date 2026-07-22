# Solutions

Complete, deployable videoflow applications built from **core nodes only** — no
models, no footage, no `videoflow_contrib` packages, and no dependencies beyond
the videoflow base image. Each is a real solution in the full sense (`build_flow`
factory, config template, prep hook, Dockerfile) that `videoflow deploy` will
take to a Kubernetes cluster in one command.

They exist to be read and to be run. A full deploy costs seconds of compute
rather than the minutes an ML solution costs, which makes them the right first
target when you want to prove the *framework* path — config Q&A, prep hook,
in-image compile, manifests, broker, workers, teardown — before spending real
time on a model.

| Solution | Flow type | What it demonstrates |
|---|---|---|
| [toy_calculator](toy_calculator) | BATCH | A diamond over a stream of integers: fan-out, a trace join re-aligning two branches, competing replicas, stateful aggregation, a two-parent consumer, a `metadata=True` consumer, and a prep hook. The smallest complete solution — **read this one first**. |
| [toy_router](toy_router) | BATCH | Partitioned parallelism: `partition_by` routing that pins each key to one replica, `ctx.set_partition_key`, an `async def process` node, replica identity, and an idempotent sink. |
| [toy_fusion](toy_fusion) | REALTIME | Independent producers fused by **event time**: time-mode `JoinPolicy` with tolerance, lateness timeout, quorum and a collect window; unbounded live sources. |

## Running one

```bash
docker compose up -d nats redis      # a broker to talk to (from the repo root)
cd solutions/toy_calculator
videoflow run-local toy_calculator.py
```

`run-local` generates `config.yaml` from `config.template.yaml` the first time
(it asks a few questions), runs `prepare.py`, and spawns one worker subprocess
per node. To deploy the same graph to a cluster instead, swap in
`videoflow deploy toy_calculator.py`. Each solution's README documents every
config key.

## They are also the end-to-end test suite

`tests/integration/test_toy_solutions.py` runs all three on every CI build and
asserts their self-checking artifacts — `report.json`'s `matches_expected`,
`counts.json`'s `matches_expected` and `sticky`, `fusion_summary.json`'s
complete moments. A green run means the distributed path computed the right
answer, not merely that nothing crashed.

So these solutions carry two responsibilities at once. If you change a graph, a
node or a config key, update the solution's `README.md`, its
`config.example.yaml` and `config.template.yaml`, **and** the matching config
dict in the test. And keep the streams short — the whole suite should stay
around 25 seconds.
