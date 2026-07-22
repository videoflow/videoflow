# Toy Router

Partitioned parallelism, end to end: a deterministic stream of sensor events
flows through an async enrichment stage into a **replicated, stateful**
per-sensor counter — the combination that is only correct because the
counter's input edge is partitioned by key. No models, no video, no
dependencies beyond the videoflow base image.

```
events ──> enrich (async, stamps the key) ──> count (N partitioned replicas) ──> ledger
```

| Mechanism | Where it shows up |
|---|---|
| Partitioned routing (`partition_by` + `nb_tasks`) | `count` — each sensor id hashes to exactly one replica |
| `ctx.set_partition_key` | `enrich` stamps the sensor id onto its output's metadata (`_partition_key`); keys don't propagate on their own, so the node feeding the partitioned edge stamps them |
| `partition_by: trace_id` alternative | flip the config: totals stay right, but sensors spread across replicas — visible in `counts.json` |
| `async def process` | `enrich` awaits its simulated lookup on the task-owned event loop |
| Replica identity (`ctx.replica_id`, `ctx.logger`) | `count` tags every update with the replica that made it |
| Idempotent consumer | `ledger` is built with `idempotent=true` — effects dedup across redelivery when Redis is present |
| Indexed-Job workloads | on Kubernetes, a partitioned BATCH stage runs as an Indexed Job (one completion index per replica) |
| Prep hook | `prepare.py` walks the producer's PRNG and bakes expected totals |

## The self-checking artifact

`prepare.py` writes `expected_counts.json` (per-sensor totals for the seeded
stream). After end-of-stream the `ledger` consumer writes `counts.json`:

- `totals` — per-sensor counts, summed across replicas' shares;
- `replicas` — which counter replicas touched each sensor;
- `sticky` — `true` iff every sensor was owned by exactly one replica (what
  `_partition_key` routing guarantees and `trace_id` routing doesn't);
- `matches_expected` — whether `totals` equals the baked ground truth.

```bash
python -c "import json; c=json.load(open('out/counts.json')); print(c); assert c['matches_expected'] and c['sticky']"
```

(Drop the `c['sticky']` assertion when running with `partition_by: trace_id` —
spreading keys across replicas is that mode's expected behaviour.)

## Deploying to Kubernetes (one command)

```bash
pip install -e ".[deploy]"          # from the repo root
cd solutions/toy_router
videoflow deploy toy_router.py
```

That asks three questions, writes `config.yaml`, builds and loads the image,
runs `prepare.py` in-image, provisions a dev NATS+Redis, runs the flow to
completion, and tears everything down. `counts.json` lands in `out/` on this
machine. CPU only — there is no `gpu.Dockerfile` here on purpose.

## Run locally (no cluster)

```bash
cd solutions/toy_router
videoflow run-local toy_router.py
```

The graph needs only core videoflow — no `videoflow_contrib` packages, no
models, no footage. Or drive it manually against your own broker:

```bash
docker compose up -d nats redis            # NATS :4222, Redis :6379
export VIDEOFLOW_BLOB_REDIS_URL=redis://localhost:6379/0   # gives the idempotent sink its store

cd solutions/toy_router
cp config.example.yaml config.yaml
python prepare.py --config config.yaml
python toy_router.py --config config.yaml
```

## In the test suite

`tests/integration/test_toy_solutions.py` runs this solution end to end on
every CI build: it copies the solution to a temp directory, writes a small fast
config, drives it with `videoflow run-local`, and asserts `counts.json` reports
both `matches_expected` and `sticky` — the two claims partitioned routing makes.
It is the regression gate for `partition_by`, so keep the solution runnable with
a short stream.

## Configuration reference (`config.yaml`)

`videoflow deploy` asks for the starred (★) values; everything else has a
sensible default. Relative paths resolve against the config file's directory.

| Key | Default | Meaning |
|---|---|---|
| `work_dir` | `./out` | Where every artifact lands (`expected_counts.json`, `ledger.jsonl`, `counts.json`). Mounted read-write into the pods at the same absolute path. |
| `seed` | `7` | PRNG seed shared by the producer and `prepare.py`. |
| `events` ★ | `300` | Stream length; `sum(totals.values())` must equal it. |
| `sensors` | `6` | Distinct keys in the stream. |
| `rate_fps` | `100` | Producer pacing, messages/second (`<= 0` = unpaced). |
| `idempotent_sink` | `true` | Construct the ledger with `idempotent=True` (dedup across redelivery when Redis is available). |
| `enrich.threshold` | `50.0` | `high`/`low` bucket boundary. |
| `enrich.lookup_ms` | `1.0` | Simulated async lookup latency per message. |
| `counter.partitions` ★ | `3` | Replicas of the stateful counter stage. |
| `counter.partition_by` ★ | `_partition_key` | `_partition_key` = sticky per sensor; `trace_id` = per lineage (sensors spread across replicas; `sticky` goes `false`). |

The flow type is fixed to `batch`: the count verification depends on the
loss-free retention policy, and a realtime run may legitimately drop.

## Files

| File | Role |
|---|---|
| `toy_router.py` | Graph module: `build_flow(cfg=None)` plus a local `main()`. |
| `toy_router_nodes.py` | `iter_events` plus the four nodes in their own importable module, so distributed workers can reconstruct them by class path. |
| `common.py` | `load_config()` — resolves `work_dir` against the config file's directory. |
| `config.example.yaml` / `config.template.yaml` | Documented example / the template deploy asks questions from. |
| `prepare.py` | Idempotent ground-truth writer, run by deploy before compiling. |
| `Dockerfile` | CPU image on `videoflow-base:py3.12`, built from the repo root. No GPU variant — nothing here wants one. |
