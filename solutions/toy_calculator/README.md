# Toy Calculator

The smallest deployable videoflow solution: a BATCH diamond over a stream of
integers, built from core nodes plus five tiny glue nodes. No models, no video,
no dependencies beyond the videoflow base image — a full deploy costs seconds
of compute, which makes this the right first target when you want to prove the
*framework* path (config Q&A, prep hook, in-image compile, manifests, broker,
workers, teardown) before spending minutes on an ML solution.

It is also a readable map of videoflow's graph machinery:

```
numbers ─┬─> square (N replicas) ─┬─> pair ─┬─> stats ─┬─> report   (also joins pair)
         └─> delay ───────────────┘         │          └─> printer
                                            ├─> pairs-log
                                            └─> (pair is report's other parent)
square ──(metadata)──> proctimes
```

| Mechanism | Where it shows up |
|---|---|
| Fan-out | `numbers` feeds both branches; `pair` feeds three sinks |
| Competing replicas (`nb_tasks`) | `square` runs `square.workers` replicas, finishing out of order |
| Trace join + `JoinPolicy` | `pair` re-aligns the branches by lineage; policy from `join.*` config |
| Stateful aggregation (`OneTaskProcessorNode`) | `stats` keeps running totals |
| Consumer with two parents | `report` consumes `(pair, stats)` grouped by trace |
| `open()`/`close()` lifecycle | `report` writes its file in `close()`; `proctimes` opens/closes its handle |
| `metadata=True` consumer | `proctimes` receives per-message timing instead of payloads |
| Prep hook | `prepare.py` bakes the closed-form ground truth into `work_dir` |

## The self-checking artifact

`prepare.py` writes `expected.json` (closed-form `count`/`sum`/`sum_squares`/
`min`/`max` for the configured range). After end-of-stream, the `report`
consumer writes `report.json` including `"matches_expected": true|false|null`.
`true` means every integer crossed every edge exactly once and the join
re-aligned all of them — the whole point of a BATCH flow. Verify a run with:

```bash
python -c "import json; r=json.load(open('out/report.json')); print(r); assert r['matches_expected']"
```

## Deploying to Kubernetes (one command)

With a local cluster (k3s / kind / minikube / Docker Desktop) plus docker and
kubectl, the host needs only the videoflow CLI:

```bash
pip install -e ".[deploy]"          # from the repo root
cd solutions/toy_calculator
videoflow deploy toy_calculator.py
```

That asks three questions, writes `config.yaml`, builds and loads the image,
runs `prepare.py` in-image, provisions a dev NATS+Redis, runs the flow to
completion, and tears everything down. `report.json` lands in `out/` on this
machine. CPU only — there is no `gpu.Dockerfile` here on purpose.

## Run locally (no cluster)

```bash
cd solutions/toy_calculator
videoflow run-local toy_calculator.py
```

The graph needs only core videoflow — no `videoflow_contrib` packages, no
models, no footage. Or drive it manually against your own broker:

```bash
docker compose up -d nats redis            # NATS :4222, Redis :6379

cd solutions/toy_calculator
cp config.example.yaml config.yaml
python prepare.py --config config.yaml
python toy_calculator.py --config config.yaml
```

## In the test suite

`tests/integration/test_toy_solutions.py` runs this solution end to end on
every CI build: it copies the solution to a temp directory, writes a small fast
config, drives it with `videoflow run-local`, and asserts
`report.json["matches_expected"]` is `true`. Changing the graph, the nodes or
the config keys means updating that test — it is the regression gate that keeps
the framework path working, so keep the solution runnable with a short stream.

## Configuration reference (`config.yaml`)

`videoflow deploy` asks for the starred (★) values; everything else has a
sensible default. Relative paths resolve against the config file's directory.

| Key | Default | Meaning |
|---|---|---|
| `work_dir` | `./out` | Where every artifact lands (`expected.json`, `report.json`, `pairs.jsonl`, `proctimes.jsonl`). Mounted read-write into the pods at the same absolute path. |
| `start_value` | `1` | First integer of the stream. |
| `end_value` ★ | `200` | Last integer (inclusive); the report's `count` must equal `end_value - start_value + 1`. |
| `producer_fps` | `50` | Producer pacing, messages/second (`<= 0` = unpaced). |
| `delay_fps` | `40` | Pacing of the identity branch. Slightly slower than the producer, so the two arms of the diamond drift apart and the join visibly re-aligns them. |
| `flow_type` | `batch` | Keep `batch`: it is loss-free, so `matches_expected` must be `true`. `realtime` works too but may legitimately drop under pressure. |
| `square.workers` ★ | `2` | Competing replicas for the stateless square stage. |
| `join.missing` ★ | `wait` | `JoinPolicy` for the pair join: `wait` (batch default), or `drop`/`error` with `join.timeout_s` to see incomplete groups acked away or dead-lettered. |
| `join.timeout_s` | `null` | How long an incomplete join group waits before `join.missing` applies (`null` = forever). |
| `join.max_pending` | `100000` | Cap on buffered incomplete groups. Must exceed `(producer_fps - delay_fps) * stream duration`, since the unpaced square branch runs that far ahead — the policy's own 256 default would silently evict pairs. |

## Files

| File | Role |
|---|---|
| `toy_calculator.py` | Graph module: `build_flow(cfg=None)` plus a local `main()`. |
| `toy_calculator_nodes.py` | The five glue nodes in their own importable module, so distributed workers can reconstruct them by class path. |
| `common.py` | `load_config()` — resolves `work_dir` against the config file's directory. |
| `config.example.yaml` / `config.template.yaml` | Documented example / the template deploy asks questions from. |
| `prepare.py` | Idempotent ground-truth writer, run by deploy before compiling. |
| `Dockerfile` | CPU image on `videoflow-base:py3.12`, built from the repo root. No GPU variant — nothing here wants one. |
