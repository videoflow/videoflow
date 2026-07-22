# Toy Fusion

The REALTIME videoflow example: N simulated fixed-rate cameras plus a
simulated 100 Hz IMU, fused by **event time**. No models, no video, no
dependencies beyond the videoflow base image — the point is the part of the
framework the other solutions don't touch: unbounded live sources, the
drop-when-full REALTIME broker policy, and a time-mode `JoinPolicy` with
tolerance, lateness timeout, quorum and a collect window.

```
cam0 (10 fps, +0ms phase) ──┐
cam1 (10 fps, +3ms phase) ──┼─> fuse (time join) ──> live-report
imu  (100 Hz)  ─────────────┘
```

The cameras are *independent producers* — they share no upstream lineage, so a
trace join could never group them. Each stamps its messages with
`ctx.set_event_timestamp`; the fusion node groups frames whose timestamps fall
within `fusion.tolerance_ms`, waits at most `fusion.timeout_s` for stragglers,
emits with at least `fusion.quorum` cameras (missing ones arrive as `None`),
and receives every IMU sample within `fusion.sensor_window_ms` of the moment
as a list. `ctx.input_info` gives the fusion node each input's exact event
time, which is how the reported `spread_ms` is measured.

| Mechanism | Where it shows up |
|---|---|
| REALTIME flow type | `flow_type: realtime` — drop rather than stall |
| Unbounded producers (`is_finite=False`) | `duration_s: 0` — Deployments on k8s, run ends at teardown |
| Time-mode join (`tolerance_ms`) | `fuse` groups frames by capture time |
| Quorum emission | a moment emits with ≥ `quorum` cameras; gaps arrive as `None` |
| Collect parent | the IMU never gates a moment; samples arrive as a list |
| `ctx.set_event_timestamp` / `ctx.input_info` | producers stamp, fusion measures |
| Live sink | `live-report` atomically rewrites `latest.json` each moment |
| Optional prep hook | there is deliberately **no** `prepare.py` here |

## What "it works" looks like

REALTIME success is observed, not awaited:

- **While running**: `work_dir/latest.json` is atomically rewritten on every
  fused moment — watch its `moment` counter and mtime advance
  (`watch -n1 cat out/latest.json`).
- **After a bounded run** (`duration_s > 0`) or a stop: `fusion_summary.json`
  reports total moments and the complete/quorum split.

```bash
python -c "import json; s=json.load(open('out/fusion_summary.json')); print(s); assert s['moments'] > 0"
```

## Deploying to Kubernetes (one command)

```bash
pip install -e ".[deploy]"          # from the repo root
cd solutions/toy_fusion
videoflow deploy toy_fusion.py
```

With the default `duration_s: 0` this is a genuine REALTIME deploy: `deploy`
applies the manifests, checks schedulability for ~30 seconds and returns while
the flow keeps running. Watch `latest.json` advance, then end the run with the
`videoflow teardown` command deploy prints. CPU only — there is no
`gpu.Dockerfile` here on purpose.

## Run locally (no cluster)

```bash
cd solutions/toy_fusion
videoflow run-local toy_fusion.py
```

The graph needs only core videoflow — no `videoflow_contrib` packages, no
models, no footage. For a smoke run that ends on its own, set `duration_s: 20`
in `config.yaml`; with `0` the flow runs until Ctrl-C. Manual variant:

```bash
docker compose up -d nats redis            # NATS :4222, Redis :6379

cd solutions/toy_fusion
cp config.example.yaml config.yaml      # set duration_s: 20 for a bounded run
python toy_fusion.py --config config.yaml
```

## In the test suite

`tests/integration/test_toy_solutions.py` runs this solution end to end on
every CI build: it copies the solution to a temp directory, writes a config
with a small `duration_s` so the unbounded sources become a bounded run, drives
it with `videoflow run-local`, and asserts `fusion_summary.json` reports
complete moments carrying IMU samples. This is the only integration test that
exercises the REALTIME path with independent producers, so keep the solution
runnable with a short `duration_s`.

## Configuration reference (`config.yaml`)

`videoflow deploy` asks for the starred (★) values; everything else has a
sensible default. Relative paths resolve against the config file's directory.

| Key | Default | Meaning |
|---|---|---|
| `work_dir` | `./out` | Where `latest.json` and `fusion_summary.json` land. Mounted read-write into the pods at the same absolute path. |
| `cameras` ★ | `2` | Number of simulated cameras — and of camera producer pods. |
| `camera_fps` | `10` | Frame rate of every camera. |
| `phase_step_ms` | `3.0` | Camera i starts `i * phase_step_ms` late, so the cameras are genuinely unsynchronized. Keep well under `fusion.tolerance_ms`. |
| `sensor_hz` | `100` | IMU sample rate (the collect parent). |
| `duration_s` ★ | `0` | `0` = sources never stop (true realtime; end with teardown/Ctrl-C). `> 0` bounds a smoke run so it drains on its own. |
| `flow_type` | `realtime` | Drop-when-full. `batch` also works for bounded runs and then nothing may be dropped. |
| `fusion.tolerance_ms` | `15` | Frames within this window are one moment. Keep under one frame period and above `phase_step_ms * (cameras - 1)`. |
| `fusion.timeout_s` | `0.25` | Lateness bound before a moment emits with whoever arrived. |
| `fusion.quorum` | `1` | Minimum cameras for a timed-out moment to emit; missing cameras arrive as `None`. Between 1 and `cameras`. |
| `fusion.sensor_window_ms` | `40` | IMU samples this close to the moment are delivered as a list. |

## Files

| File | Role |
|---|---|
| `toy_fusion.py` | Graph module: `build_flow(cfg=None)` plus a local `main()`. |
| `toy_fusion_nodes.py` | Simulated sources, fusion and sink in their own importable module, so distributed workers can reconstruct them by class path. |
| `common.py` | `load_config()` — resolves `work_dir` against the config file's directory. |
| `config.example.yaml` / `config.template.yaml` | Documented example / the template deploy asks questions from. |
| `Dockerfile` | CPU image on `videoflow-base:py3.12`, built from the repo root. No GPU variant, and no `prepare.py` — both are optional and this solution needs neither. |
