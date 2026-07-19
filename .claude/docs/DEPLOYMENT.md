# Deployment

The `videoflow` CLI, the solution conventions it expects, and how a graph becomes Kubernetes
objects.

> Keep this file in sync with [`videoflow/deploy/cli.py`](../../videoflow/deploy/cli.py),
> [`solution.py`](../../videoflow/deploy/solution.py), and [`manifests.py`](../../videoflow/deploy/manifests.py).
> New CLI flags or solution conventions belong in `../../README.md` too, and solution-side changes
> usually need a matching update in `../videoflow-contrib`.

## Commands

| Command | Purpose |
|---|---|
| `videoflow deploy <graph.py[:factory]>` | The one-command path to a running flow on Kubernetes. |
| `videoflow run-local <graph.py>` | Local twin: same solution conventions, subprocess workers. |
| `videoflow explain <graph.py>` | Human-readable summary of the compiled graph. |
| `videoflow component validate\|push\|pull\|inspect` | Component descriptor tooling over OCI. |
| `videoflow provision` / `teardown` | Broker streams; usually run automatically by deploy. |
| `videoflow debug decode [--dlq]` | Decode wire envelopes, including from the dead-letter queue. |

Entry point is `videoflow.cli:main`; handlers are `_cmd_<name>` in
[`cli.py`](../../videoflow/deploy/cli.py).

## The graph module contract

A deployable graph module exposes **`build_flow() -> Flow`** (override with `path.py:factory`),
returning a *built* flow — it must **not** call `.run()`.

`load_flow` ([compile.py](../../videoflow/deploy/compile.py)) inserts the graph's directory into
`sys.path` and picks the module name carefully, because workers must be able to import the node
classes by their fully-qualified path. This is why the convention is to **define node classes in a
sibling `<name>_nodes.py`, not in the graph module itself** — a class defined in the graph module
gets a module path that may not resolve inside a worker.

## Solutions

A "solution" is a graph module plus sibling files. See
[`solution.py`](../../videoflow/deploy/solution.py) — its module docstring is the spec.

```
my_solution/
├── flow.py                 # build_flow()
├── my_solution_nodes.py    # node classes, importable by workers
├── config.template.yaml    # x-questions + x-mounts
├── prepare.py              # idempotent prep hook
├── requirements.txt
├── Dockerfile
└── gpu.Dockerfile          # exact filename — deploy looks for it
```

**`config.template.yaml`** is a valid config plus two extension blocks, both stripped when
`config.yaml` is written:

- **`x-questions`** — what `deploy` prompts for interactively when no config exists. Each entry is
  `{key, prompt, type, default, choices, item_key, item_value}`. `key` is a dotted path (digit
  segments index int-keyed maps). Types: `str` (default), `int`, `float`, `choice`, `path` (one
  path, validated and absolutized), `paths` (comma-separated, expanded into a mapping).
- **`x-mounts`** — path templates naming what must be hostPath-mounted into prep containers and
  worker pods: `'{cameras.*.video}:ro'` (dotted lookup into the resolved config, `*` fans out),
  `'{work_dir}'`, `'~/.videoflow:/root/.videoflow'`.

A bare path resolves to a **same-path** hostPath mount — host and container see the same absolute
path. That is not cosmetic: paths get baked into node params at compile time and must resolve
identically inside the pods. A `host:container` pair maps them explicitly.

**`prepare.py`** is an idempotent prep hook (model downloads, calibration) run with the solution
directory as cwd and `--config <path>`. It runs *inside the solution image* before compilation, so
its outputs are baked into the compiled specs.

## What `deploy` does

0. Ensure a config (interactive Q&A over `config.template.yaml`), resolve `x-mounts`.
1. Resolve or build the image (`--image` wins; else build from the solution's `Dockerfile` /
   `gpu.Dockerfile`, auto-building `videoflow-base` from a source checkout if missing).
2. Run `prepare.py` inside the image.
3. Compile — locally if the graph's dependencies import on the host, otherwise inside the image
   (specs round-trip as JSON, the same format as the specs ConfigMap).
4. Provision the broker, apply manifests.
5. For `BATCH`, wait for completion and tear down (`--keep` / `--keep-infra` to skip).

`--dry-run` / `--render-only` never touch the cluster. Other flags worth knowing:
`--image-override name=ref`, `--mount`, `--namespace`, `--autoscaling`, `--gpu-mode`,
`--strict-preflight`, `--envelope-version`, `--allow-pickle`.

`run-local` mirrors this: config → `prepare.py` on the host → start or reuse dev NATS/Redis
containers → `LocalProcessEngine` → report non-zero worker exits → tear down only what it started.

## Manifests

[`manifests.py`](../../videoflow/deploy/manifests.py) builds manifests as **plain dicts** and then
`yaml.dump`s them. Never introduce text templating here.

- `BATCH`: every node is a `Job`.
- `REALTIME`: a finite producer is a `Job`; everything else is a `Deployment`, or a `StatefulSet`
  when partitioned (replicas need stable identities to own partitions).
- Plus a per-node ConfigMap, a shared NATS-URL ConfigMap, a default-deny-except-broker
  NetworkPolicy, PodDisruptionBudgets, and optional KEDA `ScaledObject`s.
- Labels: `videoflow.io/flow-id`, `videoflow.io/run-id`, `videoflow.io/node`,
  `app.kubernetes.io/managed-by`. Teardown deletes by label selector.
- `k8s_name()` enforces DNS-1123 — node names flow into resource names.

## Images

[`images.py`](../../videoflow/deploy/images.py) resolves in strict order, and **raises rather than
guessing** if none apply:

1. `--image-override <name>=<ref>` for that node
2. the node's own `image =` constructor argument
3. the deploy-time default `--image`

## GPU

The contract a GPU node produces in its manifest: `resources.limits: {nvidia.com/gpu: N}`, a
`nodeSelector` on `videoflow.io/gpu-pool: "true"`, and a toleration for the `nvidia.com/gpu` taint.
Deploy preflights both the label and the taint — it warns but does not block.

`--gpu-mode shared` drops the resource limit for dev clusters using time-slicing.
`gpu_resource_name` (per node) or `--gpu-resource-name` (per deploy) targets MIG profiles or
renamed time-sliced resources. The full cluster-preparation walkthrough is in
[`README.md`](../../README.md).

## Infrastructure ownership

Both [`infra.py`](../../videoflow/deploy/infra.py) (in-cluster) and
[`localinfra.py`](../../videoflow/deploy/localinfra.py) (Docker) follow one rule: **a pre-existing
`nats`/`redis` service or container is reused and never torn down.** Only resources deploy created
are deleted. Preserve this when touching either module — the alternative is deleting someone's
shared broker.

[`cluster.py`](../../videoflow/deploy/cluster.py) detects the cluster flavor (`k3s`, `kind`, `minikube`,
`docker-desktop`, `generic-remote`) to decide how to load images and whether hostPath mounts are
viable.
