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
5. For `BATCH`, wait for completion and tear down (`--keep` / `--keep-infra` to skip). For
   `REALTIME`, run a bounded rollout check (`rollout_report` in
   [engines/kubernetes.py](../../videoflow/engines/kubernetes.py)): wait until every pod is Ready
   (`open()` completed) — early-exiting on a confirmed failure (crash-loop, OOM kill, image-pull
   failure, or unschedulable past a grace period), in which case deploy dumps the pod logs and
   exits non-zero, leaving the flow running for inspection. The deadline is derived from the
   startup-probe window (~150s) so a probe kill of a slow `open()` is observable; a pod merely
   still loading at the deadline is a warning, not a failure.

A failing pod reports its *own* cause where it can. Workers write a structured
reason to `/dev/termination-log`, which the API server surfaces in
`containerStatuses[].lastState.terminated.message`, and `_failure_detail` prefers
it over anything it could infer — so the abort says `VF_DEVICE: CUDA out of
memory — lower the batch size` rather than "crash-looping, see the logs". The pod
spec sets `terminationMessagePolicy: FallbackToLogsOnError` so a death too abrupt
to write anything still surfaces its last log lines the same way.

`--dry-run` / `--render-only` never touch the cluster. Other flags worth knowing:
`--image-override name=ref`, `--mount`, `--namespace`, `--autoscaling`, `--gpu-mode`,
`--strict-preflight`, `--envelope-version`, `--image-pull-policy`.

**Exit codes are typed** (`videoflow.core.errors`): 2 the flow/config, 3 the
cluster/broker, 4 the flow ran and nodes failed, 5 the flow stalled, 130
interrupted. There is exactly one converter, in `cli.main`; command functions
raise a typed error and stop. `VF_DEBUG=1` restores the traceback.

Every rendered container carries an explicit `imagePullPolicy`, defaulting to `IfNotPresent`
(`DEFAULT_IMAGE_PULL_POLICY` in [images.py](../../videoflow/deploy/images.py)). This is
load-bearing, not cosmetic: left unset, k8s infers `Always` from the `:latest` tag the auto-build
produces, and the image deploy just loaded into the cluster is re-pulled from a registry that has
never seen it — the pod lands in `ImagePullBackOff`, and when it is the provision Job the only
symptom is `provision Job did not complete within 180s`. Pass `--image-pull-policy Always` only
when every image comes from a registry the nodes can reach.

`run-local` mirrors this: config → `prepare.py` on the host → **build the solution image if (and
only if) some node needs one** → start or reuse dev NATS/Redis containers → `LocalProcessEngine` →
supervise and restart failed workers → report what gave up → tear down only what it started.

The supervision is the part worth knowing: `LocalProcessEngine` honours the same
`SupervisionPolicy` object the manifests render into a Job's `backoffLimit`, so a
crash the cluster absorbs is absorbed locally too. Only the backoff differs
(1/2/4s rather than 10/20/40s) so a genuinely broken node still surfaces in
seconds; `--no-restart` turns it off. When a node exhausts its restarts the
supervisor publishes the flow-wide stop immediately — not after reaping everything
— because its children are already waiting for an end-of-stream that is not coming,
and the reaping loop is waiting for *them*. The build is gated on
`needs_container_image` ([engines/local.py](../../videoflow/engines/local.py)): only a *native*
component (no `pythonClass`, no `runtime.localCommand`) is `docker run` and needs an image. A
pure-Python flow spawns host subprocesses, so it never builds — which is every solution in
videoflow-contrib today. `--image` / `--no-build` / `--build-context` work as they do on `deploy`.

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
`nodeSelector` on `videoflow.io/gpu-pool: "true"` (under mix, a required nodeAffinity instead:
pool AND (unowned OR owned by this flow's `videoflow.io/gpu-owner` stamp)), and a toleration for
the `nvidia.com/gpu` taint. Deploy preflights both the label and the taint — it warns but does not
block. Every capacity/inventory read in `deploy/cluster.py` is pool-scoped, and capacity checks
compare demand against **free** units (allocatable minus running pods' requests,
`cluster.gpu_availability`/`gpu_units_in_use`) — the pool is multi-tenant.

Two modes (`deploy/gpu.py` strategy registry). `exclusive` (default): units are whole physical
devices, `gpu_count > 1` spans devices on one host, sharing is inexpressible. `mix`: nodes
declaring `gpu_memory_gib` get solver-chosen exclusive MIG slices (`deploy/mig.py` computes the
layout from GFD-label inventory after `gpu._partition_inventory` drops other flows' / time-sliced /
pre-MIG'd nodes and marks busy ones spanner-only; the strategy's `resolve_specs` hook stamps each
sharer's profile into `NodeSpec.gpu_resource_name`, and `prepare`/`cleanup` apply/restore geometry
through the GPU Operator: CAS-claim the target nodes with `videoflow.io/gpu-owner=<flow-id>`,
merge the generated mig-parted config into the operator's — preserving other flows' published
entries, resourceVersion-CAS publish — patch ClusterPolicy `migManager.config.name` at the merged
copy, wait for the mig-manager DaemonSet rollout, label nodes `nvidia.com/mig.config` and wait for
`mig.config.state=success` — teardown verifies the same state before restoring, restores only the
named flow's nodes, and only the last flow out restores the policy and deletes the ConfigMap; the
lifecycle hooks take `flow_id` as a keyword). The deploy-level `--gpu-resource-name` covers
clusters advertising whole devices under another name; there is no node-level resource-name knob.
The full cluster-preparation walkthrough is in [`README.md`](../../README.md).

Multi-GPU nodes (`gpu_count > 1`, RFC 0003): preflight additionally checks the **largest single
node's** allocatable count (`cluster.max_allocatable_gpus_per_node` vs `manifests.gpu_max_per_pod`
— all of one replica's devices must sit on one host, so the cluster total is not sufficient) and
**classifies the resource** (`cluster.classify_gpu_resource`, from GFD labels): a multi-unit claim
against a MIG or time-sliced pool is fatal regardless of `--strict-preflight`
(`gpu.IMPOSSIBLE_GPU_REQUEST` marker), an unclassifiable pool gets an assuming-physical note, and
`manifests.validate_gpu_specs` hard-errors a `mig-*` resolved name at render. `run-local`
partitions the host's visible devices into disjoint `CUDA_VISIBLE_DEVICES` blocks per GPU replica
(wrap-around with per-replica warnings when oversubscribed, `VF_GPU_COUNT` reporting the
*delivered* count; nothing is set on a GPU-less host; docker-run native components get neither
devices nor `VF_GPU_*`). Non-goals, deliberately: injecting `--gpus` into the docker-run path for
native components, local MIG addressing, and any local enforcement beyond cooperative
`CUDA_VISIBLE_DEVICES` masking.

## Infrastructure ownership

Both [`infra.py`](../../videoflow/deploy/infra.py) (in-cluster) and
[`localinfra.py`](../../videoflow/deploy/localinfra.py) (Docker) follow one rule: **a pre-existing
`nats`/`redis` service or container is reused and never torn down.** Only resources deploy created
are deleted. Preserve this when touching either module — the alternative is deleting someone's
shared broker.

[`cluster.py`](../../videoflow/deploy/cluster.py) detects the cluster flavor (`k3s`, `kind`, `minikube`,
`docker-desktop`, `generic-remote`) to decide how to load images and whether hostPath mounts are
viable.
