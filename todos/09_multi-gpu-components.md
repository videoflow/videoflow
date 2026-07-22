# Multi-GPU components — models that span more than one GPU

Design doc + implementation plans for components whose deep-learning model is too large for a
single GPU. Two options, each with its own implementation plan: **Option A** shards the model
across N GPUs inside one worker pod; **Option B** spans the model across multiple pods/hosts.
Recommended order: A first, then B's Track 1, then B's Track 2 only on demand.

---

## 1. Problem and current state

**The scheduling half of multi-GPU already exists; the component half doesn't.**

What works today, end to end:

- `ProcessorNode.__init__` accepts `gpu_count : int = 1` (validated `>= 1`) and
  `gpu_resource_name : Optional[str]` (`videoflow/core/node.py:271-291`, properties :307-315).
- Both round-trip through `get_params()` and land in `NodeSpec`
  (`videoflow/core/compiler.py:85-87`; `from_dict` defaults `gpu_count=1` for legacy specs).
- `ExclusiveGpu.pod_resources` (`videoflow/deploy/gpu.py:111-116`) renders
  `resources.limits: {nvidia.com/gpu: N}` on the single worker container; the k8s device plugin
  then guarantees all N whole devices land in that one pod, on one host.
- Demand math sums `nb_tasks × gpu_count` per resource (`gpu_demand`,
  `videoflow/deploy/manifests.py:180-211`); preflight compares against cluster capacity
  (`allocatable_gpus`, `videoflow/deploy/cluster.py:270`).
- The remote-component path plumbs both knobs too: `component(..., gpu_count=, gpu_resource_name=)`
  (`videoflow/core/remote.py:94-108, 141-142, 206`).

The gaps:

1. **No component can use N GPUs.** Every contrib component hard-codes single-device placement in
   `open()` — `.cuda()` (tracktor), `device='cuda'` (pose_topdown), `cfg.MODEL.DEVICE='cuda'`
   (detectron2), `tf.device('gpu')` (detector_tf). There is no contract telling node code which
   devices it was granted. `utils/system.py` has unused enumeration helpers
   (`get_gpus_available_to_process`) with no callers.
2. **Descriptors can't declare a GPU need.** `component.yaml` only has `spec.device: [cpu, gpu]`
   (`spec/descriptor/component-schema.json`, `videoflow/components/descriptor.py`). Every graph
   author must pass `gpu_count=` by hand, and native (non-Python) components have no runtime way
   to learn their grant — no GPU env var exists.
3. **The local engine ignores GPUs entirely** (`videoflow/engines/local.py`): no
   `CUDA_VISIBLE_DEVICES` partitioning, every worker sees every host GPU — which silently breaks
   the contract that run-local is supposed to rehearse.
4. **Preflight asks the wrong capacity question for multi-GPU.** `allocatable_gpus` sums across
   the whole cluster, but all N GPUs of one pod must sit on one host. A flow demanding 4 GPUs
   passes preflight on a cluster of 4 × 1-GPU nodes and then stays Pending forever.
5. **Multi-pod parallelism has no representation at all.** No rank/world-size/rendezvous env,
   replicas are independent competing pods, `GpuStrategy.pod_resources` can only emit a
   single-container resources fragment, and `runtime/worker.py` has zero GPU logic.

Sharing caveats that bound the design: time-slicing with `failRequestsGreaterThanOne: true`
rejects `gpu_count > 1` (slices of one card can't host a spanning model — README ~:407), and MIG
slices are isolated partitions that can never be combined into one model. **Spanning a model
requires whole exclusive GPUs.** Note: our own k3s dev cluster runs time-slicing (replicas=4),
so multi-GPU testing needs whole-GPU nodes or a different cluster profile.

Industry framing: within one host, tensor parallelism inside one pod requesting
`nvidia.com/gpu: N` is the standard (vLLM `--tensor-parallel-size N`, HF `device_map="auto"`;
requests must equal limits; NVLink co-location preferred) and covers roughly the 30–70B model
range. Across hosts, the ecosystem has converged on the LeaderWorkerSet (LWS) API, which KServe
multi-node and vLLM distributed serving build on. See Sources at the bottom.

---

## 2. Option A — single-node, single-pod multi-GPU

### Design

**The contract, in one sentence:** *inside a worker, the visible GPUs are exactly the granted
GPUs, numbered `cuda:0..N-1`, and `N == self.gpu_count`.*

This is already physically true on Kubernetes in exclusive mode (device-plugin injection); §A4
makes it true locally. The framework deliberately does **not** do the sharding —
`device_map="auto"`, `tensor_parallel_size`, or manual `.to('cuda:k')` are model-runtime
decisions that belong in the node's `open()`. The framework's job ends at "you were granted
exactly these devices."

#### A1. Component-side API

- **`self.gpu_count`** is authoritative for Python nodes — it already exists and round-trips via
  `get_params()`, so the reconstructed node in the worker carries it. A multi-GPU node's `open()`
  is just:

  ```python
  def open(self) -> None:
      import torch  # sanctioned function-level heavy import (contrib rule)
      self._model = AutoModelForCausalLM.from_pretrained(..., device_map = 'auto')
      # or explicit: self._reid.to(f'cuda:{min(1, self.gpu_count - 1)}')
  ```

- **New helper `videoflow.utils.system.granted_gpus() -> list[int]`** — promote the unused
  `get_gpus_available_to_process()` (intersection of nvidia-smi-visible devices and
  `CUDA_VISIBLE_DEVICES`): full annotations, module docstring, fix the bare `except`, keep the
  old name as a deprecated alias. Exists for non-torch code (TF's `device_id` strings, native
  wrappers) and defensive checks (`if len(granted_gpus()) < self.gpu_count: raise RuntimeError`).
- **New env vars `VF_GPU_COUNT` / `VF_GPU_RESOURCE_NAME`** for native components (which never see
  `get_params()`): emitted by `manifests._env_pairs` and `engines/local._worker_env` for
  `device_type == 'gpu'` specs; documented in the worker docstring env table and
  `spec/PROTOCOL.md` §1 (optional-with-default rows, like `VF_NB_TASKS`). Python nodes don't read
  them — `self.gpu_count` is the source of truth there.

Rejected alternatives: `ctx.granted_gpus()` on `RuntimeContext` (creates a second source of truth
beside `self.gpu_count`; gpu_count is build-time config, not runtime identity); "just document
`torch.cuda.device_count()`" (covers neither native/TF components nor pre-partitioning local
runs); a framework-side placement layer (over-abstraction with no second caller — sharding
strategy is model-specific).

#### A2. Descriptor + remote path

Schema addition in `spec/descriptor/component-schema.json`, under `spec` (sibling of
`constraints`):

```json
"resources": {
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "gpu": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "count":        {"type": "integer", "minimum": 1},
        "resourceName": {"type": "string"}
      }
    }
  }
}
```

`resources` (not `constraints`) mirrors the Kubernetes vocabulary and leaves room for
`resources.memory` later; nested `gpu` object rather than flat `gpuCount` for the same reason.

- `components/descriptor.py`: parse `gpu_count` (default 1) and `gpu_resource_name` with the same
  defensive `or {}` shape as the existing `constraints` parsing. Fallback-shape validation:
  `count` must be int ≥ 1, and `count > 1` requires `'gpu' ∈ spec.device` (a multi-GPU CPU-only
  component is a contradiction caught at load time). `ValueError` messages name the fix.
- `core/remote.py`: change `component(..., gpu_count : int = 1)` to `Optional[int] = None`.
  Resolution order (documented): **explicit argument → descriptor `resources.gpu` → 1 / None.**
  The descriptor value is a *default, not a floor* — an operator may run a quantized variant on
  fewer GPUs; a hard minimum belongs in the component's `open()` via `granted_gpus()`. One new
  validation: resolved `gpu_count > 1` with `device_type='cpu'` → `ValueError` naming the fix.
  `RemoteProcessor` and `_python_node_params()` need no change — already plumbed.
- Compatibility: old descriptors (no `resources`) behave byte-identically. New descriptors are
  rejected by an old videoflow's jsonschema (`additionalProperties: false`) — an observable
  contract change for the language-agnostic ecosystem, and the env rows touch PROTOCOL.md's
  normative env table. Not a wire/routing change (no golden-vector updates), but both files live
  in `spec/`, so write **RFC 0003 — "Multi-GPU component grants"**: the schema addition, the two
  env rows, the visible==granted contract, and the old-reader/new-descriptor caveat.

#### A3. Preflight

- New `cluster.max_allocatable_gpus_per_node(kubectl, resource) -> int` — same escaped-jsonpath
  query as `allocatable_gpus` but `max()` instead of `sum()`. Both stay: total answers "will the
  whole flow schedule", max-per-node answers "will any single pod schedule".
- New `manifests.gpu_max_per_pod(specs, default_resource=None) -> dict[str, int]` — sibling of
  `gpu_demand`, same iteration, `max(spec.gpu_count)` per resolved resource name. Same
  dict-not-dataclass reasoning (open-ended extended-resource keys).
- `GpuStrategy.preflight_problems` gains keyword `max_per_pod : Optional[dict[str, int]] = None`:
  - **ExclusiveGpu**: error when a single pod's demand exceeds the largest node — *"node requests
    {n} × {r} in a single pod but the largest cluster node has only {m} allocatable — all of one
    replica's GPUs must sit on one Kubernetes host; the cluster total is irrelevant. Fix: add a
    node with ≥ {n} GPUs, or reduce gpu_count."* Also error on `'mig-' in resource and count > 1`:
    MIG slices are isolated partitions and can never be combined into one model. Renamed
    time-sliced resources are undetectable from the name — stays a README caveat
    (`failRequestsGreaterThanOne` already rejects it cluster-side).
  - **SharedGpu**: advisory warning that `gpu_count > 1` is ignored under `--gpu-mode shared`
    (no limit is rendered; every shared pod already sees every device on its host). Warn, don't
    reject — `device_map='auto'` over all visible devices genuinely works on a dev box; it's the
    accounting that's absent.
- Wire-through: `cluster.gpu_preflight` passes the kwarg; `cli._cmd_deploy` computes
  `gpu_max_per_pod(specs, default_resource=args.gpu_resource_name)` next to the existing
  `gpu_demand` call. Plugin-compat note in the `GpuStrategy` docstring (override with `**kwargs`
  tolerance) — the entry-point group is new, no known external strategies.

#### A4. Local engine — partition, minimally

- New pure function `engines/local.assign_local_gpus(specs, host_gpus) ->
  dict[tuple[str, int], list[int]]` mapping `(node_name, replica_idx)` → device ordinals: walk
  GPU specs in order, hand each replica the next `gpu_count` ordinals; when the pool runs out,
  wrap around and log **one** warning ("local GPU demand X exceeds the Y visible devices —
  replicas will share devices; fine for dev, will not schedule on Kubernetes").
  Oversubscription-with-warning preserves dev-box permissiveness.
- The pool is the *parent process's* `granted_gpus()` — the operator's own
  `CUDA_VISIBLE_DEVICES` is respected as the universe. Zero-GPU host → empty pool → no env set →
  byte-identical behavior to today (CPU-fallback GPU nodes keep working).
- `_worker_env` gains `gpu_devices : list[int] | None = None`; when set it writes
  `CUDA_VISIBLE_DEVICES=','.join(...)` plus `VF_GPU_COUNT`/`VF_GPU_RESOURCE_NAME`.
- Explicit non-goals (documented in DEPLOYMENT.md): docker `--gpus` injection for native
  components run via docker locally; any enforcement/isolation — `CUDA_VISIBLE_DEVICES` is
  cooperative masking, which is exactly the dev-grade semantics wanted.

#### A5. Reference components (contrib)

1. **Flagship — new `vlm_caption/`** (dist `videoflow_contrib_vlm_caption`, import
   `videoflow_contrib.vlm_caption`, descriptor `videoflow/vlm-caption`): a `ProcessorNode` that
   captions frames with an HF VLM (e.g. Qwen2.5-VL-7B class). `open()` loads with
   `device_map='auto'` — the node body contains **zero device arithmetic**; that's the story.
   Its `component.yaml` declares `spec: {device: [gpu], resources: {gpu: {count: 2}}}`,
   demonstrating the descriptor default flowing through `component()` with no caller-side
   `gpu_count=`. Ships `gpu.Dockerfile`, weights via downloader/HF cache, weights-free unit tests
   for prompt/post-processing.
2. **Retrofit — `tracktor`** (the one existing two-model node; both models currently go to device
   0 via bare `.cuda()`): change to `obj_detect.cuda(0)` /
   `reid_network.cuda(min(1, self.gpu_count - 1))` — the manual per-submodel placement pattern
   for the many existing two-model nodes, a few lines, no new deps.

If effort must be cut, cut the flagship's solution wiring, not the component.

#### A6. Docs and tests

Docs, in the same commits as the code they describe: `.claude/docs/NODE_CONTRACT.md` (expand the
`gpu_count` bullet into the full visible==granted contract with the `device_map` example and the
MIG/time-slice exclusion), `.claude/docs/DEPLOYMENT.md` (per-host preflight, shared-mode warning,
run-local partitioning + non-goals), `README.md` ("models larger than one GPU" subsection in the
GPU walkthrough; extend the time-slicing caveat with MIG non-combinability),
`spec/PROTOCOL.md` §1 (two env rows, citing RFC 0003), `spec/rfcs/0003-multi-gpu-component-grants.md`
(new), `docs/source/distributed/` GPU page, `runtime/worker.py` docstring env table, and
`../videoflow-contrib/CLAUDE.md` (multi-GPU convention for authors).

Tests are all GPU-free (a toy-solution e2e is infeasible without hardware; the local allocator is
deliberately a pure function): descriptor parsing (`resources.gpu` parse/defaults/rejections),
remote resolution (descriptor default, explicit override, cpu+multi-gpu rejection, reaches
NodeSpec and pod limits), `gpu_max_per_pod` max-not-sum, `_env_pairs` carries `VF_GPU_COUNT`,
`max_allocatable_gpus_per_node` takes the largest node, preflight flags per-node overrun / MIG
combination / shared-mode warning, `assign_local_gpus` disjoint blocks / wrap-and-warn /
no-GPU-no-op, `_worker_env` sets `CUDA_VISIBLE_DEVICES`, `granted_gpus` respects
`CUDA_VISIBLE_DEVICES` (monkeypatched nvidia-smi).

### Implementation plan (Option A)

1. **Spec + RFC** — `spec/rfcs/0003-multi-gpu-component-grants.md`; `spec.resources` in
   `spec/descriptor/component-schema.json`; two env rows in `spec/PROTOCOL.md` §1. No vectors.
2. **Descriptor parsing** — `videoflow/components/descriptor.py` fields + fallback validation.
3. **Remote resolution** — `videoflow/core/remote.py` `gpu_count : Optional[int] = None`,
   descriptor-default resolution, cpu/multi-gpu rejection.
4. **Env plumbing** — `manifests._env_pairs` + `local._worker_env` emit `VF_GPU_COUNT`/
   `VF_GPU_RESOURCE_NAME`; worker docstring.
5. **Preflight** — `cluster.max_allocatable_gpus_per_node`, `manifests.gpu_max_per_pod`,
   `max_per_pod` kwarg through the strategies, `gpu_preflight`, `cli._cmd_deploy`.
6. **Helper** — `utils/system.granted_gpus()` promotion (+ deprecated alias).
7. **Local engine** — `assign_local_gpus()` + `gpu_devices` param + launch-loop wiring.
8. **Docs sweep** — per the table above, in the same commits as steps 2–7.
9. **Contrib** — `vlm_caption` component + `tracktor` retrofit (depends on 2–4 being available).

Dependencies: 1→2→3 is one chain; 4–7 are independent of each other after 1; 9 last.

---

## 3. Option B — multi-pod model parallelism

### Scope honesty first

Typical video models (detectors, trackers, pose, segmentation, embedders) fit on one GPU or on
one host's GPUs — Option A covers them. Models that genuinely span hosts are large LLM/VLMs, and
the industry has already built the hard parts (vLLM/SGLang distributed serving, KServe
multi-node, all converging on LWS). Reimplementing tensor/pipeline scheduling, NCCL rendezvous
recovery, and continuous batching inside videoflow would be reimplementing an inference server.
Hence two tracks, ordered:

#### Track 1 (ships first, zero core changes) — wrap an external endpoint

New contrib component `vllm_client/`: an async `ProcessorNode` calling an OpenAI-compatible
endpoint (params: `base_url`, `model`, batching/timeout knobs; `nb_tasks` scales client
concurrency). The operator deploys vLLM/KServe multi-node with that stack's own tooling (helm,
LWS recipes). **This is the recommended multi-node story for most users** and the docs should say
so plainly. Deliverables: the component + a "Serving large models" docs page stating when to use
this vs Track 2.

#### Track 2 — first-class "node groups" (the actual framework work)

One logical node = `nb_tasks` **groups** × `group_size` **pods** × `gpu_count` **GPUs per pod**,
rendered as a **LeaderWorkerSet** (`leaderworkerset.x-k8s.io/v1`): `spec.replicas = nb_tasks`,
`leaderWorkerTemplate.size = group_size`, `restartPolicy: RecreateGroupOnPodRestart`.

Why LWS over the alternatives:
- *StatefulSet + ordinal-derived rank*: needs no CRD and manifests.py already builds
  StatefulSets, but fails on group replicas (2 groups of 4 = either two STS per node or one 8-pod
  STS with `rank = ordinal % 4` and no shared fate), on restart semantics (per-pod restart leaves
  the other ranks hung in a broken NCCL collective; LWS recreates the whole group natively), and
  on leader addressing (hand-rolled `<sts>-0.<svc>` vs LWS-injected `LWS_LEADER_ADDRESS` /
  `LWS_WORKER_INDEX` / `LWS_GROUP_INDEX` — the contract vLLM's and KServe's recipes consume).
- *A `Parallelism(tensor=, pipeline=)` object on the node*: rejected — TP/PP split is the model
  runtime's concern; videoflow only needs "how many pods, how many GPUs each". The node's own
  params carry TP/PP config for its `open()`.

The CRD dependency is precedented (KEDA `ScaledObject`, `_CRD_DELETABLE_KINDS` teardown
tolerance), plus a hard preflight: a grouped node with the LWS CRD absent is a deploy-time
`ValueError` naming the one-line install.

**Node API:** `ProcessorNode` gains `group_size : int = 1`, `group_port : int = 29500`,
`group_env : dict[str, str] = {}` (each stored verbatim as `self._group_*` per the get_params
hard rule). `NodeSpec` fields appended last (field order is the constructor signature);
`from_dict` defaults for legacy specs. Descriptor support (`spec.parallelism`) is **explicitly
deferred to backlog** — MVP is native Python nodes only.

**Who talks to NATS:** only rank 0 (the leader) runs the task loop. Ranks 1..W−1 dispatch to a
new `runtime/shard.py::run_shard_from_env()`: build the node exactly as today (same class, same
params on every rank), construct a `GroupContext(rank, size, leader_addr, port, gpu_count)`
(small dataclass in `core/context.py`; rank 0 sees it as `ctx.group`, `None` when ungrouped),
call `open()` then a new optional `node.serve_shard(group_ctx)` — default raises
`NotImplementedError` — which blocks for the life of the run (torch: join the process group and
sit in the collective loop; vLLM-style: start the engine worker). A minimal shard-mode health
server serves probes. Messaging topology is **untouched** — subjects/streams/durables stay keyed
by node name; ranks 1+ simply never bind them.

**Env contract** (worker docstring is the contract of record): `VF_GROUP_SIZE`, `VF_GROUP_RANK`,
`VF_GROUP_LEADER_ADDR`, `VF_GROUP_PORT`, resolved via a `_resolve_group()` fallback chain
(VF_* first, then LWS-injected `LWS_WORKER_INDEX`/`LWS_LEADER_ADDRESS`, else size 1 / rank 0).
Leader `VF_REPLICA_ID` from `LWS_GROUP_INDEX` so EOS-durable instance ids stay stable. Videoflow
also renders convenience `MASTER_ADDR`/`MASTER_PORT` but **not** `WORLD_SIZE`/`RANK` — videoflow
runs one process per pod, and pretending to be `torchrun` would be a lie the node can't correct;
the node derives its own world from `VF_GROUP_SIZE`/`VF_GROUP_RANK` + `gpu_count`. `group_env`
passes NCCL tuning (`NCCL_SOCKET_IFNAME`, ...) through verbatim. The existing flow NetworkPolicy
already permits all intra-flow pod-to-pod ingress by label — worth a code comment, since NCCL
also uses ephemeral high ports and a port-scoped tightening would break groups. RDMA/IB device
plugins, NVLink topology, and affinity are left to the operator (documented, not rendered).

**MVP constraints** (each a `ValueError` naming the fix): grouped nodes are REALTIME-only (LWS is
a long-running workload; BATCH groups need a Job-shaped design — deferred); `partition_by` with
`group_size > 1` rejected (lift later via `LWS_GROUP_INDEX` as the stable per-group id); KEDA and
PDB skip grouped nodes. `gpu_demand` multiplies by `group_size`. Gang scheduling is deferred to a
Kueue recipe doc (LWS integrates with Kueue) — the MVP accepts that a half-scheduled group holds
GPUs while Pending, detected by the existing preflight + Unschedulable watchdogs, and the
preflight message says so. Failure semantics: any pod death recreates the whole group; the NATS
durable survives (node-named, not pod-named), so a group rejoins like a restarted replica —
REALTIME's drop-oldest retention means an outage costs frames, not correctness.

**GPU strategy seam unchanged:** per-pod `gpu_count` is what `GpuStrategy.pod_resources` already
handles; grouping is a workload-shape concern, not an allocation-mode concern — resist widening
the strategy interface.

### Implementation plan (Option B)

**Phase 1 — pragmatic story (independent of everything):**
contrib `vllm_client/` + "Serving large models" docs page (README + docs/source).

**Phase 2 — core group plumbing, engine-agnostic:**
`core/node.py` (`group_size`/`group_port`/`group_env` + validation + `serve_shard()` default);
`core/compiler.py` (NodeSpec fields appended last, copy-through, REALTIME-only + no-partition
validation); `core/context.py` (`GroupContext`, `RuntimeContext.group`); `runtime/worker.py` +
new `runtime/shard.py` (`_resolve_group()`, early shard dispatch, shard health server, docstring
env table); `engines/local.py` (spawn `group_size` processes per grouped node with
`VF_GROUP_*` env, leader at `127.0.0.1`, free port; non-zero exit of any member kills the
siblings). Tests: NodeSpec round-trip, validation errors, `_resolve_group` fallback chains, shard
dispatch with fake env, and a new CPU-only REALTIME toy solution **`solutions/toy_group`**
(leader + one shard coordinating over a plain TCP socket on `VF_GROUP_PORT`) wired into
`tests/integration/test_toy_solutions.py` — this proves "only rank 0 talks to NATS" end to end
with no GPU in CI.

**Phase 3 — Kubernetes rendering (blocked on Phase 2):**
`deploy/manifests.py` (LWS branch in `workload()`; `_pod_spec` gains a `group_role`
leader/worker param; `_env_pairs` group env; `gpu_demand × group_size`;
`_CRD_DELETABLE_KINDS += leaderworkerset`; PDB/KEDA skips with reason comments);
`deploy/cluster.py` + `cli.py` (`lws_installed()` hard preflight; `videoflow explain` shows pods
per replica); `engines/kubernetes.py` (verify label-driven pod queries/teardown/logs against
LWS-created pods — expected no structural change). Tests: golden-YAML comparison for a grouped
spec, and the behaviour-identity check that `group_size=1` renders byte-identically to today.

**Phase 4 — proof on real hardware + hardening:**
one real grouped contrib node (torch tensor-parallel demo or in-pipeline vLLM engine) on a
2-node GPU cluster; Kueue gang-scheduling recipe doc. Deferred backlog, each gated on demand:
descriptor `spec.parallelism`, partitioned groups, BATCH groups, pod-template overrides,
StatefulSet fallback for CRD-less clusters.

**Testing without a multi-node GPU cluster:** unit + golden-manifest tests (no cluster);
`toy_group` under the local engine (CPU sockets, CI); optional kind-cluster run applying rendered
LWS manifests with the CRD and a CPU image (validates scheduling/env-injection/teardown, not
NCCL); NCCL itself only in Phase 4. The single biggest design risk is the `serve_shard()`
contract feeling half-baked until Phase 4's real component exercises it — which is why that
component is in-plan, not optional.

---

## 4. Comparison and recommendation

| | Option A (single pod) | Option B Track 1 (wrap endpoint) | Option B Track 2 (node groups) |
|---|---|---|---|
| Model range | up to one host's GPUs (~70B with TP) | anything the serving stack handles | beyond one host, in-pipeline |
| Core changes | small, additive | none | large (worker mode, LWS, env contract) |
| Who shards | the node's `open()` | vLLM/KServe | the node's `open()`/`serve_shard()` |
| When | now | when a giant LLM/VLM enters a flow | only when the model must live inside the pipeline (per-frame rates where an HTTP hop is prohibitive, or stream-coupled sharded state) |

**Recommendation: implement A; add B Track 1 as a cheap contrib component; hold B Track 2 until
a concrete workload demands it.** A's contract (visible == granted) is also a prerequisite for
any future B work, so nothing is wasted.

## 5. Caveats

- **Time-slicing**: `failRequestsGreaterThanOne: true` rejects `gpu_count > 1`; slices of one
  card can't host a spanning model anyway. Our k3s dev cluster runs time-slicing (replicas=4) —
  multi-GPU components can't be tested there without switching those nodes to whole-GPU
  advertising.
- **MIG**: slices are hardware-isolated partitions; `gpu_count > 1` against a MIG profile can
  never work (preflight rejects it). Use one larger profile or whole GPUs.
- **Kubernetes**: extended-resource requests must equal limits; all N GPUs of a pod land on one
  host (hence the max-per-node preflight); prefer NVLink-connected GPUs for tensor parallelism.

## 6. Sources

- vLLM parallelism & scaling: https://docs.vllm.ai/en/stable/serving/parallelism_scaling/
- vLLM on LeaderWorkerSet: https://docs.vllm.ai/en/stable/deployment/frameworks/lws/
- KServe multi-node/multi-GPU inference: https://kserve.github.io/website/docs/model-serving/generative-inference/multi-node
- Multi-GPU LLM inference on k8s (recipe): https://kubernetes.recipes/recipes/ai/multi-gpu-llm-inference/
- Cloud-native LLM inference stack overview (KServe/vLLM/llm-d): https://jimmysong.io/blog/cloud-native-llm-inference-stack/
- GKE LLM inference best practices: https://docs.cloud.google.com/kubernetes-engine/docs/best-practices/machine-learning/inference/llm-optimization
