# RFC 0004: Declared GPU memory demand and the `mix` GPU mode

- **Status:** proposed
- **Author(s):** videoflow maintainers
- **Created:** 2026-07-22
- **Protocol version affected:** 1 (no version change — envelope bytes and routing are untouched)
- **Requirement IDs touched:** none amended; no new environment rows

## Summary

Videoflow supports exactly two GPU allocation modes:

1. **`exclusive`** (default): every unit of the GPU resource is one whole physical
   device. `gpu_count = N` grants N whole devices on one host (RFC 0003). Sharing a
   device between components is not a capability of the mode, and the API offers no
   way to ask for it.
2. **`mix`** (opt-in, this RFC): a GPU node states its *memory demand* —
   `gpu_memory_gib` on the node, `spec.resources.gpu.memoryGiB` in a component
   descriptor — and a layout solver splits the cluster's cards into MIG-partitioned
   cards for the sharers and whole cards for the spanners. A GPU node that declares
   no memory demand gets a whole physical device, exactly as under `exclusive`.

The demand vocabulary is two numbers, mutually exclusive per node:

- `gpu_count > 1` — whole physical devices, spanned by one model (needs P2P-capable
  cards; MIG-disabled).
- `gpu_memory_gib` — an isolated fraction of one device (an exclusive MIG slice of
  at least that size; the card is shared, the slice is not).

They are mutually exclusive because the hardware makes them so: one CUDA process
addresses at most one MIG instance, and there is no P2P between MIG instances, so a
model can never span slices. A node declares a fraction of one device or whole
devices, never both — enforced at graph build time.

Extended-resource *names* are not a user-facing concept. The deploy-level
`--gpu-resource-name` exists for clusters that advertise whole devices under a
non-default name (`amd.com/gpu`); slice resources (MIG profiles) are chosen and
requested only by the `mix` strategy's solver.

## Motivation

RFC 0003 gave components a multi-GPU visibility contract but left sharing to
hand-managed mechanisms (MIG profile names typed into node parameters, device-plugin
time-slicing, a no-accounting `shared` mode). Each of those broke the "units ==
devices" equivalence the rest of the machinery assumes, and each breakage produced a
distinct silent-failure mode (see the two-mode redesign review). Declaring *memory*
instead of *profile names* removes the two things operators get wrong — choosing a
profile string and matching slice counts to replica counts — and makes the
impossible configurations unexpressible rather than merely warned about.

## Proposal

### Node API

```python
detector = MyDetector(device_type=GPU, nb_tasks=4, gpu_memory_gib=10)   # sharer
captioner = component('vlm_caption', device_type='gpu', gpu_count=2)    # spanner
resizer  = MyResizer(device_type=GPU)                                   # whole device
```

Validation at construction: `gpu_memory_gib` must be a positive number, requires
`device_type=GPU`, and is mutually exclusive with `gpu_count > 1`.

### Descriptor: `spec.resources.gpu.memoryGiB`

```yaml
spec:
  device: [gpu]
  resources:
    gpu:
      memoryGiB: 20     # positive number; default request, not a floor
```

- Same resolution order as `count` (RFC 0003): explicit graph-side argument →
  descriptor → unset.
- `memoryGiB` and `count > 1` are mutually exclusive in a descriptor, same reason
  as on the node.
- A cpu-flavor run of a dual-device component drops the descriptor's `memoryGiB`
  default (the demand describes the gpu flavor); an explicit graph-side
  `gpu_memory_gib=` with `device_type='cpu'` is still an error.
- Old-videoflow compatibility matches RFC 0003's: a descriptor using `memoryGiB`
  is rejected by older schemas (`additionalProperties: false`), deliberately.

### The `mix` strategy

`--gpu-mode mix` registers a strategy whose layout solver runs against the pool's
physical inventory (GPU Feature Discovery labels):

1. Reserve whole cards for spanners (`gpu_count >= 1` with no memory demand) —
   the rigid demand: N co-located, MIG-disabled, P2P-capable cards per replica.
2. Pack sharers (`nb_tasks × gpu_memory_gib` each) into the remaining cards,
   choosing per card the smallest supported MIG profile ≥ each demand,
   first-fit-decreasing, deterministically.
3. Resolve each sharer spec's extended resource to its chosen
   `nvidia.com/mig-<profile>`; spanners keep the whole-device resource. Everything
   downstream (demand math, manifests, env delivery) is unchanged.

Cluster reconfiguration happens in the strategy's `prepare()`/`cleanup()` hooks:
where the NVIDIA GPU Operator's MIG manager is present, geometry is applied by
setting the `nvidia.com/mig.config` node label against a generated mig-parted
config, and the previous label value is recorded in a node annotation
(`videoflow.io/mig-config-restore`) so a later `teardown --gpu-mode mix` can
restore it without sharing any state with the deploy. Without the MIG manager,
preflight and `prepare` emit the generated mig-parted config for manual
application.

*Amended 2026-07-22:* the MIG manager only reads the config file mounted from
the ConfigMap named in ClusterPolicy `migManager.config.name`, so a
side-published ConfigMap was never consulted (the original design failed with
`mig.config.state=failed` on every stock GPU Operator cluster). `prepare()` now
merges the generated entries into the operator's current mig-parted config,
publishes the merge as `videoflow-mig-parted-config`, patches ClusterPolicy to
name it (recording the original name in the
`videoflow.io/mig-config-name-restore` ClusterPolicy annotation), and waits for
the mig-manager DaemonSet rollout before labeling nodes. `cleanup()` reverts
nodes (a node whose label was absent is pointed at a merged-in
`videoflow-all-disabled` entry first, since bare label removal triggers no
reconfiguration), waits for `mig.config.state=success`, and only then restores
the policy and deletes the ConfigMap — a failed revert keeps its restore
records so a retried teardown resumes. Note the mix end-to-end path cannot be
exercised on the time-sliced dev cluster; it needs a MIG-capable GPU Operator
cluster.

Local runs degrade `mix` to `exclusive` semantics: the local engine cannot
address MIG slices (RFC 0003), so a sharer simply gets a whole card — which
satisfies "at least `gpu_memory_gib`".

### Multi-tenancy

*Amended 2026-07-22:* the original inventory model assumed the whole cluster,
pristine and exclusively videoflow's. The pool is now explicitly
**multi-tenant** — several videoflow flows plus foreign GPU workloads:

- **Inventory scope.** `gpu_inventory`, `allocatable_gpus` and
  `max_allocatable_gpus_per_node` read only `videoflow.io/gpu-pool=true` nodes
  (the pods' own nodeSelector), and inventory records carry the per-node facts
  planning needs: time-slicing signals, existing MIG state (either strategy),
  the owner stamp, and units held by running pods.
- **Exclusion before solving.** The strategy — not the pure solver — excludes
  nodes owned by another flow (quietly; that is normal), time-sliced or
  foreign-MIG'd nodes (with a warning; those are pool misconfigurations), and
  refuses outright a node still stamped by *this* flow (leftover geometry: its
  GFD `gpu.count` no longer maps `card_index` to physical positions — the fix
  is a teardown). Busy nodes shrink to their free cards with MIG disallowed:
  repartitioning destroys running workloads, but scheduler-accounted whole-card
  spanner claims stay safe. An infeasible layout lists every exclusion.
- **Ownership.** `prepare()` claims each node it will partition with a
  compare-and-swap `videoflow.io/gpu-owner=<k8s_name(flow_id)>` label (no
  `--overwrite`; a lost race releases this deploy's claims and aborts).
  `cleanup(flow_id)` reverts only owned nodes and releases the claim after each
  node's geometry reverted; a bare `cleanup()` remains the global sweep. Mix
  pods swap the pool nodeSelector for a required node affinity — (pool AND
  unowned) OR (pool AND owned by this flow) — so the scheduler, not just the
  planner, keeps flows off each other's slices.
- **Shared config, last one out.** The published
  `videoflow-mig-parted-config` is merged (operator base + other flows' live
  `videoflow-*` entries + this layout) and written with resourceVersion
  compare-and-swap; cleanup strips only this flow's entries and restores
  `migManager.config.name` / deletes the map only when no other flow's entries
  remain.
- **Free-unit capacity.** Preflight (both modes) compares demand against
  allocatable *minus* units requested by non-terminated pods, and mix also
  preflights its spanner demand against free whole-device capacity.

Residual races, accepted: a foreign GPU pod landing on a planned node between
inventory read and geometry apply is disrupted (closing this needs admission
control, which videoflow does not install), and a last-flow-out restore racing
a new flow's prepare converges after a re-read with transient mig-manager
rollout churn. The mix end-to-end path — including concurrent flows — still
cannot be exercised on the time-sliced dev cluster; it needs a MIG-capable GPU
Operator cluster.

### Per-run entry names

*Amended 2026-07-22 (retry deadlock):* the MIG manager reacts only to
`nvidia.com/mig.config` label *changes*, so the original fixed
`videoflow-<node>` entry name deadlocked retries: a previous attempt's
leftover `mig.config.state=failed` with the identical label value made every
re-prepare a no-op relabel that re-read `failed` forever. Entry names (and so
label values) now carry a per-run nonce — `videoflow-<node>-<nonce>`, clamped
to the 63-char label-value limit by truncating long node names with a stable
hash suffix — so a fresh apply is always a label change. Because the names are
no longer reconstructible, each claimed node records its current entry name in
the `videoflow.io/mig-entry` annotation (stamped before the ConfigMap publish);
cleanup reads its entries off node labels and this annotation, prepare's merge
drops the previous attempt's entries for its own nodes, and a cleanup that
finds a node already sitting at its restore target with `state=failed` bounces
it through a nonce'd alias of the `videoflow-all-disabled` entry (aliases are
plumbing: they never count against last-one-out).

The lifecycle hooks (`resolve_specs`/`prepare`/`cleanup`) gained a `flow_id`
keyword (None default). Per the documented contract, new lifecycle inputs
arrive as keywords — third-party strategies should accept `**kwargs`; an
override with the old exact signature breaks only if called with the new
keyword, which the CLI now always passes.

### Environment

No new environment rows. A sharer's worker sees `VF_GPU_COUNT=1` and
`VF_GPU_RESOURCE_NAME=<its MIG profile>` — both true under the RFC 0003
visibility contract (one visible device, `cuda:0`).

## Compatibility

- **Wire:** untouched. No golden vectors, no version bump.
- **Old descriptor + new videoflow:** `memoryGiB` absent → behaviour identical.
- **New descriptor + old videoflow:** rejected by jsonschema, deliberately (a
  silently dropped memory demand would deploy a whole-device pod where the author
  sized for a slice — wasteful, not wrong, but surprising).
- **Spec dict:** `NodeSpec` gains `gpu_memory_gib` (appended; `from_dict` defaults
  it to `None`, so old serialized specs load unchanged).

## Alternatives considered

- **Letting users request MIG profiles by name** (RFC 0003's original
  `resourceName`). Rejected and removed: profile names are cluster geometry, not
  application demand; naming them by hand created the impossible
  `gpu_count > 1 × MIG` combination space and tied flows to one cluster's layout.
- **A `shared` no-limit mode for sharing.** Removed: no accounting, no isolation,
  and an environment contract (`VF_GPU_COUNT`) that could not be true. Dev-box
  sharing is served by `exclusive` over a time-sliced pool with `gpu_count = 1`.
- **Time-slicing as the `mix` sharing mechanism.** Rejected: keeps physical
  addressability but has no memory isolation, so "an exclusive slice of ≥ N GiB"
  would be a promise with no hardware backing.
- **Kubernetes DRA.** The eventual clean home for per-claim geometry; out of scope
  until the NVIDIA DRA driver is broadly deployed. The strategy seam
  (`resolve_specs`/`prepare`) is where a future `dra` mode plugs in.

## Open questions

- Should the solver support explicit profile pinning for operators who need a
  specific geometry (e.g. compliance-driven)? Deferred until a real user asks.
