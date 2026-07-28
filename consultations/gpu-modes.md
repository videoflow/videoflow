# GPU modes in a multi-tenant cluster: release, time-slicing, and node claiming

Research pass over the GPU stack on `feat/multi-gpu` — `deploy/gpu.py`, `deploy/mig.py`,
`deploy/cluster.py`, the deploy/teardown lifecycle in `deploy/cli.py`, the pod affinity in
`deploy/manifests.py`, RFC 0004, and the existing `todos/00_gpu_allocation_bugs.md` /
`consultations/fake_gpu.md`. Three questions: does capacity come back when pods end, where
time-slicing belongs, and whether "only repartition unallocated nodes" is enforceable.

Nothing here has been fixed; this is the finding set.

---

## Q1 — Is a node marked available again when a pod finishes or fails?

There are **two independent ledgers**, and only one of them is ours.

**Ledger A — the Kubernetes scheduler.** `nvidia.com/gpu` and `nvidia.com/mig-*` are integer
extended resources. The kubelet releases them when a pod reaches a terminal phase or is
deleted; nothing videoflow does affects this. So for the literal question — a pod finishing,
or failing and its work moving to another pod — **release is automatic and correct in both
modes**. A container restart inside a live pod keeps the allocation (right: the pod still owns
the device); a Job's `Succeeded` pod releases it; a Deployment replacement releases the old
one. Our own read at [cluster.py:324](../videoflow/deploy/cluster.py#L324) skips
`Succeeded`/`Failed` pods, which matches.

**Ledger B — videoflow's node-level ownership, mix mode only.** `videoflow.io/gpu-owner`,
`nvidia.com/mig.config`, `videoflow.io/mig-config-restore`, `videoflow.io/mig-entry`, plus
entries in the shared `videoflow-mig-parted-config` ConfigMap and the ClusterPolicy patch.
This ledger is **flow-scoped, not pod-scoped**, and it is released only by `MixGpu.cleanup()`.
Nothing reconciles it — there is no controller, and no CLI subcommand to list or GC orphaned
stamps (`videoflow teardown` is the only writer, and it needs `--gpu-mode mix` passed by hand).

**Exclusive mode cannot leak.** It writes no cluster state. The ratchet risk is entirely in mix.

### Mix-mode leak paths, ranked by likelihood of biting

1. **REALTIME flows torn down without `--gpu-mode mix`.** Deliberate design
   ([cli.py:350-355](../videoflow/deploy/cli.py#L350-L355)): the flow outlives deploy, so
   cleanup is deferred to teardown. But `--gpu-mode` on teardown is **optional, default
   `None`** ([cli.py:1297](../videoflow/deploy/cli.py#L1297)), and a `kubectl delete`, a
   namespace deletion, or a GitOps prune bypasses the CLI entirely. The nodes stay owned
   forever. This is the #1 ratchet.

2. **Cleanup failures are warnings, not failures.** `_gpu_cleanup` swallows every exception and
   prints to stderr ([cli.py:103-104](../videoflow/deploy/cli.py#L103-L104)); the deploy still
   exits 0. Inside `cleanup()`, the `unrestored` path deliberately returns early keeping the
   owner label, the annotations, the ConfigMap entries **and** the ClusterPolicy patch
   ([gpu.py:1231-1236](../videoflow/deploy/gpu.py#L1231-L1236)) so a retried teardown can
   resume — but nothing ever retries it. A flaky MIG manager quietly accumulates owned nodes
   across runs while CI stays green.

3. **The deploy process dying between prepare and the `finally`.** `KeyboardInterrupt` is
   handled; **SIGTERM is not** — there is no signal handler anywhere in `cli.py`, so a CI
   cancellation, a `timeout`, or a killed terminal skips the `finally` at
   [cli.py:394](../videoflow/deploy/cli.py#L394) entirely. A 600 s `MIG_APPLY_TIMEOUT_SECONDS`
   wait is a wide target for that.

4. **Ownership is node-granular, and a claimed node is excluded *whole*.** One MIG'd card on an
   8-card node stamps the entire node, and rule 2 of `_partition_inventory`
   ([gpu.py:725-728](../videoflow/deploy/gpu.py#L725-L728)) removes all 8 cards from every other
   flow's planning. Not a leak, but the same visible symptom — usable capacity shrinking as
   concurrent flows arrive. Note the asymmetry: mix pods carry the owner-aware `nodeAffinity`
   ([manifests.py:354-365](../videoflow/deploy/manifests.py#L354-L365)), but **exclusive-mode
   pods carry only `nodeSelector: gpu-pool=true`** — so an exclusive flow will happily consume
   the whole cards a mix flow reserved for its spanners.

5. **The `_stamp_node_owners` TOCTOU** (todo item 20, correctly diagnosed there). The
   multi-tenant consequence is worse than "silently stealing a node": if A stamps and B's
   ~2 ms-stale GET makes B overwrite the label, then A's `cleanup()` scans for `owner == A`,
   finds nothing, and **A's ConfigMap entry is never stripped**. Since
   `_remove_mig_config_entries` reports it in `remaining` forever, the last-one-out branch at
   [gpu.py:1247-1258](../videoflow/deploy/gpu.py#L1247-L1258) never fires again — the
   ClusterPolicy stays patched and the ConfigMap undeletable for the life of the cluster. Same
   permanent-orphan outcome if a cluster-autoscaler deletes an owned node mid-run: the entry
   survives its node.

### Two read-side bugs that shrink capacity with no leak at all

6. **`_partition_inventory` counts non-GPU extended resources as busy cards.**
   [gpu.py:747](../videoflow/deploy/gpu.py#L747) does `busy_units = sum(node.used_units.values())`,
   but `gpu_units_in_use` deliberately counts *any* domain-qualified resource
   ([cluster.py:332](../videoflow/deploy/cluster.py#L332), asserted as intended in
   [test_cluster.py:407](../tests/test_cluster.py#L407)). On exactly the hardware mix targets —
   A100/H100 nodes with the Mellanox network operator — a pod holding `nvidia.com/gpu: 1`
   **and** `rdma/hca_shared_devices_a: 1` subtracts *two* cards. Enough co-tenants and
   `busy_units >= card_count` fires, excluding a node that has free cards. This is the literal
   "little by little the pool shrinks" mechanism, and it needs no leak to happen.

7. **Silence-tolerant reads driving destructive decisions.** The unifying flaw; it shows up
   three times:
   - `gpu_units_in_use` returns `{}` on any kubectl failure
     ([cluster.py:317](../videoflow/deploy/cluster.py#L317)). Occupancy-unknown reads as
     occupancy-zero, and `_partition_inventory` has no second check — a throttled API server
     means mix decides every node is idle and **repartitions cards holding another tenant's
     work**. The docstring's "deploy-time safety decisions layer their own checks on top" is
     not true for this caller.
   - In `cleanup()`, `strip_namespace = next((ns for ns, _pod in self._mig_manager_pods(kubectl)), None)`
     ([gpu.py:1243](../videoflow/deploy/gpu.py#L1243)). If that best-effort read hiccups, the
     whole "do other flows still hold entries?" check is skipped and the code falls through to
     **restoring the ClusterPolicy pointer while other flows are live**, unpointing the manager
     from the map their nodes' labels resolve in.
   - In `prepare()`, a failed `_operator_configmap_yaml` on the base map yields `''` and only a
     warning ([gpu.py:973-975](../videoflow/deploy/gpu.py#L973-L975)) — the published file then
     drops the operator's own entries, including whatever `migManager.config.default` names, so
     unrelated nodes go `state=failed`.

### Bottom line

Pod-level release is fine and needs no work. The monotonic-shrink risk is real and lives in
(a) flow-level cluster state with no reconciler and a best-effort, exit-0 release path, and
(b) planner arithmetic that treats non-GPU devices as consumed cards.

The single highest-value fix is a reconciler-ish escape hatch: a `videoflow gpu status` /
`gpu release --flow-id` pair that lists `videoflow.io/gpu-owner` stamps against actually-running
flow pods and frees the orphans. Everything else narrows a window; that one bounds the damage
regardless of which window we miss.

---

## Q2 — Where does time-slicing fit, and should it be a third mode?

**Today it is a pool property we read, never one we set**, in three places:

- `classify_gpu_resource` detects it from GFD labels and makes `gpu_count > 1` against a sliced
  pool a fatal `IMPOSSIBLE_GPU_REQUEST`
  ([cluster.py:438-442](../videoflow/deploy/cluster.py#L438-L442),
  [gpu.py:256-263](../videoflow/deploy/gpu.py#L256-L263)).
- **`exclusive` over an operator-pre-sliced pool is the supported sharing story** —
  `renameByDefault: false` + `gpu_count = 1`, and the manifests need no change. That is the
  dev-cluster path in the docs.
- **`mix` excludes time-sliced nodes outright**
  ([gpu.py:729-735](../videoflow/deploy/gpu.py#L729-L735)) with a warning telling the operator
  to un-slice them. So mix and time-slicing are mutually exclusive per node by construction.

So "mix ≈ MIG" is accurate, and RFC 0004 rejected time-slicing as mix's mechanism for the right
reason: `gpu_memory_gib` is a promise of *isolated* memory, and replicas give none.

### The real gap is not time-slicing — it is non-MIG hardware

MIG exists only on A30/A100/H100-class cards. On L4, L40S, T4, V100, RTX — a large share of real
inference fleets — mix is unusable and `gpu_memory_gib` silently degrades to a whole device with
a NOTE ([cli.py:274-278](../videoflow/deploy/cli.py#L274-L278)). Those clusters have no way to
express "pack four 10 GiB detectors onto one 48 GiB card" other than an operator hand-editing
the device-plugin ConfigMap.

### Recommendation: do not build a time-slicing mode; if a third mode, build MPS

The reasoning that decides it: time-slicing's unit carries **no memory semantics at all**, so a
`timeshare` mode could only accept `gpu_memory_gib` and ignore it — a mode whose entire
vocabulary is a lie. MPS via the device plugin's `sharing.mps` gives each client a hard cap of
`total_memory / replicas` (`CUDA_MPS_PINNED_DEVICE_MEM_LIMIT`), so `gpu_memory_gib` has actual
hardware backing: the solver picks `replicas = floor(card_memory / gpu_memory_gib)` exactly as
`smallest_profile_for` picks a MIG profile today. The whole `mig.py` shape — declared demand →
per-card packing → per-spec resource name — transfers; only the geometry table and the apply
mechanism change. `GpuStrategy` ([gpu.py:118-214](../videoflow/deploy/gpu.py#L118-L214)) was
built for precisely this.

Two caveats to price in first: MPS replicas are a **per-node, per-resource** setting, so the
granularity is "this node is carved into N-ths", not per-card as with MIG; and MPS shares one
fault domain (a client hitting a fatal fault can take down the daemon), weaker than MIG's
hardware isolation and something the mode's contract must state. Both are the concerns in the
"coherent version of your idea does exist" paragraph of [fake_gpu.md](fake_gpu.md) — that
document's conclusion holds; MPS just makes the promise honest where time-slicing cannot.

### The cheap 80% if a new mode is not worth it

Let `mix` *use* time-sliced pool nodes for sharers instead of excluding them: request plain
`nvidia.com/gpu` units and validate that `nb_tasks × gpu_memory_gib ≤ card_memory` from the GFD
`nvidia.com/gpu.memory` label, with the demand documented as a soft budget on those nodes. Zero
cluster mutation, zero new lifecycle, and it stops mix failing outright on mixed pools.

---

## Q3 — "Only repartition nodes that aren't currently allocated"

**The proposed rule is already the implemented design.** `_partition_inventory`
([gpu.py:695-769](../videoflow/deploy/gpu.py#L695-L769)) is exactly it: another flow's node →
excluded; time-sliced → excluded; foreign MIG geometry → excluded; **busy → `mig_allowed = False`,
keeping only its free cards for whole-card spanners**. Feasibility is not the question. The
question is whether the rule is *enforced* rather than merely *evaluated* — and today it is
evaluated.

### The races, and which ones are real

**R1 — the stale-freshness window (the significant one).** The occupancy snapshot is taken in
`resolve_specs` at [cli.py:233](../videoflow/deploy/cli.py#L233). The geometry is applied in
`_gpu_prepare` at [cli.py:328](../videoflow/deploy/cli.py#L328). Between them sit the full
preflight, `ensure_infra` and `wait_infra_ready` — NATS and Redis coming up cold, tens of
seconds to minutes. `prepare()` never re-reads occupancy; it works entirely from the cached
`self._layout`. So the "don't touch busy nodes" check is minutes stale at the moment it matters,
and RFC 0004's "accepted residual race" is far wider than the phrase suggests.

*Cheapest real fix, and the one to do first:* re-read `gpu_units_in_use` immediately before
`_label_node_for_mig` and abort if any target node gained units since planning. The owner stamp
already fences other videoflow flows by then, so this only has to catch foreign pods, and
aborting there costs nothing — no geometry has been applied yet.

**R2 — claim-after-plan.** The stamp happens in `prepare()`, *after* the solve. Two flows plan
against the same free pool and discover the conflict only at claim time. Combined with the
TOCTOU in the no-`--overwrite` claim (Q1 #5), the fence is not airtight. Fix is the one already
identified in todo item 20 — `get` → `replace` with a `resourceVersion` precondition, mirroring
`_publish_mig_configmap` ([gpu.py:551-589](../videoflow/deploy/gpu.py#L551-L589)) — plus,
ideally, moving the claim *before* the solve: claim candidates → re-read inventory scoped to
what we actually hold → solve → release unclaimed. That makes the plan atomic with respect to
other videoflow flows, which is what "only modify what you own" actually requires.

**R3 — last-one-out vs. a new flow's prepare.** Flow A reads `remaining` as empty, B publishes
its entries (CAS-safe), then A **deletes the ConfigMap** — the delete carries no precondition
([gpu.py:1276-1277](../videoflow/deploy/gpu.py#L1276-L1277)) — and restores the ClusterPolicy
pointer. B's nodes then resolve their labels against a file that no longer contains their
entries → `state=failed` → B's prepare raises → rollback. Narrow, but it costs a full deploy.
It has a cheaper fix than a distributed lock: **stop deleting the map.** Leave an empty,
plumbing-only ConfigMap behind. The whole race class disappears; the cost is one orphan
ConfigMap.

**R4 — spanner demand has no reservation at all.** Whole-card claims are compared against free
capacity in preflight and then left to the scheduler. Two flows can both pass and both deploy;
the loser's pods sit `Pending`. Unfixable without admission control or DRA, and also the least
harmful failure — nothing is corrupted.

### Can we recover — "winner takes the node, loser ignores it"?

**For videoflow-vs-videoflow: yes, and that semantic already exists** — spelled *abort* rather
than *replan*. `_stamp_node_owners` releases every node it stamped and raises "another deploy
took it between planning and prepare. Re-run the deploy to plan against the remaining pool"
([gpu.py:1038-1048](../videoflow/deploy/gpu.py#L1038-L1048)). The winner keeps the node; the
loser touches nothing.

Turning that into automatic recovery is genuinely easy, because `solve_layout` is **pure,
deterministic and cluster-free** by design: wrap claim → solve → stamp in a bounded retry (say
3 attempts), re-reading the inventory each time. The expensive, irreversible part (the MIG
apply, minutes long) happens strictly after the claim succeeds, so a retry loop costs a few
kubectl round-trips and nothing else. This is the highest-leverage change for Q3.

**For videoflow-vs-foreign-workload: no, and not by this mechanism.** A foreign pod can land on
a claimed node at any instant; the owner label means nothing to the scheduler for pods that do
not select on it, and MIG reconfiguration is destructive and irreversible. Anything better than
"re-verify immediately before applying, then abort" needs either a `ValidatingAdmissionPolicy`
rejecting foreign GPU pods on stamped nodes, or a cordon/taint on the node for the duration of
the apply. A temporary taint is the pragmatic middle ground and fits the existing tolerations
story, since GPU pods already tolerate `nvidia.com/gpu:NoSchedule`. RFC 0004 is right that full
closure needs admission control; a taint around the apply window closes most of it without a
webhook.

### One thing that does not generalize

Applying this rule to a time-slice mode is materially *harder* than for MIG, not easier. The
device-plugin config is a single node-scoped object with no per-flow ownership concept, changing
`replicas` re-advertises `nvidia.com/gpu` for *every* consumer on that node, and there is no
observable convergence signal equivalent to `mig.config.state=success` to wait on. So "only
modify unallocated nodes" maps cleanly onto MIG — geometry per card, ownership per node,
convergence observable — and poorly onto time-slicing. An independent reason to prefer MPS, or
read-only time-slicing, over a videoflow-managed time-slice mode.

---

## Suggested order, if we act

1. `videoflow gpu status` / `gpu release --flow-id` — bounds the damage from every leak path in
   Q1 regardless of which one fires. Also the operator-facing answer to "why is the pool
   shrinking".
2. Q1 #6 — filter `used_units` to GPU resources in `_partition_inventory`. One line, removes a
   silent capacity ratchet on exactly the hardware mix targets.
3. Q3 R1 — re-read occupancy immediately before labeling; abort on change. Closes the widest
   real window at negligible cost.
4. Q3 R2 — `resourceVersion` CAS on the owner stamp (todo item 20), then claim-before-solve with
   a bounded retry, which turns the loser's abort into automatic replanning.
5. Q3 R3 — stop deleting the shared ConfigMap; leave the plumbing-only map behind.
6. Q1 #7 — make the three silence-tolerant reads that gate destructive decisions strict
   (raise/abort instead of degrade), or give each an explicit "unknown" state distinct from
   "empty".
7. Q2 — decide MPS-as-third-mode versus mix-accepts-time-sliced-nodes. Neither is urgent; the
   second is small enough to do opportunistically.
