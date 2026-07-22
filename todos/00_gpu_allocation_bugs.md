# GPU allocation review — feat/multi-gpu (exclusive + mix modes)

## Context

Review of the two-mode GPU design (RFC 0004) as implemented on `feat/multi-gpu`:
`deploy/gpu.py` (strategies), `deploy/mig.py` (layout solver), `deploy/cluster.py`
(inventory/classification), `deploy/manifests.py` + `deploy/cli.py` (demand math,
preflight, prepare/cleanup lifecycle), `core/node.py`/`core/remote.py`/
`components/descriptor.py` (demand vocabulary), `engines/{local,kubernetes}.py`.
The deliverable is the findings below; fixes are a follow-up decision.

**Overall:** the strategy-registry shape, the exclusive mode (incl. the
IMPOSSIBLE_GPU_REQUEST classification), the node-API validation, and the local
engine's degradation story are solid. The problems concentrate in mix mode's
interaction with the *real* cluster: how geometry is applied, how it is undone,
and what the solver believes about the inventory. None of the findings below are
in `todos/01_small_bugs.md`. Docs (README, gpu-sharing.rst, RFC) match the code.

## Critical — mix mode cannot work / damages cluster state

### 1. The mig-parted ConfigMap is never wired to the MIG manager — **RESOLVED**
*Fixed together with #2/#9 (2026-07-22): `prepare()` now merges the generated
entries into the operator's current mig-parted config, publishes the merge as
`videoflow-mig-parted-config`, records the original `migManager.config.name` in
the `videoflow.io/mig-config-name-restore` ClusterPolicy annotation, patches the
policy to name videoflow's ConfigMap, and waits for the mig-manager DaemonSet
rollout before labeling nodes. Still needs verification on a real MIG-capable
GPU Operator cluster (see the recipe amended into RFC 0004) — unit fakes model
the indirection but cannot prove it.*

Original finding:
`MixGpu.prepare()` ([gpu.py:350-405](videoflow/deploy/gpu.py#L350)) creates a
*new* ConfigMap `videoflow-mig-parted-config` in the manager's namespace and sets
`nvidia.com/mig.config=videoflow-<node>`. But the GPU Operator's mig-manager
DaemonSet reads only the config file mounted from the ConfigMap named in
ClusterPolicy `migManager.config.name` (default `default-mig-parted-config`). A
side-created ConfigMap is never mounted, so the manager looks up config
`videoflow-<node>` in *its own* file, fails ("selected config not present"),
sets `mig.config.state=failed`, and `prepare()` raises on every stock GPU
Operator cluster. The unit tests (test_mix_strategy.py) fake kubectl and assert
exactly this unwired interaction, so they can't catch it.
**Fix direction:** patch ClusterPolicy `migManager.config.name` to the videoflow
ConfigMap (and wait for the DaemonSet remount), or merge videoflow's configs into
the operator's existing ConfigMap (recording the original for cleanup). Verify
against a real GPU Operator cluster; unit fakes cannot.

### 2. cleanup() restores the label, not the geometry — **RESOLVED**
*Fixed with #1/#9 (2026-07-22): a node whose pre-videoflow label was absent is
now pointed at the merged-in `videoflow-all-disabled` config, cleanup waits for
`state=success`, and only then removes the label.*

Original finding: [gpu.py:420-458](videoflow/deploy/gpu.py#L420). When the pre-videoflow
`nvidia.com/mig.config` label was **absent** (the common case), cleanup removes
the label — but label *removal* triggers nothing in the mig-manager, so the cards
stay MIG-partitioned forever. GFD then advertises MIG resources and a reduced
`nvidia.com/gpu.count`, which breaks the *next* deploy (see #3). Restoring
"absent" requires applying an all-disabled config, waiting for `state=success`,
then removing the label.

### 3. The solver's inventory model: all nodes, pristine, exclusively owned
`solve_layout` ([mig.py:196](videoflow/deploy/mig.py#L196)) +
`gpu_inventory` ([cluster.py:353](videoflow/deploy/cluster.py#L353)):
- **No pool filter.** `gpu_inventory` reads every GFD-labeled node, but GPU pods
  carry a `videoflow.io/gpu-pool=true` nodeSelector. The solver can plan slices
  on a non-pool node → `prepare()` **repartitions a node videoflow was never
  given** (destructive in shared clusters) and the slices it creates are
  unschedulable for videoflow's own pods → flow deadlocks Pending while
  preflight passes (`allocatable_gpus` ignores the pool label too).
- **Blind to current MIG state.** Inventory is `gpu.count`/`gpu.product`; on a
  partially-MIG'd node `gpu.count` counts only non-MIG cards, so the solver's
  `card_index` 0..count-1 no longer maps to physical device positions and the
  emitted `devices: [i]` entries can partition the *wrong cards*. Because of #2,
  every second mix deploy runs against exactly this state.
- **Time-sliced nodes pass as MIG-able.** `MigTable.match_substrings`
  (`['A100','80GB']`) matches a `...-SHARED` product, and nothing in mix mode
  runs `classify_gpu_resource`. (Relevant: the dev k3s cluster is time-sliced.)
- **Occupancy ignored.** Physical cards already used by other workloads (or
  another flow) are planned as free; `status.allocatable` isn't usage-aware
  either. Single-tenant assumption is nowhere stated or enforced.

### 4. Retried prepare corrupts the restore record
`_label_node_for_mig` ([gpu.py:407-418](videoflow/deploy/gpu.py#L407)) records
"label was absent" as annotation value `''`, and its only-record-once guard is
`if not recorded:` — but the jsonpath read returns `''` both for "no annotation"
and "annotation = empty". Any second prepare without an intervening cleanup
(second mix flow, redeploy after a SIGKILL'd deploy) re-records the **current**
`videoflow-<node>` label as the "previous" value; cleanup then "restores" the
videoflow label and the geometry becomes permanent. Needs a distinct sentinel
(e.g. `__absent__`) or a key-presence check via `get -o json`.

## Design issues

### 5. Two concurrent mix flows corrupt each other
Fixed shared names: one ConfigMap (second deploy **overwrites** it — each
generated config covers only its own layout's nodes), same `videoflow-<node>`
config names, same restore annotation; plus the solver double-books inventory
(no notion of geometry already owned by a live flow). With #4, tearing down flow
B can permanently pin flow A's geometry. Minimum fix: refuse `prepare()` when
`MIG_RESTORE_ANNOTATION` already exists on a target node (someone else owns the
geometry); better: namespace config/annotation by flow id and merge configs.

### 6. mix + `--strict-preflight` can never deploy
Preflight runs before prepare ([cli.py:225](videoflow/deploy/cli.py#L225) vs
[cli.py:310](videoflow/deploy/cli.py#L310)), and on a fresh cluster
`MixGpu.preflight_problems` ([gpu.py:318-326](videoflow/deploy/gpu.py#L318))
always reports "geometry is not applied yet" — so strict mode aborts every first
deploy, and non-strict prints a scary WARNING on the happy path. When a MIG
manager is present this shouldn't be a problem string at all (prepare is about
to fix exactly that).

### 7. mix silently drops the whole-device preflight
`MixGpu.preflight_problems` checks only `layout.slice_demand`; spanner demand
(`nvidia.com/gpu`) is never compared against what the device plugin actually
advertises. GFD present + device plugin broken/absent: exclusive mode reports
"no node advertises nvidia.com/gpu"; mix deploys and hangs Pending until the
runtime watchdog fires.

### 8. Geometry churn and busy-device restores
prepare/cleanup run per deploy (BATCH `finally`, [cli.py:376](videoflow/deploy/cli.py#L376)),
so back-to-back batch runs repartition + restore every time — each MIG
reconfiguration evicts GPU clients and takes minutes. Worse: under `--keep`
(and in `_cmd_teardown`, which doesn't wait for pod termination), cleanup
relabels while pods still hold the devices — the restore apply either fails
silently or fights the kept workloads.

### 9. cleanup() doesn't wait or verify — and burns its retry state — **RESOLVED**
*Fixed with #1/#2 (2026-07-22): cleanup waits for `state=success` per node and
keeps the node annotation, the ClusterPolicy patch and the ConfigMap when any
node fails to revert, so a retried `teardown --gpu-mode mix` resumes.*

Original finding: [gpu.py:440-451](videoflow/deploy/gpu.py#L440): sets the restore label, then
immediately drops the annotation and deletes the ConfigMap. If the restore apply
fails (#8), nobody notices (`state` never checked) and the annotation is gone,
so no later run can retry the restore. Should wait for `state=success`
(mirroring prepare) before dropping the annotation.

### 10. Failed-state retry deadlock
prepare labels `videoflow-<node>`; if a previous attempt left that exact label
with `state=failed`, relabeling with the same value is a no-op for the manager
(it reacts to label *changes*), so prepare re-reads `failed` and raises forever
— even after the operator fixed the config. Needs a per-run nonce in the config
name/label, or clear-then-set.

## Smaller items

11. **Explicit `gpu_count` vs descriptor `memoryGiB`** —
    [remote.py:225-226](videoflow/core/remote.py#L225): explicit `gpu_count=2` +
    descriptor `memoryGiB: 20` keeps both; `ProcessorNode.__init__` then raises
    "mutually exclusive" naming a parameter the user never passed. An explicit
    spanning count should drop the descriptor's memory *default* (symmetric with
    the cpu-flavor drop at remote.py:231-236), or the error should name the
    descriptor as the source.
12. **Unreachable cluster → misleading LayoutError** — `resolve_specs` runs
    before preflight's reachability check ([cli.py:216](videoflow/deploy/cli.py#L216));
    `gpu_inventory` returns `[]` on any kubectl failure, so the operator is told
    to "install GFD" when the cluster is simply unreachable.
13. **Stateful singleton strategy** — `register_gpu_mode(MixGpu())`
    ([gpu.py:506](videoflow/deploy/gpu.py#L506)) shares `_layout` across every
    deploy in a process; a stale layout can drive preflight/prepare for a
    different flow in library/long-lived use.
14. **Label-value length** — `nvidia.com/mig.config=videoflow-<node>`: label
    values cap at 63 chars, node names go to 253 (EC2 FQDNs) → `kubectl label`
    fails mid-prepare. Truncate/hash.
15. **Watchdog GPU hint misses MIG** —
    [kubernetes.py:399](videoflow/engines/kubernetes.py#L399) matches
    `'gpu' in message`; "Insufficient nvidia.com/mig-1g.10gb" doesn't contain
    "gpu", and the hint text is exclusive-specific.
16. **`--gpu-autoscaling` × mix** — KEDA can scale a sharer past `nb_tasks`, but
    the layout provisioned exactly `nb_tasks` slices; extra replicas can never
    schedule (no re-solve). Exclude sharers or say so in the NOTE.
17. **`classify_gpu_resource` optimistic on mixed evidence** — one GFD-labeled
    physical node + one unlabeled advertiser → `'physical'`, no warning
    ('unknown' should arguably taint like time-sliced does).
18. **`NodeInventory.memory_gib_per_card` is dead** — collected, never read by
    the solver.

## Suggested fix order (if/when we act)

1. ~~#1 + #2 + #9 together~~ — **done** (ClusterPolicy wiring, geometry-true
   restore, terminal-state waits on both sides).
2. #4 + #5 — ownership/idempotency of the restore record (sentinel + refusal on
   foreign annotation). Small, self-contained, prevents permanent cluster damage.
   Note: the #1 fix added annotate-only-if-absent guards on the *ClusterPolicy*
   record, but that is incidental — the node-annotation sentinel (#4) and the
   concurrency story (#5) remain open.
3. #3 — filter `gpu_inventory` to `videoflow.io/gpu-pool=true` (one-line
   selector) and refuse/flag nodes with existing MIG state or `-SHARED`
   products; add the spanner-capacity check to mix preflight (#7).
4. #6 — reclassify "not applied yet + manager present" as informational.
5. The smaller items opportunistically.

## Verification

- Unit: extend `tests/test_mix_strategy.py` fakes to model the ClusterPolicy
  indirection (assert the operator's ConfigMap/ClusterPolicy is patched, not a
  side ConfigMap), a second prepare without cleanup (#4), and cleanup-with-
  absent-previous asserting an all-disabled apply + wait (#2).
- Solver: `tests/test_mig_solver.py` cases for pool-filtered inventory and
  pre-MIG'd/`-SHARED` nodes (#3).
- End-to-end: needs a real GPU Operator + MIG-capable cluster; the dev k3s box
  (time-sliced, no GPU Operator) cannot validate mix — worth stating in the RFC
  as its test gap.
