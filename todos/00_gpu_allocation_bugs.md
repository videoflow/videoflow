# GPU allocation audit — 2026-09-20

**Status: Partially resolved.** Reviewed against core commit `73e3b84`.
**13 findings are resolved, 5 are
partially resolved, and 2 remain valid.** The current audit below supersedes the
status claims in the historical text, which is preserved unchanged at the end.
“Resolved” means the original reported behavior is addressed in the inspected
code and supporting tests; it does not certify every behavior of the subsystem.

This was a code, unit-test, and model/process-test review. **No live cluster,
GPU Operator, or GPU hardware verification was performed.** Findings #1–#7 and
#9 survive only as summaries in the historical fix-order section; their entries
below classify those summaries without reconstructing missing original details.

## Findings

| Item | Status | Current evidence and remaining work |
| --- | --- | --- |
| #1 — ClusterPolicy wiring | **Resolved** | Preparation publishes the generated config, points `migManager.config.name` at it, and waits for the manager to mount it before labeling nodes. See [publication and policy wiring](../videoflow/deploy/gpu.py#L1639), [rollout wait](../videoflow/deploy/gpu.py#L1661), and [wiring regression](../tests/test_mix_strategy.py#L381). |
| #2 — Geometry-true restore | **Resolved** | Cleanup applies disabled geometry before restoring an absent/empty label and waits for restoration evidence. See [restore targets](../videoflow/deploy/gpu.py#L1957), [restore wait and final labels](../videoflow/deploy/gpu.py#L2005), [restore regression](../tests/test_mix_strategy.py#L798), and [empty/absent regression](../tests/test_mix_strategy.py#L1223). The separate concurrency and lifecycle issues below remain. |
| #3 — Pool-scoped, state-aware inventory | **Resolved** | [Inventory selects pool nodes](../videoflow/deploy/cluster.py#L628); [partitioning](../videoflow/deploy/gpu.py#L1231) excludes foreign owners, time-sliced and foreign MIG geometry, and refuses unknown occupancy. Covered by [pool scope](../tests/test_cluster.py#L350), [sharing state](../tests/test_cluster.py#L370), and [foreign-node exclusion](../tests/test_mix_strategy.py#L198) tests. |
| #4 — Absent/empty annotation sentinel | **Resolved** | [`MIG_LABEL_ABSENT`](../videoflow/deploy/gpu.py#L575) and [annotation key-presence checks](../videoflow/deploy/gpu.py#L1833) prevent a retry from recording its own MIG label as the previous value. [Cleanup distinguishes absent from empty](../videoflow/deploy/gpu.py#L2016). The previously requested [second-prepare regression](../tests/test_mix_strategy.py#L474) and [empty/absent restore regression](../tests/test_mix_strategy.py#L1223) now exist. |
| #5 — Concurrent-flow ownership/config cleanup | **Partially resolved** | [Owner CAS](../videoflow/deploy/gpu.py#L1750) and [ConfigMap merge CAS](../videoflow/deploy/gpu.py#L846) are implemented. A remaining last-owner race lets cleanup preserve another flow's entry but disconnect its ConfigMap from ClusterPolicy. See the reproduction below and [stale survivor decision](../videoflow/deploy/gpu.py#L2079). Reopen this item for coordinated policy retirement. |
| #6 — Preflight rejects geometry not yet applied | **Resolved** | [Preflight defers unapplied geometry](../videoflow/deploy/gpu.py#L1440) when a MIG manager can apply it, and reports a problem when none can. Both [manager-absent](../tests/test_mix_strategy.py#L284) and [manager-present](../tests/test_mix_strategy.py#L304) cases are tested. |
| #7 — Free capacity and owner-aware placement | **Resolved** | [Busy nodes contribute only free whole cards](../videoflow/deploy/gpu.py#L1304), the [solver places spanners on a host](../videoflow/deploy/mig.py#L376), and [mix pod affinity](../videoflow/deploy/manifests.py#L605) restricts placement to unowned or this flow's nodes. Covered by [busy-node](../tests/test_mix_strategy.py#L257) and [spanner-capacity](../tests/test_mix_strategy.py#L322) regressions. |
| #8 — Geometry churn and busy-device cleanup | **Partially resolved** | [Cleanup retains geometry while GPU holders remain](../videoflow/deploy/gpu.py#L1970), and the [allocation backend honors `keep_workloads`](../videoflow/deploy/allocation_kubernetes.py#L527). The legacy CLI still [calls cleanup under `--keep`](../videoflow/deploy/cli.py#L751), and batch geometry is still prepared/restored per run. CLI retention and cleanup completion need follow-up; geometry reuse is a separate performance/design decision. |
| #9 — Terminal-state waits | **Partially resolved** | [Apply](../videoflow/deploy/gpu.py#L1124) and [restore](../videoflow/deploy/gpu.py#L1166) waits now check readiness/restoration evidence. However, [an old `failed` state immediately fails a new apply](../videoflow/deploy/gpu.py#L1150). Share the remaining fix and regression with #10; do not count it as a second independent defect. |
| #10 — Failed-state retry deadlock | **Partially resolved** | [Per-run names](../videoflow/deploy/gpu.py#L632) and [fresh nonces](../videoflow/deploy/gpu.py#L1611) fix the unchanged-label deadlock. A delayed manager can still leave the old `failed` state visible long enough for the new apply to fail prematurely. The reproduction and test gap below show why the historical “resolved” label is too broad. |
| #11 — Explicit count versus descriptor memory default | **Resolved** | The implementation chooses the TODO's diagnostic alternative: [provenance-aware resolution](../videoflow/core/remote.py#L192) rejects the contradiction while naming both sources, rather than dropping the descriptor default. The exact combination is covered by [source-aware conflict tests](../tests/test_gpu_provenance.py#L152). |
| #12 — Unreachable cluster gives misleading LayoutError | **Resolved** | [Failed inventory reads remain Unknown](../videoflow/deploy/cluster.py#L628), and [mix raises `UnobservableState`](../videoflow/deploy/gpu.py#L1382) before solving. Covered by the [unreadable-pool regression](../tests/test_mix_strategy.py#L166). The display-only wrapper still folds unknown to an empty list, but is not the planning path. |
| #13 — Stateful strategy singleton | **Partially resolved** | [Explicit plans](../videoflow/deploy/gpu.py#L1371) isolate backend operations, and [failed/CPU-only resolution clears cached state](../videoflow/deploy/gpu.py#L1361). The [registered singleton](../videoflow/deploy/gpu.py#L2273) and [legacy cached `prepare`](../videoflow/deploy/gpu.py#L1539) remain in the CLI path. Two successful interleaved resolutions can apply one flow's plan under another flow's ID; reproduction below. |
| #14 — Label-value length | **Resolved** | [Name generation](../videoflow/deploy/gpu.py#L632) clamps values to 63 characters using a stable digest plus nonce. Covered by the [length/distinctness regression](../tests/test_mix_strategy.py#L356). |
| #15 — Watchdog GPU hint misses MIG | **Still valid** | The watchdog still checks [literal `'gpu'` in scheduler messages](../videoflow/engines/kubernetes.py#L511). `Insufficient nvidia.com/mig-1g.10gb` does not match, and the following remedy remains whole-GPU/time-slicing-specific. Recognize MIG resources and add a focused, mode-appropriate diagnostic regression. |
| #16 — `--gpu-autoscaling` with mix sharers | **Still valid** | The [solver provisions `nb_tasks` slices](../videoflow/deploy/mig.py#L388), while [scaler admission](../videoflow/runtime/scaling.py#L341) has no mix/memory input and [KEDA can exceed that count](../videoflow/deploy/manifests.py#L1056). The [CLI note](../videoflow/deploy/cli.py#L544) only gives a generic Pending-capacity warning. Exclude/cap managed sharers or explicitly explain their fixed planned capacity; no layout re-solve exists. |
| #17 — Optimistic mixed-evidence classification | **Resolved** | [`combine_classifications`](../videoflow/deploy/cluster.py#L536) ranks unknown above physical. The exact physical-plus-unlabeled case is covered by [the mixed-evidence regression](../tests/test_cluster.py#L524). |
| #18 — Unused `memory_gib_per_card` | **Resolved** | [Inventory memory reaches each card](../videoflow/deploy/mig.py#L348) and [the fit check enforces its memory budget](../videoflow/deploy/mig.py#L300). [ALLOC-001](../tests/conformance/test_alloc_planning.py#L71) independently rejects 110 GB of slices on an 80 GB card; its model test passed in this review. |
| #19 — Nondeterministic owner-CAS testing | **Resolved** | Owner-CAS tests now assert the [resourceVersion argument](../tests/test_mix_strategy.py#L591) and exercise a [deterministic competing write](../tests/test_mix_strategy.py#L626), rather than relying on two deploy subprocesses to hit a timing window. The testing lesson still applies to #5's uncovered policy-retirement window. |
| #20 — Owner stamp is not a true CAS | **Resolved** | [Claim writes carry the read resourceVersion](../videoflow/deploy/gpu.py#L1720), and [release checks owner/epoch and uses CAS](../videoflow/deploy/gpu.py#L1770). Covered by [claim-race](../tests/test_mix_strategy.py#L626) and [reclaimed-node release](../tests/test_mix_strategy.py#L652) tests. |

## Remaining defects and reproduction evidence

### #5: a new owner can arrive after the last-owner read

Using the existing in-process MIG cluster fake, apply flow A on `gpu-a` and plan
flow B on `gpu-b`. Wrap `_read_mig_configmap_yaml` so A's cleanup reads its current
map, B applies and publishes its entry, then the wrapper returns A's earlier
snapshot. Resume A's cleanup.

Observed result: B still owns `gpu-b` and its ConfigMap entry survives the CAS
merge, but ClusterPolicy points to `default-mig-parted-config` and the shared map
has a tombstone despite B being live. The [survivor decision](../videoflow/deploy/gpu.py#L2079)
is not synchronized with [policy restoration](../videoflow/deploy/gpu.py#L2119)
and [tombstoning](../videoflow/deploy/gpu.py#L2125).

The existing [ALLOC-005 test](../tests/conformance/test_alloc_mig_lifecycle.py#L233)
pauses before the map read, so its passing result does not cover this later
interleaving. The remaining fix must coordinate new publications with retirement
of the shared policy pointer; preserving entries by CAS alone is insufficient.

### #8: safe busy-node guard, incomplete CLI lifecycle integration

The [busy-holder regression](../tests/test_mix_strategy.py#L1253) and
[ALLOC-013 model](../tests/conformance/test_alloc_ownership.py#L701) confirm that
running GPU holders preserve geometry and ownership. The original claim that
cleanup blindly restores under a running holder is therefore obsolete.

However, legacy BATCH `--keep` still invokes cleanup, without conveying the
backend's `keep_workloads` intent. Explicit teardown [requests deletion](../videoflow/deploy/cli.py#L1367)
and then [calls cleanup](../videoflow/deploy/cli.py#L1398); it has no explicit
holder-drain/retry loop. If holders remain, cleanup warns and requires a later
teardown. Wire retained-workload intent through this path and make completion or
pending recovery explicit. Retaining/reusing geometry between separate completed
batch runs remains an additional design choice, not an implemented optimization.

### #9/#10: a stale failure is attributed to a new nonce

Initialize the existing MIG fake with `mig.config.state=failed` and a delayed
manager, then apply a fresh plan. The new nonce is written, but apply raises
“the MIG manager reported state=failed” while the fake manager is still pending.
Advancing the manager afterward produces `success` and the requested slice.
Thus the nonce fix landed, but failure observation still lacks correlation with
the manager's processing of the new configuration.

The [unit retry test](../tests/test_mix_strategy.py#L775) supplies immediate
`success`. More significantly, the [ALLOC-003 stale-failure assertion](../tests/conformance/test_alloc_mig_lifecycle.py#L118)
ends in `or True`, making that assertion unconditional. Correct the failure
observation and replace this ineffective guard with a delayed-manager regression
that proves the old failure does not abort the new attempt.

### #13: successful legacy calls still share the last plan

With mocked inventory, use the registered `mix` strategy to resolve A on
`gpu-a`, resolve B on `gpu-b`, then call `prepare(flow_id='flowA')`. Capturing
the `apply_plan` arguments shows `prepared_for='flowA'`, `plan_for='flowB'`,
and `mig_nodes=['gpu-b']`. This requires no cluster mutation to demonstrate.

The CLI still [resolves through the registry](../videoflow/deploy/cli.py#L517)
and later [prepares through the cached strategy](../videoflow/deploy/cli.py#L698).
[ALLOC-011](../tests/conformance/test_alloc_mig_lifecycle.py#L595) checks failed
and CPU-only cache clearing, then interleaves explicit plans; it does not test
two successful interleaved legacy resolution/prepare sequences. Finish passing
explicit plans through the legacy path or isolate strategy state per deployment,
and add that successful-interleaving regression.

### #15/#16: diagnostics and autoscaling remain reproducible

The watchdog expression evaluates false for
`Insufficient nvidia.com/mig-1g.10gb`. Separately, a one-replica mix sharer produces
one planned `nvidia.com/mig-1g.10gb` slice, no autoscaling admission rejection
with GPU autoscaling enabled, and a ScaledObject with `maxReplicaCount=4` when
four is requested. These in-memory checks confirm the remaining paths without
claiming a live KEDA or GPU scheduling test.

## Validation and limits

The following commands were run during the read-only audit; they were not rerun
while adding these annotations:

```sh
.venv/bin/python -m pytest -q tests/test_mix_strategy.py tests/test_cluster.py tests/test_mig_solver.py tests/test_gpu_provenance.py tests/test_gpu_lifecycle_cli.py tests/test_allocation_kubernetes.py tests/test_unknown_states.py
# 198 passed in 2.85s

.venv/bin/python -m pytest -q tests/conformance/test_alloc_mig_lifecycle.py tests/conformance/test_alloc_ownership.py -m 'level_model or level_process'
# 13 passed, 19 deselected in 2.87s

.venv/bin/python -m pytest -q tests/conformance/test_alloc_planning.py -m level_model
# 7 passed, 5 deselected in 1.06s
```

Additional in-memory checks reproduced #5, #9/#10, #13, #15, and #16 as described
above. Passing selected tests does not close the uncovered interleavings or the
ineffective assertion. Cluster/hardware cases were not run; the conformance
catalog's unrun cases are not evidence of a verified live deployment. No code
fixes are included in this audit.

## Historical notes (preserved)

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

## Design issues

### 8. Geometry churn and busy-device restores
prepare/cleanup run per deploy (BATCH `finally`, [cli.py:376](videoflow/deploy/cli.py#L376)),
so back-to-back batch runs repartition + restore every time — each MIG
reconfiguration evicts GPU clients and takes minutes. Worse: under `--keep`
(and in `_cmd_teardown`, which doesn't wait for pod termination), cleanup
relabels while pods still hold the devices — the restore apply either fails
silently or fights the kept workloads.


### 10. Failed-state retry deadlock — **resolved**
prepare labels `videoflow-<node>`; if a previous attempt left that exact label
with `state=failed`, relabeling with the same value is a no-op for the manager
(it reacts to label *changes*), so prepare re-reads `failed` and raises forever
— even after the operator fixed the config. Needs a per-run nonce in the config
name/label, or clear-then-set.

**Resolution:** per-run nonce in the entry name/label
(`videoflow-<node>-<nonce>` via `_mig_config_name`, 63-char-clamped — which
also fixes #14). Names are no longer reconstructible, so prepare stamps each
claimed node's entry name in `videoflow.io/mig-entry` before publishing (an
orphan's entry stays strippable), the merge drops the previous attempt's
entries for this flow's own nodes, and cleanup reads its entries off node
labels/annotations. The cleanup twin (retried teardown rewriting the same
restore target against `state=failed`) is bounced through a nonce'd
`videoflow-all-disabled-<nonce>` alias; aliases never count against
last-one-out. RFC 0004 amended ("Per-run entry names").

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
13. **Stateful singleton strategy** — `register_gpu_mode(MixGpu())` shares
    `_layout` (and now `_excluded_nodes`) across every deploy in a process; a
    stale layout can drive preflight/prepare for a different flow in
    library/long-lived use. (`flow_id` is deliberately a per-call parameter,
    not cached, for exactly this reason.)
14. ~~**Label-value length**~~ — **done** with #10: `_mig_config_name` clamps
    to 63 chars (truncated node prefix + stable hash + nonce).
15. **Watchdog GPU hint misses MIG** —
    [kubernetes.py:399](videoflow/engines/kubernetes.py#L399) matches
    `'gpu' in message`; "Insufficient nvidia.com/mig-1g.10gb" doesn't contain
    "gpu", and the hint text is exclusive-specific.  kubernetes.py:474 matches 'gpu' in message, and Insufficient nvidia.com/mig-1g.10gb contains no gpu, so the watchdog hint never fires for MIG.
16. **`--gpu-autoscaling` × mix** — KEDA can scale a sharer past `nb_tasks`, but
    the layout provisioned exactly `nb_tasks` slices; extra replicas can never
    schedule (no re-solve). Exclude sharers or say so in the NOTE.
17. **`classify_gpu_resource` optimistic on mixed evidence** — one GFD-labeled
    physical node + one unlabeled advertiser → `'physical'`, no warning
    ('unknown' should arguably taint like time-sliced does).
18. **`NodeInventory.memory_gib_per_card` is dead** — collected, never read by
    the solver.
19. The testing lesson matters more than the bug: a cluster test spawning two videoflow deploy subprocesses can never reliably hit a 2 ms window, so it would pass on the broken code and be cited as proof the CAS works. The right test is a unit test asserting the argv carries a resourceVersion precondition.
20. gpu.py:1014-1022 documents _stamp_node_owners as a compare-and-swap: "the label is applied WITHOUT --overwrite, so losing a race to a concurrent deploy fails the label command instead of silently stealing the node."

I verified with kubectl -v=8: kubectl label does a GET, then an unconditional merge patch with no resourceVersion precondition (PATCH …?fieldManager=kubectl-label, body {"metadata":{"labels":{…}}}). On an already-labelled node it sends zero PATCH requests — the check is entirely client-side.

To be precise, and correcting the reviewer's stronger claim: kubectl's own GET happens ~2 ms before its PATCH, so this is a narrow TOCTOU window, not an absence of protection. But it is not the CAS the docstring claims, and multi-tenant node ownership is the one place the difference matters. The fix already exists 450 lines above in the same file — _publish_mig_configmap does get → replace carrying resourceVersion → retry on conflict (gpu.py:574-587). ~15 lines.

## Verification notes (Sep 2026 — backend contracts + conformance suite)

The conformance suite (`tests/conformance/`, catalog case ids below) and the
Unknown-is-not-zero work landed executable checks for several items above. Nothing
here closes an item that is not marked done; it records what now guards it.

- **#12 (unreachable cluster → misleading LayoutError)** — `cluster.gpu_inventory_observed`
  returns `Unknown` when the node listing fails and `MixGpu.resolve_specs` raises
  `UnobservableState` ("the GPU pool could not be listed") instead of solving
  against `[]`; the display-only `gpu_inventory()` still folds to `[]`. Covered by
  **ALLOC-007** (`test_alloc_ownership.py`: `mix-strategy` variant; primary needs the
  k3s cluster) and `tests/test_unknown_states.py`.
- **#17 (`classify_gpu_resource` optimistic on mixed evidence)** —
  `cluster.classify_gfd_labels` + `combine_classifications`: an unlabeled advertiser
  next to a labeled one now yields `unknown`, MPS is recognised, and MIG-capable +
  `all-disabled` is physical; classification reads pool nodes only, from the same
  snapshot as capacity. Covered by **ALLOC-008** (`kubectl-fake` variant) and
  **ALLOC-009** (`test_alloc_planning.py`, negative control included).
- **#18 (`memory_gib_per_card` dead)** — read by `_Card.fits` in `deploy/mig.py`
  (compute slices and memory are separate budgets). Covered by **ALLOC-001** and
  `tests/test_mig_solver.py`.
- **#19 / #20 (the owner stamp is not a CAS)** — `_stamp_node_owners` now writes
  `videoflow.io/gpu-owner` + `videoflow.io/gpu-owner-epoch` with
  `kubectl label --resource-version=<rv>` from the read that found the node unowned,
  and raises `OwnershipConflict` on a 409; `_release_owner` is the same CAS and only
  strips the exact (owner, epoch) claim. Per #19 the guard is a unit test asserting the
  argv carries the precondition, against `tests/support_kubectl.FakeKubectl`, which
  answers 409 to a stale resourceVersion — **ALLOC-007** and `tests/test_mix_strategy.py`.
  Verified live against the k3s cluster once, by hand.
- **Tombstone instead of last-one-out delete** (decision D3) — the shared
  `videoflow-mig-parted-config` is never deleted; the last flow out strips its entries
  by CAS and annotates `videoflow.io/mig-config-tombstone`; the operator deletes it.
  `tests/test_mix_strategy.py` cleanup assertions were changed deliberately for this.
- **Per-host packing** (not an item above, but the same family) — `gpu.pack_pod_claims`
  places every pod's claim on per-node free counts in the exclusive preflight, so
  free `[3, 3]` against three `gpu_count = 2` replicas is reported. **ALLOC-010**,
  `tests/test_gpu_packing.py`.
- Still open as written: #8, #4, #11, #13, #15, #16.

## Suggested fix order (if/when we act)

1. ~~#1 + #2 + #9 together~~ — **done** (ClusterPolicy wiring, geometry-true
   restore, terminal-state waits on both sides).
2. #4 — the node-annotation sentinel (`''` is both "absent" and "empty"; needs
   `__absent__` or a key-presence check). Still open — the ~~#5~~ concurrency
   half is done (per-flow `videoflow.io/gpu-owner` node stamps, merged
   ConfigMap with resourceVersion CAS, last-one-out policy restore), which also
   shrinks #4's window: a second flow no longer touches a foreign node at all,
   so the double-record now needs a retried prepare of the *same* flow.
3. ~~#3 + #7~~ — **done** (pool-scoped, occupancy- and state-aware inventory:
   `gpu_inventory`/capacity reads select `videoflow.io/gpu-pool=true`;
   `_partition_inventory` excludes other flows' / time-sliced / pre-MIG'd
   nodes and shrinks busy ones with MIG disallowed; mix preflights spanner
   demand against free whole-device capacity; mix pods carry owner-aware
   nodeAffinity).
4. ~~#6~~ — **done** (mix preflight treats unapplied geometry as informational
   when a MIG manager is present to apply it — prepare() runs right after — and
   keeps it a blocking problem carrying the by-hand config only when no manager
   is present).
5. The smaller items opportunistically.

## Verification

- Unit: `tests/test_mix_strategy.py` models the ClusterPolicy indirection, the
  multi-tenant partitioning rules, the CAS ownership stamps, concurrent-flow
  ConfigMap merge/strip and last-one-out, the per-run nonce names (#10: a
  stuck-failed node relabels under a fresh value; stale entries dropped from
  the merge; the cleanup bounce; orphan strip via `videoflow.io/mig-entry`;
  the 63-char clamp); `tests/test_cluster.py` covers the pool-scoped reads and
  occupancy accounting. Still to add: a second prepare without cleanup (#4).
- End-to-end: needs a real GPU Operator + MIG-capable cluster; the dev k3s box
  (time-sliced, no GPU Operator) cannot validate mix — stated in RFC 0004 as
  its test gap (multi-flow concurrency included).
