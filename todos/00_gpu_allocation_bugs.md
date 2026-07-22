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
