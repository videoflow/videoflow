# Testing GPU workflows and GPU sharing without GPUs

Question: can we exercise videoflow's GPU paths — including `mix`-mode sharing — on a
Kubernetes cluster with no physical GPUs?

Answer: yes, further than expected. Everything below was **measured** on the live
`kind-videoflow` cluster (server v1.36.1, containerd 2.3.1, Docker VM x86_64 / 16 CPU /
7.75 GiB), not reasoned about. Every probe reverted itself; the cluster was verified clean
afterwards.

The headline result: the full `mix` lifecycle — `resolve_specs → prepare → cleanup`,
including the ClusterPolicy indirection, the ConfigMap merge, the nonce'd label protocol
and the last-one-out restore — **ran to completion in ~6 seconds with no GPU anywhere**.
[todos/00_gpu_allocation_bugs.md:112-114](../todos/00_gpu_allocation_bugs.md#L112) says that
path "needs a real GPU Operator + MIG-capable cluster". It does not.

The catch, which shapes the whole plan: a fake cluster is **more self-consistent than
reality**, so some tests pass on broken code. One already does — see §7.

---

## 1. What was measured

### 1.1 Labels alone carry videoflow's entire model of the world

Everything `deploy/cluster.py` uses to *build* its picture of the pool comes from node
labels, which `kubectl label` sets freely on any node. No device plugin, no GFD, no
hardware.

| What videoflow decides | Driven by | Observed |
|---|---|---|
| `gpu_inventory()` — the mix solver's whole input | `nvidia.com/gpu.{product,count,memory}` | `NodeInventory(product='NVIDIA-A100-SXM4-40GB', card_count=4, memory_gib_per_card=40.0)` |
| `classify_gpu_resource` → `physical` | GFD labels present, no sharing signals | ✔ |
| → `time-sliced` | `gpu.replicas>1` / `-SHARED` product suffix / `sharing-strategy=time-slicing` | ✔ |
| → `mig` | `mig-` resource-name prefix, or `mig.strategy=single` + `mig.capable=true` | ✔ |
| Pool membership / multi-tenant exclusion | `videoflow.io/gpu-pool`, `videoflow.io/gpu-owner` | ✔ |
| MIG apply protocol | `nvidia.com/mig.config` ⇄ `nvidia.com/mig.config.state` | ✔ |

The admission gate is strict and worth knowing when fabricating: a node contributes nothing
to `gpu_inventory` unless it has a non-empty `nvidia.com/gpu.product` **and** a digit-string
`nvidia.com/gpu.count >= 1` ([cluster.py:482-486](../videoflow/deploy/cluster.py#L482)).

### 1.2 Where labels stop, and the one patch that closes it

`status.allocatable` is not a label. The documented mechanism is a node-status patch:

```bash
# add / update (merge form — no ~1 escaping, idempotent, multi-key)
kubectl patch node <node> --subresource=status --type=merge \
  -p '{"status":{"capacity":{"nvidia.com/gpu":"4","nvidia.com/mig-1g.5gb":"14"}}}'

# remove
kubectl patch node <node> --subresource=status --type=merge \
  -p '{"status":{"capacity":{"nvidia.com/gpu":null}}}'
```

`--subresource` is GA in kubectl v1.33 (alpha 1.24, beta 1.27); this box has client v1.36.1.
JSON-Patch form also works, but needs `/` escaped as `~1` (`nvidia.com~1mig-1g.5gb` — only
the slash, not the dots).

### 1.3 The scheduler treats the fabrication as completely real

| Probe | Result |
|---|---|
| Pod requesting 2 fabricated units | **Running**, bound to the node |
| Pod requesting 99 | **Pending**: `0/1 nodes are available: 1 Insufficient nvidia.com/gpu.` |
| `gpu_units_in_use` accounting | `allocatable=4 in_use=2 free=2` |

That Pending message is the exact string [engines/kubernetes.py:474](../videoflow/engines/kubernetes.py#L474)
matches on, so the unschedulable-pod watchdog is testable for real. No component anywhere
verifies a device exists.

### 1.4 The whole preflight decision tree, driven from tags

```
physical pool (4 x A100-40GB)
  classify                 -> physical
  preflight demand=3,pp=1  -> CLEAN
  preflight demand=9,pp=1  -> "flow demands 9 x nvidia.com/gpu but the cluster has only 4 allocatable"
  preflight demand=8,pp=8  -> total-capacity error AND the per-node bound error

time-sliced pool (labels only: -SHARED, replicas=4, sharing-strategy)
  classify                 -> time-sliced
  preflight pp=2           -> FATAL  impossible GPU request: ... units are shares of a device

MIG pool (capacity nvidia.com/mig-1g.5gb: 14)
  classify                 -> mig
  preflight pp=2           -> FATAL  impossible GPU request: ... hardware-isolated partitions
```

### 1.5 The mix solver runs against the fabricated inventory

`MixGpu.resolve_specs` on a graph of `detector(nb_tasks=4, gpu_memory_gib=5)` +
`captioner(gpu_count=2)` produced a real layout — two whole cards reserved for the spanner,
card 2 partitioned into 4×`1g.5gb`:

```yaml
version: v1
mig-configs:
  videoflow-videoflow-control-plane:
    - devices: [0]
      mig-enabled: false
    - devices: [1]
      mig-enabled: false
    - devices: [2]
      mig-enabled: true
      mig-devices:
        1g.5gb: 4
    - devices: [3]
      mig-enabled: false
```

### 1.6 The full `mix` lifecycle, against a faked GPU Operator

Faked: a minimal `clusterpolicies.nvidia.com` CRD (`x-kubernetes-preserve-unknown-fields`),
a `ClusterPolicy` CR, the stock `default-mig-parted-config` ConfigMap, a busybox DaemonSet
labelled `app=nvidia-mig-manager` mounting the videoflow ConfigMap (`optional: true`), and a
~20-line bash controller reacting to `nvidia.com/mig.config` label changes.

```
--- prepare() ---   *** COMPLETED in 4.1s ***
--- cleanup() ---   *** COMPLETED in 1.7s ***

ClusterPolicy pointer restored     -> default-mig-parted-config
videoflow ConfigMap deleted        (last one out)
operator's stock ConfigMap         survived the merge intact
node labels / restore annotations  removed

fake manager log:
  observed mig.config=videoflow-videoflow-control-plane-119e7d   -> success
  observed mig.config=videoflow-all-disabled                     -> success
```

The two label transitions are exactly the real protocol, per-run nonce included.

---

## 2. Three mechanics that will bite the implementer

1. **~10 s lag from capacity to allocatable** (measured 8 s). Kubelet recomputes allocatable
   from capacity once per node-status cycle (`nodestatus/setters.go:310-321`, default 10 s).
   A harness that patches and proceeds is flaky — **poll for allocatable**. During the window
   `gpu_inventory` (label-based) already reports 4 cards while `classify_gpu_resource`
   (allocatable-based) still returns `unknown` and preflight says "no node advertises
   nvidia.com/gpu".

2. **Patch `capacity`, never `allocatable`.** `setters.go:302-309` deletes any extended
   resource in allocatable that is absent from capacity, within one cycle.

3. **It survives the periodic loop, but not kubelet re-registration.** Measured stable over
   45 s; kubelet mutates the map it read from the API server rather than rebuilding it. The
   real exposure is a one-shot zeroing at kubelet registration — i.e. `docker restart` of the
   node container. Mitigation: make the patch idempotent in `kind-up.sh`.

And one that limits what can ever be proven:

4. **No `CUDA_VISIBLE_DEVICES` / `NVIDIA_*` reaches the container.** That injection is the
   device plugin's `Allocate` response. The scheduling half of RFC 0003 is testable this way;
   the worker-side visibility contract is not.

---

## 3. The alternatives

| Option | Fidelity | Cannot prove | Cost | Verdict |
|---|---|---|---|---|
| **A. Labels + capacity patch** on the existing node | Real scheduler, real Pending, real occupancy, whole preflight tree, real solver input | No device, no CUDA env | ~1 day; **verified working today** | Take it. Best coverage-per-line |
| **B. Fake MIG operator** (CRD + CR + stub DaemonSet + controller) | Whole `mix` prepare/cleanup, ConfigMap merge, ClusterPolicy restore, last-one-out, nonce protocol | MIG placement rules; anything needing a slow apply | ~2-3 days; prototype exists (§8) | Take it — after justifying `mix` (§6) |
| **C. kwok fake nodes** | Multi-node topologies, disposable per-test nodes, `max_free_on_node` bounds | **Runs no containers at all** | ~½ day | Yes, for classification/topology only |
| **D. run:ai / NVIDIA `fake-gpu-operator`** | Real gRPC device plugin, injects env, bind-mounts a fake `nvidia-smi` into pods, DCGM metrics, RuntimeClass | **Ships the ClusterPolicy CRD but no CR, and no mig-manager** — misses 5 of the 6 things `mix` needs | ~1 day to trial | Only for the worker-side RFC 0003 contract |
| **E. Multi-node kind** | Genuine per-node placement | — | **Not free — see below** | Caution |
| **F. Custom device plugin** | The only way to inject `CUDA_VISIBLE_DEVICES` ourselves | — | Days of Go | Skip; D gives this free |

**On kwok** (v0.8.0): it is a controller that writes `status` on API objects, nothing more.
Extended resources on a fake Node work via plain `kubectl apply` (the Node API accepts status
on CREATE; kwok's `node-initialize` stage copies allocatable/capacity through untouched and
the heartbeat stage never mentions them). Two traps:

- Mutating a fake node's status *after* creation silently no-ops — `nodeStrategy.PrepareForUpdate`
  does `newNode.Status = oldNode.Status`. Use the status subresource.
- **Jobs auto-`Succeeded`, Deployments auto-`available`.** videoflow renders BATCH nodes and
  the provision init as Jobs ([manifests.py:535,640](../videoflow/deploy/manifests.py#L535)),
  so a kwok-backed end-to-end test is a guaranteed false green. This needs to be a written
  rule in `tests/integration/README.md`, not a footnote.

**On `fake-gpu-operator`**: MIT, ~296★, *actively* maintained (last commit 2026-07-28; NVIDIA
has absorbed it post-acquisition). It fakes a real device plugin per resource name, GFD-style
labels, DCGM series on a `nvidia-dcgm-exporter:9400` Service, and a 259-line Go `nvidia-smi`
bind-mounted into pods at Allocate time. But it ships the ClusterPolicy **CRD only, no CR**
(`test/helm/mock_backend_test.sh:87` asserts its absence) and no mig-manager — so it covers
requirement 6 of `mix` and none of 1-5.

**On multi-node kind — the trap.** `kind` only strips the control-plane taint on *single*-node
clusters (`kubeadminit/init.go:124-157`). The moment a worker is added, the control-plane node
keeps `node-role.kubernetes.io/control-plane:NoSchedule`, every existing test pod relocates to
the worker — and `extraMounts` for the work root is declared only on the control-plane node
([k8s/kind-cluster.yaml:29-35](../k8s/kind-cluster.yaml#L29)). Every toy-solution artifact
assertion would fail with "report.json was not written". Going multi-node means replicating
`extraMounts` onto every worker. Also note `kindest/node:v1.36.1` uses kubeadm `v1beta4` with
list-form `kubeletExtraArgs`, which makes most blog-post kind GPU recipes wrong here.

---

## 4. Recommended tiers

**T0 — free, today (~30 lines, ~2 s).** A read-only pytest calling every `cluster.py` reader
against the *unmodified* cluster, asserting each returns `0` / `[]` / `'unknown'` rather than
raising. Validates every jsonpath expression, label selector and JSON shape — precisely the
class of defect a `subprocess.run` mock is structurally blind to, and which the existing
967-line [tests/test_mix_strategy.py](../tests/test_mix_strategy.py) cannot see. No
`kind-up.sh` change; runs on the cluster as it stands.

**T1 — in-process CLI drive with a stateful fake kubectl (~2 days).** `--kubectl` is already a
flag ([cli.py:1094](../videoflow/deploy/cli.py#L1094), [cli.py:1293](../videoflow/deploy/cli.py#L1293))
and threaded everywhere. Driving `cli.main(['deploy', …])` in-process closes what
[tests/test_gpu_lifecycle_cli.py:10-16](../tests/test_gpu_lifecycle_cli.py#L10) admits is
"verified by reading cli.py rather than by a test": `_cmd_deploy`'s GPU wiring, the deliberate
absence of cleanup on the REALTIME return, both exit-3 paths, bug #12, and **bug #13** (the
stateful `MixGpu` singleton) — which no subprocess or cluster test can ever observe.

**T2 — capacity fabrication in `kind-up.sh` (~1 day).** Label + patch + **poll for allocatable**
+ gate. `cluster_ready()` gains a `gpu_capacity_ready()` reason with `FIX = run ./scripts/kind-up.sh`,
per the bucket's existing gate-never-provision discipline
([tests/integration/k8s/conftest.py:10-15](../tests/integration/k8s/conftest.py#L10)).

Prefer **kwok nodes for classification tests** over mutating the single shared node: a
`finally` does not run under SIGKILL, `pytest -x` with a hung subprocess, or a CI timeout, and
a leaked `sharing-strategy=time-slicing` label sends every later test down the
`IMPOSSIBLE_GPU_REQUEST` path. Also: `gpu_units_in_use` is cluster-global, so one leaked pod
holding units silently changes every subsequent preflight verdict — assert
`gpu_units_in_use() == {}` **before** each test, not only after, so the failure names the culprit.

**T3 — the fake MIG operator (~2-3 days).** Gated on one 5-minute experiment first: does
NVIDIA's real 152 KB ClusterPolicy CRD accept videoflow's merge patch after structural-schema
pruning? The prototype in §8 used a preserve-unknown-fields stand-in — faithful for videoflow's
reads, but it does not prove that.

**T4 — docs.** One sentence in [tests/integration/README.md](../tests/integration/README.md):
a green GPU test is not evidence a model ran. Kill the line that says mix cannot be validated
without hardware.

---

## 5. Fidelity gaps — what stays unprovable, and why it matters

- **No device, ever.** Needs option D for the RFC 0003 worker-side contract.
- **MIG placement positions are not modelled.** Real MIG constrains *where* a profile may sit
  (a 3g.20gb starts at slice 0 or 4 on A100); [mig.py:14-20](../videoflow/deploy/mig.py#L14)
  explicitly defers to `nvidia-mig-parted`. A geometry the fake stamps `success` can be
  rejected by real hardware — **the fake's verdict is inverted on the one check it exists to
  provide independently.** Any fake profile table must be a genuinely independent
  implementation cross-checked against `mig._MIG_TABLES`, or it proves nothing.
- **Everything is instantaneous.** Real MIG apply takes minutes and drains CUDA clients; the
  fake converges in ~1 s. The 300 s / 600 s timeout paths are never exercised, and any defect
  needing a slow apply — a pod scheduled mid-reconfiguration, ClusterPolicy→DaemonSet
  propagation overtaking node labeling, `_gpu_prepare`'s rollback racing a half-applied node —
  is structurally invisible. A configurable delay knob is what makes those observable at all.
- **The fake never disagrees with itself.** In reality GFD and the device plugin are separate
  DaemonSets with independent restart and lag, so a node can transiently carry
  `nvidia.com/gpu.count=8` while `allocatable` is `0`. videoflow reads those two facts
  independently and never reconciles them, so it plans a full MIG layout against cards that
  will not schedule. In a fake where one loop writes both, that can never happen — and the
  test proves the opposite of the production behaviour. **Worth a deliberate desync test**
  (`gpu.count=8`, `allocatable=1`) asserting the tolerant contract: ~20 lines.
- **Time-slicing is a label, not a behaviour.** `IMPOSSIBLE_GPU_REQUEST` for `gpu_count>1` on a
  time-sliced pool is a claim about CUDA context isolation. Setting the label tests the
  *branch*; nothing tests whether the claim is true.
- **`mig-parted` encoding drift.** The stock ConfigMap uses map form (`1g.10gb: 7`); run:ai's
  applied form is a list of dicts (`- {name: 7g.40gb, position: 0, size: 8}`). videoflow emits
  map form ([mig.py:357-369](../videoflow/deploy/mig.py#L357)). A hand-copied stub encoding one
  shape goes green forever even if real mig-parted expects the other.
- **`runtimeClassName` presence vs effect.** A kind RuntimeClass proves the plumbing renders,
  never that the NVIDIA runtime would engage.

---

## 6. Does `mix` earn a harness?

Worth writing down before spending 2-3 days. `deploy/gpu.py` is 1,389 lines, ~600 of it mix
`prepare`/`cleanup`, and it has **zero in-tree consumers** — no solution under `solutions/`
sets `device_type`, and the only contrib user logs `granted_gpus()` and moves on. A fake MIG
operator is a second-source implementation of a protocol NVIDIA owns, which carries a permanent
triage tax: every future failure is ambiguous between "videoflow is wrong" and "the fake is
wrong". That may well be worth it — the code is complex, subtle, and mutates a shared
multi-tenant cluster — but it should be an explicit decision, not an assumption.

---

## 7. Two live findings this exercise produced

### 7.1 `_stamp_node_owners` is not the compare-and-swap it documents

[gpu.py:1014-1022](../videoflow/deploy/gpu.py#L1014) states:

> *"the label is applied WITHOUT `--overwrite`, so losing a race to a concurrent deploy fails
> the label command instead of silently stealing the node."*

Verified with `kubectl -v=8`: `kubectl label` issues a GET, then an **unconditional merge
patch** with no `resourceVersion` precondition
(`PATCH /api/v1/nodes/<n>?fieldManager=kubectl-label`, body
`{"metadata":{"labels":{"…":"…"}}}`). On an already-labelled node it sends **zero** PATCH
requests — the no-overwrite check is entirely client-side.

To be precise: kubectl's own GET lands ~2 ms before its PATCH, so this is a narrow TOCTOU
window rather than an absence of protection. But it is not the CAS the docstring claims, and
multi-tenant node ownership is the one place the difference matters —
`_stamp_node_owners` widens it further with its own `get node -o jsonpath=…gpu-owner` first,
making the sequence GET → GET → unconditional PATCH.

**The fix already exists 450 lines above in the same file.** `_publish_mig_configmap` does
get → `replace` carrying `metadata.resourceVersion` → retry on `Conflict`
([gpu.py:563-589](../videoflow/deploy/gpu.py#L563)). Roughly 15 lines to port.

**The testing lesson matters more than the bug.** A cluster test spawning two
`videoflow deploy` subprocesses — each spending seconds on compile, image inspection and
inventory before reaching `_stamp_node_owners` — cannot reliably interleave inside a 2 ms
window. Such a test **passes today against the broken code** and would be cited as proof the
CAS works. The correct test is not a cluster test at all: fix the code, then unit-test that the
argv carries a `resourceVersion` precondition. Deterministic, instant, cannot be faked green.

### 7.2 Bug #15 confirmed open

[engines/kubernetes.py:474](../videoflow/engines/kubernetes.py#L474) is
`any('gpu' in (message or '') for _, message in overdue)`. `Insufficient nvidia.com/mig-1g.10gb`
contains no `gpu`, so the watchdog's GPU hint never fires for a MIG resource — and the
affinity-mismatch clause is generic, so it adds nothing either.

---

## 8. Build order

1. **Fix the owner-label CAS** + a unit test on the argv. A live multi-tenant bug, ~15 lines
   copied from the same file, and no cluster test in any tier would have found it. Don't build
   a harness to hunt for bugs of this class before fixing the one already on the floor.
   (Deploy-side behaviour change — cite RFC 0004 in the commit.)
2. **T0 read-only smoke test.** Highest coverage-per-line here; runs on the existing cluster
   with no `kind-up.sh` change.
3. **T1 in-process CLI drive.** Closes `_cmd_deploy`'s GPU wiring and bug #13; no
   infrastructure; runs on the pre-push hook.

Then T2. Defer T3 until §6 is answered in writing.

---

## 9. Probe scripts

Three self-reverting scripts were used; the third is essentially the T3 prototype. They live in
the session scratchpad, not the repo:

- `fakegpu_probe.sh` — labels + capacity patch, videoflow's own reads, real pod scheduling,
  persistence over 45 s.
- `fakegpu_probe2.sh` — the full preflight decision tree (physical / time-sliced / MIG) plus a
  live `MixGpu.resolve_specs` layout solve.
- `fake_mig_operator.sh` — the faked GPU-Operator surface and a real `prepare()` / `cleanup()`
  round trip.

Nothing in the repo was modified by them, and the cluster was verified clean afterwards
(no residual labels, annotations, capacity, CRDs, namespaces or pods).

### Docs that change if any of this ships

Per CLAUDE.md's docs-in-sync rule: [tests/integration/README.md](../tests/integration/README.md)
(what the bucket needs, and the "a green GPU test is not evidence a model ran" caveat),
[docs/source/distributed/gpu-sharing.rst](../docs/source/distributed/gpu-sharing.rst) if the
dev-cluster story changes, [todos/00_gpu_allocation_bugs.md](../todos/00_gpu_allocation_bugs.md)
§Verification (the "needs a real GPU Operator" claim), `scripts/kind-up.sh` +
`k8s/kind-cluster.yaml` together, and CLAUDE.md's Commands section if a new bucket appears.
No RFC is needed for a test harness; the CAS fix in §7.1 is a different matter.
