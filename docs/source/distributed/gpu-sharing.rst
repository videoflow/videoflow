GPU allocation: the two modes, sharing, and multi-GPU models
============================================================

Videoflow has exactly two GPU allocation modes, matching two kinds of demand a
node can declare:

- ``--gpu-mode exclusive`` (the default): every unit of the GPU resource is one
  **whole physical device**. ``gpu_count = N`` grants N whole devices on one
  host. Sharing a device between components is not a capability of this mode —
  and the node API has no way to ask for it.
- ``--gpu-mode mix`` (opt-in, MIG-capable hardware): a node that declares its
  memory demand (``gpu_memory_gib``) gets an **exclusive MIG slice** of at least
  that size, chosen by a layout solver; every other GPU node still gets whole
  physical devices. The card is shared, the slice is not.

The demand vocabulary is two numbers, mutually exclusive per node: ``gpu_count``
(whole devices, spanned by one model) and ``gpu_memory_gib`` (an isolated
fraction of one device). They are mutually exclusive because the hardware makes
them so — one CUDA process addresses at most one MIG instance, and there is no
P2P between instances, so a model can never span slices.

Why local runs work and Kubernetes runs stall
---------------------------------------------

Locally (``LocalProcessEngine``), every node is an OS subprocess on one machine.
Each GPU worker is handed a ``CUDA_VISIBLE_DEVICES`` block of ``gpu_count``
devices; when there are more claims than devices the assignment wraps around
(with a warning, and ``VF_GPU_COUNT`` reporting the devices actually delivered)
and processes share a card, bounded only by VRAM. A 9-GPU-node graph still runs
fine on a single card if the models fit.

On Kubernetes, each GPU replica requests ``nvidia.com/gpu`` — an **integer extended
resource that cannot be overcommitted**. The scheduler allocates whole devices
exclusively: a graph with N GPU replicas needs N allocatable devices. On a cluster
with fewer, some pods bind and the rest sit ``Pending``
(``Insufficient nvidia.com/gpu``) forever. The failure is silent in both flow
modes, in different ways:

- **BATCH** — the missing node's input stream fills, backpressure blocks the
  producer, and the flow hangs making no progress. (Deploy's wait loop now detects
  the unschedulable pod and aborts with an actionable error instead of hanging.)
- **REALTIME** — producers never block; frames headed for the dead node are
  silently evicted and everything downstream of it produces nothing, while every
  running pod looks healthy. (Deploy now runs a bounded post-apply rollout check:
  it waits for every pod to become Ready — ``open()`` completed — and on an
  unschedulable pod, a crash-loop, an OOM kill or an image-pull failure it dumps
  the pod logs and exits non-zero, leaving the flow running for inspection.)

``videoflow explain my_flow.py`` prints a flow's total GPU demand (and, for
multi-GPU pods, the largest single-pod claim that must fit on one node), and
deploy's preflight compares both against the cluster's *free* capacity before
applying anything (``--strict-preflight`` makes a shortfall a hard error) — and
then packs every pod's claim onto the per-node free counts, because the two
aggregate numbers are necessary, not sufficient: two nodes with 3 free GPUs each
pass both and still hold only two of three ``gpu_count = 2`` replicas. A
preflight whose occupancy read the API refused reports ``unobservable GPU state``
rather than assuming the pool is idle.

Closing the gap on a small cluster
----------------------------------

**1. Reduce demand.** Not every "GPU" stage needs one: trackers and light pose
models often run fine on CPU. Prefer solution-level device knobs (e.g. per-stage
``device.tracker: cpu`` in a solution config) — the floor is the number of nodes
doing genuinely GPU-bound inference.

**2. Dev clusters without MIG hardware: device-plugin time-slicing.** The NVIDIA
device plugin can advertise each physical GPU as N schedulable units::

    # nvidia-plugin-config.yaml (namespace kube-system)
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: nvidia-plugin-configs
      namespace: kube-system
    data:
      config.yaml: |
        version: v1
        sharing:
          timeSlicing:
            renameByDefault: false
            failRequestsGreaterThanOne: true
            resources:
              - name: nvidia.com/gpu
                replicas: 4

Mount it into the device-plugin DaemonSet and point the plugin at it (``CONFIG_FILE``
env var), then ``kubectl -n kube-system rollout restart ds/<device-plugin>``.
Allocatable ``nvidia.com/gpu`` flips from the physical count to ``replicas x``
physical, and — because ``renameByDefault: false`` keeps the resource name —
videoflow manifests need no change at all. Caveats: time-slicing is round-robin
temporal multiplexing with **no memory isolation and one shared fault domain**;
the sum of all co-tenant models must fit in VRAM, and only ``gpu_count = 1``
nodes are supported — the units are shares of one card, so a multi-device grant
against them is impossible, and deploy's preflight hard-errors on it (it reads
the GPU Feature Discovery labels to know the pool is time-sliced).

**3. MIG-capable hardware: ``--gpu-mode mix``.** Sharing with *hard* memory and
fault isolation, driven by declared demand (the solver knows the A30, A100, H100
and RTX PRO 6000 Blackwell profile tables — ``1g.24gb`` ×4 / ``2g.48gb`` ×2 /
``4g.96gb`` on the Blackwell part — and packs a card by compute slices *and*
memory as separate budgets, so a layout that fits by slice count but not by
memory is never proposed)::

    detector  = Detector(device_type = GPU, nb_tasks = 4, gpu_memory_gib = 10)(frames)
    captioner = VlmCaptioner(device_type = GPU, gpu_count = 2)(frames)

.. code-block:: console

    videoflow deploy my_flow.py --gpu-mode mix --gpu-runtime-class nvidia

At deploy time a layout solver runs against the pool's physical inventory (GPU
Feature Discovery labels — GFD is required for mix). Only nodes labeled
``videoflow.io/gpu-pool=true`` are considered — the same label the pods
schedule on — and the pool is treated as **multi-tenant**: nodes another flow
has claimed, nodes whose devices are held by running pods, and misconfigured
pool members (time-sliced, or carrying MIG geometry videoflow does not own) are
excluded before solving, with the reasons listed if the remaining capacity
cannot fit the flow. Capacity preflight likewise compares demand against *free*
units (allocatable minus what running pods hold), not raw allocatable:

1. Whole cards are reserved for the **spanners** — nodes with ``gpu_count``
   (declared or defaulted to 1) and no memory demand. These cards stay
   MIG-disabled and P2P-capable.
2. **Sharers** — nodes declaring ``gpu_memory_gib`` — are packed onto the
   remaining cards, each replica getting the smallest MIG profile that fits its
   demand (e.g. four 10 GiB detectors land as ``1g.10gb`` slices of one A100).
   All replicas of one node use one profile, so the node's pods request a single
   extended resource (``nvidia.com/mig-1g.10gb``).
3. The geometry is applied through the GPU Operator's MIG manager: videoflow
   merges its generated ``nvidia-mig-parted`` entries into the operator's
   current config, publishes the result as the ``videoflow-mig-parted-config``
   ConfigMap, points ClusterPolicy ``migManager.config.name`` at it (the MIG
   manager only reads the ConfigMap that field names), waits for the
   mig-manager DaemonSet to remount, then sets each node's
   ``nvidia.com/mig.config`` label and waits for ``mig.config.state=success``.
   The label value (and matching config entry name) carries a per-run nonce —
   ``videoflow-<node>-<nonce>``, clamped to the 63-character label limit — so
   a retried deploy is always a label *change*: the MIG manager reacts only to
   changes, and a leftover ``state=failed`` under the previous run's exact
   value would otherwise deadlock every retry.
   ``videoflow teardown --gpu-mode mix`` reverts the geometry, verifies the
   same state, and restores the policy — the pre-videoflow label and config
   name are recorded in cluster annotations, so teardown needs no state from
   the deploy. Without a MIG manager or ClusterPolicy, deploy prints the
   exact config to apply by hand. Note: if ClusterPolicy is managed by GitOps
   (ArgoCD/Flux), the reconciler will revert videoflow's patch mid-run — keep
   ``migManager.config.name`` unmanaged, or run mix with a paused sync.

Several flows can run mix against one pool at the same time, split at **node
granularity**: prepare() stamps every node it partitions with a
``videoflow.io/gpu-owner=<flow-id>`` label, later deploys plan around owned
nodes, and mix pods carry a node affinity that keeps them off other flows'
nodes — so one flow's teardown can never revert geometry under another flow's
pods. The stamp is a **server-enforced compare-and-swap**: the label write
carries the node's ``resourceVersion`` from the read that found it unowned, so
the API server rejects whichever of two racing deploys writes second (a plain
``kubectl label`` without ``--overwrite`` only checks client-side, and both could
pass). A companion ``videoflow.io/gpu-owner-epoch`` label, fresh per claim, rides
in the same write so a release can tell the claim it is undoing from a later
re-claim by the same flow and leave the newer one standing. Losing the race
releases what the deploy had stamped and stops it with ``OwnershipConflict``
(exit 3); the remedy is to redeploy against the remaining pool. The shared
``videoflow-mig-parted-config`` ConfigMap is merged, not overwritten, and only
the **last flow out** restores ``migManager.config.name`` — but it never deletes
the map: ``kubectl`` cannot delete with a precondition, and a stale map is
harmless where a wrong delete pulls the file out from under a MIG manager still
mounting it. It strips its own entries and annotates the map
``videoflow.io/mig-config-tombstone=<UTC time>``; removing it is the operator's
call, ``kubectl delete configmap videoflow-mig-parted-config -n <gpu-operator
namespace>``. Teardown therefore needs ``--flow-id`` to know which nodes are its
own (a teardown without one sweeps everything videoflow owns). Busy nodes are
never repartitioned — MIG reconfiguration destroys whatever runs on the card,
and Kubernetes does not expose which physical card a pod holds — but their free
cards still serve whole-device spanners; only GPU resources (``nvidia.com/gpu``,
``nvidia.com/mig-*``, another vendor's ``<domain>/gpu``) count as occupying a
card. And *unknown is not idle*: if the pod listing that says which cards are
held cannot be read, mix refuses to plan (``UnobservableState``, ``unobservable
GPU state``) rather than repartition a card another tenant may hold. One race
stays open by design: a foreign GPU pod that lands on a planned node between
inventory read and geometry apply will be disrupted; closing it needs admission
control, which videoflow does not install.

A node that declares nothing gets a whole physical device — a plain
``device_type=GPU`` node means the same thing in both modes, so flows do not
need rewriting to adopt mix. Under ``exclusive``, ``gpu_memory_gib`` is simply
unused (the node gets a whole device, and deploy prints a NOTE), so a
mix-authored flow still deploys on a dev cluster.

Components declare their defaults in the descriptor, overridable per graph::

    spec:
      resources:
        gpu:
          memoryGiB: 10     # sharer default; or `count: 2` for a spanner default

Other production notes
----------------------

- **MPS** (any Volta+ GPU, via the device plugin's Helm chart): concurrent kernels
  with hard per-client memory caps of ``total/replicas``. Stronger isolation than
  time-slicing; the heaviest model bounds the replica count. Pod specs are
  unchanged. Deploy recognises an MPS pool from its
  ``nvidia.com/gpu.sharing-strategy=mps`` label and treats its units as shares,
  like time-slicing: a ``gpu_count > 1`` claim against it is rejected at
  preflight. Classification reads the pool's nodes only (a sharing label on a
  node outside ``videoflow.io/gpu-pool`` cannot taint it), a MIG-capable node
  whose geometry is ``all-disabled`` counts as whole physical cards, and an
  unlabeled advertiser next to a labeled one makes the pool ``unknown`` rather
  than ``physical`` — nothing proves its units are whole devices.
- KEDA autoscaling excludes GPU nodes by default (each extra replica claims whole
  devices); opt in deliberately with ``--gpu-autoscaling`` once capacity math says
  it is safe.
- ``--gpu-resource-name`` covers clusters whose whole devices are advertised
  under a non-default name (``amd.com/gpu``). It must denote whole physical
  devices — pointing it at a MIG profile is rejected at render time.

The opposite direction: models larger than one GPU
--------------------------------------------------

Sharing splits one device among many nodes; ``gpu_count`` does the reverse — one
node claiming several whole devices for a model that exceeds a single GPU's
memory::

    captioner = VlmCaptioner(device_type = GPU, gpu_count = 2)(frames)

The pod requests ``nvidia.com/gpu: 2`` and the scheduler grants both devices to
that one worker, on one host. The worker-side contract (RFC 0003): **the visible
GPUs are exactly the granted GPUs, numbered ``cuda:0..N-1``, with
``N == gpu_count``** — the device plugin enforces it on Kubernetes, and
``run-local`` enforces it by partitioning ``CUDA_VISIBLE_DEVICES``. Sharding the
model across the grant is the node's own ``open()``: ``device_map='auto'`` for
Hugging Face models, a tensor-parallel size for engines that take one, or
explicit ``.to('cuda:1')`` placement in a multi-model node. Components declare a
default need in their descriptor (``spec: {resources: {gpu: {count: 2}}}``).

Constraints worth knowing before sizing:

- All ``gpu_count`` devices must fit on **one** cluster node. Preflight checks
  the largest node's free count, not just the cluster total — a 3-GPU pod on a
  cluster of 2-GPU nodes never schedules no matter how many nodes exist — and
  then places every replica's claim on the per-node free counts, since even that
  bound admits fragmented pools that hold only some of the pods. Prefer
  NVLink-connected devices for tensor parallelism.
- Sliced GPUs never qualify: MIG partitions are hardware-isolated and cannot be
  combined into one model, and time-sliced units are shares of one card. Deploy
  makes both impossible to request — ``gpu_memory_gib`` and ``gpu_count > 1``
  are mutually exclusive at graph build, and preflight hard-errors on a
  multi-unit claim against a pool it classifies as MIG or time-sliced.
- Under ``mix``, spanners keep working exactly as under ``exclusive``: the
  solver reserves whole MIG-disabled cards for them before packing any sharers.

MIG is Kubernetes-only: ``run-local`` counts whole physical cards and does not
enumerate MIG instances, so a local run of a mix flow gives every sharer a whole
card (which satisfies "at least ``gpu_memory_gib``").
