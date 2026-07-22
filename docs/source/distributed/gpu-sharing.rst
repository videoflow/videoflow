GPU sharing: running more GPU nodes than you have GPUs
======================================================

Why local runs work and Kubernetes runs stall
---------------------------------------------

Locally (``LocalProcessEngine``), every node is an OS subprocess on one machine.
Each GPU worker is handed a ``CUDA_VISIBLE_DEVICES`` block of ``gpu_count``
devices; when there are more claims than devices the assignment wraps around
(with one warning) and processes share a card, bounded only by VRAM. A
9-GPU-node graph still runs fine on a single card if the models fit.

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
deploy's preflight compares both against the cluster's allocatable capacity
before applying anything (``--strict-preflight`` makes a shortfall a hard error).

Three ways to close the gap
---------------------------

**1. Reduce demand.** Not every "GPU" stage needs one: trackers and light pose
models often run fine on CPU. Prefer solution-level device knobs (e.g. per-stage
``device.tracker: cpu`` in a solution config) — the floor is the number of nodes
doing genuinely GPU-bound inference.

**2. Share the physical GPU, keep scheduler accounting: device-plugin
time-slicing.** The NVIDIA device plugin can advertise each physical GPU as N
schedulable units::

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
temporal multiplexing with **no memory isolation and one shared fault domain**; the
sum of all co-tenant models must fit in VRAM, and ``gpu_count > 1`` is rejected
(``failRequestsGreaterThanOne``).

**3. Skip scheduler accounting entirely: ``--gpu-mode shared``.** ::

    videoflow deploy my_flow.py --gpu-mode shared --gpu-runtime-class nvidia

Shared mode emits **no GPU resource limit**: every GPU pod schedules onto the
GPU pool (the nodeSelector/toleration stay) and shares the physical devices
through the NVIDIA container runtime — exactly the semantics of a local run, with
VRAM as the only limit. This is a **dev-cluster tool**:

- There is no scheduler accounting and no memory isolation; an OOM takes out
  whichever pod the driver picks.
- On a multi-GPU node every shared pod sees **all** devices (the CUDA base images
  set ``NVIDIA_VISIBLE_DEVICES=all``) and frameworks default to device 0 — expect
  pile-up unless your node code picks a device.
- On clusters where the NVIDIA runtime is an opt-in RuntimeClass (k3s),
  ``--gpu-runtime-class`` is **required**: with no resource limit, the runtime
  class is the only mechanism that injects the device, so deploy refuses shared
  mode without it.

Production clusters
-------------------

Keep the default exclusive mode in production and size node pools to demand —
the preflight numbers are exactly the capacity you need. Where sharing with real
isolation is required:

- **MIG** (A100/H100-class GPUs): hardware partitions exposed as their own
  resources. Point a node at a profile with
  ``gpu_resource_name='nvidia.com/mig-1g.10gb'`` (or ``--gpu-resource-name``); no
  other videoflow change. MIG is Kubernetes-only: ``run-local`` counts whole
  physical cards and does not enumerate MIG instances.
- **MPS** (any Volta+ GPU, via the device plugin's Helm chart): concurrent kernels
  with hard per-client memory caps of ``total/replicas``. Stronger isolation than
  time-slicing; the heaviest model bounds the replica count. Pod specs are
  unchanged.
- KEDA autoscaling excludes GPU nodes by default (each extra replica claims whole
  devices); opt in deliberately with ``--gpu-autoscaling`` once capacity math says
  it is safe.

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
  the largest node's allocatable count, not just the cluster total — a 3-GPU pod
  on a cluster of 2-GPU nodes never schedules no matter how many nodes exist.
  Prefer NVLink-connected devices for tensor parallelism.
- Sliced GPUs don't qualify: MIG partitions are hardware-isolated and cannot be
  combined into one model (preflight flags ``gpu_count > 1`` against a ``mig-``
  resource), and time-slicing rejects multi-unit requests
  (``failRequestsGreaterThanOne``).
- ``--gpu-mode shared`` ignores ``gpu_count`` entirely (no limit is emitted);
  preflight warns when a shared-mode flow declares a multi-GPU need.
