# RFC 0003: Multi-GPU component grants

- **Status:** proposed
- **Author(s):** videoflow maintainers
- **Created:** 2026-07-22
- **Protocol version affected:** 1 (no version change — envelope bytes and routing are untouched)
- **Requirement IDs touched:** none amended; two additive optional rows in the §1 environment table

## Summary

A component whose deep-learning model exceeds one GPU's memory must shard it across
N devices inside its one worker process. The scheduling half already exists: a node's
`gpu_count` is rendered as an integer extended-resource limit
(`resources.limits: {nvidia.com/gpu: N}`) on the single worker container, so
Kubernetes grants all N whole devices to that one pod, on one host. What is missing
is the contract the component sees. This RFC adds it:

1. **The visibility contract.** Inside a worker, the visible GPUs are exactly the
   granted GPUs, numbered `0..count-1` (`cuda:0..N-1` for CUDA runtimes), and the
   count equals the node's `gpu_count`. On Kubernetes the device plugin already
   enforces this in exclusive mode; the local engine makes it true by partitioning
   `CUDA_VISIBLE_DEVICES` across workers. How the model is placed across those
   devices (`device_map`, tensor parallelism, per-submodel `.to()`) is entirely the
   component's concern — the framework's job ends at "you were granted exactly
   these devices."
2. **Descriptor declaration** — `spec.resources.gpu.{count, resourceName}` in
   `component.yaml`, so a component can declare its own GPU need instead of relying
   on every graph author to pass `gpu_count=` by hand.
3. **Environment delivery** — `VF_GPU_COUNT` / `VF_GPU_RESOURCE_NAME`, so native
   (non-Python) components, which never see the Python node's reconstruction
   params, can learn their grant.

## Motivation

Every existing GPU component hard-codes single-device placement (`.cuda()`,
`device='cuda'`) because nothing tells it otherwise. A graph author can already
write `gpu_count = 2` and the pod will schedule with two GPUs — and the model will
then load twice as slowly onto device 0 while device 1 idles. Multi-GPU models
(large VLM/LLM captioners, two-model nodes wanting a device per model) need a
stated, cross-language contract for "which devices are mine".

## Why an RFC

The envelope bytes and routing are unchanged — no version bump, no golden-vector
regeneration. But two `spec/` contracts that non-Python SDKs pin against change
observably:

- `spec/descriptor/component-schema.json` gains `spec.resources`. The schema sets
  `additionalProperties: false` on `spec`, so a **new descriptor is rejected by an
  old videoflow's** full jsonschema validation. Descriptors that want to remain
  loadable by pre-RFC videoflow versions must omit `resources` (and rely on
  graph-side `gpu_count=`).
- `spec/PROTOCOL.md` §1 gains two optional environment rows. Old workers ignore
  them per `ENV-4` (unknown `VF_*` MUST be ignored), so this direction is safe.

## Proposal

### Descriptor: `spec.resources.gpu`

```yaml
spec:
  device: [gpu]
  resources:
    gpu:
      count: 2                      # whole physical GPUs per replica; integer >= 1; default 1
```

- `count` is a **default, not a floor**: an explicit graph-side `gpu_count=`
  overrides it (an operator may run a quantized variant on fewer devices). A
  component with a hard minimum should verify in `open()` and raise.
- `count > 1` requires `'gpu'` in `spec.device` — a multi-GPU CPU-only component
  is a contradiction rejected at descriptor load.
- `resources.gpu` is valid on **processor** components only; on a producer or
  consumer descriptor it is rejected at load (a silently ignored GPU need would
  schedule an underprovisioned pod).
- The key is `resources` (not `constraints`) to mirror the Kubernetes vocabulary
  and leave room for future needs (e.g. `resources.gpu.memoryGiB`, RFC 0004).
- *Amended (two-mode redesign):* the original draft also had a
  `resourceName` field mirroring a node-level `gpu_resource_name` parameter.
  Both were removed before merge: resource names are not a user-facing concept —
  the deploy-level `--gpu-resource-name` covers clusters that advertise whole
  devices under a non-default name, and slice resources (MIG profiles) are
  assigned only by a GPU strategy (the `mix` solver), never by hand. Letting
  users name slice resources was the door to the impossible
  `gpu_count > 1 × MIG` combination this contract forbids.

### Environment: two optional rows (§1.1)

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `VF_GPU_COUNT` | no | `1` | Devices **delivered** to this worker (GPU nodes only). Visible devices are exactly `0..count-1`. |
| `VF_GPU_RESOURCE_NAME` | no | unset | Extended-resource name the devices were requested as (strategy-resolved, e.g. the mix solver's MIG profile). |

Both are informational, not routing: a Python component's authoritative source
remains its own reconstructed `gpu_count` param; the env rows exist for native
components and diagnostics. Set by both control planes (`manifests.py`,
`engines/local.py`) for `device_type == 'gpu'` nodes only. *Amended:* the count
is the delivered grant, not the request — when the local engine cannot deliver
the full request (an oversubscribed dev host), it reports what it actually
masked in, and a docker-run native component (which receives no devices at all
locally) gets neither variable.

### The visibility contract

- **Kubernetes (exclusive mode, the default):** already true — the device plugin
  mounts exactly the granted N devices, enumerated `0..N-1`. Unchanged. (An
  earlier `--gpu-mode shared`, under which the contract could not hold, was
  removed in the two-mode redesign; every registered mode now honours the
  contract.)
- **Local engine:** the engine partitions the host's visible devices (the parent
  process's `CUDA_VISIBLE_DEVICES` intersection, warning on non-integer entries
  such as UUID pins, which the ordinal pool cannot represent) into disjoint blocks of
  `gpu_count` per GPU replica and sets `CUDA_VISIBLE_DEVICES` accordingly. When
  demand exceeds supply it wraps around and warns (device sharing is acceptable
  for dev; the same flow will not schedule that way on Kubernetes), and
  `VF_GPU_COUNT` reports the devices actually delivered. On a host with
  no visible GPUs nothing is set — behaviour is unchanged. The host probe counts
  physical cards (`nvidia-smi -L` `GPU <n>:` lines); MIG instances are not
  enumerated, so a MIG-enabled card is one ordinal in the pool and cannot be
  subdivided locally. Docker-run native components receive no local grant at all
  (no device mask can reach them) and therefore no `VF_GPU_*` variables.
- **Non-combinable resources:** MIG slices are hardware-isolated partitions
  (one CUDA process addresses one MIG instance; no P2P between instances) and
  time-sliced units are shares of a device — `count > 1` against either can
  never satisfy the contract. Deploy rejects the MIG case at render time (the
  name is definitive) and classifies every other resource from GPU Feature
  Discovery node labels (`nvidia.com/gpu.replicas`, `.sharing-strategy`,
  `nvidia.com/mig.strategy`, `-SHARED` product suffix) at preflight, hard-failing
  a multi-unit claim against a sliced pool. An unclassifiable resource (no GFD)
  is assumed physical, with a note saying so.

## Compatibility

- **Wire:** untouched. No golden vectors, no version bump.
- **Old descriptor + new videoflow:** `resources` absent → `count` defaults to 1,
  byte-identical behaviour everywhere.
- **New descriptor + old videoflow:** rejected by jsonschema
  (`additionalProperties: false` on `spec`) with a clear validation error. This is
  the one breaking direction and is deliberate — silently ignoring a declared GPU
  need would schedule an underprovisioned pod.
- **Old worker + new control plane:** the two new env vars are ignored (`ENV-4`).
- **New worker + old control plane:** the vars are absent and default (`1` /
  unset) — today's behaviour.

## Conformance impact

- No new requirement IDs; the env rows are optional-with-default, like
  `VF_NB_TASKS`.
- Unit coverage: descriptor parsing/validation (`tests/test_component_descriptor.py`),
  env emission by both control planes (`tests/test_compiler_manifests.py`,
  `tests/test_local_engine.py`), local partitioning (`tests/test_local_engine.py`).

## Alternatives considered

- **A device-list env (`VF_GPU_DEVICES=0,1`) instead of a count.** Rejected: it
  duplicates `CUDA_VISIBLE_DEVICES` (the mechanism that actually masks devices)
  and invites disagreement between the two. Under the visibility contract a count
  is sufficient — the devices are always `0..count-1`.
- **A framework-side placement/sharding layer.** Rejected: sharding strategy is
  model- and runtime-specific (`device_map='auto'`, `tensor_parallel_size`,
  manual placement); an abstraction would have one caller per shape and would
  drift from the runtimes it wraps.
- **Descriptor `count` as a hard minimum.** Rejected: quantized/smaller variants
  of the same component legitimately run on fewer devices; the descriptor states
  the default request, the component enforces any true floor in `open()`.
- **Putting the GPU fields under `constraints`.** Rejected: `constraints`
  describes graph-wiring rules (partitionable, singleton); a GPU request is a
  resource, and the Kubernetes-shaped `resources` key leaves room for memory etc.

## Open questions

- Should `resources.gpu` eventually carry a memory hint (`memoryGiB`) to drive the
  measured-partitioning GPU strategy sketched in `deploy/gpu.py`'s docstring?
  Deferred until that strategy exists.
