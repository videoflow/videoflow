# Time-slicing audit — 2026-09-20

**Status: Design follow-up.** Reviewed against core commit `73e3b84`.
**This file primarily records a design
decision, not an unresolved bug list.** Time-slicing remains an operator-owned
pool property; dynamically managing it per flow would be an additional feature.
The current audit below supersedes any implication that the historical host
description has been reverified. The original text is preserved unchanged below.

## Current disposition

| Historical topic | Status | Current evidence and remaining work |
| --- | --- | --- |
| Q1 — Give videoflow permission to rewrite time-slicing configuration per run | **Design decision** | The current built-ins are [exclusive, mix, and DRA](../videoflow/deploy/gpu.py#L2285). Time-slicing is [recognized from inventory labels](../videoflow/deploy/cluster.py#L522), while [mix excludes time-sliced nodes](../videoflow/deploy/gpu.py#L1274) and [solves declared memory demands as MIG slices](../videoflow/deploy/mig.py#L388). There is no existing contract requiring per-run device-plugin reconfiguration. |
| Q2 — Time-sharing has no per-share memory isolation | **Design constraint; no implementation fix identified** | Maintained [GPU-sharing documentation](../docs/source/distributed/gpu-sharing.rst#L118) states the same constraint. The implementation distinguishes shared units from whole devices and rejects multi-device claims against time-sliced resources; see the [preflight regression](../tests/test_cluster.py#L274). Changing permissions or replica count is not an implemented replacement for mix's memory-aware planning. |
| Q3 — Device/node/resource targeting capabilities of the NVIDIA plugin | **Background capability claim; not independently reverified** | This describes the external plugin rather than a missing videoflow API. The repository's [documented static example](../docs/source/distributed/gpu-sharing.rst#L94) omits device selection. This audit did not inspect an installed plugin version or re-research its configuration API, so it makes no new compatibility claim about the historical targeting examples. |
| Shared node configuration, convergence, and blast radius | **Design rationale remains applicable; MIG safety needs qualification** | Videoflow currently reads time-slicing as pool state and owns only its managed-MIG lifecycle. That scope is consistent with the design rationale. However, the historical contrast with MIG must not imply fully proven concurrency safety: the [GPU audit](00_gpu_allocation_bugs.md) identifies remaining #5 shared-policy retirement and #9/#10 status-correlation gaps. |
| Proposed “videoflow-managed compute-only sharing” mode | **Unimplemented optional feature** | No such built-in mode is registered. Adding one would require an explicit product/design decision about device-plugin ownership and lifecycle; it is not a bug fix required by the current modes. |
| Specific RTX 4090 / bare-plugin / no config-manager host description | **Unverified historical environment context** | No live hardware, DaemonSet, ConfigMap, or cluster inspection was performed. The [repository instructions](../docs/source/distributed/gpu-sharing.rst#L114) describe mounting `CONFIG_FILE` and restarting the plugin, but cannot establish the user's current installation or reload behavior. Recheck the deployed version/configuration if this host-specific guidance is needed operationally. |

## Validation and limits

The related GPU allocation audit ran the following existing tests. These provide
repository-level evidence for classification, preflight, allocation, and MIG
lifecycle behavior; they do not test a live NVIDIA time-slicing installation:

```sh
.venv/bin/python -m pytest -q tests/test_mix_strategy.py tests/test_cluster.py tests/test_mig_solver.py tests/test_gpu_provenance.py tests/test_gpu_lifecycle_cli.py tests/test_allocation_kubernetes.py tests/test_unknown_states.py
# 198 passed in 2.85s

.venv/bin/python -m pytest -q tests/conformance/test_alloc_mig_lifecycle.py tests/conformance/test_alloc_ownership.py -m 'level_model or level_process'
# 13 passed, 19 deselected in 2.87s

.venv/bin/python -m pytest -q tests/conformance/test_alloc_planning.py -m level_model
# 7 passed, 5 deselected in 1.06s
```

No live cluster, GPU Operator, GPU hardware, or external plugin documentation
verification was performed. Tests were not rerun while adding these annotations.
No code or device-plugin configuration changes are included.

## Historical notes (preserved)

We are revisiting this topic:

First, a correction (Q3: can it target certain GPUs?)
Yes, it can — I understated this earlier. Each entry under sharing.timeSlicing.resources has a devices field:


type ReplicatedResource struct {
    Name     ResourceName      // e.g. nvidia.com/gpu
    Rename   ResourceName      // optional new resource name
    Devices  ReplicatedDevices // which GPUs this applies to
    Replicas int               // how many time-slices
}
devices is a union accepting:

all (the default),
an integer count (first N devices), or
an explicit list — GPU indices ([0, 1]) or GPU UUIDs (GPU-…).
So on a multi-GPU node you can say "time-slice GPU 0 into 4 replicas, leave GPU 1 whole." My earlier "uniform across all the GPUs on the node" described the default (devices: all), not a limit. Your current config omits devices, so it defaults to all — which on your box is just the one 4090, so here "all GPUs" and "that GPU" happen to be the same thing.

Q2: How coarse is it, really?
Granularity is available on three axes:

Per resource name (rename splits, e.g., nvidia.com/gpu.shared vs nvidia.com/gpu),
Per node (a nvidia.com/device-plugin.config=<name> node label selects among named configs), and
Per device (the devices list above).
But none of those is the coarseness that matters. The real bluntness is: replicas: N is a compute time-share with no memory partition. Every replica of a given GPU sees that GPU's entire memory and just interleaves execution. You cannot express "this share gets 10 GiB, that one gets 40." So "how coarse" isn't about which GPUs you pick — it's that the unit itself carries no memory guarantee.

Q1: Why not grant videoflow admin rights to rewrite the ConfigMap per run?
It's genuinely not primarily a permissions problem — you could grant the RBAC — and it's also not that time-slicing can't be dynamic (the plugin's config-manager watches the node label and, per the docs, "the plugin will immediately be updated to start serving the desired configuration" when it changes). The reasons are semantic and architectural:

It still can't deliver mix's promise. mix exists to honor gpu_memory_gib. No replica count — on any GPU selection, with any permissions — gives memory isolation. So per-run time-slicing edits are the wrong tool for the one job you'd want them for.

It's a shared, node-scoped object with no per-flow ownership. There's one selected config per node. Two flows reconfiguring the same node contend on the same object, and changing replicas forces the plugin to re-advertise nvidia.com/gpu on that node — which perturbs every consumer there, not just your flow. Contrast MIG, where mix stamps videoflow.io/gpu-owner per node, different cards hold different geometries, and it waits on an observable mig.config.state=success before proceeding. Time-slicing has no per-flow scoping and no clean convergence signal.

Blast radius. The device-plugin config is cluster-critical infrastructure. A videoflow crash mid-edit could leave the pool misadvertised for all workloads. mix's MIG changes are owner-scoped and restored at teardown; there's no equivalent guardrail for the device-plugin config.

The coherent version of your idea does exist — a "videoflow-managed compute-only sharing" mode that sets replicas = number of co-scheduled consumers, optionally scoped to specific devices. It's buildable (the plugin has the hooks). But it wouldn't be mix (no isolation), it'd require wiring the config-manager, and it puts videoflow in charge of a shared cluster-critical object. The design deliberately treats time-slicing as an operator-owned pool property it only reads, and reserves per-run provisioning for MIG, which was built for exactly that.

Footnote on your specific box
Your daemonset is the bare plugin (one nvidia-device-plugin-ctr, CONFIG_FILE=/config/config.yaml, no config-manager sidecar, no node-label selection). So the auto-reload behavior above doesn't apply to you — the plugin reads that file once at startup. That's why editing the ConfigMap wasn't enough on its own: you had to restart/redeploy the plugin for it to re-read and re-advertise. If you wanted dynamic per-node config switching, you'd deploy the config-manager and use the nvidia.com/device-plugin.config label.
