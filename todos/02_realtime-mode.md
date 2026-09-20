# REALTIME buffer-depth audit — 2026-09-20

Reviewed against videoflow core commit `73e3b84`.
**Status: Design follow-up.** Still open as a feature/design investigation,
not a confirmed runtime bug.
The redesign retains low-level support for a buffer larger than one, but does
not expose it consistently through the normal flow/deployment path.

## Current implementation

- `DEFAULT_REALTIME_BUFFER` is 1. `stream_config_for(..., realtime_buffer=...)`
  supports larger values, using LIMITS retention, `max_msgs=max(1, depth)`, and
  oldest-message eviction: [topology.py](../videoflow/messaging/topology.py#L249).
- The backend channel spec also carries a larger depth and maps it into the
  JetStream stream configuration:
  [jetstream_backend.py](../videoflow/messaging/jetstream_backend.py#L143).
- Normal provisioning supplies no depth override:
  [topology.py](../videoflow/messaging/topology.py#L605).
  Worker setup also builds channel specs with the default:
  [nats_messenger.py](../videoflow/messaging/nats_messenger.py#L540).
  A custom stream setting alone is therefore not a supported end-to-end
  configuration; workers also request their own stream settings.
- No Flow, CLI, or worker environment option threads buffer depth through this
  path. The public Flow constructor only takes consumers, flow type and ID:
  [flow.py](../videoflow/core/flow.py#L75).
- An existing broker integration test uses depth 10 to test publication dedup:
  [test_transport_semantics.py](../tests/integration/broker/test_transport_semantics.py#L23).
  It does not establish slow-consumer or configuration behavior at depth greater
  than one. That live test was inspected, not executed during this audit.

## Remaining investigation and acceptance criteria

1. Decide whether depth is a flow-wide or node-output setting, then propagate
   the same value through compilation, provisioning, worker channel requests,
   and configuration read-back. The current implementation stores one stream
   per node output, not an independent buffer for every child edge.
2. Measure the latency/loss tradeoff under sustained and bursty input with slow
   consumers. A larger bounded backlog still evicts the oldest retained
   message; it does not establish strict newest-only consumption or per-key
   freshness. Worker prefetch is a separate queue:
   [jetstream_backend.py](../videoflow/messaging/jetstream_backend.py#L535).
3. Verify fan-out, competing and partitioned replicas, joins, and payload
   reclamation at depth 1 and at larger depths. Test EOS behavior explicitly:
   data and EOS occupy subjects in the same retained stream:
   [topology.py](../videoflow/messaging/topology.py#L264).
4. Keep retention depth separate from retry policy. Default REALTIME
   best-effort consumers still have `max_deliver=1`, including with a durable
   ledger; a larger buffer alone does not recover a crashed worker's input:
   [topology.py](../videoflow/messaging/topology.py#L301).
5. Document the chosen freshness, ordering, memory, and latency semantics and
   add end-to-end configuration coverage before presenting depth as a public
   supported option. The existing documentation describes freshest-message
   behavior: [batch-versus-realtime-mode.rst](../docs/source/user-documentation/batch-versus-realtime-mode.rst#L18).

## Validation performed

This item's assessment is based on source/configuration searches and inspection
of the existing broker tests. No new configurable-depth experiment or live
broker/Kubernetes/GPU test was run. The related runtime audit executed a unit
selection with **209 passed, 1 macOS Docker-URL expectation failure**, and a
model/process conformance selection with **9 passed, 16 deselected**; exact
commands and limits are recorded in
[01_small_bugs.md](01_small_bugs.md#validation-performed).
Those results do not demonstrate end-to-end configurable buffer-depth support.

## Historical notes (preserved)

Explore the implications of having the realtime buffer depth in realtime mode is a config parameter that is larger than 1.
