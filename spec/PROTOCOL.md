# Videoflow Component Protocol

**Protocol version:** 1 (`stabilizing`)
**Status:** normative
**Applies to:** any process that runs as a videoflow graph node — the Python
reference worker (`videoflow.worker`) and every non-Python SDK.

---

## 0. Purpose and scope

Videoflow compiles a computation graph, provisions broker resources, and deploys
one process ("worker") per graph node (per replica). A worker learns everything
it needs from **injected environment variables** and talks to its neighbours
**only through the message broker**. It never sees the graph, the other nodes'
code, or the deployment topology.

This document is the contract between videoflow (the control/plumbing plane) and
a component runtime (the data plane inside one worker). Anything a
language-agnostic SDK must reproduce to interoperate with videoflow-deployed
flows is specified here. It is extracted from the Python reference implementation;
where this document and the code disagree, that is a bug in one of them — file an
RFC (`spec/rfcs/`).

Requirements are tagged with stable IDs (`ENV-1`, `EOS-3`, …). Conformance
scenarios (`conformance/scenarios/`, Phase 4) reference these IDs. Each ID is
**MUST** unless marked SHOULD or `implementation-defined`.

**Key words** MUST / MUST NOT / SHOULD / MAY follow RFC 2119.

### 0.1 The reference implementation defines behavior

The Python worker is the executable definition of protocol v1. Where this prose
is silent or ambiguous, the Python behavior is normative. Genuinely
non-deterministic timing behavior is explicitly tagged `implementation-defined`
below and MUST NOT be relied upon by components or asserted by conformance.

### 0.2 Source-of-truth map (informative)

Each section notes where the behavior lives in the reference implementation, so
the spec and the code can be cross-checked as either evolves.

| Section | Reference source |
|---|---|
| §1 Environment contract | `videoflow/runtime/worker.py`, `videoflow/engines/local.py` (`_worker_env`), `videoflow/deploy/manifests.py` (`_env_pairs`) |
| §2 Naming | `videoflow/messaging/topology.py` |
| §3 Streams & consumers | `videoflow/messaging/topology.py` |
| §4 Envelope & wire format | `videoflow/wire/serialization.py`, `spec/proto/` (Phase 1) |
| §5 Message id & dedup | `videoflow/wire/serialization.py` (`derive_message_id`) |
| §6 Node lifecycle & task loop | `videoflow/core/task.py`, `videoflow/core/engine.py` |
| §7 Delivery, ack, retry, DLQ | `videoflow/messaging/nats_messenger.py` |
| §8 Join / input-group assembly | `videoflow/core/policies.py`, `videoflow/messaging/grouping.py` |
| §9 End-of-stream drain | `videoflow/messaging/nats_messenger.py` |
| §10 Partitioning | `videoflow/messaging/nats_messenger.py` (`_owns`), `topology.py` |
| §11 Control plane | `videoflow/messaging/topology.py`, `videoflow/engines/local.py` |
| §12 Health & metrics | `videoflow/runtime/health.py` |
| §13 Blob store | `videoflow/wire/serialization.py` |
| §14 Idempotency | `videoflow/runtime/idempotency.py` |

---

## 1. Environment contract

A worker is configured entirely through environment variables. The control plane
(`manifests.py` for Kubernetes, `engines/local.py` for local subprocesses) sets
them identically; a component MUST read its configuration from these and MUST NOT
depend on any other configuration channel for routing.

### 1.1 Variables

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `VF_NODE_NAME` | yes | — | This node's stable name; the identity used for its own output subject/stream and in logs. Authoritative for routing (`ENV-2`). |
| `VF_NODE_KIND` | yes | — | `producer` \| `processor` \| `consumer`. |
| `VF_PARENT_NAMES` | yes | `''` | Comma-separated parent node names, in the exact positional order the node's `process()` expects (`ENV-3`). Empty for a producer. |
| `VF_HAS_CHILDREN` | no | `1` | `1` if anything downstream consumes this node's output, else `0`. A node with no children MUST NOT publish data and MUST NOT publish EOS (`LOOP-6`). |
| `VF_FLOW_ID` | yes | — | Stable flow identifier (constant across runs). |
| `VF_RUN_ID` | yes | — | Per-run identifier; scopes this run's streams/subjects/durables. |
| `VF_FLOW_TYPE` | no | `realtime` | `realtime` \| `batch`. Selects retention/discard/redelivery semantics (§3, §7). |
| `VF_NATS_URL` | yes | — | NATS server URL, e.g. `nats://host:4222`. |
| `VF_REPLICA_ID` | no | see §1.2 | This replica's index (0 for single-task nodes). |
| `VF_NB_TASKS` | no | `1` | Replica count of this node (partition ownership divisor, §10). |
| `VF_PARTITION_BY` | no | unset | Partition key: `trace_id` or a metadata field name. Enables partitioned consumption when set **and** `VF_NB_TASKS > 1` (§10). |
| `VF_JOIN_POLICY_JSON` | no | unset | JSON `JoinPolicy` for a multi-parent node (§8.1). Absent ⇒ the flow-type default policy. |
| `VF_ACK_WAIT_SECONDS` | no | `60` | Per-message ack deadline (§7). |
| `VF_MAX_RETRIES` | no | `3` | BATCH redelivery attempts before dead-letter; `max_deliver = retries + 1` (§7). |
| `VF_EOS_QUIESCENCE_MS` | no | `500` | Drain quiescence window before honoring EOS (§9). |
| `VF_HEALTH_PORT` | no | `0` (local) / `8080` (k8s) | Health server port; `0` disables it (§12). |
| `VF_BLOB_REDIS_URL` | no | unset | Enables the external blob store for large payloads (§13). |
| `VF_STRUCTURED_LOGS` | no | unset | Truthy ⇒ JSON structured logs. Cosmetic; not protocol. |
| `VF_ENVELOPE_VERSION` | no | see §4.1 | Wire envelope version to emit/accept for this run. The only supported version is 4. |

- **ENV-1**: A worker MUST read `VF_NODE_NAME`, `VF_NODE_KIND`, `VF_FLOW_ID`,
  `VF_RUN_ID`, `VF_NATS_URL`, and `VF_PARENT_NAMES` and fail fast with a clear
  error if a required one is missing.
- **ENV-2**: `VF_NODE_NAME` is authoritative for the node's own output routing.
  A worker MUST publish to the subject derived from `VF_NODE_NAME` (§2), even if
  some embedded parameter suggests a different name.
- **ENV-3**: A processor/consumer MUST order its per-parent inputs by the order of
  `VF_PARENT_NAMES`, not by arrival order or map iteration order. `process(*inputs)`
  is positional.
- **ENV-4**: Unknown `VF_*` variables MUST be ignored (forward-compatibility).

### 1.2 Replica-id resolution

- **ENV-5**: The replica id is resolved in this order:
  1. `VF_REPLICA_ID`, if set to a parseable integer;
  2. else the trailing ordinal parsed from `POD_NAME`, then `HOSTNAME` — i.e. for
     a value of the form `<name>-<n>` where `<n>` is all digits, use `<n>`
     (Kubernetes StatefulSet pod naming);
  3. else `0`.

  This MUST match `videoflow/runtime/worker.py:_resolve_replica_id`.

---

## 2. Naming

All broker names are deterministic string functions of `(flow_id, run_id,
node_name)`. Every component computes them the same way — there is no service
discovery. Reference: `videoflow/messaging/topology.py`.

- **NAME-1** (sanitize): before interpolation, each of `flow_id`, `run_id`, and
  `node_name` is sanitized by replacing every maximal run of characters **not** in
  `[A-Za-z0-9_-]` with a single underscore `_`. Regex: `[^A-Za-z0-9_-]+` → `_`.
- **NAME-2** (data subject): a node's own output subject is
  `vf.{flow}.{run}.{node}` (sanitized parts).
- **NAME-3** (stream): a node's stream is `vf-{flow}-{run}-{node}`.
- **NAME-4** (EOS subject): `vf.{flow}.{run}.{node}._eos`. EOS markers ride a
  **separate subject on the same stream** as data, so data consumers (filtered to
  the data subject) never see them.
- **NAME-5** (data durable): the shared durable for edge (child ← parent) is
  `{child}--from--{parent}`. Replicas of one child share this name (competing
  consumers); distinct children of one parent get distinct names (fan-out).
- **NAME-6** (partitioned durable): a partitioned child's per-replica durable is
  `{child}--from--{parent}--p{replica_id}`.
- **NAME-7** (EOS durable): per-replica EOS consumer durable is
  `{child}--eos--{parent}--{instance_id}`, where `instance_id` is unique per
  running replica (a stable ordinal or a per-process id). Every replica MUST
  observe EOS via its own EOS durable.
- **NAME-8** (control subject): `vf.{flow}.{run}._control.stop`.
- **NAME-9** (DLQ stream / subject): stream `vf-{flow}-{run}-dlq`; a node's
  dead-letter subject is `vf.{flow}.{run}._dlq.{node}` (the DLQ stream binds
  `vf.{flow}.{run}._dlq.>`).

---

## 3. Streams and durable consumers

A component that provisions lazily (or that a conformance harness inspects) MUST
use these exact configurations. Normal deployments provision up front (§3.4);
the messenger's lazy `add_stream` is an idempotent fallback. Reference:
`topology.py`.

### 3.1 Per-node stream

- **STREAM-1**: one JetStream stream per node (`NAME-3`) binding two subjects:
  the data subject (`NAME-2`) and the EOS subject (`NAME-4`).
- **STREAM-2** (REALTIME): `retention=LIMITS`, `max_msgs=max(1, realtime_buffer)`
  (default buffer 1), `discard=OLD`, `duplicate_window=120s`. A full stream evicts
  the oldest message so a publish never blocks — **freshest-wins**.
- **STREAM-3** (BATCH): `retention=INTEREST`, `max_msgs=10000`, `discard=NEW`,
  `duplicate_window=120s`. A full stream **rejects** new publishes; the publisher
  turns rejection into blocking backpressure (§7.2). Under INTEREST retention a
  message published with no registered consumer interest is dropped — hence
  up-front provisioning (§3.4) is mandatory for BATCH.

### 3.2 Data consumer (per child←parent edge)

- **STREAM-4**: a durable pull consumer on the **parent's** stream, filtered to
  the parent's **data** subject (`NAME-2`) so EOS markers are not delivered here.
  `ack_wait = VF_ACK_WAIT_SECONDS`. `max_ack_pending` bounds server-side prefetch
  (reference uses a small value, `queue_maxsize + 2`).
- **STREAM-5** (`max_deliver`): REALTIME ⇒ `1` (no redelivery). BATCH ⇒
  `VF_MAX_RETRIES + 1`.

### 3.3 EOS consumer (per child←parent edge, per replica)

- **STREAM-6**: a per-replica durable pull consumer on the parent's stream,
  filtered to the parent's **EOS** subject (`NAME-4`), `ack_wait = 30s`, with an
  `inactive_threshold` (reference 3600s) so the server reaps it after the flow
  ends.

### 3.4 Up-front provisioning

- **STREAM-7**: before any worker publishes, the control plane creates every
  node stream, the DLQ stream, and every data durable consumer (one per replica
  for partitioned children; one shared otherwise). Provisioning MUST be idempotent
  (creating an existing stream/consumer is a no-op). Reference:
  `topology.provision_flow`. A component runtime does not provision the flow; it
  MAY lazily ensure its own stream exists as a fallback.
- **STREAM-8** (DLQ stream): `vf-{flow}-{run}-dlq`, binding `vf.{flow}.{run}._dlq.>`,
  `retention=LIMITS`, `discard=OLD`, `max_age=7 days`.

---

## 4. Envelope and wire format

Every broker message is one **envelope**: routing/trace metadata plus a typed
payload. Reference: `videoflow/wire/serialization.py`; the versioned IDL lives in
`spec/proto/videoflow/v1/` (Phase 1).

### 4.1 Versioning

- **WIRE-1**: the envelope carries an integer `v`. Protocol v1's sole wire is
  **envelope v4** (protobuf, §4.2). The earlier msgpack envelopes (**v3**, and
  **v2**) — which carried a Python-only, code-executing payload codec — have been
  removed (see RFC 0001); a decoder MUST refuse them rather than parse them. A run
  is **version-homogeneous**: streams are run-scoped (`NAME-<run>`), so one run
  never mixes envelope versions.
- **WIRE-2**: the emit/accept version for a run is pinned by `VF_ENVELOPE_VERSION`
  (only `4` is supported). A worker MUST refuse to start if asked for a version it
  cannot speak. When present, a `VF-Env: <v>` NATS header SHOULD accompany each
  message so tooling can identify the format without decoding.
- **WIRE-3**: a decoder MUST reject an envelope whose `v` it does not support (and
  any pre-v4 msgpack envelope), with a clear error, rather than silently misparsing.

### 4.2 Envelope fields

Logical fields (the v4 protobuf schema in `spec/proto/videoflow/v1/envelope.proto`
is the wire encoding; these are the semantics every version preserves):

| Field | Type | Meaning |
|---|---|---|
| `v` | uint32 | Envelope version (§4.1). |
| `type` | enum | `data` or `eos` (§4.3). |
| `producer_name` | string | Name of the node that emitted this message (its `VF_NODE_NAME`). |
| `flow_id`, `run_id` | string | Scope identifiers. |
| `trace_id` | string | Lineage identity (§5, §8). |
| `seq` | uint64 | Representative sequence number, stable across redelivery of the same logical message (§5). |
| `event_ts` | optional double | Event time (epoch seconds) of the underlying real-world event; minted by the producer, carried forward unchanged. Absent ⇒ null/None (v2 had no such field). Used by time-aligned joins (§8.3). |
| `span_id`, `parent_span_id` | string | Optional trace-correlation ids; may be empty. |
| `replica_id` | uint32 | Emitting replica index (distinguishes EOS markers from different replicas of one node). |
| `metadata` | map<string, Value> | Arbitrary per-message metadata (§4.5). Producers stamp `proctime`/`actual_proctime` floats here; a partition key travels as `_partition_key` (§10). |
| `payload_type` | string | Identifies the payload codec/type (§4.4). |
| `payload` | bytes | Encoded payload (or a blob reference, §13). Empty for EOS. |

- **WIRE-4**: a decoder MUST preserve `metadata`, `trace_id`, `seq`, `event_ts`,
  `replica_id`, and `producer_name` unchanged when a node carries an input group
  forward to its output (§5, §8) — these drive dedup, ordering, partitioning, and
  time joins downstream.

### 4.3 Message types

- **WIRE-5** (`data`): a normal payload message on the data subject (`NAME-2`).
- **WIRE-6** (`eos`): an end-of-stream marker on the EOS subject (`NAME-4`) with
  an **empty payload**. It is a distinct message type, not a magic payload value.
  Its `trace_id` is `eos-r{replica_id}` and its dedup id includes `replica_id`
  (`MSGID-2`) so markers from different replicas of one node do not collapse.

### 4.4 Payload types (codecs)

- **WIRE-7** (tensor): an N-dimensional array is encoded as `videoflow.v1.Tensor`
  = `{ shape: repeated int64, dtype: string, data: bytes }`. `dtype` uses numpy
  dtype strings (`uint8`, `float32`, `int64`, …). `data` is the raw C-contiguous
  buffer as a single `bytes` field (never repeated scalars). This is the frame /
  detections / tracks contract and is fully language-neutral. Video frames travel
  by value in this encoding (subject to the blob threshold, §13).
- **WIRE-8** (well-known payloads): `spec/proto/videoflow/v1/payloads.proto`
  defines `Frame`, `Detections`, `Tracks`, `BlobRef` (§13) atop `Tensor`. A
  component MAY exchange any of these.
- **WIRE-9** (vendor extension): a payload MAY be any protobuf message; its
  `payload_type` is the message's fully-qualified name and `payload` its encoded
  bytes. An SDK exposes a type registry (FQN → decoder). An unknown `payload_type`
  MUST pass through opaquely (a node that only forwards or stores need not decode
  it) rather than erroring.
- **WIRE-10** (structured values): scalars, lists, and string-keyed maps that are
  not tensors are encoded as `videoflow.v1.Value` (§4.5), `payload_type =
  videoflow.v1.Value`.
- **WIRE-11** *(withdrawn — see RFC 0001)*: the legacy Python-only, code-executing
  payload codec has been removed. A payload with no built-in encoding uses a
  registered vendor encoder (WIRE-9); an unrecognized `payload_type` MUST be carried
  through opaquely and MUST NOT be deserialized (see WIRE-9).

### 4.5 The `Value` type

- **WIRE-12**: `videoflow.v1.Value` is a self-describing union over: double,
  signed 64-bit integer, string, bytes, bool, null, ordered list of `Value`,
  string-keyed map of `Value`, and a nested `Tensor` (WIRE-15). It MUST round-trip
  integers and doubles distinctly (an int64 id MUST NOT be silently coerced to a
  double). This is why `Value` is used rather than a JSON-object type.
- **WIRE-15**: a `Value` MAY hold a `Tensor` (`tensor_value`), so a structured
  container — a list/tuple or string-keyed map that mixes arrays with scalars, e.g.
  a `(frame_index, frame)` tuple — has a neutral encoding. A *bare* ndarray payload
  is still a top-level `Tensor` (WIRE-7), not a `Value`; only an array *nested inside*
  a container travels as `tensor_value`.

---

## 5. Message id and deduplication

- **MSGID-1**: every published message carries a `Nats-Msg-Id` header equal to
  `derive_message_id(flow_id, run_id, producer_name, trace_id, seq, msg_type)`,
  defined as the first **32 hex characters** of
  `SHA-256("{flow_id}:{run_id}:{producer_name}:{trace_id}:{seq}:{msg_type}")`
  (UTF-8). JetStream drops a duplicate id within the stream's `duplicate_window`
  (`STREAM-2`/`STREAM-3`, 120s). This string function is byte-identical across
  languages and MUST match exactly.
- **MSGID-2**: because `seq` and `trace_id` are carried forward from the input
  group (§8) rather than regenerated, a node that crashes after publishing but
  is re-run and recomputes the same output for the same input produces the **same**
  id — so the retry copy is de-duplicated. A component MUST therefore derive its
  output `seq`/`trace_id` from its input group per §8, never from a wall clock or
  local attempt counter.
- **MSGID-3** (producer trace minting): a producer (no parents) mints `trace_id =
  "{node_name}:{n}"` and `seq = n` for a monotonically increasing local counter
  `n` (starting at 1). This is the one place ids originate.

---

## 6. Node lifecycle and the task loop

An SDK reproduces the loop, not just the wire. Reference: `videoflow/core/task.py`.
A component author implements the role callbacks; the SDK's task loop drives them.

- **LOOP-1** (lifecycle order): `open()` is called once before any
  next/process/consume; `close()` is called once after the loop ends, in a
  `finally` (so it runs even on error/teardown). Heavy setup (model load, device
  open) belongs in `open()`, not construction.
- **LOOP-2** (producer): repeatedly — check for termination (§11); if terminating,
  stop. Else call `next()` to get one item; if the node signals end-of-input
  (Python `StopIteration`; SDK equivalent), stop. Else, if the node has children,
  publish the item as data. On stop, if the node has children, publish EOS
  (`WIRE-6`) and exit.
- **LOOP-3** (processor): repeatedly — receive one input group (§8). If the group
  is an all-parents-stopped signal, then (if it has children) publish EOS and exit.
  Otherwise call `process(*inputs)` in parent order (`ENV-3`); if it has children,
  publish the output as data; then **ack the inputs** (`DELIV-1`). If
  process/publish raises, **fail the inputs** (§7.3) and continue looping — a
  poison message MUST NOT crash the worker.
- **LOOP-4** (consumer): like a processor but calls `consume(*inputs)` and never
  publishes. Optional sink idempotency wraps the consume (§14). Ack/fail as in
  `LOOP-3`.
- **LOOP-5** (publish-before-ack ordering): a processor MUST publish its output
  **before** acking its inputs. Combined with content-derived ids (§5), this makes
  a mid-processing crash safe: the un-acked input is redelivered and reprocessed,
  and the duplicate output is de-duplicated.
- **LOOP-6** (no children ⇒ no output): if `VF_HAS_CHILDREN=0`, a node MUST NOT
  publish data or EOS.
- **LOOP-7** (metadata stamping): a producer/processor SHOULD stamp `proctime`
  (seconds spent in next/process) and `actual_proctime` (seconds since the prior
  iteration end) into the published message's metadata, matching the reference
  (used by metrics, §12, and observable by downstream nodes). This is SHOULD, not
  MUST — omitting it degrades metrics only.

### 6.1 Runtime context (component-facing capabilities)

A node method MAY receive a runtime context exposing at least: `flow_id`,
`run_id`, `node_name`, `replica_id`, a logger, and:

- **LOOP-8** (`set_output_partition_key(value)`): sets the partition key attached
  to this node's **next** published output; the SDK MUST place it in metadata as
  `_partition_key` and clear it after that publish (§10).
- **LOOP-9** (`set_output_event_timestamp(value)`): sets the `event_ts` (epoch
  seconds) stamped on the **next** published output; used by producers of
  time-sensitive data. The SDK MUST clear it after that publish.

### 6.2 Event-time propagation

- **LOOP-10**: the `event_ts` of a published message is chosen as: an explicit
  `set_output_event_timestamp` value if set (one-shot); else the input group's
  `event_ts` (§8, carried forward); else — for a producer with neither — the
  publish wall-clock time as a last resort.

---

## 7. Delivery, acknowledgement, retry, dead-letter

Delivery is **at-least-once**; combined with dedup (§5) and idempotent sinks (§14)
it yields exactly-once-ish effects. Reference: `nats_messenger.py`.

### 7.1 Ack-after-process

- **DELIV-1**: the input group returned by `receive_message` is acked **only after**
  the node processed it (and, for a processor, published its output) — never on
  receipt. An SDK MUST hold the broker ack handles unresolved until the task loop
  says ack or fail.
- **DELIV-2** (ack): on success, every held input handle is acked.
- **DELIV-3** (keepalive): while a group is in flight (e.g. a slow `process()`),
  the SDK MUST periodically extend the ack deadline (JetStream `in_progress`) of
  unresolved handles, so a slow node does not trigger spurious redelivery. The
  reference extends at `max(1s, ack_wait/3)` intervals.

### 7.2 Publish backpressure

- **DELIV-4** (REALTIME publish): a publish never blocks. If the broker rejects it
  (should not happen under `discard=OLD`) or the flow is stopping, drop it.
- **DELIV-5** (BATCH publish): on a "stream full" rejection (`discard=NEW`), retry
  with backoff until it succeeds — this is how a slow consumer applies real
  backpressure to upstream. A stopping flow (termination signalled) MAY abandon
  the publish. The reference backoff is `[0.05, 0.1, 0.2, 0.5, 1.0]s` (capped,
  repeated); the exact schedule is `implementation-defined`, the blocking behavior
  is not.

### 7.3 Failure, retry, dead-letter

On `fail_inputs(exc)` for the held handles:

- **DELIV-6** (REALTIME): terminate the message(s) (no redelivery — freshest wins).
- **DELIV-7** (BATCH, under retry budget): NAK with a delay so it is redelivered.
  The reference delay is `min(2**num_delivered, 30)s`; the schedule is
  `implementation-defined`, redelivery is not.
- **DELIV-8** (BATCH, budget exhausted): once `num_delivered >= max_deliver`
  (`STREAM-5`), publish the **original raw message bytes** to the node's DLQ
  subject (`NAME-9`) with headers `VF-Origin-Node`, `VF-Error` (truncated repr),
  `VF-Num-Delivered`, and an idempotent `Nats-Msg-Id` of
  `dlq:{flow}:{run}:{node}:{stream_seq}`; then terminate the original so it stops
  being redelivered. If the DLQ publish itself fails, the message MUST NOT be
  silently dropped — NAK it (with delay) so a later attempt can dead-letter it.
- **DELIV-9** (poison / undecodable): a message that fails to **decode** MUST be
  terminated (not redelivered forever) — it is a genuinely poisoned wire payload.

---

## 8. Join / input-group assembly

A multi-parent node processes **input groups**: one entry per parent, assembled
from the per-parent streams. There are two grouping strategies. This is the most
intricate part of the protocol and the most important to reproduce exactly.
Reference: `videoflow/core/policies.py` (the policy) and
`videoflow/messaging/grouping.py` (the two assemblers).

An assembler is fed decoded `(parent_name, entry, ack_handle)` triples, owns the
pending buffers, resolves the handles of anything it **discards** (acking drops,
NAKing errors), and hands the handles of anything it **emits** out **unresolved**
inside a ready group (so ack-after-process, `DELIV-1`, holds end to end).

### 8.1 JoinPolicy (`VF_JOIN_POLICY_JSON`)

A JSON object with these fields (all optional unless noted):

| Field | Type | Default | Meaning |
|---|---|---|---|
| `mode` | `"trace"` \| `"time"` | `"trace"` | Grouping strategy (§8.2 / §8.3). |
| `timeout_seconds` | number \| null | null | How long to wait for the rest of a group before applying `missing` (or emitting a quorum group). null = wait forever. For `time` mode this is the lateness bound. |
| `missing` | `"drop"` \| `"wait"` \| `"error"` | `"drop"` | What to do with an incomplete group at timeout (§8.4). `"wait"` forces `timeout_seconds=null`. |
| `max_pending` | integer | 256 | Hard cap on buffered incomplete groups; beyond it the **oldest** is evicted as a drop. |
| `tolerance_ms` | number | — (required for `time`) | Two messages from different parents join iff their `event_ts` differ by ≤ this. |
| `quorum` | integer \| null | null | (`time` only) minimum synchronized parents for a **timed-out** group to still emit (missing parents delivered as null). Requires `timeout_seconds`. |
| `collect` | map<parent, window_ms> | {} | (`time` only) high-rate parents delivered as **lists** rather than 1:1 (§8.3). |

- **JOIN-1** (policy validation): `missing="wait"` ⇒ `timeout_seconds` is forced
  to null. `mode="time"` requires positive `tolerance_ms`. `tolerance_ms`,
  `quorum`, `collect` apply only to `mode="time"` (else it is an error). `quorum >= 1`
  and requires `timeout_seconds`. A `collect` window MUST be a positive number of ms.
  These MUST match `JoinPolicy.__init__`.
- **JOIN-2** (defaults by flow type): absent `VF_JOIN_POLICY_JSON`, the default
  policy is: BATCH ⇒ `{timeout_seconds: null, missing: "wait"}` (completeness
  matters, bounded by `max_pending`); REALTIME ⇒ `{timeout_seconds: 10.0, missing:
  "drop"}` (a dropped sibling must not stall the join forever). Reference:
  `JoinPolicy.default_for`.
- **JOIN-3** (single parent): a node with 0 or 1 parent never assembles; its one
  input passes straight through (no timeout applies).
- **JOIN-4** (replica constraint): `mode="time"` with more than one parent requires
  `nb_tasks == 1` (replicas would each see only some halves and never complete a
  group). An SDK MUST reject this configuration.

### 8.2 Trace grouping (`mode="trace"`, default)

Groups by exact `trace_id` (diamond topologies descending from one producer).

- **JOIN-5** (completeness): a group keyed by `trace_id` is ready once **every**
  parent's entry with that id has arrived.
- **JOIN-6** (representative identity): a ready group's carried-forward `seq` is
  the **min** `seq` over its parent entries; its `event_ts` is the **min** of the
  present entries' `event_ts` (ignoring nulls), or null if none. Its `trace_id` is
  the shared parent `trace_id`. (Min is stable across redelivery because the same
  messages reassemble.)
- **JOIN-7** (redelivery-supersede): if a parent half already buffered for a group
  is redelivered before the group completed, terminate the stale handle and keep
  the fresh delivery (restarting its ack deadline). MUST NOT seed a duplicate group.
- **JOIN-8** (timeout eviction): if `timeout_seconds` is set and the node has ≥2
  parents, a group whose age since first-seen ≥ timeout is evicted per `missing`
  (§8.4). Age uses a monotonic clock.
- **JOIN-9** (`max_pending`): when the buffered-group count exceeds `max_pending`,
  evict the **oldest** (insertion order) as a **drop** (ack its handles),
  regardless of `missing`.

### 8.3 Time grouping (`mode="time"`)

Groups by event time; fuses **independent** producers (e.g. multiple cameras).
Parents are split into **sync** parents (gate completeness) and **collect** parents
(delivered as lists, never gate). Reference: `TimeGroupAssembler`.

- **JOIN-10** (validation): every `collect` key MUST be a real parent; at least one
  **sync** parent MUST remain (to anchor group time); `quorum` MUST NOT exceed the
  number of sync parents.
- **JOIN-11** (group time): a group's time `ts` is the **min** `event_ts` over its
  members. A message with no `event_ts` falls back to its **receiver arrival time**
  (wall clock at ingestion) — normative; real deployments should stamp at the
  producer.
- **JOIN-12** (matching): a sync-parent message joins the pending group, among
  those not already holding that parent, whose `ts` is **nearest** to the message's
  `event_ts` **and** within `tolerance_ms`; ties broken toward the smaller absolute
  difference (first-found on equal diff). If none qualifies, it **seeds a new
  group**. On joining, the group's `ts` becomes `min(ts, message.event_ts)`.
- **JOIN-13** (redelivery-supersede): a sync-parent message matching an entry
  already in a group by `(trace_id, seq)` supersedes it in place (term old handle,
  keep new); MUST NOT seed a duplicate.
- **JOIN-14** (collect buffering): a collect-parent message is appended to that
  parent's buffer (not matched to a group on arrival). The buffer is bounded at
  `max(1024, max_pending*16)` entries; beyond it the **oldest** is dropped (acked).
- **JOIN-15** (settle window): let `settle = max(collect window seconds)` (0 if no
  collect parents). A **complete** group (all sync parents present) is held for
  `settle` after first-seen before emission, so trailing high-rate collect samples
  can still arrive.
- **JOIN-16** (ready emission): `pop_ready` first returns any group **staged** by
  sweep (quorum/timeout emissions, in order), then the first complete group past
  its settle window.
- **JOIN-17** (timeout / quorum via sweep): on each sweep, for a group older than
  `timeout_seconds`: if complete, or if `quorum` is set and present-sync-count ≥
  `quorum`, **stage it for emission** (a below-complete quorum emission delivers the
  missing sync parents as **null**); otherwise **evict** it per `missing` (§8.4).
- **JOIN-18** (collect attachment at emission): when a group is emitted, for each
  collect parent, claim every buffered sample whose `|event_ts − group.ts| ≤ window`,
  remove them from the buffer, sort by `event_ts`, and deliver them as a **list**
  in that parent's position (message/metadata/event_ts each become parallel lists).
- **JOIN-19** (collect buffer pruning): a buffered collect sample older than
  `timeout_seconds` (or 30s if no timeout) plus `settle`, that no group claimed, is
  dropped (acked) as stale.
- **JOIN-20** (minted identity — determinism-critical): an emitted time group's
  identity is derived from its time: `seq = round(group.ts * 1e6)` and `trace_id =
  "tw-{seq}"`, `event_ts = group.ts`. `round` MUST be **round-half-to-even**
  (banker's rounding, matching Python 3's `round`) over IEEE-754 doubles, so Rust,
  C++, and Python mint **byte-identical** ids for the same `ts` — downstream dedup
  (§5) depends on this. This is normative and MUST be conformance-tested.

### 8.4 Eviction / missing policy

- **JOIN-21**: evicting an incomplete group applies `missing`: `drop` and `wait`
  ⇒ **ack** the partial handles (give up on the group); `error` ⇒ **NAK** them
  (redeliver — the missing half may still arrive, and eventually dead-letter). Note
  `wait` never times out, so its eviction only happens via `max_pending` (`JOIN-9`),
  which always acts as a drop.

---

## 9. End-of-stream drain

A processor/consumer stops only after every parent is **fully drained**, not
merely when EOS is seen — otherwise in-flight data behind the EOS marker would be
lost. Reference: `nats_messenger.py` (`_is_parent_stopped`, `_all_parents_stopped`).

- **EOS-1** (per-replica observation): every replica observes each parent's EOS via
  its own EOS durable (`NAME-7`, `STREAM-6`). The EOS message is **held un-acked**
  until that parent is declared drained.
- **EOS-2** (duplicate EOS): a second EOS from a parent (e.g. another replica's
  marker) after that parent's EOS is already seen is simply acked and ignored.
- **EOS-3** (drain condition): a parent is **stopped** once **all** hold:
  (a) its EOS has been observed;
  (b) no data from it is buffered locally (its prefetch queue is empty);
  (c) no pending join group holds a half from it (`has_pending_from`, including
  staged-but-not-yet-emitted groups and non-empty collect buffers, §8);
  (d) its data durable reports `num_pending == 0` **and** `num_ack_pending == 0`;
  and (d) has held continuously for `VF_EOS_QUIESCENCE_MS` (checked on two probes
  that far apart). The quiescence window tolerates a replicated parent momentarily
  between finishing and publishing. Any regression (new data, non-empty pending)
  resets the quiescence timer.
- **EOS-4** (ack on stop): when a parent becomes stopped, its held EOS handle is
  acked. A crash mid-drain leaves EOS un-acked and re-observable on restart.
- **EOS-5** (loop termination): when **all** parents are stopped, `receive_message`
  returns an all-parents-stopped result (every parent entry marked `is_stop_signal`),
  which drives the task loop to (publish EOS if it has children, then) run `close()`
  and exit (§6).
- **EOS-6** (`has_pending_from` for time groups): for a time-mode assembler, a
  parent counts as pending if any staged ready group, any pending group, or (for a
  collect parent) any non-empty collect buffer still holds its message. This MUST be
  reproduced or EOS could strand a staged group.

---

## 10. Partitioning

A partitioned node scales a stateful stage by key: every replica sees every
message (broadcast via per-replica durables) and keeps only the ones it owns.
Reference: `nats_messenger.py` (`_owns`), `topology.py`.

- **PART-1** (enabled): partitioning is active iff `VF_PARTITION_BY` is set **and**
  `VF_NB_TASKS > 1`. Otherwise every message is owned.
- **PART-2** (key extraction): if `VF_PARTITION_BY == "trace_id"`, the key is the
  entry's `trace_id`; otherwise it is `metadata[VF_PARTITION_BY]` (may be absent →
  `None`, stringified as below).
- **PART-3** (ownership — exact algorithm): compute `digest = SHA-256(str(key))`
  (UTF-8) as lowercase hex. Let `h = int(digest[:8], 16)` — the **first 8 hex
  characters** interpreted as a 32-bit unsigned integer (NOT the full digest). The
  replica owns the message iff `h % VF_NB_TASKS == VF_REPLICA_ID`. This truncation
  is deliberate and MUST match exactly across languages. `str(key)` MUST match
  Python's `str()` for the key types in use (a string is itself; `None` → `"None"`).
- **PART-4** (skip non-owned): a non-owned message is **acked and skipped** on the
  replica's own durable (every replica has its own durable, so acking does not
  deprive another replica).

---

## 11. Control plane

- **CTRL-1** (stop subject): a flow-wide stop is a plain-NATS (not JetStream)
  message with payload `stop` on `vf.{flow}.{run}._control.stop` (`NAME-8`). A
  worker subscribes to it; receipt sets a termination flag.
- **CTRL-2** (producer honors stop): a producer checks the termination flag each
  iteration and stops pulling new input when set (then publishes EOS if it has
  children).
- **CTRL-3** (consumer/processor honors stop): when the termination flag is set,
  `receive_message` returns an all-parents-stopped result immediately (even
  mid-stream), so the loop breaks and runs `close()`. This is a hard stop, distinct
  from the graceful EOS drain (§9).

---

## 12. Health and metrics

A worker with `VF_HEALTH_PORT > 0` MUST serve a plain HTTP server on that port
(`0.0.0.0`) with these endpoints. Reference: `videoflow/runtime/health.py`.

- **HEALTH-1** (`/readyz`): 200 `ready` once the node has begun processing (marked
  on first messenger activity — first publish or receive — which is **after**
  `open()` returns, so a slow model-loading `open()` correctly stays un-ready);
  else 503 `not-ready`.
- **HEALTH-2** (`/healthz`): 200 `ok` while the run loop is beating; 503 `stalled`
  if no beat within `LIVENESS_STALL_SECONDS` (reference 60s). The loop beats on
  each receive/publish/termination check.
- **HEALTH-3** (`/metrics`): 200 with Prometheus text exposition. The reference
  emits, labelled `{node="<name>"}`: `videoflow_<metric>_count` /
  `videoflow_<metric>_sum` for observed histograms `proctime_seconds` and
  `actual_proctime_seconds`, and counters `videoflow_messages_published_total`,
  `videoflow_messages_received_total`, `videoflow_messages_processed_total`,
  `videoflow_messages_failed_total`. Metric names/labels SHOULD match so dashboards
  are portable.
- **HEALTH-4** (unknown path): 404.

---

## 13. Blob store (large payloads)

NATS caps message size (~1MB default `max_payload`); large frames are offloaded.
Reference: `videoflow/wire/serialization.py`.

- **BLOB-1** (threshold): if an encoded payload exceeds `MAX_INLINE_PAYLOAD_BYTES`
  (default 512KiB, override via `VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES`) **and** a blob
  store is configured (`VF_BLOB_REDIS_URL`), the payload bytes are written to the
  store and the envelope carries a `videoflow.v1.BlobRef` = `{ ref: string,
  inner_payload_type: string, size: uint64 }` in place of the inline payload
  (`payload_type = videoflow.v1.BlobRef`).
- **BLOB-2** (no store): if the threshold is exceeded and no store is configured,
  the encode MUST fail with a clear error (never silently truncate or over-send).
- **BLOB-3** (resolve): decoding a `BlobRef` fetches `ref` from the store and
  decodes the inner payload as `inner_payload_type`. `ref` is opaque; the reference
  Redis store uses keys `vf-blob-<uuid>` with a TTL (default 3600s).
- **BLOB-4** (interop): the blob store is the same for all languages in a flow; the
  ref is a plain string. An SDK MUST support at least the Redis store to interoperate
  with flows that offload.

---

## 14. Sink idempotency (optional)

- **IDEM-1**: a consumer MAY opt into effect-dedup. When enabled and a store is
  configured, before consuming an input the SDK computes the input group's stable
  key (`last_input_key` = `derive_message_id(flow, run, node, trace_id, seq, "data")`,
  §5), and if the store reports it **seen**, skips the consume and acks. Otherwise
  it consumes, marks the key, then acks.
- **IDEM-2** (key): the store key is `"vf-idem-" + SHA-256("{flow}:{node}:{message_id}")`
  (full hex). The reference Redis store sets it with a 24h TTL. Consumers are single
  sinks (not replicated), so plain check-then-mark is race-free.

---

## 15. Conformance

Protocol v1 remains `stabilizing` until at least two non-Python SDKs pass the full
conformance suite (`conformance/`, Phase 4). Every MUST above is exercised by a
scenario that references its ID; the cross-index lives alongside the scenarios.
The Python worker is the reference oracle and MUST pass the suite first.

Changes to this document follow the RFC process in `spec/rfcs/`. A change that
alters observable wire or routing behavior requires a protocol major bump and a
`buf breaking` review of `spec/proto/`.
