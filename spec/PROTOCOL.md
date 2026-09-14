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
cases (`tests/conformance/`, catalogued in `tests/conformance/catalog/`; the
ID ↔ test cross-index is `spec/conformance-map.md`) reference these IDs. Each ID
is **MUST** unless marked SHOULD or `implementation-defined`.

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
| Backend contracts, capabilities, the runtime ledger (`EOS-7`, `CTRL-4`, `BLOB-13`…`BLOB-15`) | `videoflow/backends/` (`messaging.py`, `payload.py`, `allocation.py`, `runtime.py`, `capabilities.py`, `identity.py`; `memory/` holds the reference implementations), `videoflow/messaging/obligations.py` |

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
| `VF_MAX_RETRIES` | no | `3` | Redelivery attempts before dead-letter for an at-least-once node; `max_deliver = retries + 1` (§7). |
| `VF_DELIVERY` | no | flow-type preset | `at-least-once` or `best-effort`, overriding the flow type for this node (`DELIV-10`). |
| `VF_ON_ERROR` | no | `transient` | Disposition for exceptions the SDK cannot classify: `poison`, `transient`, `worker_fatal` (`ERR-2`). |
| `VF_BREAKER_THRESHOLD` | no | `10` | Consecutive failures before the worker declares itself unhealthy and exits (`ERR-5`); `0` disables. |
| `VF_PROGRESS_TIMEOUT_SECONDS` | no | `300` | Seconds with no ack while work is pending before the node is declared stalled (`ERR-7`); `0` disables. |
| `VF_WATCHDOG_INTERVAL_SECONDS` | no | `5` | Seconds between re-checks of the `ERR-7` deadline from a watchdog thread, so a callback that never returns is caught while the loop cannot check; `0` disables the thread (the in-loop check remains). A stall found this way is written to the termination log and exits with the error's exit code. Absent ⇒ unchanged behaviour: the default interval. |
| `VF_PROFILE_REQUESTS_JSON` | no | unset | The operator's explicit channel profiles (`deploy --require-profile CHANNEL=PROFILE`) as JSON, emitted only when there are any. Absent ⇒ the flow-type presets, admitted against the composed backends before anything is provisioned (`ENV-13`). |
| `VF_RUNTIME_STORE_URL` | no | `memory://` | The run ledger (`ENV-10`, `CTRL-4`): `memory://`, `file://<dir>` (one host) or `redis://`/`rediss://` (read back for persistence). A durable, shared store turns on the `EOS-7` completion barrier, the partition lease and the ledger-budgeted delivery cap (`STREAM-15`). |
| `VF_PARENT_REPLICAS` | no | unset | Per-parent replica counts, comma-separated, positionally aligned with `VF_PARENT_NAMES` (`ENV-11`); a count mismatch fails fast. Absent ⇒ the `EOS-3` drain. |
| `VF_BLOB_READER_IDS` | no | unset | The reader obligations each payload this node publishes is held for (`ENV-12`, `BLOB-13`): `<child>` per competing child, `<child>/p<i>` per partitioned replica. Absent ⇒ `VF_BLOB_READERS` count semantics, else TTL-only. |
| `VF_GPU_GRANT_JSON` | no | unset | The launcher's delivered grant (`ENV-14`, `DeliveredGrant.to_dict()`), measured by the worker against the node's `gpu_count` before `open()`. Written by `run-local`; never by a Kubernetes pod, whose device-plugin mask is the grant. |
| `VF_FAULT_SCHEDULE_JSON` | no | unset | A conformance fault schedule (`ENV-16`), installed by the worker at start; a control plane never emits it. |
| `VF_FAULT_MARKER_DIR` | no | unset | Where a fault schedule records the barriers it hit (`ENV-17`). |
| `VF_STREAM_REPLICAS` | no | unset | Stream copies the provision Job requests for every stream of the run (`ENV-18`), read back like any other field. |
| `VF_REPLICA_SLOTS` | no | `VF_NB_TASKS` | How many replica identities a process with no declared id may claim through the run ledger (`ENV-5` step 3, `ENV-19`): the scaler's ceiling for an autoscaled Deployment, whose pods beyond `VF_NB_TASKS` are wanted replicas. Emitted only when it exceeds `VF_NB_TASKS`. |
| `VF_TERMINATION_LOG` | no | `/dev/termination-log` | Path the worker writes its structured death reason to; Kubernetes surfaces it in `containerStatuses` (`ERR-3`). |
| `VF_EOS_QUIESCENCE_MS` | no | `500` | Drain quiescence window before honoring EOS (§9). |
| `VF_HEALTH_PORT` | no | `0` (local) / `8080` (k8s) | Health server port; `0` disables it (§12). |
| `VF_BLOB_REDIS_URL` | no | unset | Enables the external blob store for large payloads (§13). |
| `VF_BLOB_READERS` | no | unset | Downstream read count of this node's published messages; enables refcounted blob reclamation (`BLOB-5`). Unset ⇒ TTL-only blobs. |
| `VF_BLOB_TTL_SECONDS` | no | unset | Blob (and counter) TTL override. Unset ⇒ flow-type default: 3600 realtime / 86400 batch (`BLOB-7`). |
| `VF_GPU_COUNT` | no | `1` | Devices **delivered** to this worker (GPU nodes only). The visible devices are exactly the delivered devices, numbered `0..count-1` (RFC 0003, amended). Informational — not routing. |
| `VF_GPU_RESOURCE_NAME` | no | unset | Kubernetes extended-resource name the devices were requested as — strategy-resolved, e.g. the mix solver's MIG profile (RFC 0003/0004). Informational — not routing. |
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
  `VF_RFC0006`, the transitional switch RFC 0006 sat behind while proposed, is
  such a variable since its acceptance: read by nothing.
- **ENV-10** … **ENV-19** are the rows above they name (RFC 0006 §8). Each is
  optional; absent, each means the behaviour the row's default names. Not
  assigned: `ENV-6`…`ENV-9`, `ENV-15` is `VF_WATCHDOG_INTERVAL_SECONDS`.

### 1.2 Replica-id resolution

- **ENV-5**: The replica id is resolved in this order:
  1. `VF_REPLICA_ID`, if set to a parseable integer (the local engine sets it;
     a Kubernetes Indexed Job — every BATCH node with `nb_tasks > 1`,
     partitioned or competing — feeds its completion index into it);
  2. else the trailing ordinal parsed from `POD_NAME`, then `HOSTNAME` — i.e. for
     a value of the form `<name>-<n>` where `<n>` is all digits, use `<n>`
     (Kubernetes StatefulSet pod naming);
  3. else, when `VF_NB_TASKS > 1`, the node is not partitioned and the run
     ledger (`ENV-10`) is durable and shared across processes: the lowest slot
     `0 .. VF_NB_TASKS-1` whose partition lease (`CTRL-4`) is free or lapsed,
     claimed through the ledger — free and lapsed slots at once, then each live
     slot waited on for at most one lease length; a process that finds every
     slot held under a live lease is an extra replica and MUST stop with
     `VF_OWNERSHIP_CONFLICT`. This is how a Kubernetes Deployment pod, which
     has no ordinal, gets an identity that is distinct among the live replicas
     and stable across a restart (the replacement resumes the crashed pod's
     slot and its ledger records);
  4. else `0`.

  This MUST match `videoflow/runtime/worker.py:_resolve_replica_id` and
  `backends.runtime.claim_replica_slot`.

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
- **NAME-9** (DLQ stream / subject): stream `vf-{flow}-dlq`; a node's dead-letter
  subject is `vf.{flow}._dlq.{run}.{node}` (the DLQ stream binds
  `vf.{flow}._dlq.>`). The stream is scoped to the **flow**, not the run: teardown
  deletes a run's streams, and dead letters are wanted precisely after a run that
  failed and was torn down. The run id lives in the subject so entries stay
  attributable and filterable (RFC 0005).

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
  `ack_wait = VF_ACK_WAIT_SECONDS`. `max_ack_pending` is the credit `STREAM-15`
  derives from the consuming node's replica count and working set.
- **STREAM-5** (`max_deliver`): REALTIME ⇒ `1` (no redelivery). BATCH ⇒
  `VF_MAX_RETRIES + 1`, except under a durable shared ledger, where `STREAM-15`
  lifts the broker cap (`-1`) and the ledger enforces the same budget.

### 3.3 EOS consumer (per child←parent edge, per replica)

- **STREAM-6**: a per-replica durable pull consumer on the parent's stream,
  filtered to the parent's **EOS** subject (`NAME-4`), `ack_wait = 30s`, with an
  `inactive_threshold` (reference 3600s) so the server reaps it after the flow
  ends.

### 3.4 Up-front provisioning

- **STREAM-7**: before any worker publishes, the control plane creates every
  node stream, the DLQ stream, and every data durable consumer (one per replica
  for partitioned children; one shared otherwise). Provisioning MUST be idempotent
  (creating an existing stream/consumer finds it), and every resource MUST be
  read back and compared with the request (`STREAM-14`). Reference:
  `topology.provision_flow`. A component runtime does not provision the flow; it
  MAY lazily ensure its own stream exists as a fallback.
- **STREAM-8** (DLQ stream): `vf-{flow}-dlq`, binding `vf.{flow}._dlq.>`,
  `retention=LIMITS`, `discard=OLD`, `max_age=7 days`. It MUST NOT be deleted by
  run teardown (`STREAM-9`).
- **STREAM-9** (flow-scoped DLQ): the DLQ stream belongs to the flow, not to a
  run. A run's teardown MUST NOT delete it, and its owner labels (`STREAM-14`)
  carry the flow id and the kind `dlq` only, so neither the label rule nor the
  subject rule can ever attribute it to a run.
- **STREAM-14** (owner metadata; exact-ownership teardown; RFC 0006): every
  stream and durable consumer a run provisions MUST carry JetStream `metadata`
  equal to `owner_labels(flow_id, run_id, node, kind, generation)`
  (`videoflow/backends/identity.py`): `videoflow.io/flow-id` and
  `videoflow.io/run-id` hold the **unsanitized** ids, `videoflow.io/node` the
  node, `videoflow.io/kind` one of `stream`, `durable`, `partitioned_durable`,
  `eos_durable`, `eos_anchor` (or `dlq` for the flow-scoped DLQ stream, which
  carries the flow id and the kind only), `videoflow.io/generation` the
  provisioning operation id. Teardown decides ownership per stream, exactly and
  never by prefix: a labelled stream is a run's iff its flow and run labels equal
  the raw ids (and the generation, when the caller narrows to one provisioning);
  an unlabelled stream is attributed by the tokens of its bound data subject,
  which are dot-delimited and therefore exact; a stream with neither is left
  standing. A listing that fails or comes back short is reported as an
  incomplete cleanup (nothing is "nothing to delete"), and the CLI exits `3`
  when a cleanup is incomplete. Read-back (amends `STREAM-7`): every stream and
  consumer MUST be read back after it is created or found, and the fields the
  request set explicitly compared with the effective configuration (metadata as
  a subset — the server adds `_nats.*` keys); any mismatch — an existing stream
  whose retention or replicas an update cannot change, a value clamped to an
  account limit, labels naming another run, a server that did not carry the
  metadata — raises `IncompatibleProfile` (`VF_INCOMPATIBLE_PROFILE`), and a
  resource that could be neither created nor read back raises `BrokerUnavailable`.
- **STREAM-15** (delivery cap and credit; RFC 0006). *Cap:* when the node's
  runtime store advertises `durable = Known(True)` and
  `shared_across_processes = True` (`CTRL-4`), an at-least-once node's data
  durables MUST be provisioned with `max_deliver = -1` and the retry budget of
  `VF_MAX_RETRIES + 1` attempts MUST be enforced by the ledger's per-message
  attempt count, which a `worker_fatal` failure never increments; otherwise
  `max_deliver` stays `STREAM-5`'s cap. Provisioning and the worker MUST derive
  the cap identically (both from `VF_RUNTIME_STORE_URL`). *Credit:*
  `max_ack_pending` MUST be `nb_tasks × (item_credit + prefetch)` for a shared
  (competing) durable and `item_credit + prefetch` for a per-replica durable,
  where `item_credit` is the node's admitted processing concurrency per replica
  (a join binds its policy's working set, `max_pending × (parents − 1) + 1`, and
  a bind whose effective credit is below it is refused with `VF_CAPABILITY`
  before any input is taken) and `prefetch` the local prefetch depth (reference
  4). A scale change re-derives it and reads it back; a backend that cannot
  resize credit MUST reject the scale request rather than add idle replicas.

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
- **MSGID-3** (producer trace minting): a producer (no parents) mints its ids
  per `MSGID-5` (a live source) or `MSGID-6` (a replayable source); this is the
  one place ids originate. The bare `{node_name}:{n}` form of protocol v1 before
  RFC 0006 is no longer emitted.
- **MSGID-5** (live sources; RFC 0006): a producer that is not replayable MUST
  mint `trace_id = f'{node}:{epoch}:{n}'` and `seq = n`, where `n` is a local
  counter starting at 1 per epoch and `epoch` is minted once per producer replica
  process at `open()` — an opaque string over `[A-Za-z0-9_-]` (no `:`), at most
  32 characters, unique across the restarts of that replica within the run
  (reference: 12 lowercase hex characters of a random UUID). Retries within one
  process reuse the same `n` and therefore the same id (`MSGID-2`); a replacement
  process never does, so nothing it publishes is mistaken for a pre-restart
  duplicate. Consumers treat the string as opaque.
- **MSGID-6** (replayable sources; RFC 0006): a producer that declares itself
  replayable (`ProducerNode.replayable = True`) MUST mint `trace_id =
  f'{node}:{offset}'` and `seq = offset`, `offset` being the source's own stable
  position, so a re-run or a restart re-mints the identical id and downstream
  dedup and sink idempotency (`IDEM-1`) engage; a deliberately new analysis of the
  same source declares an `analysis_version` and mints
  `f'{node}:{analysis_version}:{offset}'`. The runtime MUST checkpoint the last
  **accepted** offset in the runtime store (`CTRL-4`), MUST reconcile every
  `PublicationUnknown` intent before advancing it, and on restart MUST resume from
  `last_accepted + 1`. A node is either live or replayable, never both.
  Golden vectors: `spec/vectors/message_id/vectors.json` pins the three forms.

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

On `fail_inputs(exc)` for the held handles, the action is a function of the error's
**disposition** (§15) and the node's **delivery mode**, not of the flow type alone.
Reference: `core/policies.py::DeliveryPolicy.action_for`.

- **DELIV-6** (the ladder): given a disposition and the broker's `num_delivered`,
  the action MUST be:

  | disposition | best-effort | at-least-once |
  |---|---|---|
  | `poison` | sampled DLQ (`ERR-6`), then term | DLQ immediately, then term |
  | `transient` | term | NAK until `num_delivered >= max_deliver`, then DLQ |
  | `worker_fatal` | NAK | NAK |

  A `poison` message is never retried in either mode: it failed on its content and
  its content will not change. A `worker_fatal` failure is never dead-lettered in
  either mode: the message is fine, the worker is not, so it goes back for another
  replica — and the worker then **stops** (`ERR-5`).
- **DELIV-7** (retry delay): a NAK carries a delay so the message is redelivered
  later. The reference delay is `min(2**num_delivered, 30)s` multiplied by a random
  jitter in `[0.5, 1.5]`. The schedule is `implementation-defined`; redelivery is
  not, and the jitter SHOULD be reproduced — a deterministic schedule makes N
  replicas that failed together retry together.
- **DELIV-8** (dead-letter record): to dead-letter, publish the **original raw
  message bytes** to the node's DLQ subject (`NAME-9`) with headers
  `VF-Origin-Node`, `VF-Run-Id`, `VF-Code`, `VF-Disposition`, `VF-Error`
  (truncated message), `VF-Remedy` (truncated), `VF-Num-Delivered`, and an
  idempotent `Nats-Msg-Id` of `dlq:{flow}:{run}:{node}:{stream_seq}`; then terminate
  the original so it stops being redelivered. If the DLQ publish itself fails, the
  message MUST NOT be silently dropped — NAK it (with delay) so a later attempt can
  dead-letter it; with `max_deliver = -1` (`STREAM-15`) that NAK can never strand,
  and without it a NAK past the broker cap leaves the message retained but
  undeliverable, which the node's subscription observation MUST report as
  `unresolved ≥ 1` rather than as zero pending. `VF-Code` is what makes dead
  letters groupable; a free-text error string is not.
- **DELIV-9** (poison / undecodable): a message that fails to **decode** is
  `poison` classified at the transport layer, before any node sees it, and MUST be
  settled per `DELIV-15` — never redelivered forever, never terminated without a
  record.
- **DELIV-10** (delivery mode): a node's mode comes from `VF_DELIVERY`, defaulting
  to the flow type's preset (REALTIME ⇒ best-effort, BATCH ⇒ at-least-once). It
  decides `max_deliver` for that node's durables (`STREAM-5`, with the
  `STREAM-15` cap lifted under a durable shared ledger), so provisioning and the
  worker MUST derive it the same way, from the same store URL.
- **DELIV-15** (undecodable bytes get a durable record; RFC 0006): an envelope
  that fails to decode (`DecodeError`, code `VF_POISON_DECODE`) MUST follow the
  `DELIV-6` ladder for `poison`. Under at-least-once the SDK MUST publish the
  **raw wire bytes** to the node's DLQ subject with the `DELIV-8` headers
  (`VF-Code: VF_POISON_DECODE`, `VF-Disposition: poison`, `VF-Error`, `VF-Remedy`,
  `VF-Num-Delivered`, `VF-Origin-Node`, `VF-Run-Id`, `Nats-Msg-Id:
  dlq:{flow}:{run}:{node}:{stream_seq}`) and only after that publish is
  `Accepted` (a `duplicate` acceptance counts) terminate the delivery. Under
  best-effort the `ERR-6` sampler applies, then TERM. A `Terminal` settlement
  MUST carry a durable record reference: the DLQ publication's `Nats-Msg-Id` when
  dead-lettered, otherwise the position of an entry appended to the node's
  terminal log in the runtime store (`record_ref = 'ledger:{node}/terminal:{position}'`).
  An SDK MUST NOT terminate a message whose only trace would be its own
  disappearance. If the DLQ publish is `Rejected`, `PublicationUnknown` or times
  out, the delivery MUST be kept (`Retry`, a NAK with delay, never TERM) and a
  `pending_handoff` record MUST be written to the runtime store so a later
  attempt, or a restarted worker, retries the dead-letter with the same
  `Nats-Msg-Id`. A transient payload-store failure during decode (`BLOB-15`) is
  `transient`, not undecodable, and is retried.
- **DELIV-16** (`VF-Replay-Target`; RFC 0006): `videoflow dlq replay` republishes
  a dead letter to the **parent's** data subject of the target run with a fresh
  `Nats-Msg-Id` (`replay:<uuid4 hex>`) and `VF-Replay: <source run id>`, which
  fans out to every child of that parent; the replay publisher MUST also set
  `VF-Replay-Target: <origin node>` (the DLQ entry's `VF-Origin-Node`, verbatim).
  A data pull loop whose `VF_NODE_NAME` differs from the header value MUST
  ack-and-skip the delivery before hydrating any payload, as a non-owner does
  under `PART-4`, and MUST NOT release a payload obligation it never acquired.
  Absent header ⇒ every child processes it. Terminators are never replayed.
  Replay acquires its own payload obligations (`BLOB-14` step 3) and never
  decrements the original readers' shares.

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
  sequence is derived from its time: `seq = round(group.ts * 1e6)` and
  `event_ts = group.ts`; its `trace_id` is minted per `JOIN-23`. `round` MUST be
  **round-half-to-even** (banker's rounding, matching Python 3's `round`) over
  IEEE-754 doubles, so Rust, C++, and Python mint **byte-identical** ids for the
  same `ts` — downstream dedup (§5) depends on this. This is normative and MUST
  be conformance-tested.
- **JOIN-23** (member-hashed identity; RFC 0006): an emitted time group's
  `trace_id` MUST be `f'tw-{seq}-{digest}'` where `digest` is the first 12
  lowercase hex characters of `SHA-256(f'{window_id or ""}|{canonical}')` over
  UTF-8 and `canonical = '|'.join(f'{parent}={producer}:{trace_id}:{seq}' for
  parent in sorted(members))` — one entry per **sync** parent present in the
  group, in lexicographic parent order, each carrying that member's
  `producer_name`, `trace_id` and `seq`. Collect parents (`JOIN-14`) are
  attachments and do not enter the identity; a parent absent from a quorum
  emission (`JOIN-17`) is omitted. `window_id` is `None` in protocol v1 (the hash
  input starts with `|`). The same members regroup to the same id across
  redelivery and replay (`MSGID-2`); two groups at one rounded microsecond with
  different members never share an id. Golden vectors:
  `spec/vectors/join/group_identity.json`.

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

- **EOS-1** (per-replica observation): every replica observes each parent's
  terminator via its own EOS durable (`NAME-7`, `STREAM-6`). A terminator is either
  `MSG_TYPE_EOS` (clean) or `MSG_TYPE_ABORT` (abnormal, §16); both ride the same
  subject. The terminator is **held un-acked** until that parent is declared drained.
- **EOS-2** (duplicate terminator): every terminator is recorded per
  `(parent, replica_id, kind)` before it is acked (`EOS-7`); a duplicate of a
  recorded one is acked. An ABORT MUST still be recorded (`ABORT-3`) and outranks
  a clean EOS from the same parent: one replica finishing cleanly does not undo
  another one dying.
- **EOS-3** (drain condition — REALTIME, and any node without a durable shared
  runtime store): a parent is **stopped** once **all** hold:
  (a) its EOS has been observed;
  (b) no data from it is buffered locally (its prefetch queue is empty);
  (c) no pending join group holds a half from it (`has_pending_from`, including
  staged-but-not-yet-emitted groups and non-empty collect buffers, §8);
  (d) its data durable reports a **`Known`** observation with `num_pending == 0`
  **and** `num_ack_pending == 0` — an `Unknown` observation (a failed read) never
  satisfies the condition;
  and (d) has held continuously for `VF_EOS_QUIESCENCE_MS` (checked on two probes
  that far apart). The quiescence window tolerates a replicated parent momentarily
  between finishing and publishing. Any regression (new data, non-empty pending)
  resets the quiescence timer. Under a durable shared store a BATCH child uses
  the `EOS-7` barrier instead.
- **EOS-7** (per-replica final count and the completion barrier; RFC 0006): the
  `seq` of a terminator published by replica `r` of node `N` MUST equal the number
  of **distinct** DATA `Nats-Msg-Id`s that replica published on `N`'s data subject
  during the run — a publication counts once whether the broker stored or
  deduplicated it, and never if it was definitely `Rejected`; for an ABORT it is
  the count at the moment of death. The count lives in the runtime ledger's outbox
  when the store is durable (`CTRL-4`), so a restarted replica continues it. A
  receiver MUST record every terminator as a fact keyed by
  `(parent, replica_id, kind)` — kind, `seq`, and the `Error` of an ABORT —
  **before** acking it, and MUST NOT collapse a parent's terminators onto the first
  one seen; with a durable shared store the record is what a replacement recovers
  and a recorded terminator is acked on record, otherwise it is held un-acked
  until the parent is drained (`EOS-4`). *Completion barrier:* when the node's
  runtime store is durable and shared across processes, a BATCH child `C` MUST
  declare parent `P` complete only when all hold: (a) a terminator is recorded for
  every replica `0 .. VF_PARENT_REPLICAS[P] − 1` (`ENV-11`); (b) the number of
  distinct DATA ids delivered to `C` from `P` equals the sum of the recorded
  terminators' `seq` — the union over `C`'s replicas' received sets for a
  competing child, this replica's own set for a partitioned one; (c) no pending
  join group holds a half from `P`; (d) the broker observation of the durable is
  `Known` with `num_pending == 0` and `num_ack_pending == 0`. Received sets and
  terminator records are kept in the store so competing replicas aggregate;
  completion is committed by compare-and-swap under `C`'s current ownership epoch,
  and a commit under a superseded epoch is refused (`StaleAuthority`). Golden
  vector: `spec/vectors/envelope/eos.bin` (`seq: 9` — replica 0 published 9 DATA
  messages).
- **EOS-4** (ack on stop): when a parent becomes stopped, its held EOS handle is
  acked. A crash mid-drain leaves EOS un-acked and re-observable on restart.
- **EOS-5** (loop termination): when **all** parents are stopped, `receive_message`
  returns an all-parents-stopped result (every parent entry marked `is_stop_signal`),
  which drives the task loop to (publish EOS if it has children, then) run `close()`
  and exit (§6). It MUST return the same result early when **any** parent has
  aborted and drained (`ABORT-4`), without waiting for the others.
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
- **CTRL-1a** (no retention): because the subject is plain NATS, a stop reaches
  only the workers subscribed at the instant it is published — there is no
  history for a worker that connects later to read. A publisher that needs a stop
  to be *observed* rather than merely sent MUST repeat it (see `ABORT-6`); a
  one-shot publish is sufficient only for an operator-initiated stop of a run that
  is already fully up. A transient notification MAY wake workers but MUST NOT be
  the sole record of a stop, an abort or a completion for a node operating under
  `reliable_work`, `durable_control` or `restart_safe` (`CTRL-4`).
- **CTRL-4** (the runtime ledger; RFC 0006): such a node MUST keep the following
  records in a `RuntimeStore` — a versioned key-value store whose every mutation
  is a compare-and-swap (`cas(key, expected_version, value)`, `expected_version =
  None` meaning "must not exist"; `append(log, record)`; `scan(prefix)`;
  `delete(key, expected_version)`) — and never only in memory: terminators per
  `(parent, replica_id, kind)`; received sets per `(parent, durable)`; ownership
  epochs per partition `<node>/p<replica>` → `(epoch, fencing_token, holder,
  lease_until)`, CAS-incremented at `acquire_partition`, every commit carrying
  the token (a commit under a superseded epoch is refused: `StaleAuthority`,
  `VF_STALE_AUTHORITY`, `worker_fatal`), the lease renewed by the holder and
  refused to another live holder (`OwnershipConflict`, `VF_OWNERSHIP_CONFLICT`)
  so a singleton scaled out by adding replicas fails explicitly at bind time
  while a crashed holder's replacement takes over once the lease lapses; the
  outbox `publication_id` → digest, payload refs, the reader obligations
  acquired, outcome and accepted stream sequence (a `PublicationUnknown` intent
  is reconciled at start through `observe_publication` with the **same**
  `publication_id`; a second identity is never minted); attempt counts per
  message id and disposition (`worker_fatal` never increments); checkpoints
  (state bytes and replay position in one CAS write); pending dead-letter
  handoffs; open join groups by logical member id. Assembler buffers, prefetch
  queues and in-flight ack handles stay in memory. Key layout is
  implementation-defined; the reference prefixes every key with
  `vf/{flow}/{run}/{node}/`. *Qualification:* `restart_safe` and
  `durable_control` are admitted only when the composed store advertises
  `durable = Known(True)` **and** `shared_across_processes = True`; `Unknown` is
  rejected as `UnobservableState`, `False` as `IncompatibleProfile` — never
  faked. `memory://` never qualifies; `file://<dir>` qualifies on one host;
  `redis://` only when a read-back of `CONFIG GET appendonly`/`save` shows
  persistence on and `maxmemory-policy` is `noeviction`.
- **CTRL-2** (producer honors stop): a producer checks the termination flag each
  iteration and stops pulling new input when set, then publishes EOS if it has
  children **and the stop is the flow-wide control stop** (`Messenger.stop_reason()
  == 'control'`). A producer that is being quiesced (`'quiesce'`: SIGTERM, a
  rollout, a scale-down) or whose partition authority went to a replacement
  (`'authority-lost'`, `CTRL-4`) publishes nothing: its replacement continues the
  very stream an EOS would end for every child.
- **CTRL-3** (consumer/processor honors stop): when the termination flag is set,
  `receive_message` returns an all-parents-stopped result immediately (even
  mid-stream), marked `is_hard_stop` when no parent actually ended, so the loop
  breaks and runs `close()` **without relaying an EOS** (a processor relays EOS
  only when every parent's EOS was seen and drained, §9). This is a hard stop,
  distinct from the graceful EOS drain. A SIGTERMed worker quiesces and then
  dies of the signal — as PID 1 of a container, where a default-action signal
  is withheld, it exits `128 + SIGTERM` itself rather than live on into this
  path.

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
  `videoflow_messages_failed_total`; plus
  `videoflow_errors_total{node,code,disposition}`, which is what makes *what* is
  failing answerable — an undimensioned failure count cannot distinguish a wedged
  device from a malformed payload. Metric names/labels SHOULD match so dashboards
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
  decodes the inner payload as `inner_payload_type`. `ref` is opaque to consumers;
  the reference Redis store uses keys `vf-blob-<hex>` (32 lowercase hex characters
  of a random UUID, no dashes) with a TTL (`BLOB-7`). The store maintains
  companion obligation and metadata records (`BLOB-13`); a counter (`BLOB-5`) is
  the fallback contract of an RFC 0002 publisher.
- **BLOB-4** (interop): the blob store is the same for all languages in a flow; the
  ref is a plain string. An SDK MUST support at least the Redis store to interoperate
  with flows that offload.
- **BLOB-5** (reader-counted put, RFC 0002 — the fallback contract when
  obligations are absent): when the deployment supplies only the number of
  downstream reads each published message receives (`VF_BLOB_READERS`, computed at
  compile time as the sum over consuming children of `nb_tasks` for a partitioned
  child and 1 otherwise), the publisher SHOULD write, in the same store and with the
  same TTL as the blob, a counter key initialized to that count (reference store:
  blob `vf-blob-<hex>` → counter `vf-blobrc-<hex>`). The blob MUST be written
  before its counter, so an interrupted put degrades to a counterless (TTL-only)
  blob. A publisher without the count MUST write the blob without a counter (plain
  `BLOB-3` semantics).
- **BLOB-6** (release on ack, RFC 0002 — the fallback contract when obligations
  are absent): a reader that resolved a `BlobRef` MUST decrement the blob's counter
  at most once per delivered message, and only after the broker acknowledgment of
  that message succeeds. It MUST NOT decrement on nak, term, or dead-letter (a
  redelivery or DLQ inspection re-reads the blob). It MUST NOT decrement — or
  create — a counter key that does not exist. When a decrement observes a value
  ≤ 0, the reader SHOULD delete both the blob and the counter.
- **BLOB-7** (TTL backstop, RFC 0002): the TTL remains on both keys and is the
  authoritative upper bound on blob lifetime; refcounted deletion is an optimization
  for the common path (REALTIME eviction, crashed readers, and dead-lettered
  messages all leave counts that never reach zero). The TTL is chosen by the
  publisher and MUST exceed the worst-case publish-to-final-ack latency of the flow
  type. Reference defaults: 3600s for REALTIME (delivery is near-immediate; the TTL
  bounds only leaks) and 86400s for BATCH (a full Interest-retention backlog can
  legitimately delay a first read past an hour; a too-short TTL is silent data
  loss). Override via `VF_BLOB_TTL_SECONDS`. A decoder MUST tolerate a missing blob
  (a `Missing` read outcome, dead-lettered per `BLOB-15`). All three keys of
  `BLOB-13` carry the TTL; a `durable_required` retention contract whose
  `ttl_seconds` is shorter than the flow's `horizon_seconds` is rejected at
  admission unless obligations pin the object for the whole horizon.
- **BLOB-13** (obligations; RFC 0006): the reference store (`RedisPayloadStore`,
  scheme `redis`) keeps two companion keys **hash-tagged onto the blob key** so
  all three hash to one Redis Cluster slot: `vf-blobobl-{vf-blob-<hex>}`, a SET of
  obligation ids whose `EXPIRE` is the latest obligation deadline, and
  `vf-blobmeta-{vf-blob-<hex>}`, a HASH of `size`, `digest` (lowercase hex
  SHA-256 of the bytes), `generation` (opaque, minted at put), `content_id` (the
  publishing message's `'{producer_name}:{trace_id}:{seq}'`) and `created_at`.
  Obligation ids are over `[A-Za-z0-9_./-]` and take exactly these forms:
  `<child>` (a competing child, one obligation for all its replicas),
  `<child>/p<i>` (replica `i` of a partitioned child), `dlq/<flow>` (a dead
  letter, until the DLQ entry ages out), `archive/<flow>` (a replay archive),
  `intent/<publication_id>` (the publisher, from before publish until the outcome
  is known). `VF_BLOB_READER_IDS` (`ENV-12`) lists the reader obligations a
  publisher acquires at put. The store reports `atomic_multikey = Known(True)`
  when the server is not clustered or `CLUSTER KEYSLOT` agrees for the three keys,
  else `Unknown`; a profile that depends on atomic release is rejected on `Unknown`.
- **BLOB-14** (obligation lifecycle; RFC 0006). *Put:* blob first, then metadata,
  then the obligation set — an interrupted put degrades to a counterless, TTL-only
  blob; the publisher acquires `intent/<publication_id>` before publishing and
  releases it on `Accepted` (duplicate included) or definite `Rejected`; on
  `PublicationUnknown` the intent is kept until `observe_publication` or
  reconciliation resolves it. *Release:* a reader MUST release its obligation at
  most once per delivered message and only after a **confirmed** settlement
  (`SettleConfirmed`); `SettleUnknown` MUST NOT release; a partitioned non-owner
  releases its `<child>/p<i>` on its ack-and-skip; release is idempotent by
  `(obligation_id, generation)`, and a release naming another generation is
  `stale` and touches nothing. The reference is `WATCH obl meta` → read → `MULTI`
  `SREM obl id` [+ `UNLINK` of all three keys when the set would become empty and
  the generation matches] → `EXEC`, retried on a `nil` reply; **no Lua/`EVAL`**.
  A missing obligation set MUST NOT be created by a release nor cause a delete;
  the reader applies `BLOB-6` counter semantics if a counter exists and otherwise
  leaves the blob to its TTL. *Dead letters and replay:* before terminating a
  dead-lettered message the worker MUST acquire `dlq/<flow>` on its payload with
  deadline = the DLQ retention; a replay acquires `intent/<publication_id>` and
  the target's reader obligations afresh and never touches the original readers'
  obligations. *Reconcile:* `reconcile(ledger, operation_id)` — at worker start
  and periodically — MUST reclaim objects whose set is empty or whose only
  obligations are `intent/*` of publications that are definitely `Rejected`, and
  MUST cancel the obligations of messages the broker evicted (REALTIME: the
  message's stream sequence is below `stream_info().state.first_seq`). The
  reference runtime derives that ledger itself (`RuntimeObligationLedger`) from
  its parents' outboxes (refs, readers, accepted sequences), the channels'
  retained ranges and every reader's ack floor; a ledger is authoritative only
  for the obligation families it knows (the reader ids and `intent/*`), so an
  archive's or a dead-letter's pin is never cancelled on its word; an
  unobservable floor or channel keeps the obligation. It reports
  `ReclamationObservation(reclaimed, retained, unknown)`; an inventory read that
  failed is `unknown`, never "nothing to reclaim". *TTL backstop:* `BLOB-7`.
- **BLOB-15** (typed reads; RFC 0006): `read` returns one of `PayloadBytes`
  (digest verified when metadata exists), `TransientFailure` (the store was
  unreachable or slow — disposition `transient`, retryable, and NEVER classified
  as malformed bytes), `Missing` (nil) or `Corrupt` (digest mismatch). `Missing`
  and `Corrupt` are terminal for that delivery and are dead-lettered per
  `DELIV-15` with `VF-Code: VF_POISON_DECODE` and a `VF-Error` naming the ref and
  `missing` / `corrupt`. `decode_envelope` takes `resolve_blobs = True` so a
  receiver can decide ownership (`PART-3`) and replay scope (`DELIV-16`) from
  metadata before fetching bytes.

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

## 15. Error taxonomy and dispositions

Reference: `videoflow/core/errors.py`, `spec/proto/videoflow/v1/error.proto`.

An error crossing an SDK boundary is a `videoflow.v1.Error`. It exists as a proto
rather than as one SDK's exception hierarchy because a taxonomy that lives in one
language's class tree is invisible to every other implementation and to the tooling
that aggregates failures across them.

- **ERR-1** (disposition): every runtime failure has one of three dispositions:
  `poison` (the data is bad), `transient` (the world was briefly unavailable), or
  `worker_fatal` (this worker cannot process anything). `DISPOSITION_UNSPECIFIED`
  MUST be treated as `transient`.
- **ERR-2** (default): an exception the SDK cannot classify is `transient`. This is
  the pre-taxonomy behaviour, so an SDK that does nothing keeps working. A node MAY
  override its default via `VF_ON_ERROR`.
- **ERR-3** (code): every error carries a stable, greppable `code`. Messages may be
  reworded freely; codes may not, because metrics and dead-letter queries key on
  them. An SDK MUST NOT synthesize a code from the message text.
- **ERR-4** (remedy): an error SHOULD carry a `remedy` — what the reader should
  *do*. It is a separate field so every renderer presents it the same way.
- **ERR-5** (worker-fatal ends the worker): on a `worker_fatal` failure an SDK
  MUST NAK the in-flight message (`DELIV-6`) and then **stop the worker**,
  publishing an ABORT (`ABORT-1`) and exiting non-zero. The disposition is the
  node asserting that nothing it is given will succeed; staying alive would only
  NAK the rest of the stream one message at a time before reaching the same
  conclusion.
- **ERR-5a** (circuit breaker): for failures that arrive *unclassified*, an SDK
  MUST stop a worker that fails `VF_BREAKER_THRESHOLD` messages **consecutively**
  (default 10; any successful ack resets the count), leaving those inputs un-acked
  so they return to the broker. Data failures are sparse and independent; worker
  failures are dense and correlated, and counting a run of them separates the two
  without relying on the taxonomy being right. Without this, one wedged worker
  whose library error nothing recognizes dead-letters an entire healthy stream a
  few messages at a time.
- **ERR-6** (sampled dead-lettering): under a best-effort delivery mode an SDK
  SHOULD dead-letter a bounded number of specimens per `(code, node)` per minute
  (reference: 5) rather than none. Dropping a message under load shedding is a
  policy; dropping the evidence of an exception is losing the bug report.
- **ERR-7** (progress deadline): an SDK SHOULD stop a node that has acknowledged
  nothing for `VF_PROGRESS_TIMEOUT_SECONDS` (default 300) **while its durables
  report pending work**. The pending check is required: a wall-clock deadline
  cannot tell a slow node from a wedged one, and an idle node is not stalled at all.

---

## 16. Abnormal termination (ABORT)

Reference: `nats_messenger.py::publish_abort`, `core/task.py::raise_if_aborted`.

A clean end of stream and a crash are different facts, and before RFC 0005 only the
first existed on the wire — so a node that died mid-run left every descendant
blocked on an end-of-stream that was never coming.

- **ABORT-1** (marker): a node terminating abnormally SHOULD publish a
  `MSG_TYPE_ABORT` envelope on **its own `_eos` subject** (`NAME-4`), carrying the
  `Error` that killed it in `Envelope.error`. It reuses the EOS consumers and the
  provision-time interest anchor unchanged; no new topology is involved. Its dedup
  id MUST differ from the clean marker's (the reference uses trace id
  `abort-r{replica}` against `eos-r{replica}`), so an abort is never mistaken for a
  clean finish.
- **ABORT-1a** (who announces what): a worker MUST publish an abort only for a
  death **no restart can fix** — in practice a `poison` disposition, which will
  repeat identically in any replacement. Every other death is announced by the
  supervisor (`ABORT-6`) *after* it gives up. The restraint is required, not
  stylistic: a worker that announced every death would end its children while its
  own replacement was still starting, turning a recoverable crash into a flow-wide
  failure. A worker that is dying MUST NOT publish a clean `MSG_TYPE_EOS` in
  either case.
- **ABORT-2** (bounded publish): publishing an abort MUST be bounded in time and
  attempts (reference: 3 attempts, 10s overall) even under BATCH backpressure. The
  publisher is already dying; the layers behind it (`ABORT-6`, `ERR-7`) cover the
  case where it never gets out.
- **ABORT-3** (receiver: recording): a receiver MUST treat `MSG_TYPE_ABORT` as a
  terminator — `is_stop_signal` is true for **both** terminator types, so a reader
  that predates this RFC still stops rather than hanging — and MUST additionally
  record that the parent aborted, with its error. An abort outranks a clean EOS
  from the same parent.
- **ABORT-4** (receiver: early stop): once an aborted parent is **drained**
  (`EOS-3`), the node MUST stop, without waiting for its other parents' end of
  stream. No further input group involving that parent can ever complete, so
  waiting is waiting for nothing. Draining first is required: work already
  published is still processed.
- **ABORT-5** (receiver: propagation): before stopping, a node with children MUST
  publish its own ABORT carrying the originating error, and MUST exit non-zero.
  Failure walks the graph the way end-of-stream does.
- **ABORT-6** (supervisor abort): a control plane that gives up restarting a worker
  MUST signal flow termination on the control subject (`NAME-8`), and MUST keep
  repeating that signal until every worker of the run is gone. This covers the
  worker that died too abruptly to publish anything. Repetition is required
  because the subject has no retention (`CTRL-1a`) and the death that triggers
  this is typically an early one: the workers deeper in the graph are still
  connecting when it happens, so a single publish reaches precisely the nodes that
  did not need it. The reference republishes every 2s until the last worker is
  reaped.
- **ABORT-7** (in-flight work): a node that receives an ABORT MUST finish and ack
  the input group it already holds before stopping. The abort ends the stream; it
  does not discard work already done.

---

## 17. Conformance

Protocol v1 remains `stabilizing` until at least two non-Python SDKs pass the full
conformance suite (`tests/conformance/`, whose catalog is
`tests/conformance/catalog/test_catalog.json`). Every MUST above is exercised by a
case that references its ID; the cross-index is `spec/conformance-map.md`.
The Python worker is the reference oracle and MUST pass the suite first.

Changes to this document follow the RFC process in `spec/rfcs/`. A change that
alters observable wire or routing behavior requires a protocol major bump and a
`buf breaking` review of `spec/proto/`.

RFC 0006 (`spec/rfcs/0006-backend-contracts-and-runtime-ledger.md`, *accepted*
2026-09-14) introduced `EOS-7`, `MSGID-5`/`6`, `JOIN-23`, `STREAM-9`/`14`/`15`,
`DELIV-15`/`16`, `BLOB-13`…`15`, `ENV-10`…`18` and `CTRL-4`, amended the
requirements they cite, and made the Kubernetes resource names run-scoped
(`vf-<flow>-<run>-<node>`; Kubernetes names are not protocol names — `NAME-1`…`9`
are unchanged). The transitional `VF_RFC0006` switch it sat behind is gone.
