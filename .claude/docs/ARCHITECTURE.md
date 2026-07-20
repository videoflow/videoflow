# Architecture

How a videoflow graph becomes running workers, and how those workers talk to each other.

> Keep this file in sync with the code it describes. If you change flow validation, the compiler,
> the worker entrypoint, or broker topology, update this document in the same commit — and check
> whether `spec/PROTOCOL.md` and `README.md` also need updating.

## Build → run

```
Flow([consumers])  →  producer discovery  →  GraphEngine validation
                   →  build_tasks_data()  →  ExecutionEngine.allocate_and_run_tasks()
```

1. **`Flow(consumers, flow_type = REALTIME, flow_id = None)`**
   ([core/flow.py:75](../../videoflow/core/flow.py#L75)). You pass only the *consumers*.
   Producers are discovered by walking `.parents` backwards; a parentless node that isn't a
   `ProducerNode` is an error.

2. **`GraphEngine`** ([core/graph.py](../../videoflow/core/graph.py)) validates: every discovered
   root is a `ProducerNode`, the graph is acyclic, every consumer is reachable, **node names are
   unique**, and a replicated multi-parent node declares `partition_by`.

3. **`build_tasks_data()`** produces `[(node, parent_names, is_last)]` in topological order.
   `parent_names` order is load-bearing: it fixes the positional argument order of
   `process(*inputs)`.

4. **`flow.run(engine, run_id = None)`** hands that to an `ExecutionEngine`
   (`LocalProcessEngine` or `KubernetesExecutionEngine`), then `join()` / `stop()`.

## Compilation

`compiler.compile_flow(flow) -> [NodeSpec]` ([compiler.py](../../videoflow/core/compiler.py)) flattens
the graph into JSON-serializable per-node deployment records: `name`, `node_class` (FQN),
`params` (from `get_params()`), `parents`, `kind`, `has_children`, `nb_tasks`, `device_type`, GPU
fields, `image`, `partition_by`, `join_policy`.

`NodeSpec.to_dict()` is the same format written into the Kubernetes specs ConfigMap and printed
by `python -m videoflow.compile graph.py[:factory]` — which `deploy` uses to compile *inside* the
solution image when the host lacks the graph's dependencies.

`validate_wire_compatibility` runs here: a graph containing a non-Python component forces
envelope v4.

## The worker

[`videoflow/runtime/worker.py`](../../videoflow/runtime/worker.py) runs exactly **one** node, and is driven
entirely by `VF_*` environment variables — identical whether launched as a local subprocess or a
Kubernetes pod. It never sees the graph-building script. The full variable list is the module
docstring; the important ones are `VF_NODE_CLASS`, `VF_NODE_PARAMS_JSON`, `VF_PARENT_NAMES`,
`VF_FLOW_ID`/`VF_RUN_ID`, `VF_NATS_URL`, `VF_JOIN_POLICY_JSON`, `VF_ENVELOPE_VERSION`.

Inside, [`core/task.py`](../../videoflow/core/task.py) runs the loop:

- `ProducerTask` calls `next()` until `StopIteration`.
- `ProcessorTask` blocks on `receive_message()`, **reorders inputs to match `parent_names`**,
  calls `process(*inputs)`, publishes, then **acks after processing** (so a crash redelivers).
  On exception it calls `fail_inputs(e)` — a poison message never takes down the pod.
- `_call()` injects a `ctx`/`context` argument only if the node's method declares it, and bridges
  `async def` node methods onto a task-owned event loop.

## Messaging topology

All naming lives in [`messaging/topology.py`](../../videoflow/messaging/topology.py) — the
messenger, compiler, manifests, and provisioner all read from it, so change it in one place.

Names are scoped by **both `flow_id` and `run_id`**, so re-deploying gets fresh streams instead of
colliding with the previous run's durables:

| Thing | Format |
|---|---|
| Data subject | `vf.{flow_id}.{run_id}.{node_name}` |
| EOS subject | `vf.{flow_id}.{run_id}.{node_name}._eos` |
| Control (stop) | `vf.{flow_id}.{run_id}._control.stop` |
| Stream | `vf-{flow_id}-{run_id}-{node_name}` (one per node) |
| Durable | `{consumer_node}--from--{parent_node}` |
| DLQ subject | `vf.{flow_id}.{run_id}._dlq.{node_name}` |

The durable naming is what produces the routing semantics: replicas of one consuming node **share**
a durable (competing consumers, work is split), while distinct children of one parent get
**distinct** durables (broadcast fan-out). Partitioned nodes instead get a per-replica durable
(`--p{n}`) so every replica sees every message and keeps only the keys it owns
(`hash(key) % N == replica_id`).

EOS rides a separate subject on the node's own stream so data consumers filtered to the data
subject never see it, and every replica can observe it through its own dedicated consumer.

**Flow types.** `REALTIME` uses a drop-when-full discard policy — latency wins over completeness.
`BATCH` uses interest retention with backpressure, bounded retries (`VF_MAX_RETRIES`, default 3),
and a dead-letter stream. Provisioning happens up front via `python -m videoflow.provision`
(a Kubernetes init Job) so streams and durables exist before any worker starts.

## Joins

A node with multiple parents needs its inputs grouped. `JoinPolicy`
([core/policies.py](../../videoflow/core/policies.py)) has two modes:

- **`trace`** (default) — group by lineage (`trace_id`). The right choice when inputs descend from
  a common producer.
- **`time`** — group by `event_ts` within `tolerance_ms`. For fusing independent sources (e.g.
  several cameras). Supports `quorum` ("emit with at least k of N parents") and `collect`
  (per-parent windows for optional late inputs).

Implemented by `TraceGroupAssembler` / `TimeGroupAssembler` in
[`messaging/grouping.py`](../../videoflow/messaging/grouping.py).

## Wire format

[`serialization.py`](../../videoflow/wire/serialization.py). The sole wire is envelope version 4, the
language-neutral protobuf `videoflow.v1.Envelope`. Payloads are `Tensor` (arrays), `Value`
(structured scalars/lists/maps — which may nest a `Tensor`, so mixed containers encode neutrally),
a well-known/vendor proto, or opaque `RawPayload` bytes for an unknown type. There is no
code-executing codec: arbitrary Python objects register a neutral encoder via
`register_payload_encoder` (see [`spec/rfcs/0001`](../../spec/rfcs/0001-v4-only-wire.md)).
The earlier msgpack envelopes (v2/v3) are removed; `decode_envelope` refuses them.

Large payloads can spill to an external blob store (Redis) via `VF_BLOB_REDIS_URL`. Blobs are
**refcounted** (RFC 0002, `BLOB-5..7`): the compiler counts each node's downstream readers into
`NodeSpec.blob_readers` → `VF_BLOB_READERS`, the publisher writes a companion `vf-blobrc-*`
counter, and each reader decrements it on successful ack — the last one deletes the blob. The
TTL (3600s realtime / 86400s batch, `VF_BLOB_TTL_SECONDS` to override) stays on both keys as
the backstop for everything acks can't cover: REALTIME eviction, crashes, dead-letters. The
BATCH TTL is deliberately long — an Interest-retention backlog can delay a blob's first read
past an hour, and refcounting is what makes that affordable. Never release on nak/term/DLQ.
`BlobStore.put_with_readers`/`release` default to TTL-only/no-op, so third-party stores
registered via `register_blob_store` keep working unchanged.

The normative contract, with stable requirement IDs, is [`spec/PROTOCOL.md`](../../spec/PROTOCOL.md).
Golden vectors in `spec/vectors/` are replayed by `tests/test_golden_vectors.py` — an observable
change to the wire needs an RFC under `spec/rfcs/` and updated vectors.

## Extension seams

Places where adding a variant is a registration rather than an edit. All follow the same
shape — a module-level registry seeded with the built-ins, an explicit `register_*()`, and a
`get_*()`/`make_*()` that raises on an unknown name with a message naming the known values and
the fix. That error is a `ValueError` everywhere except `get_cluster_flavor`, which raises
`RuntimeError` because `load_images` always has and callers catch that type. Registries are
pre-seeded with exactly today's behavior, so with nothing registered the observable output is
unchanged.

| Seam | Where | Add a variant by |
|---|---|---|
| Blob store | [wire/serialization.py](../../videoflow/wire/serialization.py) | `register_blob_store(scheme, factory)` — selected by the blob URL's scheme |
| Payload encoding (v4) | [wire/serialization.py](../../videoflow/wire/serialization.py) | `register_payload_encoder(type, encoder)`, paired with `register_payload_type` for decode |
| Cluster flavor | [deploy/cluster.py](../../videoflow/deploy/cluster.py) | `register_cluster_flavor(handler)` — one class covers detection, image loading, hostPath warning |
| GPU allocation | [deploy/gpu.py](../../videoflow/deploy/gpu.py) | `register_gpu_mode(strategy)` — pod resources, preflight, and per-run prepare/cleanup |
| `x-questions` type | [deploy/solution.py](../../videoflow/deploy/solution.py) | `register_question_type(qtype, coercer)` |

Registration normally happens on import of the package that provides it. Where nothing would
import it first — a blob store named only in config, a vendor payload reaching host-side
`videoflow debug decode` — the registry consults an `importlib.metadata` entry-point group once
on a miss, via [utils/plugins.py](../../videoflow/utils/plugins.py). Groups:
`videoflow.blob_stores`, `videoflow.payload_types`, `videoflow.gpu_strategies`.

Two ordering rules are load-bearing and should survive refactoring:

- **Payload encoders are consulted after every built-in check.** Registering one — even a rule
  matching `object` — must not change how a built-in payload encodes. Those mappings are fixed
  by `spec/PROTOCOL.md` §4.4 and proven by the golden vectors.
- **`generic-remote` stays last in the cluster flavor list.** It matches everything, so anything
  registered after it is unreachable; `register_cluster_flavor` inserts before it by default.

### Deliberately not abstracted

These were considered and rejected. The reasoning matters more than the verdict — if a premise
changes, so should the decision.

- **Execution engine registry.** Blocked on a real prerequisite, not on effort: the CLI-facing
  lifecycle (`wait_for_completion`, `teardown`, `dump_failed_logs`, `schedulability_report`) is
  not part of the `ExecutionEngine` ABC, and the two engines' constructors share no signature. A
  registry over that is worthless. Unify the lifecycle into the ABC when a third engine actually
  exists, and do both together.
- **Transport abstraction.** `Messenger` and `BlobStore` are the preserved seams; everything
  below them is spec-fixed — `topology.py` returns `nats.js.api` objects, KEDA triggers are NATS
  triggers, `VF_NATS_URL` is in the worker's environment contract. A second transport starts as
  an RFC under `spec/rfcs/`, not as a refactor.
- **Flow-type semantics object.** A `FlowTypeSemantics` consolidating the REALTIME/BATCH string
  comparisons across ~6 files is tempting, but a third flow type would change retention and
  routing semantics — RFC territory by definition. The refactor should ride that RFC rather than
  precede it.
- **Join-mode registry.** `make_assembler` is already a clean factory, and a third mode must edit
  `core/policies.py` validation anyway, so a registry removes nothing from the blast radius.
- **Dev-infra service providers.** With exactly two hard-coded services (NATS, Redis) the
  abstraction is speculative. Build it in the same change as the first third service.
- **Container CLI (docker → podman).** Would add an env-var contract for zero current users.

## Language-agnostic components

`core/remote.py` + `components/descriptor.py`: `component(ref, params = ...)` loads a `component.yaml`
descriptor and returns a `RemoteProducer`/`RemoteProcessor`/`RemoteConsumer` that **subclasses the
normal node bases**, so every `isinstance` check downstream keeps working. Descriptor validation
(params schema, device, image, singleton vs partitionable, join capability) happens at
graph-build time, not at deploy time.
