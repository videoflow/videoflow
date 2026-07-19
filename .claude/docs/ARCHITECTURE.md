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

`compiler.compile_flow(flow) -> [NodeSpec]` ([compiler.py](../../videoflow/compiler.py)) flattens
the graph into JSON-serializable per-node deployment records: `name`, `node_class` (FQN),
`params` (from `get_params()`), `parents`, `kind`, `has_children`, `nb_tasks`, `device_type`, GPU
fields, `image`, `partition_by`, `join_policy`.

`NodeSpec.to_dict()` is the same format written into the Kubernetes specs ConfigMap and printed
by `python -m videoflow.compile graph.py[:factory]` — which `deploy` uses to compile *inside* the
solution image when the host lacks the graph's dependencies.

`validate_wire_compatibility` runs here: a graph containing a non-Python component forces
envelope v4.

## The worker

[`videoflow/worker.py`](../../videoflow/worker.py) runs exactly **one** node, and is driven
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

[`serialization.py`](../../videoflow/serialization.py). Envelope versions 2/3 are msgpack (with an
opt-in, Python-only pickle payload codec behind `VF_ALLOW_PICKLE`); version 4 is protobuf
(`videoflow.v1.Envelope`) and is language-neutral. `decode_envelope` auto-detects from the leading
byte, so mixed-version runs decode correctly. Default emit version is 3
(`VF_ENVELOPE_VERSION`); native (non-Python) components force 4.

Large payloads can spill to an external blob store (Redis) via `VF_BLOB_REDIS_URL`.

The normative contract, with stable requirement IDs, is [`spec/PROTOCOL.md`](../../spec/PROTOCOL.md).
Golden vectors in `spec/vectors/` are replayed by `tests/test_golden_vectors.py` — an observable
change to the wire needs an RFC under `spec/rfcs/` and updated vectors.

## Language-agnostic components

`core/remote.py` + `component.py`: `component(ref, params = ...)` loads a `component.yaml`
descriptor and returns a `RemoteProducer`/`RemoteProcessor`/`RemoteConsumer` that **subclasses the
normal node bases**, so every `isinstance` check downstream keeps working. Descriptor validation
(params schema, device, image, singleton vs partitionable, join capability) happens at
graph-build time, not at deploy time.
