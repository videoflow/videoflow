# The node contract

Everything you must satisfy for a node to work in a distributed run, not just on a laptop.

> Keep this file in sync with [`videoflow/core/node.py`](../../videoflow/core/node.py). If you
> change the node hierarchy, `get_params()`, or the scaling knobs, update this document and
> `../../README.md` in the same commit — and tell `../videoflow-contrib` if components must adapt.

## The four node types

| Base class | Implement | Notes |
|---|---|---|
| `ProducerNode` | `next()` | Raises `StopIteration` when done. `is_finite = False` for live sources (RTSP) — the k8s engine uses this to pick `Job` vs `Deployment`. |
| `ProcessorNode` | `process(*inputs)` | One positional argument per parent, ordered by `parent_names`. |
| `OneTaskProcessorNode` | `process(*inputs)` | Forces `nb_tasks = 1`. Use for stateful nodes: trackers, aggregators. |
| `ConsumerNode` | `consume(item)` | A `Leaf` — cannot have children. |

Producers are deliberately *not* generators: a node is shipped to its worker as
`(class path, get_params())` and rebuilt with `type(node)(**get_params())`, and a generator has no
such reconstructable form (the runtime is multiprocess).

Wiring is by call: `child(parent_a, parent_b)`. A node can only be called once.

## The reconstruction contract

This is the rule that breaks the most code, because it works fine locally and fails in a pod.

A graph is built on the operator's machine. Each node is then serialized to
`(class path, params)` and **rebuilt inside its own worker container** via
`type(node)(**node.get_params())`. The default `get_params()`
([core/node.py:103](../../videoflow/core/node.py#L103)) walks every class in the MRO that declares
its own `__init__`, reads that signature's named parameters, and for each looks up
`self._<name>`, then `self.<name>`.

So:

- **Store every named constructor argument verbatim as `self._<name>`.** A missing attribute
  raises `AttributeError: Cannot auto-capture constructor parameter '<x>'` at build time.
- **Every parameter must be JSON-serializable.** No node references, file handles, or models.
- **Accept and forward `**kwargs` to `super().__init__`** so base-class knobs keep working.
- **Override `get_params()`** when argument names don't match stored attributes (e.g. you renamed
  a base class's first argument), or when a parameter can't be serialized. `TaskModuleNode`,
  `ModuleNode`, and `FunctionProcessorNode` override it to raise `NotImplementedError` with an
  actionable message — they can't run distributed.
- **If a subclass fixes a parent parameter, pop it first:** `kwargs.pop('nb_tasks', None)` before
  passing a literal, or reconstruction collides with the value `get_params()` captured from the
  `ProcessorNode` level of the MRO. `OneTaskProcessorNode` does exactly this.

```python
class MyDetector(ProcessorNode):
    def __init__(self, threshold : float = 0.5, nb_tasks : int = 1, **kwargs) -> None:
        self._threshold = threshold          # verbatim, so get_params() round-trips it
        self._model = None                   # not a ctor arg — not captured
        super().__init__(nb_tasks = nb_tasks, **kwargs)

    def open(self) -> None:                  # runs in the worker, not on the build machine
        self._model = load_model(self._threshold)

    def close(self) -> None:
        self._model = None

    def process(self, frame) -> dict:
        return self._model.predict(frame)
```

## Lifecycle

`__init__` → (serialize / ship to worker) → `open()` → `next()`/`process()`/`consume()` repeatedly
→ `close()`.

`__init__` must do **no I/O**: it runs on the machine building the graph, which may not have the
GPU, the model weights, or the video file. Open resources in `open()`, release them in `close()`.

A node method may optionally declare a `ctx` (or `context`) parameter — the task injects the
runtime context only if the signature asks for it. `async def` node methods are supported and run
on a task-owned event loop.

## Scaling and placement knobs

Constructor arguments on `ProcessorNode`:

- **`nb_tasks`** — replica count. Replicas of one node share a durable, so they compete for work.
- **`device_type`** — `CPU` or `GPU` (from `videoflow.core.constants`).
- **`partition_by`** — with `nb_tasks > 1`, route by key instead of competing: each message goes to
  exactly one replica via `hash(key) % nb_tasks`. The key is `'trace_id'` (which co-locates both
  halves of a join on one replica) or a metadata field name. **Required** for a multi-parent node
  with `nb_tasks > 1` — graph validation rejects it otherwise.
- **`join_policy`** — a `JoinPolicy` for multi-parent nodes; see
  [ARCHITECTURE.md](ARCHITECTURE.md#joins).
- **`gpu_count`** — whole GPUs each replica requests on Kubernetes. Not overcommittable.
- **`gpu_resource_name`** — for clusters not exposing plain `nvidia.com/gpu`, e.g. a MIG profile
  (`nvidia.com/mig-1g.10gb`) or a renamed time-sliced resource (`nvidia.com/gpu.shared`).

On `Node` itself: **`name`** (the node's identity everywhere outside the build process — broker
subjects, k8s resource names, logs; auto-generated from the class name and a counter if omitted,
with uniqueness checked at flow-build time) and **`image`** (the container this node's worker runs
in, when the node intrinsically needs a specific environment).

## Gotchas

- **`process()` returning `None` does not drop the message.** End-of-stream travels on a separate
  `_eos` subject, not as a `None` sentinel in the data stream.
- **Input order is `parent_names` order**, which comes from the call site
  `child(parent_a, parent_b)` — not from any sorting.
- **Acks happen after processing**, so a crash mid-process causes redelivery. Nodes with external
  side effects should be idempotent; `ConsumerNode` has an `idempotent = True` flag that
  deduplicates across redelivery when a Redis store is configured.
- **A raised exception fails the input rather than killing the worker.** Don't add a bare
  `try/except: pass` around `process()` — the task layer already handles it, and swallowing the
  error hides it from the DLQ.
