Nodes and Flows
===============

This page describes the building blocks of a Videoflow application and the rules
that make a graph deployable in a distributed setting.

The three node types
--------------------

+-----------+-------------------+---------------------+------------------------------------------------------+
| Type      | Base class        | Method to implement | Role                                                 |
+===========+===================+=====================+======================================================+
| Producer  | ``ProducerNode``  | ``next()``          | Creates data from an external source (file, camera,  |
|           |                   |                     | stream). Set ``is_finite=False`` for unbounded ones. |
+-----------+-------------------+---------------------+------------------------------------------------------+
| Processor | ``ProcessorNode`` | ``process(*inputs)``| Transforms inputs into an output. Supports           |
|           |                   |                     | ``nb_tasks``, ``device_type``, ``partition_by``,     |
|           |                   |                     | ``join_policy``.                                     |
+-----------+-------------------+---------------------+------------------------------------------------------+
| Consumer  | ``ConsumerNode``  | ``consume(item)``   | Terminal sink; produces no output. Supports          |
|           |                   |                     | ``idempotent`` and ``join_policy``.                  |
+-----------+-------------------+---------------------+------------------------------------------------------+

Every node also has ``open()`` and ``close()`` lifecycle hooks for acquiring and
releasing resources. Any lifecycle or processing method may be an ``async def`` (the
worker awaits it) and may take a final ``ctx`` parameter to receive a
:doc:`runtime context <../distributed/distributed-execution>` — see
:doc:`writing-your-own-components`.

Node identity: the ``name``
---------------------------

Each node has a **stable, unique** string ``name``. In a distributed flow that name
is the node's identity everywhere it matters: the broker subjects it publishes and
subscribes to, and the Kubernetes resources generated for it. If you do not pass a
``name``, one is generated from the class name, but for anything you intend to
deploy you should give each node an explicit, meaningful name::

    detector = ObjectDetector(name='detector')(frame)

Names must be unique within a flow; a duplicate raises a ``ValueError`` when the
flow is built.

Constructor arguments must be serializable
------------------------------------------

Because a worker reconstructs **only its one node** from configuration (it never
receives a live Python object), a node's constructor arguments must be
**JSON-serializable** — numbers, strings, booleans, and lists/dicts of those.

Two consequences follow:

- **Do heavy or stateful setup in** ``open()``, not ``__init__``. Opening a camera,
  loading a model, or creating a database connection belongs in ``open()``, which
  runs inside the worker just before processing begins.
- **Store each constructor argument on** ``self`` **under the same name** so it can
  be captured automatically for reconstruction. Always accept and forward
  ``**kwargs`` to ``super().__init__()``.

See :doc:`writing-your-own-components` for full examples.

Building a Flow
---------------

A ``Flow`` is created from the **consumers** (the leaves of the graph); producers
are discovered automatically::

    from videoflow.core import Flow
    from videoflow.core.constants import REALTIME

    flow = Flow([consumer_a, consumer_b], flow_type=REALTIME, flow_id='my-flow')

- ``flow_type`` is ``REALTIME`` (drop stale messages, never block producers) or
  ``BATCH`` (at-least-once, loss-free delivery). See :doc:`batch-versus-realtime-mode`.
- ``flow_id`` namespaces the broker subjects and Kubernetes resources; pass a stable
  value when you want to redeploy the same logical flow.

When the flow is built it validates the graph: it must be acyclic, every consumer
must be reachable from a producer, all names must be unique, and a replicated
multi-parent join node must set ``partition_by`` (see :doc:`task-allocation`).

Running a Flow
--------------

A flow runs through an **execution engine**::

    from videoflow.engines.local import LocalProcessEngine
    flow.run(LocalProcessEngine())   # local subprocesses; or KubernetesExecutionEngine
    flow.join()                       # block until done
    # flow.stop()                     # signal termination early

``flow.run(engine)`` mints a fresh ``run_id`` and is non-blocking. ``flow.join()``
blocks until the flow finishes naturally (all producers exhausted). ``flow.stop()``
publishes a termination signal on the broker's control channel that every worker is
subscribed to, then waits for them to drain and exit.

See :doc:`../distributed/distributed-execution` for the available engines.

Reliability
-----------

Each call to ``flow.run()`` is scoped by a **run id**, so re-running a flow gets a
fresh set of broker streams instead of colliding with a previous run.

Delivery is **at-least-once**: a worker acknowledges a message only after it has
processed it (and published its output), so a crash mid-processing causes
redelivery, not loss. In BATCH mode a message that keeps failing is retried a few
times and then **dead-lettered** to a DLQ stream (``vf-<flow>-<run>-dlq``) with the
error attached, instead of being dropped or crashing the pod. See
:doc:`../distributed/distributed-execution` and :doc:`batch-versus-realtime-mode`.
