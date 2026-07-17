Distributed execution
=====================

Videoflow runs a flow as a set of **independent workers** that communicate over a
NATS JetStream broker. Every node becomes one worker; the workers do not share
memory and can run on different machines. What differs between "local" and
"Kubernetes" execution is only **how the workers are launched** — the worker code
is identical.

.. code-block:: text

       ┌──────────┐      ┌───────────┐      ┌───────────┐      ┌──────────┐
       │ producer │─────▶│ processor │─────▶│ processor │─────▶│ consumer │
       └──────────┘      └───────────┘      └───────────┘      └──────────┘
            │                  │                  │                  │
            └──────────────────┴───── NATS JetStream ───────────────┘
                            (one stream per node)

The worker model
----------------

Each node publishes its output to its own broker subject. Each node subscribes to
the subjects of its **real parents** and reassembles its inputs before calling
``process()`` / ``consume()``. This per-edge routing is what makes arbitrary DAGs —
multi-parent joins, multiple producers, fan-out — work without any special cases.

A worker is given only what it needs to run one node: the node's class, its
JSON-serializable constructor parameters, its parents' names, the broker URL, the
flow id and the run id. It reconstructs the node, connects to the broker, and runs
the node's loop. This is why constructor arguments must be serializable and heavy
setup belongs in ``open()`` (see :doc:`../user-documentation/nodes-and-flows`).

The optional ``ctx`` argument that a node method can declare is a ``RuntimeContext``
carrying exactly this identity — ``ctx.flow_id`` / ``ctx.run_id`` /
``ctx.node_name`` / ``ctx.replica_id`` / ``ctx.logger`` — plus
``ctx.set_partition_key(...)``.

Run scoping and provisioning
----------------------------

Every ``flow.run()`` (or ``videoflow deploy``) mints a **run id**. All of a run's
broker streams, subjects and durable consumers are named by ``flow_id`` **and**
``run_id``, so re-running or redeploying never collides with a previous run's
streams. Before any worker starts, the flow's streams and consumers are
**provisioned** up front (the local engine does this inline; on Kubernetes a
one-shot init Job does) — this is required for BATCH, whose interest-retention
streams would drop a message published before its consumer exists.

Delivery guarantees
-------------------

Workers **acknowledge a message only after processing it** (and, for a processor,
after publishing its output). A crash in between therefore redelivers the message
rather than losing it. Each message carries a content-derived id, so the re-run's
republished output is de-duplicated by the broker instead of double-emitted. In
BATCH mode a message that keeps failing is retried and then dead-lettered to a
per-run DLQ stream; in REALTIME mode a failed message is dropped (freshness wins).
See :doc:`../user-documentation/batch-versus-realtime-mode`.

Execution engines
-----------------

An **execution engine** is the object you pass to ``flow.run(...)``. It turns the
compiled graph into running workers.

``LocalProcessEngine``
^^^^^^^^^^^^^^^^^^^^^^^

Runs the flow on your machine, one OS subprocess per node (per replica, for
parallel processors), all connected to a NATS server. This is the fast path for
development and testing; it exercises the exact same worker code Kubernetes uses.

::

    from videoflow.engines.local import LocalProcessEngine

    flow = build_flow()
    flow.run(LocalProcessEngine(nats_url='nats://localhost:4222'))
    flow.join()

The only prerequisite is a running broker::

    docker compose up -d       # NATS on :4222 (and Redis for large payloads)
    # or
    nats-server -js

You can also run a graph module directly from the CLI without writing a
``__main__`` block::

    videoflow run-local my_flow.py:build_flow --nats nats://localhost:4222

``KubernetesExecutionEngine``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Renders Kubernetes manifests for the flow and applies them, one Deployment (or Job)
plus a ConfigMap per node. In practice you will usually use the ``videoflow deploy``
CLI, which is documented on the next page. See
:doc:`deploying-to-kubernetes`.

Stopping a flow
---------------

A flow stops when its producers are exhausted, or when you call ``flow.stop()``.
When a producer finishes it emits an **end-of-stream** marker; this is
**replica-safe** — every replica of a downstream node observes it and keeps
draining its input until the broker reports nothing pending before it terminates, so
no in-flight data is lost when a node has several replicas. ``flow.stop()`` instead
publishes on the broker's **control channel**, to which every worker is subscribed,
for an immediate flow-wide stop. Either way, each node's ``close()`` runs as it
exits.

Large payloads
--------------

Uncompressed video frames can exceed a broker's per-message size limit. Videoflow
serializes NumPy arrays efficiently and, for payloads above a configurable
threshold, offloads the bytes to an external blob store (Redis by default) and sends
only a small reference over the broker. This is transparent to your node code; you
enable it by pointing workers at a Redis instance (the ``docker-compose.yml``
includes one, and ``videoflow deploy`` accepts ``--blob-redis-url``).
