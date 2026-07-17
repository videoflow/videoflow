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
JSON-serializable constructor parameters, its parents' names, the broker URL and the
flow id. It reconstructs the node, connects to the broker, and runs the node's loop.
This is why constructor arguments must be serializable and heavy setup belongs in
``open()`` (see :doc:`../user-documentation/nodes-and-flows`).

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
``stop()`` publishes a message on the broker's **control channel**, to which every
worker is subscribed. Producers stop first; the remaining nodes drain their inputs
and exit in turn, and their resources are released as each node's ``close()`` runs.

Large payloads
--------------

Uncompressed video frames can exceed a broker's per-message size limit. Videoflow
serializes NumPy arrays efficiently and, for payloads above a configurable
threshold, offloads the bytes to an external blob store (Redis by default) and sends
only a small reference over the broker. This is transparent to your node code; you
enable it by pointing workers at a Redis instance (the ``docker-compose.yml``
includes one, and ``videoflow deploy`` accepts ``--blob-redis-url``).
