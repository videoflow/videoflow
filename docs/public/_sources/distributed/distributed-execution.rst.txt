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

Used as a library like this, the one prerequisite is a running broker::

    docker compose up -d       # NATS on :4222 (and Redis for large payloads)
    # or
    nats-server -js

Because each worker is a separate process that reconstructs its node by importing
the node's class, node classes must live in an importable module. The engine
re-exports this process's own ``sys.path`` additions as each worker's
``PYTHONPATH``, so a class defined next to your graph is importable in the worker
without any manual environment handling. Two knobs control it: ``python_path``
(extra directories to prepend) and ``inherit_python_path`` (default ``True``; set
``False`` for a hermetic child environment). ``PYTHONPATH`` is deliberately not
passed to remote components launched with ``docker run`` — those bring their own
image.

After ``flow.join()`` returns, ``engine.failures()`` lists
``(node_name, replica_idx, returncode)`` for every worker that exited non-zero and
``engine.report_failures()`` prints them; ``engine.wait_for_completion()`` returns
the failed node names, the same contract ``KubernetesExecutionEngine`` offers.
Without checking one of these, a worker that died on startup is indistinguishable
from a clean run.

``videoflow run-local``
^^^^^^^^^^^^^^^^^^^^^^^^

The CLI runs a graph module without writing a ``__main__`` block, and is the local
twin of ``videoflow deploy`` — no broker setup required::

    videoflow run-local my_flow.py

It generates the solution config (interactive Q&A over a ``config.template.yaml``
when the graph ships one and no ``config.yaml`` exists), runs the solution's
``prepare.py`` hook on this host, starts a dev NATS + Redis in Docker **when
nothing is already listening**, runs the flow, reports any node that exited
non-zero (exiting non-zero itself), and stops only the containers it started.

A broker you started yourself — ``docker compose up -d``, ``nats-server -js``, or a
previous ``--keep-infra`` run — is detected, reused, and never torn down.

Overrides: ``--nats`` / ``--blob-redis-url`` (bring your own broker; also read from
``$VIDEOFLOW_BLOB_REDIS_URL``), ``--config`` / ``--non-interactive``,
``--no-prepare``, ``--no-infra`` (never start containers), ``--no-redis``,
``--keep-infra`` (leave the containers up for faster reruns), ``--run-id``.

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
enable it by pointing workers at a Redis instance. Both ``videoflow deploy`` and
``videoflow run-local`` provision one automatically (in-cluster and in Docker
respectively) unless you pass ``--blob-redis-url``; the ``docker-compose.yml``
also includes one, and ``run-local`` additionally honors
``$VIDEOFLOW_BLOB_REDIS_URL``.

Offloaded payloads are reclaimed automatically: each blob carries a reference count
of its downstream readers, and the last reader to acknowledge its message deletes
the blob — so the store's steady-state size tracks the in-flight backlog, not the
flow's throughput. A TTL remains on every key as the backstop for messages that are
never acknowledged (a realtime flow evicting stale frames, a crashed worker, a
dead-lettered message): one hour for realtime flows and 24 hours for batch flows,
whose backlog can legitimately delay a payload's first read well past an hour.
Override it with ``--blob-ttl-seconds`` on ``deploy``/``run-local`` if your batch
flows drain slower than that. The provisioned Redis runs with ``maxmemory 4gb`` and
``volatile-lru`` eviction, so under memory pressure the oldest payloads are evicted
rather than the server growing without bound.
