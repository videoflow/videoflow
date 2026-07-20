Debugging flow applications
===========================

Because a flow is a set of independent workers, debugging is mostly about
observing each worker and the streams between them.

Inspect the graph without running it
------------------------------------

``videoflow explain my_flow.py`` prints the compiled topology — every node with its
kind, replicas, image, partitioning, its broker subject, and the DLQ stream name —
so you can sanity-check wiring and naming before anything touches a broker or cluster.

Start locally
-------------

Reproduce problems with ``LocalProcessEngine`` before deploying. Each node runs as
a subprocess and logs to the same terminal, so a stack trace points straight at the
offending node. Shrink the input (fewer frames, a small integer range) to get fast,
deterministic runs.

Inspecting the broker
---------------------

The traffic between nodes lives on the broker. Streams are named by flow **and run**
id (``vf-<flow_id>-<run_id>-<node>``); the run id is printed when the flow starts (or
choose it with ``--run-id``). With the
`NATS CLI <https://github.com/nats-io/natscli>`_::

    nats stream ls                             # vf-<flow_id>-<run_id>-<node>, plus the DLQ stream
    nats stream info vf-<flow_id>-<run_id>-<node>
    nats consumer report vf-<flow_id>-<run_id>-<node>

A consumer with a growing number of pending messages is a **bottleneck** — that node
is slower than its input. Replicate it with ``nb_tasks`` or move it to more capable
hardware (see :doc:`task-allocation`).

Dead-lettered messages
----------------------

A BATCH message that keeps failing ends up on the per-run DLQ stream
(``vf-<flow_id>-<run_id>-dlq``) after exhausting its retries. Read it to see what
failed and why — each message keeps the original payload and headers recording the
origin node, the error, and the delivery count::

    nats stream info vf-<flow_id>-<run_id>-dlq
    nats stream view vf-<flow_id>-<run_id>-dlq

Metrics and health
------------------

Each worker exposes an HTTP server (port 8080) with Prometheus metrics and health
probes:

- ``/metrics`` — per-node processing-time counters, labelled by node name. Scrape
  these with Prometheus and chart them in Grafana to find the slow stage of a flow.
- ``/readyz`` — reports ready only after the node's ``open()`` returns. If a pod
  never becomes ready, its ``open()`` is failing or hanging (a bad model path, an
  unreachable data source).
- ``/healthz`` — a liveness heartbeat. If a pod is repeatedly restarted, its run
  loop is stalling — often a wedged broker connection or a blocking call inside
  ``process()``.

On Kubernetes, ``kubectl logs`` and ``kubectl describe pod`` for a node's pod show
its output and probe status.

Common issues
-------------

Nothing is produced downstream of a join
    A join needs one message from **every** parent for the same event. If one parent
    branch is dropping messages (realtime mode) or has stalled, the join can never
    complete. Check each parent branch's stream backlog, and prefer ``BATCH`` mode
    when completeness matters.

A replicated join was rejected
    A processor with more than one parent and ``nb_tasks > 1`` must set
    ``partition_by`` (usually ``partition_by='trace_id'``, which keeps both halves of
    a join on the same replica). Otherwise set ``nb_tasks=1``.

A node cannot be reconstructed in its worker
    If a worker fails to start, a constructor argument is probably not
    JSON-serializable, or it is not stored on ``self`` under the same name. Move
    heavy/opaque setup into ``open()`` (see :doc:`writing-your-own-components`).

Frames look dropped
    That is expected in ``REALTIME`` mode — it keeps only the freshest message per
    edge. Use ``BATCH`` for finite sources you must process completely
    (see :doc:`batch-versus-realtime-mode`).
