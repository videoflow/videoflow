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

A message that cannot be processed ends up on the flow's dead-letter stream
(``vf-<flow_id>-dlq``) — a poison message on its first failure, a transient one
after exhausting its retries. The stream is scoped to the **flow**, not the run,
so it survives ``videoflow teardown``: the moment you want it is usually right
after a run that failed and was cleaned up.

``videoflow dlq`` is the way in. ``ls`` is the triage view — what failed, where,
and under which stable error code::

    videoflow dlq ls --flow-id <flow_id>
    videoflow dlq ls --flow-id <flow_id> --code VF_DEVICE   # one failure mode
    videoflow dlq show --flow-id <flow_id> --id 3           # full decode, payload included

The error codes are the useful part: ``VF_POISON_SCHEMA`` means the message was
malformed, ``VF_DEVICE`` means a worker was sick when it happened to hold it.
Their meanings and what each one costs are in
:doc:`error-handling-and-recovery`.

Once the bug is fixed, put the messages back rather than losing the work::

    videoflow dlq replay --flow-id <flow_id> --to-run <run_id>
    videoflow dlq replay --flow-id <flow_id> --code VF_POISON_SCHEMA --dry-run

The raw stream is still there if you prefer the NATS CLI::

    nats stream info vf-<flow_id>-dlq
    nats stream view vf-<flow_id>-dlq

Metrics and health
------------------

Each worker exposes an HTTP server (port 8080) with Prometheus metrics and health
probes:

- ``/metrics`` — per-node processing-time counters, labelled by node name. Scrape
  these with Prometheus and chart them in Grafana to find the slow stage of a flow.
  ``videoflow_errors_total{node,code,disposition}`` is the one to alert on: it
  answers *what* is failing, which an undimensioned failure count cannot.
- ``/readyz`` — reports ready only after the node's ``open()`` returns. If a pod
  never becomes ready, its ``open()`` is failing or hanging (a bad model path, an
  unreachable data source).
- ``/healthz`` — a liveness heartbeat. If a pod is repeatedly restarted, its run
  loop is stalling — often a wedged broker connection or a blocking call inside
  ``process()``.

On Kubernetes, ``kubectl logs`` and ``kubectl describe pod`` for a node's pod show
its output and probe status. A worker that died of a typed failure also records
*why* in its termination message, which the API carries whether or not the logs
are still around::

    kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].lastState.terminated.message}'

That is the same structured reason ``videoflow deploy`` prints when it aborts a
rollout, so a crash-looping pod reports ``VF_DEVICE: CUDA out of memory`` rather
than "see the logs".

Common issues
-------------

Nothing is produced downstream of a join
    A join needs one message from **every** parent for the same event. If one parent
    branch is dropping messages (realtime mode) or has stalled, the join can never
    complete. Check each parent branch's stream backlog, and prefer ``BATCH`` mode
    when completeness matters.

The flow stopped and a node exited non-zero
    Something died. ``videoflow dlq ls`` shows what was quarantined, the pod's
    termination message shows what killed the worker, and the exit code says which
    kind of problem it was (2 your flow, 3 your cluster, 4 nodes failed, 5
    stalled). See :doc:`error-handling-and-recovery`.

A BATCH run never finishes
    A node has stopped making progress. Workers stop themselves after
    ``VF_PROGRESS_TIMEOUT_SECONDS`` (default 300) of acking nothing while work is
    pending, and log which parent they were waiting on; if a node is legitimately
    slower than that, raise the timeout rather than disabling it.

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
