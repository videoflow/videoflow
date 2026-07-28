Error handling and recovery
===========================

A flow is a set of independent workers, so "what happens when something fails" is
not one question but several: what happens to the *message*, to the *worker*, to
the *flow*, and to the person who has to find out why. Videoflow answers them
differently on purpose, and this page is the whole model in one place.

The one sentence version: **build time fails loud, run time quarantines.** A
graph that is wrong stops immediately with a message naming the fix; a message
that fails at run time is isolated so the rest of the stream keeps moving.

.. contents::
   :local:
   :depth: 1

The model
---------

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * -
     - Build / deploy time
     - Run time
   * - Philosophy
     - Fail fast and loud, naming the fix
     - Never crash on data; quarantine the message
   * - Vehicle
     - An exception, rendered by the CLI
     - Broker ack / nak / dead-letter
   * - Audience
     - A human at a terminal
     - A log line, a metric, and a kubelet
   * - Unit of failure
     - The whole deploy
     - One message

Everything below follows from that split.

Exception taxonomy
------------------

Every error videoflow raises deliberately is a ``VideoflowError``, and its class
says who is at fault:

.. code-block:: text

    VideoflowError
    ├── VideoflowUserError          your graph or config is invalid   → exit 2
    │   ├── GraphError              cycles, duplicate names, bad joins
    │   ├── NodeContractError       get_params round trip, kind mismatch
    │   ├── ConfigError             flow type, join policy, mounts, images
    │   └── CapabilityError         asking a component for what it cannot do
    ├── VideoflowEnvironmentError   the world is not as required      → exit 3
    │   ├── BrokerUnavailable
    │   ├── ClusterError
    │   └── ResourceUnavailable
    └── VideoflowRuntimeError       something failed mid-stream
        ├── PoisonMessage           the DATA is bad
        ├── TransientFailure        the WORLD blipped
        └── WorkerFatal             THIS WORKER is sick

Every one carries a stable ``code`` (``VF_POISON_SCHEMA``), a ``message``, and a
``remedy`` — the fix, kept as its own field so the CLI, the dead-letter inspector
and the Kubernetes termination log all render it the same way. Codes never change
once published, because metrics and dead-letter queries key on them.

Dispositions: which kind of failure is this?
--------------------------------------------

The three leaves of the runtime branch are the **dispositions**, and they are the
only part of the taxonomy the hot path reads. Choosing the right one is the main
thing a component author needs to get right:

``poison`` — *the message is bad*
    It will fail identically no matter who processes it or how many times.
    Malformed payloads, schema violations, values outside the domain.
    Retrying is pure waste, so it is dead-lettered on the first failure.

``transient`` — *the world was briefly unavailable*
    A socket reset, a throttled API, a lock timeout. Retrying is exactly right.
    This is the **default** for anything unclassified, which is why adopting the
    taxonomy changes nothing until you opt in.

``worker_fatal`` — *this worker cannot process anything*
    A wedged GPU, a missing model file, an exhausted disk. The message is fine;
    this process is not. It is handed back to the broker for a healthy replica,
    never dead-lettered — and the worker then **stops**, so a replacement can take
    over.

The failure this prevents is worth stating plainly. Before dispositions existed,
a pod whose GPU wedged failed every message it touched, and each one was
redelivered four times and then dead-lettered under a ``CUDA out of memory``
error that had nothing to do with the message. In twenty minutes one bad pod
could move an entire healthy stream into the dead-letter queue while the flow's
own health signals stayed green.

Raise them directly from a node::

    from videoflow.core.errors import SchemaError, DeviceError, UpstreamUnavailable

    class Detector(ProcessorNode):
        def process(self, frame):
            if frame.ndim != 3:
                raise SchemaError(f'expected an HWC frame, got {frame.shape}',
                                  remedy = 'Insert a reshape upstream.')
            try:
                return self._model(frame)
            except OutOfMemoryError as e:
                raise DeviceError('the GPU is out of memory') from e

The ladder: what a failure costs
--------------------------------

Given a disposition and the node's delivery mode, the action is fixed:

.. list-table::
   :header-rows: 1

   * - disposition
     - best-effort
     - at-least-once
   * - ``poison``
     - sampled dead-letter, then drop
     - dead-letter immediately, then drop
   * - ``transient``
     - drop
     - retry until the budget, then dead-letter
   * - ``worker_fatal``
     - hand back, stop the worker
     - hand back, stop the worker

Retries are jittered (``min(2**n, 30) × uniform(0.5, 1.5)`` seconds), because a
deterministic schedule makes N replicas that failed together retry together.

Acks always happen **after** processing, so a crash mid-message redelivers rather
than loses. That is the guarantee everything else here rests on.

Worker self-protection
----------------------

Two watchdogs catch what the taxonomy cannot.

**The circuit breaker** stops a worker that fails
``VF_BREAKER_THRESHOLD`` messages *consecutively* (default 10; any success resets
the count). It exists for failures that arrive **unclassified** — a library error
nothing recognizes, arriving on every message. Data failures are sparse and
independent; worker failures are dense and correlated, and counting a run of them
separates the two without needing the taxonomy to be right.

**The progress deadline** stops a node that has acknowledged nothing for
``VF_PROGRESS_TIMEOUT_SECONDS`` (default 300) *while its durables report pending
work*. The pending check is what makes it usable: a wall-clock timer cannot tell
a slow model from a hung one, and an idle node is not stalled at all. This is the
only stall detection a BATCH flow has, since BATCH pods are Jobs and Job pods
have no probes.

If a node legitimately takes longer than that per message, raise the timeout
rather than disabling it.

Failure propagation
-------------------

A clean end of stream and a crash are different facts. Before both existed on the
wire, a producer that died mid-run left every descendant blocked forever on an
end-of-stream that was never coming — the workers alive, the broker healthy, and
the flow simply stopped.

``MSG_TYPE_ABORT`` is the missing fact. It rides the same subject as the clean
end-of-stream marker, carries the error that caused it, and a node that receives
one finishes its in-flight work, passes the abort to its own children, and exits
non-zero. Failure walks the graph the way success does.

Three layers, because each covers the one before it:

1. **In-band abort** from a worker whose death no restart can fix. Fast, and it
   carries the cause. A worker deliberately stays silent about a *recoverable*
   death — announcing it would kill its children while its own replacement was
   still starting.
2. **Supervisor abort** when a node exhausts its restarts. Covers the death too
   abrupt to publish anything: an OOM kill, a SIGKILL.
3. **Progress deadline** on the receiving side. Covers everything else, including
   a partitioned network. Slowest, and undefeatable.

Restarts
--------

Both engines honour the same ``SupervisionPolicy``: three restarts, and a
``poison``-classified death is never restarted (a worker that died of a bad
message will die of it again). Only the backoff differs — Kubernetes uses
10/20/40s via the Job's ``backoffLimit``; ``run-local`` compresses it to 1/2/4s so
a genuinely broken node still surfaces in seconds.

That parity is deliberate. A crash used to recover in the cluster and hang
locally, which made the development environment the one place the recovery path
was never exercised. Use ``videoflow run-local --no-restart`` for a tight debug
loop when you want the first failure to be the last thing that happens.

The dead-letter queue
---------------------

The DLQ is scoped to the **flow**, not the run — ``vf-<flow_id>-dlq``. Everything
else about a run is disposable and is deleted with it; dead letters are the
opposite, and are most wanted right after a run that failed and was torn down.

.. code-block:: bash

    videoflow dlq ls --flow-id my-flow                    # triage view
    videoflow dlq ls --flow-id my-flow --code VF_DEVICE   # one failure mode
    videoflow dlq show --flow-id my-flow --id 3           # full decode, payload included
    videoflow dlq replay --flow-id my-flow --to-run <run> # put them back
    videoflow dlq purge --flow-id my-flow --older-than 7d

``ls`` groups by code, which is the question you actually have:

.. code-block:: text

      #  NODE       CODE                 DELIV  RUN / ERROR
      1  detector   VF_POISON_SCHEMA         1  a41f9c / expected an HWC frame, got (2, 2)
      2  detector   VF_DEVICE                4  a41f9c / the GPU is out of memory

    by code: VF_POISON_SCHEMA=1  VF_DEVICE=1

``replay`` re-publishes the original bytes onto the subject the failing node reads
from, with a fresh message id (reusing the original would land inside the stream's
de-duplication window and be silently discarded) and a ``VF-Replay`` header naming
the run it came from.

Exit codes
----------

The CLI's exit status carries the *class* of failure, so CI can triage without
parsing stderr:

.. list-table::
   :header-rows: 1

   * - Code
     - Meaning
     - Retry the command?
   * - ``0``
     - success
     - —
   * - ``2``
     - your flow or config is wrong
     - no — fix the code
   * - ``3``
     - the cluster, broker or registry is wrong
     - maybe, after fixing the infrastructure
   * - ``4``
     - the flow ran and nodes failed
     - look at the dead-letter queue
   * - ``5``
     - the flow stalled and was aborted
     - look at the stall reason
   * - ``130``
     - interrupted
     - —

Errors print as a message and a fix, never a traceback. Set ``VF_DEBUG=1`` when
the traceback is the thing you want.

Per-node overrides
------------------

The flow type sets the defaults — REALTIME is best-effort, BATCH is at-least-once
— but loss tolerance is really a property of *what a node does with a message*,
not of the flow it happens to live in. A REALTIME flow may want freshest-wins
frames and a durable alert sink in the same graph::

    frames  = CameraProducer(name = 'frames')
    detect  = Detector(name = 'detect')(frames)                  # freshest wins
    alerts  = AlertSink(name = 'alerts',
                        delivery = 'at-least-once')(detect)      # never drop one

``on_error`` sets the disposition for exceptions nothing classifies, for a node
whose failures are known to be data-shaped::

    parse = JsonParser(name = 'parse', on_error = 'poison')(source)

Classifying third-party exceptions
----------------------------------

You cannot subclass ``torch.cuda.OutOfMemoryError``, so register it instead —
once, on import of the package that raises it. Every flow using the component
then inherits the right behaviour::

    from videoflow.core.errors import WORKER_FATAL, register_error_classifier

    register_error_classifier(torch.cuda.OutOfMemoryError, WORKER_FATAL)

Later registrations win, so a component may deliberately override a built-in
mapping.

Observability
-------------

Every failure produces three artifacts, all keyed by the same ``code``:

- **A structured log line** with ``code``, ``node``, ``replica``, ``trace_id``,
  ``disposition`` and ``remedy``.
- **A metric**: ``videoflow_errors_total{node,code,disposition}``. The dimensions
  are the point — "how many failed" is nearly useless, and "what is failing" is
  the question an alert asks.
- **A dead-letter entry** (or, for a worker that died, a Kubernetes termination
  message) carrying the full error and the original payload.

A crash-looping pod reports its own cause: ``rollout_report`` reads the worker's
termination message and says ``VF_DEVICE: CUDA out of memory — lower the batch
size`` instead of ``crash-looping, see the logs``.

Worked example
--------------

``solutions/toy_recovery`` is a complete, deployable flow that produces one bad
message and one sick worker and checks that each is handled correctly. It is also
part of the test suite, so the behaviour on this page is verified on every build.

.. code-block:: bash

    videoflow run-local solutions/toy_recovery/toy_recovery.py

See also
--------

- :doc:`batch-versus-realtime-mode` — where the delivery defaults come from.
- :doc:`debugging-flow-applications` — inspecting a running flow.
- :doc:`writing-your-own-components` — the node contract.
- ``spec/PROTOCOL.md`` §7, §15, §16 — the normative contract, for SDK authors.
- ``spec/rfcs/0005-error-taxonomy-and-abort.md`` — why all of this exists.
