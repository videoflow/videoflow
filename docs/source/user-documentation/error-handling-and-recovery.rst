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
    │   ├── CapabilityError         asking a component for what it cannot do
    │   ├── IncompatibleProfile     VF_INCOMPATIBLE_PROFILE: the broker/store cannot
    │   │                           provide a guarantee a channel requires (at deploy,
    │   │                           in the provision Job, and in a worker at bind when
    │   │                           an explicit --require-profile is not carried by
    │   │                           the streams it reads back)
    │   └── IdentityCollision       VF_IDENTITY_COLLISION: two names encode to one
    │                               broker or Kubernetes name
    ├── VideoflowEnvironmentError   the world is not as required      → exit 3
    │   ├── BrokerUnavailable
    │   ├── ClusterError
    │   ├── ResourceUnavailable
    │   ├── UnobservableState       VF_STATE_UNKNOWN: a read the decision needed
    │   │                           could not be made, and unknown is not zero
    │   └── OwnershipConflict       VF_OWNERSHIP_CONFLICT: a compare-and-swap on
    │                               shared cluster state lost to another writer
    └── VideoflowRuntimeError       something failed mid-stream
        ├── PoisonMessage           the DATA is bad
        ├── TransientFailure        the WORLD blipped
        ├── WorkerFatal             THIS WORKER is sick
        └── StaleAuthority          VF_STALE_AUTHORITY: this worker's ownership
                                    epoch was superseded (worker_fatal)

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
    over. ``StaleAuthority`` is the same disposition for a different reason: the
    worker tried to commit under an ownership epoch a newer owner has superseded
    (a partition transferred, a replacement replica started). The message is fine;
    this writer is not the one allowed to decide it.

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

The deadline is consulted in two places. The run loop checks it between
messages, which catches a node that is alive but no longer acking — and misses
the one failure the deadline was written for: a ``process()`` that never
returns. Such a callback never gets back to the loop, so the loop never checks,
while the broker lease heartbeats stay perfectly healthy. So a **watchdog
thread** re-checks the *same* deadline every ``VF_WATCHDOG_INTERVAL_SECONDS``
(default 5; ``0`` disables the thread and leaves the loop's own check). A node
that is merely slow keeps recording progress on every ack and is never touched; a
node stuck with work pending trips the deadline within ``timeout + interval``,
and because a thread cannot unwind the main thread's wedged frame, the watchdog
writes the reason to the termination log and ends the process with the error's
exit code — ``5`` (the flow stalled) for a ``ProgressStalled``. The un-acked
inputs go back to the broker for the replacement, exactly as after any other
death.

**Unknown is not zero.** The deadline's pending probe is a broker query, and a
query that failed used to read as "nothing pending" — which is how a node whose
broker connection had wedged could look idle. The probe now answers *unknown*
when it could not observe the broker, and the deadline treats that
differently from both other answers: it neither resets the silence window (that
would hide a stall) nor trips it (that would blame the node for the broker).
Sustained unobservability is its own failure: after a grace period (twice the
progress timeout by default) the node stops with ``BrokerUnavailable`` (exit
``3``), whose message says how long the broker state was unknown — it neither
completed nor stalled while the query was failing.

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
de-duplication window and be silently discarded), a ``VF-Replay`` header naming
the run it came from, and a ``VF-Replay-Target`` header naming the node that failed.
That subject is the parent's, which every child of the parent reads: with
``VF_RFC0006=1`` the other children acknowledge and skip a replay addressed to a
sibling before fetching its payload, so a node-scoped replay reprocesses nothing
elsewhere; without the switch every child sees it, as before.

Routing a dead letter needs only its envelope metadata, so ``--dry-run`` prints the
target subject of every entry — inline or offloaded — while the payload store is
unreachable, and an offloaded entry is never skipped for lack of a store. Whether
the bytes can actually be obtained is a separate check: pass ``--blob-redis-url``
and every offloaded payload is read before anything is published; a payload the
store cannot return fails the whole replay with ``VF_RESOURCE_UNAVAILABLE`` and
leaves every entry in place, rather than replaying messages that would only be
dead-lettered again.

Undecodable bytes and terminal records
--------------------------------------

Bytes that cannot be decoded — a foreign publisher, a truncated envelope, an
offloaded payload the store reports as missing or corrupt — are poison at the
transport layer: no node ever sees them, so no node can classify them. A payload
store that was merely *unreachable* is not that case: the fetch is retried with
the transient ladder, never terminated, because the bytes may well exist.

A delivery is only ever terminated against a durable record of why. With
``VF_RFC0006=1`` an undecodable message follows the poison ladder exactly as a
node that raised ``PoisonMessage`` would: its raw bytes are dead-lettered under
``VF_POISON_DECODE`` (``dlq show`` prints the headers and the byte length), and
the delivery is terminated only once the broker accepted that dead letter — a
dead-letter publish that failed keeps the delivery for a later attempt. The same
holds for any dead letter whose payload lives in the store: the worker pins the
payload for the DLQ retention first (``dlq/<flow>`` obligation), and a pin that
could not be taken also keeps the delivery, because a dead letter whose bytes may
vanish before anyone inspects it is not a record. Without the switch the
delivery is terminated against the node's terminal log, as it always was. A dead letter that was sampled out (``dlq: sampled``) or switched off
(``dlq: off``) also leaves a terminal-log entry, so a message never disappears with
its own disappearance as the only trace.

The delivery count the retry ladder consults is the broker's. A ``worker_fatal``
failure hands the message back without blaming it, but the broker still counts
that delivery against ``max_deliver``; a budget that ignores worker-fatal attempts
needs the durable runtime ledger of RFC 0006 (plan Phase 3) and is not in place yet.

Teardown and incomplete cleanup
-------------------------------

``videoflow teardown`` (and both engines, from their ``finally``) deletes a run's
streams by **exact ownership**, never by name prefix: a stream is this run's if the
owner labels in its JetStream metadata say so (``VF_RFC0006=1``), or — for a stream
created without labels — if its dot-delimited data subject names this flow and run
token for token. Tearing down run ``r`` cannot touch run ``r-x``, and the flow's
dead-letter stream is never a candidate.

The result is reported truthfully. When the stream listing could not be read, or
an owned stream's delete did not land, ``teardown`` prints

.. code-block:: text

    WARNING: broker cleanup incomplete for flow <flow> run <run>: <reason>; removed: ...; remaining: ...

on stderr and carries on with the workloads and infra it was asked to delete; the
engines log the same line. A listing that failed deletes nothing and is not
"nothing to delete" — re-run the teardown once the broker answers.

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

Which class an error belongs to decides the code, so the newer codes fall where
their branch of the taxonomy puts them: ``VF_INCOMPATIBLE_PROFILE`` and
``VF_IDENTITY_COLLISION`` are ``2`` (change the flow or the request),
``VF_STATE_UNKNOWN`` and ``VF_OWNERSHIP_CONFLICT`` are ``3`` (restore the read, or
redeploy against the current state), and a stall the watchdog thread found is
``5`` with the reason in the termination log.

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
size`` instead of ``crash-looping, see the logs``. A stall the watchdog thread
detected is written the same way, by that thread, before the process exits.

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
