Batch versus realtime mode
==========================

A flow is one of two types, ``realtime`` or ``batch``. The type sets the
**retention policy** of the broker streams that connect the nodes, which decides
what happens when a downstream node cannot keep up with an upstream one.

The type is chosen at flow creation time::

    from videoflow.core import Flow
    from videoflow.core.constants import BATCH, REALTIME

    flow = Flow([consumer], flow_type=BATCH)   # or REALTIME

Realtime mode
-------------

In ``realtime`` mode each edge keeps only the **freshest** message. When a producer
publishes faster than a consumer can read, the older, unread message is dropped and
replaced by the newer one. Producers therefore **never block** — the pipeline runs
at the speed of the producer, and slow stages simply see fewer, more recent items.

This is the right choice for live sources — an RTSP camera, a webcam — where
processing the latest frame matters more than processing every frame.

Batch mode
----------

In ``batch`` mode every message is retained and delivered **at least once**, and
nothing is silently dropped. A consumer acknowledges each message only after it
finishes processing it; the stream frees acked messages and bounds the backlog, so
when a slow stage fills the buffer the broker **blocks the upstream publisher**
(real backpressure) rather than discarding data. The pipeline moves at the speed of
the slowest node, losing nothing.

This is the right choice for finite sources you must process completely — a video
file, a batch of images — where losing frames is unacceptable.

.. note::
    A node is a **bottleneck** when it processes slower than the producer feeds it.
    In batch mode a bottleneck slows the whole flow; in realtime mode it causes
    dropped messages. Either way, one remedy is to replicate the bottleneck node
    with ``nb_tasks`` (see :doc:`task-allocation`).

Crash recovery, retries and the dead-letter queue
-------------------------------------------------

Because batch mode acknowledges only *after* processing, a message whose worker
crashes before acking is redelivered — a reliability property the old single-machine
queues never had. If processing a message *raises*, it is retried up to a limit
(``VF_MAX_RETRIES``, default 3); a message that exhausts its retries is
**dead-lettered** to a per-run DLQ stream (``vf-<flow>-<run>-dlq``) with the error
and origin recorded in message headers, and the worker keeps running — one poison
message never crashes the pod or stalls the flow. Inspect the DLQ with the ``nats``
CLI (see :doc:`debugging-flow-applications`).

Retries are safe because each message carries a content-derived id: if a worker
crashes *after* publishing its output but before acking its input, the re-run
republishes the same id and the broker de-duplicates it, so downstream nodes never
see the duplicate.

Realtime mode makes none of these guarantees by design — its whole point is to
discard anything that is not the latest, so a failed message is simply dropped.

.. warning::
    Do not read a video **file** in ``REALTIME`` mode. A file reader emits frames
    far faster than a detector can process them, so realtime's drop policy would
    discard most of the video. Use ``BATCH`` for files and other finite sources, and
    reserve ``REALTIME`` for genuinely live streams.
