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

In ``batch`` mode every message is retained and delivered **at least once**. A
consumer acknowledges each message after it finishes processing it; nothing is
dropped as long as the buffer (a deep, bounded stream) is not exhausted. If a stage
is slower than its input, its backlog grows and the pipeline effectively moves at
the speed of the slowest node.

This is the right choice for finite sources you must process completely — a video
file, a batch of images — where losing frames is unacceptable.

.. note::
    A node is a **bottleneck** when it processes slower than the producer feeds it.
    In batch mode a bottleneck slows the whole flow; in realtime mode it causes
    dropped messages. Either way, one remedy is to replicate the bottleneck node
    with ``nb_tasks`` (see :doc:`task-allocation`).

Crash recovery
--------------

Because batch mode uses explicit acknowledgements, a message whose worker crashes
before acking is redelivered to another worker — a reliability property the old
single-machine, in-memory queues never had. Realtime mode makes no such guarantee
by design: its whole point is to discard anything that is not the latest.

.. warning::
    Do not read a video **file** in ``REALTIME`` mode. A file reader emits frames
    far faster than a detector can process them, so realtime's drop policy would
    discard most of the video. Use ``BATCH`` for files and other finite sources, and
    reserve ``REALTIME`` for genuinely live streams.
