Common flow patterns
====================

This page collects graph shapes that come up often when building real pipelines.

Multiple producers (multi-source ingestion)
-------------------------------------------

A flow can ingest from several independent sources and fan them into shared
downstream processing — for example, several cameras feeding one detector pool::

    from videoflow.core import Flow
    from videoflow.processors import JoinerProcessor
    from videoflow.consumers import CommandlineConsumer

    def build_flow():
        cam_a = VideoUrlReader('rtsp://.../a', name='cam-a')
        cam_b = VideoUrlReader('rtsp://.../b', name='cam-b')
        detector_a = ObjectDetector(name='det-a', nb_tasks=2)(cam_a)
        detector_b = ObjectDetector(name='det-b', nb_tasks=2)(cam_b)
        merged = JoinerProcessor(name='merged')(detector_a, detector_b)
        out = CommandlineConsumer(name='out')(merged)
        return Flow([out])

You list only the consumers; both producers are discovered automatically.

Fan-out to parallel branches
----------------------------

A single node's output can feed several children. Each child gets its **own** copy
of every message (broadcast), and the branches run independently::

    frame   = FrameReader(name='frame')
    boxes   = ObjectDetector(name='boxes')(frame)
    faces   = FaceDetector(name='faces')(frame)      # same frame, different branch
    overlay = JoinerProcessor(name='overlay')(frame, boxes, faces)

Contrast this with ``nb_tasks``: distinct **children** each receive all messages
(fan-out/broadcast), whereas replicas of the **same** node compete for messages
(load balancing).

Joins
-----

A processor with several parents is a **join**. It waits for one message from each
parent that originated from the same upstream event, then runs ``process()`` with
all of them. The classic use is re-attaching a derived result to the frame it came
from, so a downstream annotator can draw on the original image::

    frame     = FrameReader(name='frame')
    detector  = ObjectDetector(name='detector', nb_tasks=4)(frame)
    annotator = BoundingBoxAnnotator(name='annotator')(frame, detector)

.. warning::
    A join must keep ``nb_tasks=1``; only the nodes around it can be replicated (here
    ``detector`` runs with ``nb_tasks=4`` while the join stays single). See
    :doc:`task-allocation`.

Reducing broker hops
--------------------

Every edge in the graph is a network hop through the broker. For a chain of cheap,
sequential CPU steps, that overhead can dominate the actual work. Two ways to reduce
it:

- **Do more per node.** Fold several trivial transformations into one processor's
  ``process()`` method instead of chaining several one-line processors.
- **Choose the right mode.** In :doc:`batch-versus-realtime-mode`, realtime mode
  avoids buffering entirely and keeps only the latest message per edge, which is
  cheaper for high-rate live sources.

.. note::
    Earlier single-machine versions offered a ``TaskModuleNode`` to fuse a subgraph
    into one operating-system process. Fusing nodes into a single **container** in
    the distributed engine is not supported yet; for now, express the fused logic as
    a single processor as described above.
