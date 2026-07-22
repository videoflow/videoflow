# Videoflow roadmap

Current release: **v1.0.0**.

The design of record for everything below is [todos/07_gap_analysis.md](todos/07_gap_analysis.md);
this page is the release-shaped view of it. Nothing here is implemented yet.

## Where v1.0 stands

Videoflow today builds a graph on one machine and runs it on many — one worker per node, locally
or as a Kubernetes workload — with event-time joins across parents, partitioned routing,
end-of-stream drain, at-least-once delivery with deduplication and a dead-letter queue,
large-payload offload, sink idempotency, and a deployment CLI.

The gap is **temporal**. There is windowing for *aligning several parents*, but none for
*aggregating a single stream over time*; a node emits exactly one output per input, and has no way
to emit what it has buffered when the stream ends. Most of what a complex video solution has to
hand-roll today follows from that, and so does most of this roadmap.

---

## v1.0.1 — component fills

Straight library work with no changes to the framework itself. Ships first because it unblocks
real solutions today and depends on nothing else.

- **Mp4 video writer** — output format chosen from the file extension, with H.264 for `.mp4` and
  the current codec preserved for `.avi`. Every solution currently emits video that browsers
  can't play.
- **Image-folder and video-folder readers** — iterate a directory of images or videos in sorted
  order as a single stream. These are declared today but unimplemented.
- **Pose-estimation base class** — a domain base with standard keypoint constants, so pose
  components share a contract the way detectors already do.

## v1.0.2 — temporal core

The foundation the rest of the roadmap stands on.

**Zero-or-many emission and an end-of-stream hook.** A processor can publish nothing for an input,
or several outputs, instead of exactly one — a skip signal distinct from `None`, which stays a
legal payload, plus an emit call usable any number of times per input. A new flush hook runs once
after every parent has drained and before the node signals end of stream, so a node that has been
accumulating can emit its final result. Redelivery stays safe: re-running an input produces the
same outputs with the same identities, so duplicates are still suppressed.

**Windowed processors.** A node can keep the last N items or the last T seconds of its input and
process against that history rather than a single item, with a read-only window view offering
nearest-timestamp lookup and time-range slices. The window is the node's own memory of history it
has already acknowledged, which leaves delivery semantics untouched and makes the contract honest:
a crash loses the buffered tail and it refills. Per-key windows are available where the stream is
partitioned by key; a windowed node with several replicas requires a partition key, since
competing replicas would otherwise each see a random interleaving of the stream.

This changes both the node contract and the protocol's message-identity rules, so it needs an RFC.

## v1.0.3 — triggers and media

**Control inputs.** A parent can be designated as a control (marker) stream rather than a data
input. It never gates join completeness, it is broadcast to *every* replica instead of being
routed to one, it arrives through a dedicated callback that never runs concurrently with normal
processing, and it is delivered promptly even when the data streams are quiet. End of stream is
driven by the data parents alone, so a trigger source that never ends can't block shutdown, and
one that ends early can't stop the node. Markers get the same at-least-once delivery as data — a
missed trigger is a missed event.

**Frame store.** An opt-in store for recent frames held outside the message stream, addressed by
camera and time, with retention as an expiry. Downstream nodes fetch only the rare frames they
actually need, instead of every frame flowing through the broker. For batch flows, re-reading the
source file from disk stays the supported answer; this earns its keep on live streams.

**Event clip writer.** Consumes events, pulls the surrounding pre- and post-roll from the frame
store, and writes a clip — the end-to-end use case the previous two features exist to serve.

Control inputs change routing and loop behaviour, so this phase needs an RFC.

## v1.0.4 — named output ports

A node can declare several named outputs and return a value for each, and downstream nodes
subscribe to one port by name. This replaces today's pattern of a node returning a bundle followed
by a chain of splitter nodes that pull it apart: a subscriber interested in one output no longer
receives or decodes the others, which is the real bandwidth win, and the intermediate splitter
workers disappear entirely. Producers get ports too, so a reader can expose its frame and its
index as separate outputs.

End of stream stays per node rather than per port — all of a node's ports end when the node does,
and a child subscribed to one port still observes it. Existing single-output flows must stay
byte-identical in every routing decision; that identity check is the acceptance test for this
phase, not a passing test suite.

This is the largest single item on the roadmap and needs an RFC. It does not lift the
single-replica ceiling on event-time joins.

## v1.0.5 — analytics components

Independent of v1.0.4 and buildable alongside it.

- **Line-crossing counter and zone-intrusion detector** — the natural stage after tracking,
  emitting both running counts and discrete events.
- **RTSP/RTMP restreamer** — live video out, for solutions whose result is a stream rather than a
  file.
- **OCR** — plate and jersey reading.

Rejected: a WebRTC output consumer. Per-viewer signaling and session state don't fit one node per
worker; restream to a media server and let it terminate WebRTC.

## v2.0 — durable state and event-time completeness

Each of these is large enough to need its own design cycle:

- **Watermarks** and event-time completeness beyond wall-clock timeouts, which needs a notion of
  progress shared across parallel sources.
- **Aligned tumbling and session windows**, and event-time timers. The windows in v1.0.2 are a
  sliding tail, not an aligned window; this is where that gets fixed.
- **Durable keyed state and checkpointing**, so a replica restart doesn't lose its history. See
  [todos/08_checkpointing of buffers.md](todos/08_checkpointing%20of%20buffers.md).
- **Scaling event-time joins beyond a single replica.**
- **Ordering guarantees** — sequence numbers exist but are used only for deduplication; there is
  no reorder buffer.
- **Cycles and iterative dataflow.**

---

## Decisions already made

Recorded because they shape everything above and shouldn't be relitigated:

- Windows hold history the node has already acknowledged, never messages still in flight.
- `None` will not become a drop signal — that would silently change the behaviour of every
  existing flow. A separate skip value is used instead.
- Emitting several outputs is done by calling an emit method, not by turning processing into a
  generator, which would break the rebuild-in-a-worker contract every node depends on.
- Control parents are named, not passed as node references, so a graph still serializes.
- Markers are delivered with the same guarantees as data, not fire-and-forget.
- Per-key windows require a partition key, enforced when the graph is built.
