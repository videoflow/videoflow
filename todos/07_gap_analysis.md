# Videoflow functionality gap analysis — missing primitives for complex video processing

## Context

The user asked: review videoflow's functionality and identify missing-but-useful capabilities
for video processing — specifically whether the processor types are sufficient, whether stateful
processing is adequate, whether we need buffer-of-last-n-frames processors or marker-triggered
buffers, and generally what would enable complex use cases like the soccer offside solution.

This plan is the resulting gap analysis plus a prioritized, detailed design for the primitives
worth building. **Scope decision (user, 2026-07-21): analysis/plan only for now — no
implementation yet.** The roadmap below is the reference for whenever implementation is picked
up. The evidence base is a full sweep of `videoflow/core`, `videoflow/messaging`,
`spec/PROTOCOL.md`, all built-in and contrib components, and a trace of the offside solution's
workarounds.

## What exists today (baseline)

- **Node types**: ProducerNode / ProcessorNode / ConsumerNode, plus `OneTaskProcessorNode`
  (stateful ⇒ forced `nb_tasks=1`) — the only structural acknowledgement of state.
  `TaskModuleNode` / `FunctionProcessorNode` / `ModuleNode` exist but are not distributable
  (`get_params()` raises; `ModuleNode.__call__` is an unimplemented stub).
- **Event-time joins** (`JoinPolicy(mode='time')`, `messaging/grouping.py`): tolerance-based
  multi-parent alignment with quorum, per-parent `collect` windows, settle window, wall-clock
  timeout. Real windowing — but only for *aligning parents*, not for aggregating a single
  stream over time.
- **Partitioned routing** (`partition_by`, PART-1..4): SHA-256 key-hash → replica ownership;
  the closest thing to keyed state (a replica can hold per-key state in memory, but nothing
  persists or migrates it).
- **EOS drain** (EOS-1..6), at-least-once + DLQ, content-derived msg-id dedup (MSGID-1..3),
  blob offload for >512KB payloads (`RedisBlobStore`, BLOB-1..7), sink idempotency (IDEM),
  event-time propagation (LOOP-10), `ctx.set_event_timestamp` / `set_output_partition_key`.

## The gaps (what the offside solution had to hand-roll)

| # | Missing capability | Workaround observed |
|---|---|---|
| 1 | **Buffered/windowed processing** (last N items / last T seconds) | `offside_engine/engine.py` hand-rolls a `deque` of world states, wall-time trimming (`_trim_buffer`), nearest-timestamp lookup (`_state_nearest`), slice extraction |
| 2 | **Marker/control streams** (trigger input that doesn't gate joins, broadcast to all replicas) | Touch/kick "markers" detected inline with manual `detection_lag_frames` future-context waits and `_committed_ts` dedup |
| 3 | **Zero-or-many emission (flatMap)** | Event detectors return `None` on non-events; every downstream node open-codes `if item is None` guards (None is a legal payload, not a drop) |
| 4 | **EOS flush / timer hooks** | No way to emit buffered state at end of stream; only `open()/close()` + per-item method |
| 5 | **Managed keyed state / checkpointing** | State lives in instance attributes; replica restart loses it; time-join stages capped at `nb_tasks=1` |
| 6 | **Named multi-output ports** | Splitter glue nodes (`FrameIndexSplitter`, `PersonBoxes`/`BallPick`, `KeypointsExtractor`/`BoundingBoxesExtractor`) + manual row-index re-alignment downstream |
| 7 | **Frame store / media references** | `offside_visualizer` re-opens the source videos from disk (frames too big to ship through the broker); needs out-of-band paths + sync offsets |
| 8 | **Watermarks** | Event-time completeness is wall-clock-timeout only |
| 9 | **Ordering guarantees** | `seq` exists but is used only for dedup; no reorder buffer |
| 10 | **Component stubs** | `ImageFolderReader`/`VideoFolderReader` raise `NotImplementedError`; `pose.py`/`counters.py` empty; writer is MJPG `.avi` only; no RTSP-out/mp4/clip-writer/OCR/zone-analytics |

---

## Recommended primitives (design)

Build order: **P3 → P1 → P2 → P4/P5/P6**. P3 is smallest and both P1's EOS story and P2's
triggered emission depend on it.

### P3 (first): zero-or-many emission + `flush()` EOS hook

**API surface** — two opt-in additions; every existing node stays byte-identical in behavior:

```python
# videoflow/core/node.py
class _Skip:
    '''Returned from process() to publish nothing for this input (unlike None,
    which is a legal payload).'''
    __slots__ = ()
SKIP = _Skip()

# on ProcessorNode (no-op default on ConsumerNode for symmetry):
def flush(self) -> Any:
    '''Called once after every parent has drained, *before* this node publishes
    EOS. Emit buffered state via ctx.emit() (declare a ctx parameter); return
    value treated exactly like a process() return (SKIP = publish nothing).'''
    return SKIP
```

```python
# videoflow/core/context.py — addition to RuntimeContext
def emit(self, message : Any, metadata : Optional[dict] = None,
        event_ts : Optional[float] = None, partition_key : Any = None) -> None:
    '''Publish one output now, inside process()/flush(). Zero or more calls per
    input; emissions publish in call order, before inputs are acked (LOOP-5
    holds for every emission).'''
```

**Return-value rule in `ProcessorTask._run`** (exact back-compat): `SKIP` → publish nothing;
anything else including `None` → publish as today. `ctx.emit()` calls are independent of the
return value. Existing nodes never call emit and never return SKIP → identical behavior.

**Identity minting (the protocol-sensitive part).** Today `publish_message` reuses
`_last_trace_id/_last_seq`; two emissions for one input would collide on `Nats-Msg-Id` and
JetStream would silently drop all but the first. Fix: messenger keeps a per-input-group
emission counter `k`, reset in `receive_message`:
- `k = 0`: carried-forward `trace_id`/`seq` unchanged — the single-output path keeps today's
  exact wire identity including its message id.
- `k >= 1`: `trace_id = f'{input_trace_id}.{k}'`, same `seq`. Deterministic across redelivery
  (re-run re-emits in the same order → same ids → dedup holds, MSGID-2 preserved); distinct
  trace ids also let a downstream trace-mode diamond join each emission with its own
  descendants.
- `flush()` emissions (no input group): `trace_id = f'{node_name}:flush:{k}'`, `seq = k`.
  Documented as stable only across a graceful drain — a crash mid-flush loses buffered state
  anyway.
- `event_ts`: inherit the input group's (LOOP-10) unless the per-emission kwarg overrides.

**Where it plugs in:**
- `core/task.py` — `ProcessorTask._run`: check `output is SKIP`; in the stop-signal branch call
  `self._call(self._processor.flush)` (publish its non-SKIP return / let emits go out)
  **before** `publish_stop_signal()`. Same in `ConsumerTask._run` before `break`. Flush
  failures are logged and must not prevent EOS propagation.
- `core/engine.py` — add `emit_message(...)` to the `Messenger` base.
- `messaging/nats_messenger.py` — implement `emit_message` (suffix logic, per-emission
  `_partition_key` metadata), reset counter in `receive_message`, honor `has_children = False`
  (emit becomes a no-op, mirroring LOOP-6).

**RFC 0003** (`spec/rfcs/`): new MSGID-4 (fan-out sub-identity `{trace_id}.{k}`), LOOP-11
(emit), LOOP-12 (flush-before-EOS), amend LOOP-3 (SKIP) and LOOP-10 (per-emission event_ts).
No `.proto`/envelope change. Golden vectors: add message-id vectors for suffixed trace ids.
**Size:** ~150 LOC code + ~250 LOC tests + spec/docs.

### P1: `WindowedProcessorNode` — buffer of last N items / last T seconds

**Decisive design constraint:** the window must be **node-local memory of already-acked
history**, not messenger/assembler-held in-flight messages — `max_ack_pending` is deliberately
tiny (6) and `ack_wait` bounds unresolved deliveries; a 450-frame window of un-acked handles
would stall the pull loop or force early acks that break ack-after-process (DELIV-1). This
means: no task.py/messenger/wire change at all; honest contract is "crash loses the buffer
tail, it repopulates" — consistent with existing at-least-once semantics.

```python
# videoflow/core/window.py (new module — pure, broker-free, easily testable)
class Window:
    '''Read-only view over a node's buffered recent inputs, newest last.'''
    @property
    def items(self) -> list: ...
    @property
    def timestamps(self) -> list: ...        # event_ts, epoch seconds
    def nearest(self, ts : float) -> tuple[Any, float]: ...
    def between(self, start_ts : float, end_ts : float) -> 'Window': ...

# videoflow/core/node.py
class WindowedProcessorNode(ProcessorNode):
    def __init__(self, window_count : Optional[int] = None,
                window_seconds : Optional[float] = None,
                window_key : Optional[str] = None, **kwargs : Any) -> None:
        # at least one bound required; window_key ⇒ per-key windows, requires partition_by
        ...
    def process(self, inp : Any, ctx = None) -> Any:   # type: ignore[override]
        # framework-implemented: append (event_ts, inp), dedup tail by (trace_id, seq)
        # from ctx.input_info (redelivery-stable), trim by count/time, delegate:
        ...
    def process_window(self, window : Window, current : Any) -> Any:
        raise NotImplementedError('process_window needs to be implemented by subclass')
```

Design points:
- **Validation**: `nb_tasks > 1` without `partition_by` raises at build (competing replicas
  would each see a random interleaving — no coherent window). `window_key` without
  `partition_by` likewise rejected. Single **data** parent only in v1 (multi-camera cases
  compose as time-join upstream → window downstream, exactly the fuser → engine chain);
  enforced in `graph.py` next to the existing partition check.
- Event time comes from `ctx.input_info[parent]['event_ts']` — already plumbed (LOOP-10).
- EOS: subclasses that aggregate override `flush()` (P3) to emit final state.
- `get_params()`: all three params are JSON scalars stored as `self._<name>`; buffer state is
  not a ctor arg. Nothing to override.
- Why not `ctx.window`: RuntimeContext is a per-call injection surface with no state; the
  buffer must live across calls and its natural owner is the node instance (the
  `OneTaskProcessorNode` precedent).

**No RFC** (nothing observable on wire/routing) — but it is a node-contract change:
update `.claude/docs/NODE_CONTRACT.md`, `docs/source/`, and contrib.
**Files:** new `core/window.py` (~150 LOC), `core/node.py` (~90), `core/graph.py` (~20),
~250 LOC tests (pure Window tests + task-level redelivery-dedup test).
**Proving ground:** port `offside_engine`'s deque/`_state_nearest`/`_trim_buffer` onto
`Window.between`/`nearest`.

### P2: control/marker inputs — `control=['parent-name']`

A parent designated as **control** is not a data input: it never gates join completeness or
quorum, is **broadcast to every replica** (bypasses `_owns()`), and is delivered via a
dedicated callback. Declared by parent *name* (a node's name is already its cross-process
identity — live node references would break the JSON `get_params()` round trip):

```python
# ProcessorNode/ConsumerNode.__init__ gain: control : Optional[list] = None
clip_maker = ClipMaker(window_seconds = 8.0, control = ['kick-detector'],
                    name = 'clip-maker')(fuser, kick_detector)

# node callback (optional ctx; async supported via the existing _call bridge):
def on_control(self, parent_name : str, message : Any, ctx : RuntimeContext) -> None:
    '''Called per message from a control parent, on the same single task thread
    as process() — never concurrently with it.'''
```

**Semantics:**
1. Control parents are excluded from `parent_names` positional order; `process(*inputs)` sees
   only data parents; the assembler never sees control entries.
2. **Broadcast delivery**: new durable family in `topology.py` —
   `{child}--ctl--{parent}--p{replica_id}` (new NAME-10), provisioned for every replica in
   `provision_flow` (mirroring the partitioned branch). Dedicated pull loop + queue per
   control parent in the messenger; `_owns()` bypassed.
3. **Prompt delivery**: `receive_message()` checks control queues *before* the assembler, so a
   marker on a quiet data stream returns immediately (entry carries `'is_control': True` +
   parent name). `ProcessorTask._run`/`ConsumerTask._run` branch: dispatch
   `self._call(node.on_control, parent, message)`, then `ack_inputs()`/`fail_inputs()` with
   the same at-least-once discipline. The messenger sets `_last_trace_id/_last_seq/_last_event_ts`
   from the control entry, so `ctx.emit()` inside `on_control` mints output identity derived
   from the marker — a redelivered marker re-emits the same clip id and JetStream dedups it
   (this is why P2 depends on P3).
4. **EOS**: loop termination driven by **data** parents only (new EOS-7); on data-drain the
   messenger drains queued control messages, terminates control consumers, calls `flush()`,
   publishes EOS. A never-ending trigger source must not wedge BATCH shutdown; a control
   parent finishing early must not stop the node.
5. **Delivery class**: control rides JetStream at-least-once (a missed kick is a missed clip),
   *not* the fire-and-forget plain-NATS `_control.stop` style.
6. **Validation** (`graph.py`): every `control` name is an actual parent; ≥1 data parent
   remains; the class overrides `on_control` (reject the silent-drop foot-gun); windowed/
   partition rules count data parents only.

**End-to-end clip use case:** `ClipMaker(WindowedProcessorNode)` with `window_seconds = 8`;
`on_control` appends trigger `t` (dedup by marker trace_id); `process_window` checks per data
input whether `newest_event_ts >= t + 2.0` and then
`ctx.emit(window.between(t - 5.0, t + 2.0).items)` — past frames from P1's buffer, future
frames by letting event time pass the trigger, 0..N emission via P3.

**Files:** `core/node.py` (param + stub), `core/compiler.py` (`NodeSpec.control` — **appended
last**; field order is the constructor signature; `to_dict`/`from_dict`), `runtime/worker.py` +
`engines/local.py` + `deploy/manifests.py` (`VF_CONTROL_PARENTS`), `core/graph.py`,
`messaging/topology.py`, `messaging/nats_messenger.py` (control pull loops, queue priority,
`_owns` bypass, EOS-7 drain), `core/task.py`, `core/engine.py` (document extended
`receive_message` shape).

**RFC 0004** — the heaviest: NAME-10; a CONTROL requirement family (delivery/ordering/ack);
EOS-7; PART-5 note (control bypasses ownership); ENV row for `VF_CONTROL_PARENTS`; amend ENV-3
(positional order = data parents). No wire/envelope change — control messages are ordinary
envelopes on the parent's ordinary stream; only routing (durables) and loop behavior change.
**Size:** ~450–700 LOC + ~300 LOC tests (broadcast to `nb_tasks=3`, marker-redelivery dedup of
the emitted clip, EOS-7 with a non-terminating control parent) — largest of the three temporal
primitives.

### P4: named multi-output ports

**Declaration** — ports are intrinsic to a class, so a class attribute (read by the compiler,
not `get_params()`); contract v1: a multi-port node returns a dict containing **every**
declared port each round (`None` legal and published — preserves exact trace-alignment so
downstream trace joins never stall; selective emission deliberately deferred):

```python
class SoccerDetector(ProcessorNode):
    output_ports : ClassVar[tuple[str, ...]] = ('players', 'ball')
    def process(self, frame : np.ndarray) -> dict:
        return {'players': person_dets, 'ball': ball_pick}

# wiring — Node.__getitem__ returns a frozen OutputPort(node, port) accepted by __call__:
dets = SoccerDetector(...)(frame)
tracks = BoxmotTracker(...)(frame, dets['players'])
packer = FeaturePacker(...)(tracks, pose, team, dets['ball'])
```

Producers get ports too — `VideostreamReader` can declare `('frame', 'index')`, killing
`FrameIndexSplitter`. `__call__` unwraps ports into `self._parents` (still Node objects, so
`has_cycle`/`topological_sort` untouched) + a parallel `self._parent_ports`.

**Edge identity (load-bearing):** qualified refs `'{parent}:{port}'` threaded through
unchanged plumbing — `build_tasks_data()` emits them, `NodeSpec.parents` stores them,
`VF_PARENT_NAMES` carries them, one `split_edge(ref) -> (node, port | None)` helper in
`topology.py` parses them. Two ports of the same parent to one child work for free (distinct
dict keys in `receive_message()`, distinct durables). `NodeSpec` gains appended
`output_ports : Optional[List[str]]`.

**Subject/stream mapping:**
- Port subject `vf.{flow}.{run}.{node}.{port}` (default output keeps the bare subject); port
  names sanitized, must not start with `_` (the `._eos` namespace position).
- Still one stream per node, binding data + port + eos subjects.
- **REALTIME trap:** today `max_msgs = max(1, realtime_buffer)` + `DiscardPolicy.OLD` — port B
  would evict port A. Multi-port streams must use `max_msgs_per_subject` instead; scope to
  multi-port streams so existing configs stay byte-identical.
- Durables `{child}--from--{parent}--{port}` (NAME-5/6 amendment); partition suffix `--p{n}`
  composes on top. Filter subject = port subject, so a `'ball'` subscriber never receives
  `'players'` payloads — the real bandwidth/decode win over splitters.
- **EOS stays per node, not per port** (all ports end when the task loop ends; a
  ball-only child still observes node EOS) — the single biggest scope-saver.
- `publish_message` splits the returned dict, one envelope per port with the same
  `trace_id`/`seq`/`event_ts` (downstream re-zip by trace join is automatic). Envelope format
  unchanged → no golden-vector changes (verify, don't assume).
- Blob refcounting becomes per-port: `blob_readers_by_port : Optional[Dict[str, int]]`
  (appended field) + `VF_BLOB_READERS_JSON`; over-counting is safe (TTL backstop),
  under-counting never.

**Also touched:** `component.yaml` needs `io.outputs: [{name, type}]`
(`components/descriptor.py`, `core/remote.py`); KEDA trigger durable enumeration in
`deploy/manifests.py` must include port durables. Does **not** fix the time-join `nb_tasks=1`
ceiling — out of scope.

**RFC 0005**: amend NAME-2/5/6, STREAM-1/2, BLOB-5, ENV (`VF_PARENT_NAMES` qualified refs,
`VF_OUTPUT_PORTS`, `VF_BLOB_READERS_JSON`). **Acceptance test:** portless flows produce
byte-identical `NodeSpec.to_dict()`, subjects, durables, stream configs.
**Size:** ~700–900 LOC — the largest single item. In offside it deletes 3 glue classes and
~9–12 worker pods (3 glue × N cameras) plus both human_tracking extractors.

### P5: frame store / media references (no protocol change)

**Verdict:** thin policy layer over the existing `BlobStore` machinery; do **not** reuse the
wire-level `BlobRef` — that is auto-resolved at decode (BLOB-3) and refcount-released on ack
(BLOB-6), exactly wrong here: resolution must be lazy/conditional (only rare event frames are
fetched) and readership is unknown at compile time. A `FrameRef` is a plain JSON dict payload;
storage via `BlobStore.put()` (TTL, no refcount — BLOB-7 semantics). Pure library addition.

```python
# new: videoflow/media/frames.py
class FrameStore:
    def __init__(self, blob_url : str, retention_seconds : int = 120,
                encoding : str = 'jpeg', jpeg_quality : int = 90) -> None: ...
    def open(self) -> None: ...     # make_blob_store(url) here — no I/O in __init__
    def put(self, cam : str, frame_idx : int, event_ts : float,
            frame : np.ndarray) -> dict:
        '''Returns {'cam','frame_idx','event_ts','ref','encoding','shape'}'''
    def resolve(self, frame_ref : dict) -> np.ndarray: ...

class RedisFrameCache(FrameStore):
    '''Adds a per-camera Redis zset time index (score = event_ts), trimmed on put.'''
    def frames_between(self, cam : str, t0 : float, t1 : float) -> list[dict]: ...
    def nearest(self, cam : str, event_ts : float, tolerance_s : float = 0.05) -> dict | None: ...
```

- Producer opt-in: `VideostreamReader(..., frame_cache_url : str | None = None,
  frame_retention_seconds : int = 120)` — `next()` puts each frame and exposes a `frame_ref`
  (extra output port once P4 lands; opt-in tuple element before that).
- Retention is the TTL: size guidance `fps × cams × retention × jpeg_size` (30fps × 3 × 120s ×
  ~150KB ≈ 1.6GB) — document Redis `maxmemory` + optional downscale param. A missing frame at
  resolve time degrades (skip overlay), never crashes.
- **Honest scoping:** for the current BATCH offside solution, re-reading files from disk is
  actually principled (the file outlives any cache; Interest-retention backlog can exceed any
  affordable window). The win is REALTIME/live flows and the event-clip writer. Keep the disk
  path as the BATCH fallback.

**No RFC.** ~250 LOC `media/frames.py` + ~40 in `producers/video.py` + tests/docs.

### P6: component fills (prioritized by demand from real solutions)

| # | Component | Placement | Size | Notes |
|---|---|---|---|---|
| 1 | **Mp4 `VideoWriter`** — extension-driven fourcc (`.mp4`→`avc1`, fallback `mp4v`; `.avi`→`MJPG`), `fourcc : str = 'auto'`; writer created lazily on first frame (needs dims); keep `VideofileWriter` as frozen alias | core (`consumers/video.py`) | ~120 LOC | All three solutions emit browser-unplayable MJPG `.avi` today. Contrib `pyav_writer/` for guaranteed H.264 (check PyAV codec licensing) |
| 2 | **`ImageFolderReader`/`VideoFolderReader`** — glob + sorted order, per-file iteration, `event_ts` stamping, hand-written `get_params()` | core (`producers/video.py`) | ~150 LOC | Currently `NotImplementedError` stubs |
| 3 | **`counters.py`**: `LineCrossingCounter(lines : dict[str, list[list[float]]])` + `ZoneIntrusionDetector(zones, min_frames : int = 1)`, both `OneTaskProcessorNode`, input `(N,5)` tracks, output `{'counts', 'events'}`, pure numpy | core | ~250 LOC | Natural next stage after tracking; v2 can `partition_by='cam'` |
| 4 | **`EventClipWriter`** consumer — consumes `{'cam','event_ts'}` events, pulls `[t−pre, t+post]` via `RedisFrameCache.frames_between`, writes mp4. Post-roll via a deferred-work queue drained on later `consume()` calls (blocking `consume()` stalls acks) | core | ~180 LOC | **Gated on P5** — pre-roll cannot live in-node (REALTIME retention; crash loses in-memory buffers) |
| 5 | **RTSP/RTMP restreamer** — `RtmpRestreamer(url, fps = 30.0, bitrate = '3M', encoder = 'libx264')`; ffmpeg subprocess pipe opened in `open()`, restarted on death | contrib + `component.yaml` + Dockerfile | ~200 LOC | Live output for offside/human_tracking |
| 6 | **`pose.py` abstract base** — `PoseEstimator(ProcessorNode)` + COCO keypoint constants, mirroring `detectors.py`; `pose_topdown` subclasses it | core | ~60 LOC | Consistency with the domain-base rule |
| 7 | **OCR** | contrib (heavy ML deps) | ~150 LOC | Plates/jerseys demand exists; nothing current needs it |

Rejected for now: WebRTC output consumer — per-viewer signaling/session state doesn't fit the
one-node-per-pod model; restream to MediaMTX and let it terminate WebRTC.

---

## Design rejects (recorded so we don't relitigate)

- **Messenger/assembler-held windows** (a JoinPolicy-style `window=` knob): window contents
  would be un-acked in-flight deliveries; `max_ack_pending`(=6)/`ack_wait` make that a stall
  or an early-ack that breaks DELIV-1.
- **Redis-backed keyed state / checkpointing of buffers**: a future, much larger RFC
  (watermark/timer/checkpoint territory). Buffers are best-effort task-local memory for now.
- **Generator/`yield`-based `process()`** for flatMap: breaks the `(class path, get_params())`
  round trip and the async `_call` bridge; `ctx.emit` achieves 0..N without changing the
  callable form.
- **Making `None` a drop sentinel**: silently changes wire behavior of every existing flow.
- **Control parents by node reference**: live refs break JSON serialization — names only.
- **Broadcasting control on plain NATS** (like `_control.stop`): loses at-least-once for
  data-critical markers.
- **`window_key` without `partition_by`**: per-key windows on competing replicas shard each
  key's history randomly — enforced by validation.
- **Watermarks / tumbling / session windows / event-time timers**: need a progress notion
  across parallel sources + a timer service, none of which the envelope carries. Deferred;
  `window_seconds` is documented as a sliding tail, not an aligned window.

## Roadmap (suggested sequencing)

| Phase | Items | RFC | Rough size |
|---|---|---|---|
| 1 — quick wins | Mp4 writer, folder readers, `pose.py` base | none | ~330 LOC |
| 2 — temporal core | P3 emit/SKIP/flush → P1 `WindowedProcessorNode` | 0003 (P3 only) | ~400 LOC + tests |
| 3 — triggers | P2 control inputs → P5 frame store → `EventClipWriter` | 0004 | ~900 LOC + tests |
| 4 — structure | P4 named output ports | 0005 | ~700–900 LOC |
| 5 — analytics | counters, restreamer, OCR | none | parallel with 4 |

(RFC numbers assigned here to resolve a collision between the two designs; final numbers
follow whatever exists under `spec/rfcs/` at implementation time.)

Deferred to a future, larger effort (explicitly not in this plan): watermarks, aligned
tumbling/session windows, durable keyed state + checkpointing, scaling time-mode joins past
`nb_tasks=1`, cycles/iterative dataflow.

## Verification

- **Unit:** pure `Window` tests (nearest/between/trim/dedup); ctor validation tests
  (window bounds, `window_key`⇒`partition_by`, control-name validation);
  `NodeSpec.to_dict` byte-identity before/after (the repo's stated acceptance test);
  `topology.py` naming tests for `--ctl--` and port durables.
- **Integration** (`tests/integration/`, live NATS): fan-out dedup under forced redelivery
  (P3); flush-before-EOS ordering (P3); control broadcast to `nb_tasks=3` replicas +
  marker-redelivery dedup of the emitted clip + EOS-7 with a non-terminating control parent
  (P2); multi-port stream retention under REALTIME (P4).
- **Golden vectors:** MSGID-4 suffixed ids (`spec/vectors/`, regenerate via `generate.py`);
  verify P4 leaves vectors unchanged.
- **Contrib proving ground:** migrate `offside_engine`'s buffer to `WindowedProcessorNode`;
  add a `toy_*`-style dependency-free exerciser solution for control inputs (the established
  pattern for framework features); later, port the offside branch to output ports.
- **Docs in same commits:** `spec/PROTOCOL.md` + RFCs, `.claude/docs/NODE_CONTRACT.md` +
  `ARCHITECTURE.md` extension-seam table, `docs/source/`, `README.md`, contrib `CLAUDE.md`.
