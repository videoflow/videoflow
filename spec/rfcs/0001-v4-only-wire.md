# RFC 0001: Remove the pickle codec; make protobuf v4 the sole wire

- **Status:** accepted
- **Author(s):** videoflow maintainers
- **Created:** 2026-07-19
- **Protocol version affected:** 1 (no major bump — v4 was always protocol v1's target)
- **Requirement IDs touched:** `WIRE-1`, `WIRE-2`, `WIRE-3`, `WIRE-11` (withdrawn), `WIRE-12`, `WIRE-15` (new)

## Summary

The wire deserialized attacker-controllable broker bytes with Python `pickle`, and
the `VF_ALLOW_PICKLE` policy that was meant to gate it applied only on *encode*, never
on *decode*. Any peer that could publish to a flow's NATS subject therefore got remote
code execution on every worker (and on an operator's machine via `videoflow debug
decode`). This RFC removes the pickle codec entirely — there is no gated,
opt-in, or legacy pickle path left — and makes the language-neutral protobuf envelope
(v4) the sole wire. The msgpack envelopes (v2/v3), whose non-array fallback was pickle,
are removed with it. To keep real flows working without pickle, `videoflow.v1.Value`
gains the ability to nest a `Tensor`, so a mixed container such as a
`(frame_index, frame)` tuple has a neutral encoding.

## Motivation

`pickle.loads` executes arbitrary code during unpickling (via `__reduce__`), so it must
never be run on bytes from an untrusted source. Videoflow's broker (NATS) ships with no
authentication and no subject-level authorization, so "trusted source" cannot be
assumed. The decode path ran pickle unconditionally on the v3 codec and on the v4
`x-python-pickle` marker, and `decode_envelope` auto-selected the v3 decoder from the
leading byte — so a worker hardened to emit only v4 could still be handed a
version-downgraded v3/pickle envelope and execute it. This is the same vulnerability
class as CVE-2025-61765 (python-socketio), whose fix removed pickle from the inter-node
wire in favor of a data-only format.

There is no safe way to keep pickle: restricted unpicklers and bytecode scanners are
documented as bypassable, and the payloads that motivated pickle (numpy arrays, mixed
containers of arrays and scalars) already have — or now gain — neutral encodings.

## Why an RFC

Removing a payload codec and an envelope version is an observable **wire** change: a
deployed component or a stored/DLQ'd message encoded the old way is affected. Adding a
field to `Value` is also a wire change. Per the process, these are captured here and
reflected in `spec/PROTOCOL.md` + `spec/vectors/`.

## Proposal

### Remove pickle and the legacy msgpack wire

- **`WIRE-11`** is **withdrawn**. Before: "`payload_type = x-python-pickle` is a Python
  `pickle` payload … permitted only when `VF_ALLOW_PICKLE` is set". After: the codec is
  removed; a payload with no built-in encoding must use a registered vendor encoder
  (`WIRE-9`), and an unrecognized `payload_type` MUST be carried through opaquely and
  MUST NOT be deserialized. The ID is retained as a tombstone (never reused).
- **`WIRE-1`** before: "Protocol v1 targets envelope v4 … the current build additionally
  emits/accepts v3 (msgpack) and decodes v2; these are being retired." After: "Protocol
  v1's sole wire is envelope v4. The earlier msgpack envelopes (v3, v2) … have been
  removed; a decoder MUST refuse them."
- **`WIRE-2`/`WIRE-3`**: `VF_ENVELOPE_VERSION` may only be `4`; a decoder MUST reject any
  pre-v4 (msgpack) envelope with a clear error.
- The `VF_ALLOW_PICKLE` environment variable is removed. The `--allow-pickle` CLI flag
  (on `deploy`, `run`, `run-local`, `compile`) is removed. The `allow_pickle` parameter
  is removed from the public `encode_envelope`, from the engines, and from the manifest
  renderer.

### Close the encoding gap so v4 can be the sole wire

- **`WIRE-15`** (new): a `Value` MAY hold a `Tensor` (`tensor_value`). A structured
  container (list/tuple or string-keyed map) that mixes arrays with scalars therefore
  has a neutral encoding; a *bare* ndarray payload stays a top-level `Tensor` (`WIRE-7`),
  and only an array *nested inside* a container travels as `tensor_value`.
- **`WIRE-12`** gains `tensor` to the `Value` union enumeration.

`.proto` diff (`spec/proto/videoflow/v1/value.proto`), field-number-safe (append-only in
an existing `oneof`), so it is `buf breaking`-clean and needs **no** envelope version
bump — and v4 was not yet the shipped default, so there is no deployed v4 traffic to
break:

```proto
 import "videoflow/v1/payloads.proto";   // new
 message Value {
   oneof kind {
     ...
     MapValue map_value = 8;
+    Tensor   tensor_value = 9;
   }
 }
```

The `x-python-pickle` comment is removed from `envelope.proto`.

## Compatibility

- **Wire compatibility:** a run is version-homogeneous (`WIRE-1`) and streams are
  run-scoped, so no single run mixes versions. A **new** deploy speaks only v4. The
  break is for bytes at rest: any pre-existing v2/v3 (pickle) message still in a DLQ or
  an in-flight stream becomes undecodable after upgrade. This is intentional — retaining
  a pickle decoder "for forensics" retains the RCE. Drain or discard old DLQs before
  upgrading. A running v3 flow cannot be image-upgraded in place (its pinned
  `VF_ENVELOPE_VERSION=3` no longer emittable); redeploy it as a new v4 run.
- **Behavioral compatibility:** a flow that relied on pickle to move an arbitrary custom
  Python object between nodes must now either emit a neutral type (ndarray / a
  `videoflow.v1` proto / a JSON-like `Value`, which may nest ndarrays) or register an
  encoder with `videoflow.wire.serialization.register_payload_encoder`. Flows that move
  frames, detections, tracks, scalars, dicts, or `(frame_index, frame)`-style tuples are
  unaffected. An old host CLI passing the removed `--allow-pickle` to a newer
  `compile.py` fails on the unknown flag.
- **Protocol version:** no major bump. v4 was always protocol v1's target; `Value` gains
  a field additively.

## Conformance impact

- `WIRE-15` gets a positive golden vector (`spec/vectors/envelope/data_value_nested_tensor.bin`)
  and a unit test.
- A new negative-vector convention under `spec/vectors/reject/`: `legacy_msgpack.bin`
  (decode MUST raise) and `unknown_payload_type.bin` (decode MUST return opaque bytes,
  never deserializing) — replayed by `tests/test_golden_vectors.py::test_reject_vector`.
- `DELIV-9` (poison/undecodable → terminate) is exercised at the decode layer by the
  legacy-wire and unknown-type tests; the `_pull_loop` `term()` wiring remains
  integration-only.

## Alternatives considered

- **Gate pickle on decode (default-deny, opt-in via `VF_ALLOW_PICKLE`).** Rejected: it
  leaves a foot-gun that re-opens the RCE the moment someone sets the flag, and it keeps
  the v3 wire (which pickles unconditionally) alive. The maintainers chose full removal.
- **A "safe pickle" (restricted `Unpickler`, or a scanner like `fickling`).** Rejected:
  documented as insufficient/bypassable, and for videoflow's payloads it collapses into
  "use a neutral encoding" anyway.
- **Adopt the `safetensors` library.** Rejected: the v4 `Tensor` proto already is a
  data-only tensor encoding; `safetensors` metadata is string-only (it would stringify a
  `frame_index`), it is a file/mmap format rather than a streaming envelope, and it can't
  carry arbitrary custom objects either. `Value`-nests-`Tensor` gives the same safety
  plus typed scalars and nesting.
- **Model the producer edge as a `Frame` proto instead of a nested-tensor container.**
  Deferred: cleaner long-term, but it changes the node contract across `videoflow-contrib`
  and user graphs. `Value`-nests-`Tensor` is wire-local and non-breaking, and does not
  preclude that path later.

## Open questions

None outstanding. NATS broker authentication (the enabling condition that made the RCE
reachable) is out of scope for this RFC and tracked separately.
