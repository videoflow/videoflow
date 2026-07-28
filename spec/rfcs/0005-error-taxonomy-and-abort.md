# RFC 0005: Error taxonomy, disposition-driven delivery, and the ABORT marker

- **Status:** accepted
- **Author(s):** videoflow maintainers
- **Created:** 2026-07-27
- **Protocol version affected:** no version change (envelope stays v4; additive
  enum value and additive field)
- **Requirement IDs touched:** new `ERR-1`…`ERR-7`, `ABORT-1`…`ABORT-7`;
  revised `DELIV-6`…`DELIV-9`, `NAME-9`, `STREAM-8`, `EOS-1`, `EOS-2`, `EOS-5`

## Summary

Failures gain a **class** on the wire. A message that fails is now handled
according to *why* it failed — the data is bad, the world blipped, or this worker
is sick — rather than only according to the flow type and the delivery count. And
a node that dies abnormally now says so: a new `MSG_TYPE_ABORT` terminator carries
the error downstream, so a crash propagates through the graph the way end-of-stream
does instead of leaving every descendant blocked forever.

## Motivation

Two concrete failures motivated this, both observed in practice.

**A sick worker shreds a healthy stream.** The retry ladder was a function of
`(flow_type, num_delivered)` alone. A pod whose GPU wedges therefore fails every
message it touches, and each one is redelivered four times and then dead-lettered
— under a `CUDA out of memory` error that has nothing to do with the message. In
twenty minutes a single bad pod can move an entire stream into the DLQ while the
flow's own health signals stay green. Nothing in the protocol let an SDK say "this
message is fine, *I* am not."

The inverse waste is quieter: a message that will never decode is retried four
times with exponential backoff on its way to the dead-letter queue it was always
destined for.

**A dead producer hangs the graph.** A producer that raises mid-run never reaches
its `publish_stop_signal()`, so its children wait on an end-of-stream that is never
coming. A BATCH flow in that state hangs until a human notices: the workers are
healthy, the broker is healthy, and the only thing wrong is that the fact of the
producer's death exists nowhere on the wire. The protocol could express "I
finished" but not "I died", and one of those is not optional.

## Why an RFC

Both halves are observable across SDK boundaries. A non-Python component must
know that a terminator may now be an ABORT (and that it carries an error), and any
SDK implementing the retry ladder must agree on what each disposition costs, or two
components in one flow will disagree about whether a message is retried,
dead-lettered or dropped. The DLQ stream also changes name, which anything reading
dead letters observes.

## Proposal

### 1. `videoflow.v1.Error` (new file, `spec/proto/videoflow/v1/error.proto`)

```protobuf
enum Disposition {
  DISPOSITION_UNSPECIFIED = 0;   // treat as DISPOSITION_TRANSIENT
  DISPOSITION_POISON = 1;
  DISPOSITION_TRANSIENT = 2;
  DISPOSITION_WORKER_FATAL = 3;
}

message Error {
  string code = 1;               // stable, e.g. "VF_POISON_DECODE"
  string message = 2;
  string remedy = 3;
  Disposition disposition = 4;
  string node = 5;
  string trace_id = 6;
  uint32 num_delivered = 7;
  map<string, string> context = 8;
}
```

The taxonomy is normative as a **proto**, not as one SDK's class hierarchy: a
component may be written in any language, and a taxonomy that lives in Python's
exception tree is invisible to the rest of the system and to the tooling that
aggregates failures across it.

### 2. `MSG_TYPE_ABORT = 3` and `Envelope.error = 15`

```diff
 enum MsgType {
   MSG_TYPE_UNSPECIFIED = 0;
   MSG_TYPE_DATA = 1;
   MSG_TYPE_EOS = 2;
+  MSG_TYPE_ABORT = 3;
 }

 message Envelope {
   ...
   bytes payload = 14;
+  optional Error error = 15;
 }
```

`buf breaking`-clean: a new enum value and a new field number, no reuse, no
renumbering.

An ABORT rides the **existing `_eos` subject**. That is the whole reason it is
cheap: it reuses the per-replica EOS consumers and the provision-time interest
anchor, so no topology, durable or provisioning step changes.

**Who announces a death matters as much as the marker itself.** A worker
publishes an abort only for a death no restart can fix; everything else is
announced by the supervisor once it has actually given up. Without that split, a
worker that crashed recoverably would kill its children while its own replacement
was starting — converting the crash Kubernetes was about to absorb into a
flow-wide failure.

### 3. Disposition-driven delivery (revises `DELIV-6`…`DELIV-9`)

| disposition | best-effort | at-least-once |
|---|---|---|
| poison | sampled DLQ, then term | DLQ immediately, then term |
| transient | term | NAK until the budget, then DLQ |
| worker_fatal | NAK | NAK |

`worker_fatal` never dead-letters in either mode: the message is fine. It also
ends the worker — the disposition means "nothing I am given will succeed", so the
message goes back for a healthy replica and this one stops. The circuit breaker
below is the same protection for failures that never say so.

### 4. Per-node delivery mode

`VF_DELIVERY` (`at-least-once` | `best-effort`) and `VF_ON_ERROR` let one node
override the flow type's preset. Loss tolerance is a property of what a node does
with a message, not of the flow it lives in — a REALTIME flow may want
freshest-wins frames *and* a durable alert sink. A node's mode decides its
durables' `max_deliver`, so provisioning reads it too.

### 5. Flow-scoped dead-letter stream (revises `NAME-9`, `STREAM-8`)

- Stream: `vf-{flow}-{run}-dlq` → **`vf-{flow}-dlq`**
- Subject: `vf.{flow}.{run}._dlq.{node}` → **`vf.{flow}._dlq.{run}.{node}`**

Everything else about a run is disposable and is deleted with it. Dead letters are
the opposite: they are most wanted precisely after a run that failed and was torn
down. Run-scoping meant teardown — which runs in a `finally`, on success and
failure alike — destroyed the evidence, and the stream's week-long retention never
applied to anyone. The run id moves into the subject, so entries stay attributable
and filterable.

## Compatibility

**Wire compatibility.** Additive. An envelope with no `error` field decodes
exactly as before, and the golden vectors for DATA and EOS are byte-identical. A
reader that predates `MSG_TYPE_ABORT` sees an unknown enum value; the reference
decoder treats **any** terminator as end-of-stream (`is_stop_signal` is true for
both), so such a reader stops rather than hangs — the failure mode degrades to the
old behaviour rather than to a worse one. This is why `is_stop_signal` was widened
rather than a separate flag being added in its place.

**Behavioural compatibility.** An SDK that does nothing keeps working: an
unclassified error is `DISPOSITION_TRANSIENT`, which is precisely the old
behaviour (retry, then dead-letter). A component adopts the taxonomy by choosing
to, not by being forced to.

**The DLQ rename is observable.** Anything reading `vf-{flow}-{run}-dlq` by name
must move to `vf-{flow}-dlq`. Accepted deliberately: the old name's contents were
being deleted at teardown, so what breaks is a reader of something that mostly did
not survive to be read.

**Protocol version.** No bump. The envelope stays v4.

## Alternatives considered

**Carry the error as a normal DATA message on a side subject.** Rejected: it
would need its own stream, durables and interest anchor, and it would race with
the EOS it is meant to replace. Riding the terminator subject makes ordering
trivially correct — a node cannot see the abort before the data that preceded it.

**Infer the disposition from the exception type in each SDK.** Rejected: two
SDKs would disagree, and the disagreement would show up as messages being dropped
in one language and dead-lettered in another. The wire has to carry it.

**A separate `is_abort` boolean instead of widening `is_stop_signal`.** Rejected
for the compatibility reason above: an old reader that checks only
`is_stop_signal` must still terminate.

**Rely on the breaker alone, with no immediate stop on `worker_fatal`.**
Rejected: it wastes the one thing the taxonomy bought. When a node has explicitly
said "this worker cannot process anything", waiting for nine more identical
failures NAKs nine more messages and learns nothing. The breaker keeps its job for
the unlabelled case, which is the case it was actually designed for.

**Conversely, kill the pod on any single failure.** Rejected for the same reason
in reverse: an unclassified exception may well be a bad message, and one of those
must never take a worker down.

## Conformance

New golden vectors under `spec/vectors/envelope/`:

- `abort_v4.bin` — an ABORT envelope with a fully populated `Error`
- `abort_minimal_v4.bin` — an ABORT with only `code` and `message`

Both replay through the same harness as the existing vectors
(`tests/test_golden_vectors.py`), and the pre-existing DATA/EOS vectors must remain
byte-identical — an identity check, not merely a green suite.
