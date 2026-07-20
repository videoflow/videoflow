# RFC 0002: Refcounted blob reclamation and flow-type blob TTLs

- **Status:** proposed
- **Author(s):** videoflow maintainers
- **Created:** 2026-07-19
- **Protocol version affected:** 1 (no version change — envelope bytes are untouched)
- **Requirement IDs touched:** `BLOB-3` (amended), `BLOB-5` (new), `BLOB-6` (new), `BLOB-7` (new)

## Summary

Payloads offloaded to the blob store per `BLOB-1` are today reclaimed only by their
TTL (default 3600s per `BLOB-3`): nothing ever deletes a blob, no matter how quickly
every downstream reader consumed it. In a video flow nearly every frame exceeds the
inline threshold, so the store's steady-state residency is a full TTL window of
frames per edge — 30fps of ~6.2MB 1080p frames held for 3600s is on the order of
670GB — with the reference Redis deployment running uncapped and `noeviction`
underneath. This RFC adds a companion refcount so each blob is deleted as soon as
every downstream reader has **acked** the message that consumed it, keeps the TTL as
the authoritative backstop for everything a refcount cannot cover, and makes the TTL
flow-type-aware: the flat 3600s default is simultaneously ~1000× more than REALTIME
needs and *short enough to silently lose data* in BATCH, whose Interest-retention
backlog can legitimately delay a blob's first read past an hour.

## Motivation

Two independent problems share one root cause (blob lifetime is a fixed TTL,
disconnected from actual consumption):

1. **Residency.** A blob is dead the moment its last reader acks — typically
   milliseconds to seconds after publish — yet it occupies Redis for the full TTL.
   The store's working set is therefore proportional to *throughput × TTL* instead
   of *in-flight backlog*, a difference of roughly three orders of magnitude for a
   REALTIME video flow.
2. **BATCH data loss.** A BATCH stream admits a deep backlog (default 10,000
   messages) before backpressure engages, and a message's blob is only read when a
   consumer *reaches* it. A consumer draining a full backlog slower than
   ~2.8 messages/second takes over an hour to reach the tail; those blobs expire
   before their first read, the decode raises on the missing ref, and the delivery
   is terminated as poison — silent loss in the flow type whose retention, retry,
   and DLQ design exists to prevent exactly that.

Refcounting fixes (1) directly and *enables* the fix for (2): raising the BATCH TTL
without refcounting would multiply residency by the same factor; with refcounting,
acked blobs vanish promptly and a long TTL bounds only *leaked* blobs.

## Why an RFC

The envelope bytes, the `BlobRef` proto, and routing are all unchanged — this is not
a wire change and needs no version bump or golden-vector regeneration. It is,
however, an observable change to the §13 **store contract**, which `BLOB-4` makes
multi-language: a non-Python SDK that publishes must write the counter key for a
Python reader's reclamation to engage, and a reader that never decrements keeps a
publisher's blobs alive to their TTL. Cross-SDK observable behaviour is captured
here per the process.

## Proposal

### Amend `BLOB-3`

Before: "`ref` is opaque; the reference Redis store uses keys `vf-blob-<uuid>` with
a TTL (default 3600s)."

After: "`ref` is opaque to consumers; the reference Redis store uses keys
`vf-blob-<uuid>` with a TTL (`BLOB-7`). The store additionally maintains a companion
reclamation counter per blob (`BLOB-5`) whose key naming is internal to the store."

### `BLOB-5` (new — reader-counted put)

When the deployment supplies the number of downstream reads each published message
receives (`VF_BLOB_READERS`, computed at compile time as the sum over consuming
children of `nb_tasks` for a partitioned child and 1 otherwise — partitioned
replicas each hold their own broker consumer and each decode every message, while
non-partitioned replicas compete on a shared consumer), the publisher SHOULD write,
in the same store and with the same TTL as the blob, a counter key initialized to
that count (reference store: blob `vf-blob-<uuid>` → counter `vf-blobrc-<uuid>`).
The blob MUST be written before its counter, so an interrupted put degrades to a
counterless blob (TTL-only) rather than a counter without a blob. A publisher
without the count MUST write the blob without a counter (plain `BLOB-3` semantics).

### `BLOB-6` (new — release on ack)

A reader that resolved a `BlobRef` MUST decrement the blob's counter at most once
per delivered message, and only after the broker acknowledgment of that message
**succeeds**. It MUST NOT decrement on nak, term, or dead-letter (a redelivery or a
DLQ inspection re-reads the blob). It MUST NOT decrement — or create — a counter key
that does not exist (a counterless blob belongs to its TTL). When a decrement
observes a value ≤ 0, the reader SHOULD delete both the blob and the counter.

### `BLOB-7` (new — TTL backstop and flow-type defaults)

The `BLOB-3` TTL remains on both keys and is the authoritative upper bound on blob
lifetime; refcounted deletion is an optimization for the common path. A REALTIME
stream may evict a message no consumer ever acks, a crashed reader may never
release, and a dead-lettered message's blob is deliberately left to its TTL — so
ack-driven decrements alone can never be relied on to reach zero. The TTL is chosen
by the **publisher** and MUST exceed the worst-case publish-to-final-ack latency of
the flow type. Reference defaults: 3600s for REALTIME (delivery is near-immediate;
the TTL bounds only leaks) and 86400s for BATCH (a full Interest-retention backlog
can legitimately delay a first read past an hour; a too-short TTL is silent data
loss). Deployments MAY override via `VF_BLOB_TTL_SECONDS`. A decoder MUST tolerate a
missing blob exactly as before (the delivery fails to decode and is terminated);
reclamation does not change that contract.

## Compatibility

- **Wire compatibility:** none affected. `BlobRef` and the envelope are byte-for-byte
  unchanged; no golden vector changes, no `buf breaking` concern, no version bump.
- **Behavioral compatibility:** a run is version-homogeneous, so mixed
  publisher/reader versions do not occur within a run; regardless, both directions
  degrade safely. Old publisher + new reader: no counter key exists, the `BLOB-6`
  existence guard means the reader never decrements or deletes — exactly today's
  TTL-only behaviour. New publisher + old reader: nobody decrements, blob and
  counter both expire by TTL. Third-party `BlobStore` implementations that predate
  `put_with_readers`/`release` inherit base-class defaults (delegate-to-`put`,
  no-op) and keep working as TTL-only stores. Manifests rendered by an older CLI
  lack `VF_BLOB_READERS`, which disables refcounting rather than miscounting.
- **Protocol version:** 1, unchanged.

## Conformance impact

- New conformance-map rows for `BLOB-5`, `BLOB-6`, `BLOB-7` (unit coverage in
  `tests/test_blob_refcount.py`; broker-backed coverage in
  `tests/integration/test_blob_reclamation.py`).
- Scenario IDs for the eventual `conformance/` suite: fan-out ack-reclaim (all
  readers ack → both keys gone), partial fan-out (one of two acks → blob survives,
  counter = 1), dead-letter-preserves-blob, partitioned ack-skip decrement.

## Alternatives considered

- **Shorten the TTL instead.** Rejected: makes the BATCH loss window *worse*, and
  in REALTIME the TTL is already not the residency bound once refcounting lands.
- **Decrement at `get()` time.** Rejected: the blob is read at decode, long before
  ack; a crash between decode and ack triggers a broker redelivery that re-reads
  the blob, which a get-time decrement may already have destroyed — turning a
  recoverable redelivery into poison-message data loss.
- **A per-durable membership set (`SREM`/`SCARD`) instead of a counter.** Rejected:
  the publisher would need the full durable-name list plumbed to it, the set is
  heavier than an integer, and it is still not idempotent across
  ack-then-redeliver, so it buys no correctness over `DECR`.
- **A Lua script for atomic decrement-and-delete.** Rejected: `DECR` is already
  atomic and exactly one releaser observes the transition to ≤ 0, so the follow-up
  deletes need no cross-key atomicity. A multi-key script is a cross-slot operation
  on Redis Cluster (`vf-blob-<x>` and `vf-blobrc-<x>` hash to different slots) and
  `EVAL` is restricted on some managed offerings. Every race plain commands admit
  degrades to leak-until-TTL, which is the designed-safe direction.
- **Adding a `readers` parameter to `BlobStore.put()`.** Rejected: every existing
  third-party subclass overriding `put(data, ttl_seconds)` would break (mypy
  `override` error in-repo, `TypeError` at runtime out-of-repo). New methods with
  safe base-class defaults are strictly compatible.

## Open questions

- Should a dead-lettered message's blob have its TTL refreshed to the DLQ retention
  (7 days) at dead-letter time, so DLQ replay can always resolve it? Today (and
  under this RFC) replay after TTL expiry must tolerate a missing blob; refreshing
  would trade Redis residency for replayability. Deferred as future work.
- `blob_readers` is baked at compile time; manually scaling a partitioned child
  (`kubectl scale`) already breaks `hash % nb_tasks` ownership and now also skews
  the count. The existing invariant ("redeploy, don't hand-scale") covers both;
  recording it here rather than solving it.
