> **RESOLVED (2026-07-19) — see [spec/rfcs/0001](../spec/rfcs/0001-v4-only-wire.md).**
> The codec was not gated on decode; rather than gate it, the pickle codec was removed
> entirely and the legacy msgpack (v2/v3) wire retired, leaving the language-neutral
> protobuf v4 envelope as the sole wire. `Value` can now nest a `Tensor` so mixed
> containers still encode neutrally. The original report is kept below for the record.

---

The wire decode path deserializes pickle payloads without gating. In videoflow/wire/serialization.py, decode_payload (v3, ~line 260) and _decode_payload_v4 (v4, ~line 499) both call pickle.loads(buf) whenever the envelope's codec/type field says pickle.

The allow_pickle control only gates the encode side. The decode functions take no allow_pickle parameter, and COMPATIBLE_ENVELOPE_VERSIONS still accepts v3 — so a node hardened to never emit pickle will still decode an attacker's version-downgraded pickle envelope.

The NATS consumer (videoflow/messaging/nats_messenger.py) connects without authentication and calls decode_envelope(msg.data) on every message. So any peer able to publish to a flow's NATS subject gets code execution on every worker.

Why I think it's real despite the pickle guardrails



You clearly already treat pickle as dangerous (the allow_pickle flag, and the "mixed flow that would need pickle on the wire is a hard compile error" rule). The gap is that all of that is enforced on emit, not on decode — so the protection can be bypassed by simply sending a v3/pickle envelope to a worker.



Suggested fix



Gate the decode path on the same policy as encode (default-deny pickle on decode unless explicitly enabled), or drop the pickle codec for cross-node transport altogether. This is the same fix python-socketio shipped for the analogous CVE-2025-61765 (they removed pickle in favor of JSON in 5.14.0).
