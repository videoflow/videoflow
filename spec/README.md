# Videoflow protocol specification

This directory is the **language-agnostic contract** between videoflow (which
compiles graphs, provisions the broker, and deploys workers) and a component
runtime (the code inside one worker). It exists so components can be written in
any language and still interoperate with videoflow-deployed flows.

## Contents

- **[`PROTOCOL.md`](PROTOCOL.md)** — the normative protocol (v1). Everything an SDK
  must implement: the environment contract, broker naming, JetStream stream/consumer
  configuration, the message envelope and payload types, message-id/dedup, the node
  task loop, delivery/ack/retry/DLQ, join (input-group) assembly, end-of-stream
  drain, partitioning, the control plane, health/metrics, blob storage, and sink
  idempotency. Requirements carry stable IDs (`ENV-1`, `EOS-3`, …) referenced by
  conformance scenarios.
- **`proto/videoflow/v1/`** — the protobuf IDL for the envelope and well-known
  payload types (`Tensor`, `Frame`, `Detections`, `Tracks`, `BlobRef`, `Value`).
  Landed in Phase 1 of the migration; the wire encoding for envelope v4.
- **`vectors/`** — golden test vectors (envelope round-trips; `vectors/join/` holds
  clock-injected join scenarios) replayed against every SDK to enforce lockstep.
- **[`rfcs/`](rfcs/)** — the change process. The protocol is a public API; changes
  to observable wire/routing behavior go through an RFC (`rfcs/0000-template.md`).

## How this maps to the code

`PROTOCOL.md` was extracted from the Python reference implementation. Its
"source-of-truth map" (§0.2) points each section back to the file that defines it.
The Python worker (`videoflow.worker` + `videoflow.messaging`) **is** the executable
definition of protocol v1 — where the prose is ambiguous, the Python behavior wins,
and the conformance suite (`conformance/`, Phase 4) pins both together.

## Status

Protocol v1 is **stabilizing**: it will not be declared frozen until at least two
non-Python SDKs pass the full conformance suite. Until then, requirement IDs are
stable but the set may still grow as extraction gaps are found.
