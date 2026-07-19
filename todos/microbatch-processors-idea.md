# Generators for Producers/Processors — assessment + RFC 0001 (microbatch processors)

## Context

`videoflow-contrib/TODO.md` item 2 asks whether generator-based interfaces for Producers and
Processors would enable buffer accumulation for GPU batching or CPU multiprocessing. The
investigation is done; the user chose the **RFC + assessment doc** deliverable: record the
verdict and the accepted design as a *proposed* RFC, annotate the TODO item. **No code
changes, no PROTOCOL.md edits** — those land later with the implementation (RFC → accepted).

## The verdict (what the RFC records)

**Generators as the node interface: no. Buffer accumulation: yes — via an opt-in,
length-preserving microbatch contract.** Evidence, all verified in code:

1. **The documented objection is obsolete.** `node.py:530-532` and `NODE_CONTRACT.md:18` say
   producers aren't generators because "generators can't be pickled." But nodes are shipped as
   `(class path, params)` and rebuilt in the worker via `get_params()`; a generator created
   *after* reconstruction never crosses a process boundary. The rationale is stale — yet the
   verdict doesn't change, because the real constraints are in the protocol:
2. **Full generator `process()` (N-in/M-out streams) breaks per-item identity.**
   - Outputs carry forward the input group's `trace_id`/`seq` (MSGID-2); N outputs from one
     input would derive the *same* `Nats-Msg-Id` and JetStream dedup collapses them
     ([nats_messenger.py:468-477](videoflow/messaging/nats_messenger.py#L468-L477)).
   - The ack model holds exactly one group: `receive_message` **overwrites**
     `_inflight_handles` ([nats_messenger.py:635](videoflow/messaging/nats_messenger.py#L635));
     buffering across receives orphans acks → `ack_wait` redelivery storms.
   - The stop branch breaks without any flush hook ([task.py:177-180](videoflow/core/task.py#L177-L180)) —
     buffered items would be lost at EOS.
   - Joins, idempotency keys, and DLQ semantics are all keyed per item.
3. **The efficiency goal is real and achievable wire-invisibly**: `BatchProcessorNode` with
   `process_batch(*per_parent_lists) -> list`, strict 1:1 — `output[i]` published with input
   group *i*'s own `trace_id`/`seq`/`event_ts`. Dedup, joins, idempotency, golden vectors all
   unchanged. Precedent: contrib `HumanEncoder` already GPU-batches *within* one message
   (crops of one frame); this generalizes it across messages. `ROADMAP.md:12` ("microbatches")
   already wants this.
4. **Producers**: a generator is pure ergonomics (optional `produce()` sugar driven by base
   `next()`), zero throughput delta — one envelope per item either way.
5. **CPU multiprocessing**: already served horizontally by `nb_tasks` (competing consumers on
   a shared durable); batching adds intra-process vectorization, not a worker-level pool.

Key operational finding to record (it shapes the design): consumer `max_ack_pending` is fixed
at **provision time** (`topology.provision_flow`, run by the init Job / local engine before any
worker), and nats-py `pull_subscribe` **binds to an existing durable ignoring passed config** —
so batch-aware ack windows must be provisioned per `NodeSpec`, not set by the worker. With
REALTIME's 1-message stream buffer (`DEFAULT_REALTIME_BUFFER`, `topology.py:86`), an unraised
`max_ack_pending < batch_size` would stall delivery and *evict* frames — correctness, not tuning.

## Changes

### 1. Create `spec/rfcs/0001-microbatch-processors.md` (status: **proposed**)

Follow `spec/rfcs/0000-template.md` headings exactly. Content:

- **Header**: no protocol version change; requirement IDs touched: `LOOP-3`, `LOOP-5`,
  `DELIV-1`, `EOS-5`; adds `LOOP-11`, `LOOP-12`, `LOOP-13`.
- **Summary / Motivation**: GPU inference amortization; the TODO question; HumanEncoder
  intra-message precedent; why now.
- **Proposal** (normative text to be applied to PROTOCOL.md §6 when accepted — quoted in the
  RFC, *not* applied now):
  - `LOOP-11` (accumulate): an SDK MAY buffer up to `batch_size` input groups or until
    `max_wait_s` after the first buffered group, then invoke `process_batch` once with
    per-parent parallel lists in ENV-3 order.
  - `LOOP-12` (pairwise identity — the LOOP-3 relaxation): exactly one output per input group;
    output *i* published with group *i*'s carried-forward identity; per-pair publish-before-ack
    (LOOP-5); length mismatch fails the whole batch.
  - `LOOP-13` (flush-before-EOS): on the all-parents-stopped result, flush the buffer before
    publishing own EOS.
  - `DELIV-1` amendment: an SDK MAY hold multiple groups' handles unresolved concurrently;
    DELIV-3 keepalive covers all held handles.
  - `EOS-5` note: buffered un-acked groups keep `num_ack_pending > 0`, so the drain algorithm
    already defers the stop result past the final flush — no drain change.
- **Design sketch (informative section)**: `BatchProcessorNode(batch_size, max_wait_s=0.1)`
  with `process()` singleton shim; Messenger extension (`receive_group(timeout)`,
  `publish_message_for(group, ...)`, `ack_group`, `fail_group`, `InputGroup` dataclass);
  `NodeSpec.batch_size`; provision-time `max_ack_pending_for(B) = max(base, 2*B + 2)` with the
  bind-ignores-config + REALTIME-eviction rationale; KEDA `lagThreshold ≥ batch_size` note;
  failure policy (batch raise → fail all groups; partial publish sweep → already-published
  outputs dedup on retry, same mechanism LOOP-5 relies on).
- **Alternatives rejected**: full generator stream-transformers (identity/join/ack analysis,
  with the pickle-rationale correction); messenger-side accumulation (batching is loop policy);
  `VF_BATCH_SIZE` env var (params already round-trip; provisioning reads NodeSpec). Producer
  `produce()` sugar noted as a severable, protocol-free companion.
- **Compatibility**: wire — none; `spec/vectors/` byte-identical stated as an explicit
  invariant; behavioral — opt-in per node, `batch_size == 1` reproduces today's provisioned
  values exactly.

### 2. Annotate `../videoflow-contrib/TODO.md` line 2

Mark the item resolved with the one-line verdict and a pointer to
`videoflow/spec/rfcs/0001-microbatch-processors.md`.

### Explicitly NOT in this change

- No edits to `spec/PROTOCOL.md`, `spec/vectors/`, `spec/conformance-map.md` — they change
  when the RFC is accepted alongside the implementation.
- No code changes anywhere (`node.py` docstring and `NODE_CONTRACT.md:18` stale-rationale
  fixes ride the implementation commit, where the new contract text is written anyway).

## Verification

- Diff `0001-microbatch-processors.md` section headings against `0000-template.md`.
- Spot-check every requirement ID cited (`LOOP-3/5`, `DELIV-1/3`, `EOS-3/5`, `MSGID-1/2`,
  `ENV-3`, `JOIN-6/14/20`) against `spec/PROTOCOL.md` so the RFC quotes them accurately.
- `uv run pytest --ignore=tests/integration` — must stay green (docs-only change; also proves
  `tests/test_shims.py` and golden vectors untouched).
- Commit message cites the touched requirement IDs per house rule.
