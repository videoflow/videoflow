# Exploration: Redis-backed keyed state / checkpointing of buffers

> **Deliverable decision (user, 2026-07-21): answer only — no files written to the repo.**
> This document is the exploration record; nothing below is to be implemented now.

## Context

`todos/07_gap_analysis.md` gap #5 records that videoflow has no managed state: state lives in
instance attributes (`OneTaskProcessorNode` forces `nb_tasks=1`; partitioned replicas hold
per-key state in memory), a replica restart loses it, and the item was deferred as "a future,
much larger RFC (watermarks/timers/checkpoint territory)". This exploration designs that RFC's
core: durable per-key state for processor/consumer nodes so a worker restart within a run
resumes instead of cold-starting. It was produced by a three-way codebase exploration (state
& buffers, Redis infrastructure, protocol/lifecycle) plus two independent design passes
(mechanism design + adversarial risk review) reconciled below.

## Finding that narrows the scope

**Join/window buffers held by the messenger need no checkpointing.** `messaging/grouping.py`
assemblers hold entries with **un-acked** broker handles; a crash leaves them pending on
JetStream and redelivery repopulates the buffers — the broker already *is* the checkpoint.
The genuinely unprotected asset is **per-key node state accumulated over already-acked
history** (trackers, aggregators, counters). That is the sole target. (P1's future
`WindowedProcessorNode` keeps its honest "crash loses the tail" node-local buffer; a durable
window becomes a later opt-in built on this feature's ListState successor.)

## Design summary

### API: `ctx.state` (ValueState only in v1)

```python
# inside process()/consume() of a node declared durable_state=True
n = ctx.state.get('count', default=0)     # cold-loads scope from Redis on first touch
ctx.state.put('count', n + 1)             # eager wire-encode; staged, not yet persisted
ctx.state.delete('old_slot')
```

- Scope is implicit: partitioned node → the current message's partition key; `nb_tasks=1`
  node or consumer → a singleton `_` scope. No cross-key reads (a replica only owns its keys).
- Values = anything the v4 payload codec accepts (ndarray, protobuf, JSON-like). Encoded via
  new thin public wrappers `encode_payload`/`decode_payload` over the existing private
  `_encode_payload_v4`/`_decode_payload_v4` in `videoflow/wire/serialization.py` — **no blob
  offload** (BlobRef's compile-time reader counts and ack-driven release don't fit state);
  warn >1MB, hard error at a cap. `put()` is the persistence point (`get` returns by
  reference; mutating without `put` persists nothing).
- `ctx.state` without `durable_state=True` or outside a group → `RuntimeError` naming the fix.

### Commit protocol (the heart of the design)

Per input group, extending the existing loop in `core/task.py` (`ProcessorTask._run`
lines 197–218, `ConsumerTask._run` 246–267), which mirrors the existing consumer-idempotency
pattern (seen → consume → mark → ack):

```
receive → [if redelivered (num_delivered>1) and marker exists: ack, skip]
        → session.begin(scope, gid)      # gid = messenger.last_input_key()  (already exists)
        → process/consume                # puts go to a per-group STAGING buffer
        → publish (unchanged, LOOP-5)
        → session.commit()               # atomic Redis MULTI: dirty slots + marker; no-op if clean
        → ack_inputs()
on exception: session.discard() → fail_inputs()   # staging dropped — retry can't double-apply
```

Crash matrix (BATCH): crash before commit → redeliver → reprocess against pre-group state →
republish (deduped by content-derived `Nats-Msg-Id`, MSGID-1/2) → commit once. Crash after
commit, before ack (incl. silently-swallowed ack failures) → redeliver → **marker hit → skip
process and publish** (publish already broker-acked before commit) → ack. Delta applied
exactly once relative to acked history.

Key reconciled decisions and why:

1. **Per-group marker keys, not a per-scope marker field.** Swallowed ack failures redeliver
   ~`ack_wait` (60s) later; a busy scope commits ~1800 more groups meanwhile, overwriting any
   single marker → double-apply. Marker = `vf-state-app:{<state hash key>}:<gid>` (braces =
   Redis Cluster hash tag, so marker + state hash share a slot and one MULTI covers both).
   Written only on dirty commits; TTL `max(600, 10 × ack_wait)`; consulted only when
   `num_delivered > 1` → **zero happy-path reads**. `gid` reuses `last_input_key()`
   (`nats_messenger.py:518` — `derive_message_id` over trace_id+seq), the same identity the
   sink-idempotency store already trusts.
2. **Staging buffer, not naive write-through cache.** `fail_inputs` retry is a *routine* path
   (task.py:216–218); a cache mutated before a publish failure would double-apply without any
   crash. Writes stage per group; fold into the read cache only on successful commit.
3. **Per-group commit, not interval snapshots.** Exactly-once snapshots need ack deferral;
   `max_ack_pending = 6` caps that at 6 messages — epochs are structurally unavailable
   (same reason the gap analysis rejected messenger-held windows). One pipelined RTT per
   *dirty* group on colocated Redis is noise against ML proctime.
4. **Marker consultation is BATCH-only.** REALTIME has `max_deliver=1` (no redelivery — the
   marker's trigger can't fire), and a restarted REALTIME producer re-mints trace ids from 1
   (MSGID-3), so a long-lived marker would silently skip *new* frames. REALTIME still
   commits (restart recovery); a crash between publish and commit loses that one delta —
   consistent with freshest-wins.
5. **Fencing epoch in v1** (risk reviewer's finding, adopted): Indexed-Job replacement pods
   overlap terminating ones (~30s dual-writer per index on BATCH node loss), and StatefulSet
   at-most-one isn't absolute. Per-(node, replica) epoch key: `INCR` at task open; every
   commit is `WATCH epoch / GET / compare / MULTI…EXEC` (no Lua, honoring the RFC-0002
   no-EVAL precedent; +1 cheap RTT on dirty commits only). Fenced writer logs and exits; its
   un-acked inputs redeliver to the survivor, whose marker check absorbs already-published
   groups. Combined with workload hardening (below) so fencing events stay rare.
6. **Commit failure ≠ fail_inputs on BATCH.** Naks cap at 30s → a 2-minute Redis blip would
   DLQ groups flow-wide. Instead: blocking retry with backoff until the termination event
   (the DELIV-5 publish-backpressure precedent; the keepalive loop already extends held acks
   on its own thread, so a blocked task thread never triggers spurious redelivery). REALTIME:
   log-and-ack (state lags one message; freshest-wins-consistent).

### Redis data model

```
vf-state:<flow_id>:<run_id>:<node>:<scope>          # ONE hash per scope; field s:<slot> = framed payload
vf-state-app:{vf-state:<flow>:<run>:<node>:<scope>}:<gid> = 1, EX ~600   # applied marker
vf-state-epoch:<flow>:<run>:<node>:<replica>        # fencing epoch (INCR at open, WATCHed at commit)
```

Framing per slot: `0x01 | u16 len | payload_type utf-8 | payload bytes` (language-neutral for
future SDKs). Scope TTL `VF_STATE_TTL_SECONDS` (default 86400), refreshed on commit and cold
read — only idle scopes expire. Missing/expired/evicted/undecodable state reads as defaults
with a loud log — **never a crash** (run-scoped; new `run_id` = deliberate cold start).
Cleanup: TTL backstop (BLOB-7 philosophy) + best-effort `SCAN`+`UNLINK` of the run prefix at
teardown. **No scheme registry in v1** — direct `RedisKeyedStateStore(url)` behind a small ABC
(the `IdempotencyStore` precedent; `Redis.from_url` already handles redis/rediss/unix);
the blob-store-style registry is the obvious seam when a second backend appears.

### Who may use it — graph validation (`core/graph.py`, beside the existing join/partition rule)

| Config | Verdict |
|---|---|
| Processor/consumer `nb_tasks=1` (incl. `OneTaskProcessorNode`) | allowed — singleton scope |
| Processor `partition_by=<metadata field>`, `nb_tasks>1` | allowed — per-key scope; static `hash % nb_tasks` ownership (PART-3) |
| `partition_by='trace_id'` + `nb_tasks>1` | **rejected** — trace_id unique per message: one dead hash per message, no continuity |
| Non-partitioned `nb_tasks>1` | **rejected** — competing consumers, no stable scope |
| Multi-parent **time-mode** join | **rejected in v1** — group identity (`tw-<µs>`) shifts when membership re-forms after a crash; marker undefined. Single-parent nodes *downstream* of a time join are fine (their gid is the fused message's own stable id) |
| Producers | **not offered** — a durable resume cursor without a durable trace counter collides restarted `Nats-Msg-Id`s into silent dedup-loss (MSGID-3); needs its own RFC |

Deploy/run-time: `durable_state` anywhere + no Redis URL ⇒ fail before start (CLI) and at
worker startup — never a silent in-memory fallback.

### Deployment consequences

- REALTIME `durable_state` nodes become **StatefulSets even at `nb_tasks=1`** (Deployments
  surge/overlap on rolling updates and eviction — routine dual-writers). `workload()` gate in
  `deploy/manifests.py` becomes `_is_partitioned(spec) or (spec.durable_state and not batch)`;
  headless service + `POD_NAME` injection follow.
- **KEDA never autoscales a `durable_state` node** (`scaled_object` exclusion alongside the
  existing partitioned exclusion).
- Env plumbing: `VF_DURABLE_STATE` per node; `VF_STATE_REDIS_URL` (fallback
  `VF_BLOB_REDIS_URL` — reaches every pod via the existing nats ConfigMap already) and
  `VF_STATE_TTL_SECONDS` in the ConfigMap; CLI flags `--state-redis-url`/`--state-ttl-seconds`.
- Honesty: dev Redis is `--save "" --appendonly no --maxmemory 4gb --maxmemory-policy
  volatile-lru` — state survives **pod** restarts (the target case), not Redis restarts, and
  TTL'd keys are LRU-evictable under blob memory pressure (logical DBs don't isolate
  maxmemory). Production posture: dedicated persistent (AOF) `noeviction` Redis via
  `VF_STATE_REDIS_URL` — deliberately different tuning from the blob cache.

## Modifications inventory

| File | Change | ~LOC |
|---|---|---|
| `videoflow/runtime/state.py` **(new)** | `KeyedStateStore` ABC, `RedisKeyedStateStore` (lazy `import redis`), `StateSession` (staging, cache, begin/commit/discard, marker check, epoch fence), `scope_for`, key builders | ~330 |
| `videoflow/wire/serialization.py` | public `encode_payload`/`decode_payload` wrappers | ~15 |
| `videoflow/core/context.py` | `state` property (TYPE_CHECKING import per task.py precedent); actionable `RuntimeError` when absent | ~15 |
| `videoflow/core/task.py` | optional `state_session=` ctor arg (idempotency-store precedent); marker-skip + begin/commit/discard around the two run loops; consumer idem ordering (marker check → consume → state commit → idem mark → ack) | ~50 |
| `videoflow/core/node.py` | `durable_state : bool = False` param + property on `ProcessorNode`/`ConsumerNode`, stored as `self._durable_state` (get_params auto-capture) | ~15 |
| `videoflow/core/graph.py` | the rejection rules above, `ValueError`s naming the fix | ~25 |
| `videoflow/core/compiler.py` | `NodeSpec.durable_state : bool = False` **appended last** (field order is the ctor signature); `to_dict`/`from_dict` | ~10 |
| `videoflow/core/engine.py` + `messaging/nats_messenger.py` + `runtime/health.py` | `Messenger.last_input_redelivered()` (max `num_delivered` over the group's handles, recorded in `receive_message`); `InstrumentedMessenger` delegation | ~25 |
| `videoflow/runtime/worker.py` | env parse, fail-fast, store/session construction, epoch mint, pass to task + ctx | ~35 |
| `videoflow/engines/local.py` | `VF_DURABLE_STATE`/`VF_STATE_REDIS_URL` pass-through | ~10 |
| `videoflow/deploy/manifests.py` | env pairs, ConfigMap entries, StatefulSet gate, KEDA exclusion | ~40 |
| `videoflow/deploy/cli.py` | flags + deploy-time "durable_state needs Redis" check | ~30 |
| `spec/PROTOCOL.md` | new §15 Keyed state: **STATE-1..8** (enablement, scope, layout, commit order incl. commit-only-after-PubAck as a MUST, marker, guarantee levels, TTL/missing-reads-as-default, single-writer + fencing) | ~90 lines |
| `spec/rfcs/000N-keyed-state.md` | next free number; template sections; Alternatives records: interval snapshots (max_ack_pending), single/ring markers, fingerprint gids, NATS KV backend, Lua commits; Open questions: always-consult markers vs redelivered-only, fencing ping-pong under kubelet partition, retry-forever vs eventual-DLQ on Redis outage | ~180 lines |
| Docs same-commit | `.claude/docs/NODE_CONTRACT.md` + `ARCHITECTURE.md` seam table, `docs/source/`, `README.md`, contrib `CLAUDE.md` | ~80 lines |

**Total ≈ 600 LOC code + ~500 LOC tests** (`tests/test_keyed_state.py` with a `_FakeRedis`
dict fake per the blob-refcount test pattern; graph rejection cases; NodeSpec round-trip +
**byte-identity of specs/manifests for flows without the flag** as the acceptance test;
integration: kill-and-restart a stateful replica mid-BATCH and assert exact counts, forced
redelivery hits the marker, 3-replica per-key isolation, REALTIME loss-tolerance smoke).

## Sequencing & out of scope

Lands independently of the gap-analysis roadmap but best **after Phase 2 (P3 → P1)**; the one
forward-compatibility ask on P3 is to route ack/fail through a single task-level choke point
(commit/abort hook beside `ack_inputs`/`fail_inputs`) that both this feature and P2's
`on_control` will need, and to keep every `ctx.emit` PubAck synchronous so commit-after-publish
stays sound.

Deferred, one line each: **timers/watermarks** (need a progress notion + tick service the
envelope lacks); **rescaling partitioned nodes** (static `hash % nb_tasks`; rescale = key
migration protocol); **time-join scaling past nb_tasks=1** (broker-topology problem, untouched);
**ListState/durable windows** (single prospective caller; P1 stays memory-first);
**cross-run resume** (fresh trace ids vs stale markers = institutionalized skip-loss);
**producer state** (MSGID-3 amendment required); **IDEM unification** (markers subsume it
conceptually; freeze and note convergence in the RFC).

## Verification (when implemented)

- Unit: commit atomicity ordering, marker-on-dirty-only, staging discard on fail, `scope_for`
  PART-2 stringification, framing round-trip, TTL refresh, fence rejection.
- Byte-identity: rendered manifests + `NodeSpec.to_dict` unchanged for existing flows.
- Integration (live NATS + Redis): kill a stateful replica mid-BATCH → exact counter on
  completion; forced redelivery → marker skip; REALTIME crash → bounded single-delta loss.
- Contrib proving ground: offside `offside_engine` committed-timestamp dedup state moves to
  `ctx.state`; restart the engine pod mid-game and verify no duplicate/lost events.
