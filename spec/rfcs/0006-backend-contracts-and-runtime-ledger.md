# RFC 0006: Backend contracts and the runtime ledger

- **Status:** proposed
- **Author(s):** videoflow maintainers
- **Created:** 2026-09-11
- **Protocol version affected:** 1 (no version change — the envelope bytes and every
  `.proto` are untouched; the observable changes are to identities minted into
  existing fields, to stream/consumer configuration, to dead-letter and replay
  routing, to the blob-store key contract and to the environment contract)
- **Requirement IDs touched:** new `EOS-7`, `MSGID-5`, `MSGID-6`, `JOIN-23`,
  `STREAM-14`, `STREAM-15`, `DELIV-15`, `DELIV-16`, `BLOB-13`, `BLOB-14`, `BLOB-15`,
  `ENV-10`…`ENV-18`, `CTRL-4`; amended `EOS-2`, `EOS-3`, `MSGID-3`, `JOIN-20`,
  `STREAM-4`, `STREAM-5`, `STREAM-7`, `DELIV-8`, `DELIV-9`, `DELIV-10`, `BLOB-3`,
  `BLOB-5`, `BLOB-6`, `CTRL-1a`, the §1.1 environment table and the §0.2
  source-of-truth map (which gains `videoflow/backends/`)

**Numbering.** The new IDs are the ones pre-allocated by the backend-conformance plan
so that the conformance catalogue, the code comments and this document agree. The
numbers they skip (`MSGID-4`, `JOIN-22`, `STREAM-9`…`STREAM-13`, `DELIV-11`…`DELIV-14`,
`BLOB-8`…`BLOB-12`, `ENV-6`…`ENV-9`) are left unassigned by this RFC; they are not
tombstones and carry no meaning. (`STREAM-9` is already cited by `STREAM-8` without
being defined — see Open questions.)

## Summary

Videoflow's transport, payload store, accelerator allocator and runtime are being
given explicit contracts (`videoflow/backends/`), reference in-memory implementations,
and a 130-case conformance suite (`tests/conformance/`). Most of that work changes
nothing a peer can observe. This RFC records the part that does. Every item below is
gated behind one switch, `VF_RFC0006` (default off), so the default path stays
byte-identical until the RFC is accepted:

1. **Terminators carry the per-replica final count** (`EOS-7`): an EOS or ABORT's
   `seq` is the number of DATA messages that replica published, and a receiver
   records every replica's terminator instead of collapsing on the first one, so a
   competing child can complete a `(node, partition, epoch)` barrier on evidence
   rather than on a quiescence timer.
2. **Source identities that survive a restart** (`MSGID-5`, `MSGID-6`): a live source
   mints `{node}:{epoch}:{n}`, a replayable source mints `{node}:{offset}` and
   checkpoints the last accepted offset.
3. **Member-hashed time-group identity** (`JOIN-23`): `tw-{µs}-{hash}` so two groups
   whose event times round to the same microsecond cannot collide.
4. **Exact ownership on the broker** (`STREAM-14`) and **ledger-budgeted delivery**
   (`STREAM-15`): owner labels in JetStream stream/consumer `metadata`, teardown by
   exact ownership (never by prefix), `max_deliver = -1` only where a durable shared
   ledger counts attempts, and `max_ack_pending` derived from admitted concurrency.
5. **A recorded terminal disposition for undecodable bytes** (`DELIV-15`) and
   **scoped replay** (`DELIV-16`).
6. **Blob obligations instead of a counter** (`BLOB-13`…`BLOB-15`): named,
   generation-fenced, idempotent claims by logical readers, on companion keys
   hash-tagged onto the unchanged `vf-blob-<hex>` key.
7. **Nine optional environment variables** (`ENV-10`…`ENV-18`), absent ⇒ today's
   behaviour.
8. **Durable control state** (`CTRL-4`) in a `RuntimeStore` with compare-and-swap,
   and which stores qualify a flow as `restart_safe`.
9. **Run-scoped Kubernetes names** (opt-in `--run-scoped-names` — not yet
   implemented, plan Phase 6 — the default at acceptance) and compile-time
   rejection of identity collisions (implemented), with no renaming of today's
   names.

## Motivation

Each item answers a defect the conformance catalogue reproduced against the current
code. The failures share a shape: an unobservable state was reported as a confident
one, or an identity that had to be unique was not.

- **An undecodable message vanishes.** `_pull_loop` terminates a message that fails
  to decode with no record anywhere (`DELIV-9`). Under `reliable_work` that is silent
  data loss with a log line as its only trace (MSG-010).
- **A dead-letter outage strands work.** When the DLQ publish fails the message is
  NAKed with a 5 s delay; with a broker cap of `max_retries + 1` that NAK can exceed
  the cap, after which the broker retains a message it will never deliver again and
  every counter reads zero (MSG-008, MSG-009, MSG-012).
- **Teardown deletes the wrong run.** `delete_run_streams` matches
  `startswith('vf-{flow}-{run}-')`, so tearing down run `r` deletes run `r-x`; and a
  listing failure returns silently as success (MSG-020). Names are lossy in the
  other direction too: `a.b` and `a_b` sanitize to one stream, `Node` and `node` to
  one Deployment (MSG-019).
- **A restarted producer's first frames are dropped.** A producer mints
  `{node}:{n}` from an in-memory counter; after a crash the replacement mints
  `{node}:1` again and JetStream deduplicates the genuinely new frames inside the
  120 s window (RUN-014).
- **Two groups, one id.** A time join mints `tw-{µs}` from the group's rounded time
  alone, so distinct member sets with equal rounded times produce one message id and
  the second is deduplicated away (RUN-016).
- **A blob counter cannot tell readers apart.** The RFC 0002 counter is decremented
  per delivery, not per logical reader: a redelivery to the same reader decrements
  twice, a counter that expired between `EXISTS` and `DECR` deletes a blob another
  reader still needs, and a crash after ack but before release leaks until TTL
  (PAY-004, PAY-005, PAY-008).
- **One credit number for every stage.** `max_ack_pending` is a constant (8 at
  provisioning, `queue_maxsize + 2` = 6 at bind), so a ten-replica stage can lease
  at most eight messages and two replicas idle by construction (MSG-017, RUN-024).
- **Two runs of one flow overwrite each other.** Every run renders the same
  Kubernetes names, so a second concurrent run applies over the first's ConfigMaps
  and pod templates and hits an immutable-Job error on the provision Job (RUN-047).
- **Completion on a quiescence timer.** A child declares a parent drained when the
  broker reports zero pending twice, 500 ms apart (`EOS-3`). A replica that is
  merely slow, or a broker query that failed and was read as `(0, 0)`, both satisfy
  that (RUN-007, RUN-009).

## Why an RFC

None of this changes envelope bytes, so `buf breaking` is not involved and no
envelope version bump is needed. It is nevertheless observable across SDK
boundaries, in five places:

- **Identities minted into existing fields.** `trace_id` and `seq` are wire fields
  every SDK derives dedup ids from (`MSGID-1`) and partition ownership from
  (`PART-3`). `MSGID-5`, `MSGID-6`, `JOIN-23` and `EOS-7` change what the reference
  puts there; a non-Python producer or join must mint the same strings or two SDKs
  in one flow will disagree about what is a duplicate.
- **Stream and consumer configuration** is a normative contract (§3). Metadata,
  `max_deliver` and `max_ack_pending` are read back by every provisioner and bound
  by every worker.
- **Dead-letter and replay routing.** What lands in the DLQ, when a message is
  terminated, and which node processes a replayed message are routing behaviour.
- **The blob-store key contract** is multi-language (`BLOB-4`): a non-Python
  publisher must write the obligation keys for a Python reader's reclamation to
  engage, exactly as RFC 0002 argued for the counter.
- **The environment contract** (§1) gains rows, and Kubernetes resource names —
  which operators script against — change under `--run-scoped-names`.

## Terms

The vocabulary below is the code's (`videoflow/backends/`); this RFC uses it
unchanged so the requirement text, the contracts and the tests name the same things.

| Term | Module | Meaning |
|---|---|---|
| `Observation[T]` = `Known[T] \| Unknown` | `outcomes.py` | A read either carries a value (with `observed_at` and the provider's `generation`) or says why it could not be made (`reason` ∈ `timeout`, `auth`, `malformed`, `unreachable`, `unsupported`). Nothing coerces `Unknown` to zero, empty or complete. |
| `PublicationOutcome` = `Accepted \| Rejected \| PublicationUnknown \| PublicationUnresolvable` | `outcomes.py` | What happened to a publish. `Accepted.duplicate` marks a deduplicated retry; `Unknown` means the receipt was lost; `Unresolvable` means the backend keeps no ledger to ask. |
| `SettlementOutcome` = `SettleConfirmed \| SettleUnknown \| SettleStale` | `outcomes.py` | What happened to an ack/nak/term. `Stale` is refused before the broker: a newer attempt owns the message. |
| `Settlement` = `Completed \| Retry(delay_seconds) \| Terminal(record_ref)` | `messaging.py` | What the runtime asks for. `Terminal` requires a durable record reference. |
| `MessagingBackend`, `ChannelSpec`, `SubscriptionSpec`, `Envelope`, `DeliveryToken` | `messaging.py` | The transport contract. A `SubscriptionId` is a *logical* consumer; `DeliveryToken` names run, subscription, message, `attempt` and ownership `generation`. `Envelope.publication_id` is the dedup key (the `MSGID-1` id); `event_id` the logical event. |
| `PayloadStore`, `ImmutablePayloadRef`, `RetentionContract`, `ReadOutcome` | `payload.py` | The blob contract. A ref carries `key` (the wire `BlobRef.ref`), `size`, `digest`, `generation`, `content_id`. An *obligation* is a named claim by a logical reader. |
| `RuntimeStore` | `runtime.py` | Versioned KV with `get/cas/append/scan/delete`; versions are opaque strings the store mints. Implementations: `MemoryRuntimeStore`, `FileRuntimeStore` (`backends/memory/runtime_store.py`), `RedisRuntimeStore` (plan step 3). |
| `FlowRuntime` | `runtime.py` (plan step 3) | The ledger built on a `RuntimeStore`: open groups, ownership epochs, completion barriers, the publication outbox, source epochs, checkpoints. |
| `MessagingCapabilities`, `PayloadCapabilities`, `RuntimeCapabilities`, `plan_composition` | `capabilities.py` | What an adapter *as configured* guarantees, and the planner that admits a `FlowRequirements` or raises `IncompatibleProfile` / `UnobservableState` — never downgrades. Profiles: `live_latest`, `reliable_work`, `durable_control`, `replay_archive`. |
| `owner_labels`, `owns`, `derived_names`, `collisions` | `identity.py` | Exact-ownership metadata and the enumeration of every physical name a compiled flow uses. |
| `FaultSchedule`, `BARRIERS` | `faults.py` | Named injection points and the env transport that carries a schedule into a worker subprocess. |

## Proposal

### 1. The `VF_RFC0006` switch

`VF_RFC0006=1` is read once, in `videoflow/core/constants.py`, as `RFC0006`. It is
**off by default**. While off, every rendered manifest, compiled spec, broker name,
stream/consumer configuration and wire identity is byte-identical to the pre-RFC
build; `tests/test_render_goldens.py` and `tests/test_golden_vectors.py` are the
check. With it on, the conformance suite exercises the semantics below, and the
same goldens are recorded separately under `tests/golden/rfc0006/`.

The switch gates exactly:

| Gated | Sections |
|---|---|
| Identity minting on the wire: terminator `seq`, source-epoch and replayable trace ids, member-hashed group ids | §2, §3, §4 |
| Provisioning: owner metadata, `max_deliver = -1` under the ledger condition, credit-derived `max_ack_pending`, read-back verification | §5 |
| Delivery: dead-letter-before-TERM for undecodable bytes, `VF-Replay-Target` | §6 |
| The blob store: the `redis` scheme resolves to the obligation store (`RedisPayloadStore`); off, it remains `RedisBlobStore` (RFC 0002) | §7 |
| The observable half of the runtime ledger: durable terminators, received sets and the completion barrier, the outbox as a source of truth | §9 |
| Default *emission* of the new environment rows by the control planes (§8); *reading* them is never gated — a row that is absent means today's behaviour whether or not the switch is on | §8 |

Not gated: `--run-scoped-names` (an explicit flag whose default flips at
acceptance, §10; not yet implemented — plan Phase 6), compile-time
identity-collision rejection (§10), and everything in
[Shipped behaviourally](#shipped-behaviourally-not-rfc-worthy).

**Acceptance plan (plan Phase 6).** Preconditions: the conformance run at every
reachable level (memory, compose broker, k3s, GPU) reports no `FAIL`, and the broker
and local integration suites are green with the switch on. Then, in one change set:
flip the default on; delete the off-path (no permanent dual path — decision D1);
regenerate `tests/golden/` and `spec/vectors/` (the EOS manifest description, new
`message_id` entries for the three trace-id forms, a new `join/group_identity.json`);
land the amended requirement text and the new IDs in `spec/PROTOCOL.md`; update
`spec/conformance-map.md`; set this RFC to `accepted`. After that the variable is no
longer read; a value still set is ignored per `ENV-4`.

### 2. Terminators carry the per-replica final count — `EOS-7`

Today a terminator's `seq` is unspecified: the reference publishes `_last_seq`, which
is the `seq` of the last input group a processor handled, and `0` for a producer.
A receiver acks and ignores every terminator after the first from a parent
(`EOS-2`), and declares the parent drained on a quiescence timer (`EOS-3`).

- **`EOS-7`** (terminator `seq`): the `seq` of a terminator (`MSG_TYPE_EOS` or
  `MSG_TYPE_ABORT`) published by replica `r` of node `N` MUST equal the number of
  **distinct** DATA `Nats-Msg-Id`s that replica published on `N`'s data subject
  during the run — a publication counts once whether the broker stored it or
  deduplicated it, and never counts if it was definitely `Rejected`. For an ABORT
  it is the count at the moment of death. The count lives in the runtime ledger's
  outbox when the store is durable (§9) so a restarted replica continues it; with a
  memory store it restarts with the process, and the barrier below is then not
  evaluated. Its `trace_id` and `replica_id` are unchanged (`WIRE-6`, `ABORT-1`).

  A receiver MUST record every terminator as a fact keyed by
  `(parent, replica_id, kind)` — kind, `seq`, and the `Error` of an ABORT —
  **before** acking it, and MUST NOT collapse the parent's terminators onto the
  first one seen. A duplicate of an already-recorded `(parent, replica_id, kind)`
  is acked. An ABORT still outranks a clean EOS from the same parent
  (`ABORT-3`). Under BATCH the recorded terminators are held un-acked until the
  parent is drained (`EOS-4` now applies to each replica's marker).

  **Completion barrier.** When the node's runtime store is durable and shared
  across processes (§9), a BATCH child `C` MUST declare parent `P` complete only
  when all hold: (a) a terminator is recorded for every replica
  `0 .. VF_PARENT_REPLICAS[P] − 1` (`ENV-11`); (b) the number of distinct DATA ids
  delivered to `C` from `P` equals the sum of the recorded terminators' `seq` — for
  a competing child (shared durable) the union over `C`'s replicas' received sets,
  for a partitioned child (per-replica durables) this replica's own set; (c) no
  pending join group holds a half from `P` (`EOS-3c`); (d) the broker observation
  of the durable is `Known` with `num_pending == 0` and `num_ack_pending == 0` — an
  `Unknown` observation never satisfies the barrier (RUN-009). The received sets
  and terminator records are kept in the store (`CTRL-4`) so competing replicas
  aggregate. Completion is committed by compare-and-swap under `C`'s current
  ownership epoch; a commit under a superseded epoch is refused
  (`StaleAuthority`), and because records are idempotent by
  `(parent, replica_id, kind)` a duplicate terminator can never count twice
  (RUN-008).

  Without a durable shared store, and for REALTIME in every case (a LIMITS stream
  evicts, so counts cannot reconcile), `EOS-3` applies unchanged.

- **`EOS-2`** amended. Before: "a second terminator from a parent … is simply acked
  and ignored — except that an ABORT MUST still be recorded". After: "every
  terminator is recorded per `(parent, replica_id, kind)` before it is acked
  (`EOS-7`); a duplicate of a recorded one is acked; an ABORT MUST still be recorded
  and outranks a clean EOS".
- **`EOS-3`** amended: its drain condition becomes the rule for REALTIME and for
  nodes without a durable shared store; the barrier in `EOS-7` is the rule
  otherwise. Clause (d) additionally requires the observation to be `Known`.

*Example.* `cam` has `nb_tasks = 2`. Replica 0 published 40 DATA messages, replica 1
published 37. Their terminators are `{trace_id: "eos-r0", seq: 40, replica_id: 0}`
and `{trace_id: "eos-r1", seq: 37, replica_id: 1}`. The child `det` (three competing
replicas, `VF_PARENT_NAMES=cam`, `VF_PARENT_REPLICAS=2`) completes `cam` once its
replicas have together received 77 distinct ids, both terminators are recorded, and
the durable reads `Known((0, 0))`. Today `det` would have completed on the first
terminator plus 500 ms of quiet — possibly before replica 1's last frame arrived.

The golden vector `spec/vectors/envelope/eos.bin` (`seq: 9`) stays byte-identical;
its manifest description becomes "replica 0 published 9 DATA messages".

### 3. Source identity — `MSGID-5`, `MSGID-6`

- **`MSGID-3`** amended. Before: "a producer (no parents) mints `trace_id =
  "{node_name}:{n}"` and `seq = n` for a monotonically increasing local counter".
  After: "a producer mints per `MSGID-5` (live source) or `MSGID-6` (replayable
  source); this is the one place ids originate". The bare `{node}:{n}` form is no
  longer emitted once this RFC is accepted.
- **`MSGID-5`** (live sources): a producer that is not replayable MUST mint
  `trace_id = f'{node}:{epoch}:{n}'` and `seq = n`, where `n` is a local counter
  starting at 1 **per epoch** and `epoch` is minted once per producer replica
  process at `open()` (reference: `FlowRuntime.source_epoch()`;
  `videoflow.backends.runtime.source_epoch_trace_id`). An epoch is an opaque
  string over `[A-Za-z0-9_-]` (no `:`), at most 32 characters, unique across the
  restarts of that replica within the run. Reference form: 12 lowercase hex
  characters of a random UUID (`uuid4().hex[:12]`). Retries **within** one process
  reuse the same `n` and therefore the same id (`MSGID-2` holds); a replacement
  process never does, so nothing it publishes is mistaken for a pre-restart
  duplicate (RUN-014). A consumer MUST treat the string as opaque.
- **`MSGID-6`** (replayable sources): a producer that declares itself replayable
  (`ProducerNode.replayable = True`, an optional attribute this RFC adds to the node
  contract, off by default) MUST mint `trace_id = f'{node}:{offset}'` and
  `seq = offset`, where `offset` is the source's own stable position (a frame
  index, a record offset; a non-negative integer), so a re-run or a restart re-mints
  the identical id and downstream dedup and sink idempotency (`IDEM-1`) engage
  (RUN-015). A deliberately new analysis of the same source declares an
  `analysis_version` and mints `f'{node}:{analysis_version}:{offset}'`
  (`replayable_trace_id`). The runtime MUST checkpoint the last **accepted** offset
  — an `Accepted` publication outcome, not a sent one — in the runtime store
  (`CTRL-4`, `FlowRuntime.checkpoint`), MUST reconcile every `PublicationUnknown`
  intent before advancing it, and on restart MUST resume from `last_accepted + 1`.
  A node is either live or replayable, never both, so the shape overlap between
  `{node}:{analysis_version}:{offset}` and `{node}:{epoch}:{n}` cannot collide
  within one node; across nodes `producer_name` is part of the dedup id
  (`MSGID-1`).

*Examples.* `source_epoch_trace_id('cam', '3f9c1a2b7d4e', 1)` → `cam:3f9c1a2b7d4e:1`,
then `cam:3f9c1a2b7d4e:2`, …; after a crash the replacement mints
`cam:a01b7c2d9e3f:1`. `replayable_trace_id('file', 42)` → `file:42`;
`replayable_trace_id('file', 42, 'v2')` → `file:v2:42`. `partition_by = 'trace_id'`
hashes the new strings (`PART-3`); the distribution changes but is consistent within
a run, which is all `PART-3` promises.

### 4. Member-hashed time-group identity — `JOIN-23`

- **`JOIN-20`** amended. `seq = round(group.ts * 1e6)` (round-half-to-even over
  IEEE-754 doubles) and `event_ts = group.ts` are unchanged. Before:
  `trace_id = "tw-{seq}"`. After: `trace_id` is minted per `JOIN-23`.
- **`JOIN-23`** (member hash): an emitted time group's `trace_id` MUST be
  `f'tw-{seq}-{digest}'` where `digest` is the first 12 lowercase hex characters of
  `SHA-256(f'{window_id or ""}|{canonical}')` over UTF-8, and
  `canonical = '|'.join(f'{parent}={producer}:{trace_id}:{seq}' for parent in sorted(members))`
  — one entry per **sync** parent present in the group, in lexicographic parent
  order, each carrying that member's `producer_name`, `trace_id` and `seq`. Collect
  parents (`JOIN-14`) are attachments and do not enter the identity; a parent absent
  from a quorum emission (`JOIN-17`) is omitted. In protocol v1 `window_id` is `None`
  (the hash input starts with `|`); see Open questions. Reference:
  `videoflow.backends.runtime.group_identity(members, window_id, rounded_micros)`.
  The `tw-{seq}` prefix keeps ids sortable by time; distinctness comes from the
  digest. The same members regroup to the same id across redelivery and replay, so
  `MSGID-2` holds. Like `JOIN-20`, this is byte-identity critical and MUST be
  conformance-tested (RUN-016).

*Example.* Members `{'cam': ('cam', 'cam:3f9c1a2b7d4e:5', 5), 'imu': ('imu', 'imu:77e1b2c3d4f5:11', 11)}`
with `group.ts = 1700000000.5`: `seq = 1700000000500000`, hash input
`|cam=cam:cam:3f9c1a2b7d4e:5:5|imu=imu:imu:77e1b2c3d4f5:11:11`, id
`tw-1700000000500000-2604c79085f2`. A second group at the same rounded time whose
`cam` member is `cam:3f9c1a2b7d4e:6` (seq 6) mints
`tw-1700000000500000-c5024ad18fdc`; under `JOIN-20` both were
`tw-1700000000500000` and the second was deduplicated away.

### 5. Streams and consumers — `STREAM-14`, `STREAM-15`

- **`STREAM-14`** (owner metadata; exact-ownership teardown): every stream and
  durable consumer a run provisions MUST carry JetStream `metadata` equal to
  `owner_labels(flow_id, run_id, node, kind, generation)`
  (`videoflow.backends.identity`):

  | Key | Value |
  |---|---|
  | `videoflow.io/flow-id` | the flow id, **unsanitized** — the exact logical identity, which is what makes `a.b` and `a_b` distinguishable |
  | `videoflow.io/run-id` | the run id, unsanitized; absent on the flow-scoped DLQ stream |
  | `videoflow.io/node` | the node whose stream/consumer this is; absent on the DLQ stream |
  | `videoflow.io/kind` | `stream`, `durable`, `partitioned_durable`, `eos_durable`, `eos_anchor` (as written by `topology.stream_config_for` / `consumer_config_for` / `eos_consumer_config` / `eos_anchor_config`), or `dlq` for the flow-scoped dead-letter stream (`flow_labels(flow_id, 'dlq')`: flow id and kind only) |
  | `videoflow.io/generation` | an opaque provisioning-operation id (reference: a UUID4 hex minted once per provisioning run, the `operation_id` passed to `ensure_channel`/`ensure_subscription`) |

  Teardown (`delete_run_streams`, `videoflow teardown`, the engines' `finally`)
  decides ownership per stream, exactly and never by prefix
  (`topology._owned_stream`): a stream carrying owner labels (`has_owner_labels`)
  is this run's iff `owns(metadata, flow_id, run_id, generation)` — flow and run
  labels present and equal to the **raw** ids, and equal to the provisioning
  `generation` too when the caller supplies one, so a teardown can be narrowed to
  what one provisioning created. A stream with **no** labels (provisioned before
  this RFC) is attributed by its bound subjects (`subject_owner`): a data or EOS
  subject `vf.{flow}.{run}.{node}[._eos]` is dot-delimited and `NAME-1` never lets
  a dot through, so its flow and run tokens are compared verbatim against the
  sanitized ids — `vf.f.r.x-n` and `vf.f.r-x.n` differ although both hyphen-join to
  the stream name `vf-f-r-x-n`. An unlabelled stream with no attributable subject
  is left standing. A caller MAY narrow deletion to its own nodes (`node_names`).
  `stream_label_selector` is diagnostic only and MUST NOT select anything for
  deletion. The listing is paginated to the server's `total`; a listing failure or
  a short page MUST be reported as `CleanupObservation(complete = False)` with the
  reason — a failed inventory deletes nothing and is not "nothing to delete" — and
  an owned stream whose delete failed is listed in `remaining`. The CLI MUST print
  `remaining` and exit `3` (`EXIT_ENVIRONMENT`) when cleanup is incomplete. The DLQ
  stream carries flow-level labels only and its subject is not a data subject, so
  neither rule can ever match it (`STREAM-8`).

  **Read-back** (amends `STREAM-7`): "creating an existing stream/consumer is a
  no-op" becomes "every stream and consumer MUST be read back after it is created
  or found existing, and the fields the request set explicitly MUST be compared
  with the effective configuration" (`_STREAM_FIELDS`: name, subjects, retention,
  discard, `max_msgs`, `max_bytes`, `max_age`, `duplicate_window`, storage,
  `num_replicas`, metadata; `_CONSUMER_FIELDS`: durable name, filter subject,
  deliver/ack/replay policy, `ack_wait`, `max_deliver`, `max_ack_pending`,
  `inactive_threshold`, metadata). Metadata is compared as a subset — the server
  adds its own `_nats.*` keys, which a reader MUST ignore. Any mismatch — an
  existing stream whose retention or replicas an update cannot change, a value
  clamped to an account limit, labels naming another run, or a server that did
  not carry the metadata — raises `IncompatibleProfile` (`VF_INCOMPATIBLE_PROFILE`)
  with a remedy naming the fix (tear the run down or provision under a fresh run
  id; raise the account limit; upgrade the server); a resource that could be
  neither created nor read back raises `BrokerUnavailable` — an unobserved
  configuration is not a verified one. Mismatches travel in
  `VerifiedStream.mismatches` / `VerifiedConsumer.mismatches` (`topology.py`; the
  backend contract's `VerifiedChannel` / `VerifiedSubscription`). With the switch
  off a mismatch is a warning and provisioning proceeds as before; the reference
  `_ensure_consumer`, which previously swallowed every error at debug level, now
  reads back under both settings.

  Stream/consumer `metadata` needs nats-server ≥ 2.10 (the dev broker and
  `k8s/nats.yaml` pin `nats:2.10`; nats-py 2.15.0 exposes `StreamConfig.metadata`
  and `ConsumerConfig.metadata`, and omits a `None` field from the request, which
  is what keeps the switched-off configs byte-identical). An older server silently
  drops the field; the read-back then reports `metadata` as not carried, which
  under the switch is an `IncompatibleProfile` whose remedy says to upgrade the
  server or run without `VF_RFC0006`.

- **`STREAM-15`** (delivery cap and credit):

  *Delivery cap.* When the node's runtime store advertises
  `durable = Known(True)` and `shared_across_processes = True` (§9), an
  at-least-once node's data durables MUST be provisioned with `max_deliver = -1`
  (the broker never strands a message) and the retry budget of
  `VF_MAX_RETRIES + 1` attempts MUST be enforced by the ledger's per-message
  attempt count (`FlowRuntime.attempts_for`), which a `worker_fatal` failure never
  increments (MSG-008). Otherwise `max_deliver` stays `STREAM-5`'s cap. Best-effort
  durables keep `max_deliver = 1`. Decision D11: with a memory-only ledger, attempt
  counts reset on restart and a crashing worker would redeliver a poison message
  forever. `STREAM-5` and `DELIV-10` are amended to add the store capability as an
  input; provisioning and the worker MUST derive the cap identically — the
  provisioner learns the store from `VF_RUNTIME_STORE_URL` (probing Redis
  persistence, §9) and the worker from the same variable.

  *Credit.* `max_ack_pending` MUST be `nb_tasks × (item_credit + prefetch)` for a
  shared (competing) durable and `item_credit + prefetch` for a per-replica
  (partitioned) durable, where `item_credit` is the node's admitted processing
  concurrency per replica (default 1: one input group at a time) and `prefetch`
  is the local prefetch depth (reference `_QUEUE_MAXSIZE = 4`). A scale change MUST
  re-derive and update it (the field is mutable) and read it back; a backend that
  cannot resize credit (`credit_resizable = False`) MUST reject the scale request
  with a capacity reason rather than add idle replicas (MSG-017). `STREAM-4`'s
  "reference uses a small value, `queue_maxsize + 2`" is replaced by this formula.
  Byte budgets (`byte_credit`) are enforced by the runtime's `receive` and are not
  consumer configuration.

*Example.* Ten competing replicas of `detector` under `parent`: durable
`detector--from--parent`, `max_ack_pending = 10 × (1 + 4) = 50` (today 8, so two
replicas could never hold a message); with a durable shared ledger,
`max_deliver = -1` and the ledger dead-letters at the fourth failed attempt; with a
memory ledger, `max_deliver = 4` exactly as today. The consumer metadata reads
`{videoflow.io/flow-id: f, videoflow.io/run-id: r, videoflow.io/node: detector,
videoflow.io/kind: durable, videoflow.io/generation: 5e1c…}`.

### 6. Delivery — `DELIV-15`, `DELIV-16`

- **`DELIV-9`** amended. Before: "a message that fails to decode MUST be terminated
  (not redelivered forever)". After: "a message that fails to decode is `poison`
  classified at the transport layer and MUST be settled per `DELIV-15`".
- **`DELIV-15`** (undecodable bytes get a durable record): an envelope that fails to
  decode (`DecodeError`, code `VF_POISON_DECODE`) MUST follow the `DELIV-6` ladder
  for `poison`. Under at-least-once the SDK MUST publish the **raw wire bytes** to
  the node's DLQ subject (`NAME-9`) with the `DELIV-8` headers — `VF-Code:
  VF_POISON_DECODE`, `VF-Disposition: poison`, `VF-Error` (the decoder's message,
  truncated), `VF-Remedy`, `VF-Num-Delivered`, `VF-Origin-Node`, `VF-Run-Id`,
  `Nats-Msg-Id: dlq:{flow}:{run}:{node}:{stream_seq}` — and only after that publish
  is `Accepted` (a `duplicate` acceptance counts) terminate the delivery. Under
  best-effort the `ERR-6` sampler applies, then TERM. A `Terminal` settlement MUST
  carry a durable record reference: the DLQ publication's `Nats-Msg-Id` when
  dead-lettered, otherwise (sampled out, or `dlq: off`) the position of an entry
  appended to the node's terminal log in the runtime store
  (`record_ref = 'ledger:{node}/terminal:{position}'`). An SDK MUST NOT terminate a
  message whose only trace would be its own disappearance (MSG-010).

  If the DLQ publish is `Rejected`, `PublicationUnknown` or times out, the delivery
  MUST be kept — `Retry` (NAK with delay), never TERM — and a `pending_handoff`
  record MUST be written to the runtime store so a later attempt, or a restarted
  worker, retries the dead-letter with the same `Nats-Msg-Id` (MSG-009). `DELIV-8`'s
  "NAK it (with delay)" gains: with `max_deliver = -1` (`STREAM-15`) that NAK can
  never strand; without it a NAK past the broker cap leaves the message retained
  but undeliverable, and the node's subscription observation MUST report it as
  `unresolved ≥ 1` rather than as zero pending (MSG-012).

  A transient payload-store failure during decode (`TransientFailure` read outcome,
  `BLOB-15`) is **not** undecodable: it is `transient` and retried. A terminator
  that fails to decode still counts as a terminator (`_eos_pull_loop`, unchanged).

- **`DELIV-16`** (`VF-Replay-Target`): `videoflow dlq replay` republishes a dead
  letter to the **parent's** data subject of the target run with a fresh
  `Nats-Msg-Id` (`replay:<uuid4 hex>`) and `VF-Replay: <source run id>` (today's
  behaviour, kept), which fans out to every child of that parent — including
  siblings that never failed the message (PAY-016). The replay publisher MUST also
  set `VF-Replay-Target: <origin node>` (the DLQ entry's `VF-Origin-Node`,
  verbatim). A data pull loop whose `VF_NODE_NAME` differs from the header value
  MUST ack-and-skip the delivery before hydrating any payload, exactly as a
  non-owner does under `PART-4`, and MUST NOT release a payload obligation it never
  acquired. The target node processes it normally. Absent header ⇒ every child
  processes it (pre-RFC scope). Terminators are never replayed, so EOS consumers are
  unaffected. Replay acquires its own payload obligations (`BLOB-14` step 3) and
  never decrements the original readers' shares.

*Example.* A replayed dead letter carries
`Nats-Msg-Id: replay:5e1cd0b7…`, `VF-Replay: run-1`, `VF-Replay-Target: detector`,
`VF-Env: 4`. `tracker`, `detector`'s sibling under `cam`, acks and skips it without a
blob fetch; `detector` processes it.

### 7. Payload obligations — `BLOB-13`, `BLOB-14`, `BLOB-15`

The RFC 0002 counter (`vf-blobrc-<hex>`) is replaced by *obligations*: named,
idempotent, generation-fenced claims by logical readers. A payload is reclaimable
only when no obligation names it.

- **`BLOB-3`** amended: the wire `BlobRef.ref` is unchanged (`vf-blob-<hex>`, hex
  being `uuid4().hex` — 32 lowercase hex characters, no dashes; RFC 0002 wrote
  `<uuid>`, and the reference has always minted the dash-less form). "The store
  additionally maintains a companion reclamation counter (`BLOB-5`)" becomes "the
  store maintains companion obligation and metadata records (`BLOB-13`)".
- **`BLOB-13`** (identities and keys): the reference store (`RedisPayloadStore`,
  registered for scheme `redis` under the switch) keeps two companion keys
  **hash-tagged onto the blob key** so all three hash to the slot of the string
  `vf-blob-<hex>` on Redis Cluster (decision D10):

  | Key | Type | Content |
  |---|---|---|
  | `vf-blob-<hex>` | string | the payload bytes (unchanged) |
  | `vf-blobobl-{vf-blob-<hex>}` | SET | obligation ids; `EXPIRE` = the latest obligation deadline |
  | `vf-blobmeta-{vf-blob-<hex>}` | HASH | `size` (decimal bytes), `digest` (lowercase hex SHA-256 of the bytes), `generation` (opaque, minted at put; reference 8 hex), `content_id` (the logical content identity, reference `'{producer_name}:{trace_id}:{seq}'` of the publishing message; a forwarding stage MAY pass its input's through so an unchanged frame shares one object), `created_at` (epoch seconds) |

  All three carry the `BLOB-7` TTL. The store reports
  `PayloadCapabilities.atomic_multikey = Known(True)` when the server is not
  clustered or `CLUSTER KEYSLOT` agrees for the three keys, else `Unknown`; a
  profile that depends on atomic release is rejected on `Unknown` (PAY-008).

  Obligation ids are over `[A-Za-z0-9_./-]` and take exactly these forms:

  | Form | Held by |
  |---|---|
  | `<child>` | a competing child node — one obligation for all its replicas |
  | `<child>/p<i>` | replica `i` of a partitioned child (every replica decodes every message) |
  | `dlq/<flow>` | a dead letter, until the DLQ entry ages out (`STREAM-8`'s 7 days) |
  | `archive/<flow>` | a replay archive (`replay_archive` profile) |
  | `intent/<publication_id>` | the publisher, from before publish until the outcome is known; `publication_id` is the message's `MSGID-1` id |

  `VF_BLOB_READER_IDS` (`ENV-12`) lists the reader obligations a publisher acquires
  at put, computed at compile time: `<child>` per competing child and
  `<child>/p<i>` for `i in range(nb_tasks)` per partitioned child.

- **`BLOB-14`** (lifecycle):
  1. *Put.* Blob first, then metadata, then the obligation set — an interrupted put
     degrades to a counterless, TTL-only blob (the safe direction, as `BLOB-5`).
     The publisher acquires `intent/<publication_id>` before publishing and
     releases it on `Accepted` (duplicate included) or definite `Rejected`; on
     `PublicationUnknown` the intent is kept until `observe_publication` resolves it
     or reconciliation does (PAY-007, PAY-013).
  2. *Release.* A reader MUST release its obligation at most once per delivered
     message and only after a **confirmed** settlement (`SettleConfirmed`);
     `SettleUnknown` MUST NOT release (PAY-004). A partitioned non-owner releases
     its `<child>/p<i>` on its ack-and-skip. Release is idempotent by
     `(obligation_id, generation)`; a release naming a generation other than the
     stored one is `stale` and touches nothing (PAY-005, PAY-008, PAY-021). The
     reference implementation is `WATCH obl meta` → read → `MULTI` `SREM obl id`
     [+ `UNLINK` of all three keys when the set would become empty and the
     generation matches] → `EXEC`; a `nil` reply (concurrent modification) retries,
     bounded (reference 5). **No Lua/`EVAL`**: RFC 0002's Alternatives rejected
     scripts because a multi-key script is cross-slot on Redis Cluster and `EVAL`
     is restricted on managed offerings; hash-tagging is what makes the transaction
     single-slot without one. A missing obligation set (expired, evicted, or written
     by an RFC 0002 publisher) MUST NOT be created by a release nor cause a delete;
     the reader applies `BLOB-6` counter semantics if `vf-blobrc-<hex>` exists and
     otherwise leaves the blob to its TTL. (`BLOB-5`/`BLOB-6` are amended to say
     they are the fallback contract when obligations are absent.)
  3. *Dead letters and replay.* Before terminating a dead-lettered message the
     worker MUST acquire `dlq/<flow>` on its payload with deadline = the DLQ
     retention, so the bytes outlive the run for the forensic and replay horizon
     (PAY-014; this resolves RFC 0002's first open question). A replay acquires
     `intent/<publication_id>` and the target's reader obligations afresh and never
     touches the original readers' obligations (PAY-016).
  4. *Reconcile.* `reconcile(ledger, operation_id)` — at worker start and
     periodically — MUST reclaim objects whose set is empty or whose only
     obligations are `intent/*` of publications that are definitely `Rejected`,
     and MUST cancel the obligations of messages the broker evicted (REALTIME:
     the message's stream sequence is below `stream_info().state.first_seq`,
     PAY-012). It reports `ReclamationObservation(reclaimed, retained, unknown)`;
     an inventory read that failed is `unknown`, never "nothing to reclaim".
  5. *TTL backstop.* `BLOB-7` remains the upper bound on all three keys. A
     `durable_required` retention contract whose `ttl_seconds` is shorter than the
     flow's `horizon_seconds` is rejected at admission unless obligations pin the
     object for the whole horizon (PAY-009).

- **`BLOB-15`** (typed reads): `read` returns one of `PayloadBytes` (digest verified
  when metadata exists, `digest_verified = True`), `TransientFailure` (the store was
  unreachable or slow — disposition `transient`, retryable, and NEVER classified as
  malformed bytes; MSG-010, PAY-002), `Missing` (nil) or `Corrupt` (digest
  mismatch). `Missing` and `Corrupt` are terminal for that delivery and are
  dead-lettered per `DELIV-15` with `VF-Code: VF_POISON_DECODE` and a `VF-Error`
  naming the ref and `missing` / `corrupt` (PAY-003). `decode_envelope` gains a
  `resolve_blobs = True` keyword so a receiver can decide ownership (`PART-3`) and
  replay scope (`DELIV-16`) from metadata before fetching bytes (PAY-018); the
  frozen shim `videoflow.serialization` re-exports it unchanged (additive default).

*Example.* Blob `vf-blob-9f1c…` published by `cam` for children `det` (competing)
and `track` (partitioned, 2 replicas):
`vf-blobobl-{vf-blob-9f1c…}` = `{intent/745708c2eeaf98a60fdbebafe1d04eff, det, track/p0, track/p1}`,
`vf-blobmeta-{vf-blob-9f1c…}` = `{size: 6220800, digest: 3b2f…, generation: c0ffee12, content_id: cam:cam:3f9c1a2b7d4e:5:5, created_at: 1757600000.123}`.
The PubAck arrives → `intent/…` released; `det` acks (confirmed) → `det` released;
`track/p1` is the owner and acks → released; `track/p0` acks-and-skips → released;
the set is empty and the generation matches → all three keys `UNLINK`ed in one
`EXEC`. A redelivery to `det` after a lost ack releases `det` again: idempotent,
nothing changes.

### 8. Environment — `ENV-10` … `ENV-18`

Nine optional rows join the §1.1 table. Each is ignored by an older worker
(`ENV-4`); absent, each means today's behaviour. The control planes
(`manifests.py`, `engines/local.py`) emit them only when set or derived (decision
D8: nothing is added to `NodeSpec`), so a flow that never touches them renders
byte-identical manifests and worker environments.

| ID | Variable | Type | Default (absent ⇒) | Set by | Read by |
|---|---|---|---|---|---|
| **`ENV-10`** | `VF_RUNTIME_STORE_URL` | URL: `memory://`, `file://<dir>`, `redis://…` / `rediss://…` | `memory://` — no durable state, `restart_safe` rejected | local engine: `file://<termination_dir>/ledger` (under the switch; default at acceptance); manifests: the value of `VF_BLOB_REDIS_URL` when a blob store is configured | worker (`FlowRuntime`), `provision.py` and the CLI (admission, `STREAM-15` cap) |
| **`ENV-11`** | `VF_PARENT_REPLICAS` | comma-separated non-negative integers, positionally aligned with `VF_PARENT_NAMES` | unset — the `EOS-7` barrier is not evaluated; `EOS-3` applies | both control planes, from each parent's `nb_tasks` | worker (`EOS-7`); an entry count that differs from the parent count fails fast (`ConfigError`, as `ENV-1`) |
| **`ENV-12`** | `VF_BLOB_READER_IDS` | comma-separated obligation ids (`BLOB-13`) | unset — `VF_BLOB_READERS` count semantics (`BLOB-5`), else TTL-only | compile time, alongside `VF_BLOB_READERS` | the publisher's put (`BLOB-14`) |
| **`ENV-13`** | `VF_PROFILE_REQUESTS_JSON` | JSON list of the operator's explicit requests, `[{channel, profile, options}]` (`backends.capabilities.requests_env`; the other `FlowRequirements` fields travel beside the specs, not here) | unset — `default_requirements(flow_type, specs)`, i.e. today's presets (BATCH ⇒ `reliable_work`, REALTIME ⇒ `live_latest`, per-node `delivery` overrides honoured), and nothing is read back | both control planes, on every node ConfigMap / worker env **and the provision Job**, only when the operator passed `--require-profile`. Deploy-time admission (`deploy/admission.py`) judges an auto-provisioned broker/store by its declared profile and reads a bring-your-own one back live (`jetstream_capabilities_observed`: `max_payload`, the account's file-store allowance, existing streams' `storage`/`num_replicas`; `redis_payload_capabilities_observed`), `Unknown` with a reason for whatever it could not read | `provision.py`: admission against the live broker and store before any stream is created (also under `VF_RFC0006=1` without requests, where an unobserved capability is a warning), then `topology.read_back_streams` + `verify_channel_profiles` on the streams it created — a stream contradicting its requested profile or request (`reliable_work` on limits/discard-old, `num_replicas` below `VF_STREAM_REPLICAS`) is `IncompatibleProfile` (exit 2), a missing one `BrokerUnavailable`, an unreadable one `UnobservableState` (exit 3); a rejection is rendered `ERROR [code]: message` + remedy, never a traceback. The worker (`verify_explicit_profiles`): the same read-back of its own channel and its parents' before the node is built or opened — a producer publishes nothing on rejection — over a connection of its own, never the messenger's. Both bound by `VF_ADMISSION_TIMEOUT_SECONDS` (default 60) |
| **`ENV-14`** | `VF_GPU_GRANT_JSON` | JSON, `DeliveredGrant.to_dict()`: `workload_id`, `devices: [{node, ordinal, uuid, mig_uuid, product, memory_bytes, mig_profile}]`, `exclusive`, `requested`, `policy` | unset — `VF_GPU_COUNT` / `VF_GPU_RESOURCE_NAME` only (RFC 0003) | the local allocation backend; Kubernetes bindings | components (`runtime/gpucheck.verify_grant`) and diagnostics. Informational, not routing; `exclusive = false` under `--gpu-policy shared` |
| **`ENV-15`** | `VF_WATCHDOG_INTERVAL_SECONDS` | float ≥ 0 | `5` (`runtime/watchdog.py::DEFAULT_WATCHDOG_INTERVAL_SECONDS`); `0` disables the watchdog thread (the in-loop `ProgressDeadline.check` remains), as does `VF_PROGRESS_TIMEOUT_SECONDS=0` | control planes, only when the operator overrides | the worker (`watchdog_interval_from_env`), for non-producers; a non-numeric or negative value fails fast (`ConfigError`); on a stall it writes the termination reason and exits `5` (`EXIT_FLOW_STALLED`) |
| **`ENV-16`** | `VF_FAULT_SCHEDULE_JSON` | JSON object: barrier name (∈ `faults.BARRIERS`) → action spec — `{"kind": "crash", "exit_code": 137}`, `{"kind": "raise", "code", "message", "disposition"}`, `{"kind": "delay", "seconds"}`, `{"kind": "drop"}`, `{"kind": "pause", "name", "timeout_seconds"}`, `{"kind": "nth", "n", "action": {…}}` | unset — no schedule; a barrier is one attribute read | the conformance harness (`FaultSchedule.to_env()`); a control plane MUST NOT emit it for a deployment | the worker at start (`FaultSchedule.from_env().install()`); an unknown barrier name is refused (`UnknownBarrier`) |
| **`ENV-17`** | `VF_FAULT_MARKER_DIR` | absolute directory path | unset — in-process counting only | with `ENV-16` | the worker: created on install; each hit appends `<pid> <time>` to `<dir>/<barrier>.fired`; `Pause(name)` waits for `<dir>/<name>.release` |

| **`ENV-18`** | `VF_STREAM_REPLICAS` | integer ≥ 1 | unset — the server's default replica count, i.e. the single-server request as it always was | manifests, on the provision Job only, when the broker profile's `jetstream_replicas` exceeds 1 (`--broker-profile durable`) | `provision.py`: requested as every stream's `num_replicas` (the DLQ stream included) and read back like any other field (`STREAM-14`) |

`VF_RFC0006` itself is a transitional variable, not a protocol row: it is deleted at
acceptance (§1). `VF_PAYLOAD_CONCURRENCY` (hydration thread-pool size, default 4) is
a tuning knob of the reference worker and is not part of the contract.

### 9. Durable control state — `CTRL-4`

- **`CTRL-1a`** amended: a transient control notification (the plain-NATS stop
  subject) MAY wake workers but MUST NOT be the sole record of a stop, an abort or a
  completion for a node operating under `reliable_work`, `durable_control` or
  `restart_safe`.
- **`CTRL-4`** (the runtime ledger): such a node MUST keep the following records in
  a `RuntimeStore` — a versioned key-value store whose every mutation is a
  compare-and-swap (`cas(key, expected_version, value)`, `expected_version = None`
  meaning "must not exist"; `append(log, record)`; `scan(prefix)`;
  `delete(key, expected_version)`) — and never only in memory:

  | Record | Written | Used by |
  |---|---|---|
  | terminators: per `(parent, replica_id, kind)` → `seq`, `Error` | before the terminator is acked | `EOS-7` barrier; ABORT precedence survives restart (RUN-010) |
  | received sets: per `(parent, durable)` → distinct DATA ids delivered | on delivery, deduplicated by message id | `EOS-7` barrier (b); competing replicas aggregate |
  | ownership epochs: partition id `<node>/p<replica>` → `(epoch, fencing_token)` | CAS-incremented at `acquire_partition`; every commit carries the token | a commit under a superseded epoch is refused: `StaleAuthority` (`VF_STALE_AUTHORITY`, `worker_fatal`) (RUN-023) |
  | outbox: `publication_id` → envelope digest, payload refs, outcome | intent before `publish`; outcome after | `PublicationUnknown` intents are reconciled at start through `observe_publication` with the **same** `publication_id`; a second identity is never minted (RUN-003, RUN-013, MSG-013, MSG-014) |
  | attempt counts: message id → attempts per disposition | on `fail_inputs`; `worker_fatal` never increments | the ladder when `max_deliver = -1` (`STREAM-15`) |
  | checkpoints: state bytes + replay position per source | one CAS write, so state and position describe the same committed prefix | `MSGID-6`; restart-safe stateful nodes (RUN-022) |
  | pending DLQ handoffs | when a dead-letter publish is not `Accepted` | `DELIV-15` retry after restart (MSG-009) |
  | open join groups: members by logical id `(producer, trace_id, seq)` + tokens | as members arrive | a restarted worker reloads them and re-binds redelivered members (RUN-001, RUN-002) |

  Assembler buffers, prefetch queues and in-flight ack handles stay in memory. Key
  layout is implementation-defined; the reference prefixes every key with
  `vf/{flow}/{run}/{node}/`.

  **Stores and qualification.** `restart_safe` (and `durable_control`) is admitted
  only when the composed store advertises `durable = Known(True)` **and**
  `shared_across_processes = True`; `Unknown` is rejected as `UnobservableState`
  (`VF_STATE_UNKNOWN`), `False` as `IncompatibleProfile`. Never faked:

  | Scheme | Implementation | `durable` | `shared_across_processes` | Qualifies |
  |---|---|---|---|---|
  | `memory://` | `MemoryRuntimeStore` | `Known(False)` | `False` | never (tests, dev) |
  | `file://<dir>` | `FileRuntimeStore` — one JSON file per key, `fcntl` lock per key | `Known(True)` | `True` on one host | single-host runs (the local engine) |
  | `redis://`, `rediss://` | `RedisRuntimeStore` — single-key versioned values, CAS via `WATCH`/`MULTI`/`EXEC` | `Known(True)` only when a read-back of `CONFIG GET appendonly` / `save` shows persistence on and `maxmemory-policy` is `noeviction`; `Known(False)` when persistence is off; `Unknown` when `CONFIG` is denied (managed Redis) | `True` | multi-host runs with persistence |

  The Kubernetes default (`ENV-10`) points the ledger at the blob Redis, so a
  deployment that offloads payloads and asks for `restart_safe` must run that Redis
  with `appendonly yes` and `noeviction` — the same condition `reliable_work` with
  payload refs already imposes on the payload store.

### 10. Run-scoped Kubernetes names and identity collisions

Today every run of a flow renders the same resource names — `vf-<flow>-<node>`
(workload), `-env`, `-hl`, `-pdb`, `-scaler`, and the flow-wide `vf-<flow>-specs`,
`-provision`, `-broker`, `-netpol` — and workload selectors match
`videoflow.io/node` + `videoflow.io/flow-id` only. A second concurrent run
`kubectl apply`s over the first (RUN-047).

- **`--run-scoped-names`** (`deploy`, `render`, `teardown`; opt-in now, the default
  at acceptance — **not yet implemented**, plan Phase 6): the per-run resources are named
  `k8s_name('vf', flow_id, run_id, node[, suffix])` — workloads, env ConfigMaps,
  headless Services, PDBs, ScaledObjects — and the run-wide ones
  `k8s_name('vf', flow_id, run_id, 'specs' | 'provision' | 'broker')`. Selectors
  (`matchLabels`, the headless Service selector, KEDA `scaleTargetRef`) MUST include
  `videoflow.io/run-id`. The NetworkPolicy stays flow-scoped: it selects by flow
  label, is shared by concurrent runs, and is removed only by a flow-wide teardown.
  `k8s_name` still truncates to 63 characters, so the collision check below MUST
  run over the run-scoped names. Goldens for the flag live under
  `tests/golden/rfc0006/manifests/`.
- **`--single-run`** (opt-in; **not yet implemented**, plan Phase 6): `deploy` MUST
  refuse to start run B of a flow before
  mutating anything when resources labelled `videoflow.io/flow-id=<flow>` with a
  different `videoflow.io/run-id` exist (RUN-047 A3). With flow-scoped names and
  without `--single-run`, today's overwrite happens; that hazard is documented, and
  is why the default flips.
- **Identity collisions are rejected at compile time, without renaming anything**
  (decision D2). `core/graph.py::_check_name_collisions` emits a
  `VF_GRAPH_NAME_COLLISION` diagnostic for node names that encode to one broker
  name (`topology.sanitize`) or one Kubernetes name (`manifests.k8s_name`), and
  the compiler raises `IdentityCollision` (`VF_IDENTITY_COLLISION`, exit `2`) when
  `identity.collisions(specs, flow_id, run_id)` finds a physical name reached by two
  distinct logical identities of the same kind — subjects, streams, durables, EOS
  durables, DLQ subjects, Kubernetes names. `collisions_across_runs` guards
  teardown (`r` vs `r-x`). The remedy names both colliding nodes. Different kinds
  may share a string (a subject and a stream never occupy one namespace).

*Examples.* Nodes `a.b` and `a_b` both sanitize to `a_b` → rejected. Nodes `Node`
and `node` both render `vf-f-node` → rejected. Two 70-character names that truncate
to one 63-character Deployment name → rejected. Under `--run-scoped-names`, flow
`f`, run `r`, node `det` renders Deployment `vf-f-r-det`, ConfigMap `vf-f-r-det-env`,
Job `vf-f-r-provision`; the stream stays `vf-f-r-det` (`NAME-3`, unchanged).

## Compatibility

- **Wire compatibility:** the envelope, every payload proto and every routing
  *name* (`NAME-1`…`NAME-9`) are byte-for-byte unchanged; no `.proto` diff, no
  version bump. A run is version-homogeneous on the wire (`WIRE-1`) and stays so.
  The identities in §2–§4 are strings placed in existing fields that every SDK
  already treats as opaque except through `MSGID-1` and `PART-3`, both of which
  keep working on the new strings.
- **Behavioral compatibility** — the matrix:

  | Combination | Supported? | What happens |
  |---|---|---|
  | Old worker image, new control plane (new provisioning, new env rows) | yes | Metadata is invisible to the worker. `max_deliver = -1`: the old ladder still dead-letters at `num_delivered ≥ retries + 1` (`DeliveryPolicy.max_deliver`), the broker just never strands. Larger `max_ack_pending`: the worker's local queue still bounds its own prefetch. `VF_PARENT_REPLICAS`, `VF_RUNTIME_STORE_URL`, … ignored (`ENV-4`) ⇒ `EOS-3` drain, today's DLQ path. `VF-Replay-Target` ignored ⇒ replay reaches every child (pre-RFC scope). Not rejected: the control plane cannot see SDK versions. |
  | New worker, old control plane (legacy streams without metadata, `max_ack_pending = 8`, cap `retries + 1`) | yes | The worker binds durables by name; read-back yields the `effective` configuration; the ledger budget is bounded by the broker cap; the new rows are absent ⇒ pre-RFC behaviour throughout; teardown by exact-name membership. |
  | Mixed SDK versions in one flow (vendor components) | yes, degraded where noted | New producers' `{node}:{epoch}:{n}` ids and `tw-…-{hash}` ids are opaque downstream. Terminator `seq` is ignored by old receivers. Blobs: new publisher + old reader — the old reader finds no `vf-blobrc-` counter, never deletes, the obligation set retains the blob until TTL (`BLOB-7`): safe, leak-until-TTL. Old publisher + new reader — counter semantics via the `BLOB-14` fallback. |
  | Legacy streams without metadata, same run id reused | yes, conservatively | Teardown attributes them by their subject tokens (`subject_owner`), never by name prefix; an unattributable stream is left standing; anything owned whose delete failed is `remaining`, and the CLI exits `3`. Provisioning onto them: read-back; a retention mismatch, or labels naming another run, is rejected (`IncompatibleProfile`). |
  | nats-server < 2.10 | switch off: yes; switch on: rejected | The server drops `metadata`; the read-back reports it as not carried. With the switch off that is a warning and teardown attributes streams by subject tokens; with it on, provisioning raises `IncompatibleProfile` (remedy: upgrade the server, or run without `VF_RFC0006`). |
  | DLQ readers and tooling | yes | Stream and subject names unchanged. New `VF_POISON_DECODE` entries carry bodies that may not be envelopes; `videoflow dlq show` MUST tolerate an undecodable body (headers plus a hex dump). |
  | Third-party `BlobStore` subclasses (RFC 0002) | yes | The `BlobStore` base class is unchanged; obligations are a separate `PayloadStore` ABC. A store registered for a scheme keeps count semantics. |
  | Manifests rendered by an older CLI | yes | No new row is present ⇒ every default above. |

- **Rejected combinations** (at admission or compile time, before anything is
  provisioned, started or published): a profile the composed backends cannot
  provide (`IncompatibleProfile`), one whose dependency could not be observed
  (`UnobservableState`), `restart_safe` without a durable shared store, mixed
  retention classes on one channel, `exactly_once_effects` without an external
  idempotency primitive, identity collisions, and a second run under `--single-run`.
- **Protocol version:** 1, unchanged. Descriptors pinning `spec.protocol: 1` remain
  valid. The optional node attributes this RFC introduces (`replayable`,
  `analysis_version`) are additive.

## Conformance impact

New conformance-map rows, each validated by the catalogue cases in
`tests/conformance/` (the case ids are the ones the tests carry as
`@pytest.mark.case`):

| ID | Cases | Module |
|---|---|---|
| `EOS-7` | RUN-007, RUN-008, RUN-009, RUN-010 | `test_run_completion.py` |
| `MSGID-5` | RUN-014 | `test_run_sources.py` |
| `MSGID-6` | RUN-015 | `test_run_sources.py` |
| `JOIN-23` | RUN-016 | `test_run_joins.py`; unit `tests/test_backends_core.py` |
| `STREAM-14` | MSG-006, MSG-019, MSG-020 | `test_msg_provisioning.py`, `test_msg_capabilities.py` |
| `STREAM-15` | MSG-008, MSG-017, RUN-024 | `test_msg_recovery.py`, `test_msg_subscriptions.py`, `test_run_credits.py` |
| `DELIV-15` | MSG-009, MSG-010, MSG-012 | `test_msg_recovery.py` |
| `DELIV-16` | PAY-015, PAY-016 | `test_pay_replay.py` |
| `BLOB-13`…`BLOB-15` | PAY-002, PAY-003, PAY-004, PAY-005, PAY-007, PAY-008, PAY-012, PAY-013, PAY-014, PAY-018, PAY-021 | `test_pay_access.py`, `test_pay_obligations.py`, `test_pay_orphans.py`, `test_pay_retention.py`, `test_pay_routing.py` |
| `ENV-10`…`ENV-18` | env emission and parsing: `tests/test_local_engine.py`, `tests/test_compiler_manifests.py`, `tests/test_backends_core.py` | — |
| `CTRL-4` | RUN-001, RUN-002, RUN-003, RUN-013, RUN-022, RUN-023, MSG-018 | `test_run_joins.py`, `test_run_commit.py`, `test_run_partitions.py`, `test_msg_retention.py` |
| run-scoped names, `--single-run` | RUN-047 | `test_run_rollout.py`; goldens `tests/golden/rfc0006/manifests/` |
| identity collisions | MSG-019 | `test_msg_capabilities.py`; unit `tests/test_backends_core.py` |

Golden vectors: `spec/vectors/message_id/vectors.json` gains entries for the three
trace-id forms (`cam:3f9c1a2b7d4e:1`, `file:42`, `file:v2:42`) and a
`tw-…-<digest>` id; a new `spec/vectors/join/group_identity.json` pins
members → id including the rounding and the digest (the example in §4 is one
entry); `envelope/manifest.json`'s EOS entry gets the `EOS-7` description. Every
existing `.bin` MUST remain byte-identical — an identity check, not a green suite.

Goldens: `tests/golden/rfc0006/{manifests,broker}` are recorded with the switch on
during Phases 2–4; the base goldens do not change until the flip, at which point the
`rfc0006` set becomes the base.

Existing tests that change **deliberately** at acceptance, with the reason in the
commit: `tests/integration/broker/test_delivery_ladder.py::test_an_undecodable_payload_is_terminated_not_retried_forever`
(additionally asserts exactly one DLQ record with `VF-Code = VF_POISON_DECODE` and
the raw body) and `::test_a_dlq_publish_failure_naks_rather_than_dropping` (asserts
`unresolved ≥ 1` / a `pending_handoff` record rather than only
`pending + unacked ≥ 1`); `tests/test_topology.py` assertions on `max_ack_pending`
and consumer metadata. Negative controls in `tests/conformance/defects.py` — prefix
teardown, TERM without a record, timestamp-only group ids, the decrement-only
counter, the `(0, 0)` fail-open — must fail against the oracle or the case is
`INVALID_TEST`.

## Shipped behaviourally, not RFC-worthy

> Scope note: this list covers the plan's Phases 1–4. As of Phase 1 the items that have landed are the
> Unknown-state preflight/cleanup, CAS ownership with the epoch label, the ConfigMap tombstone, the
> Blackwell table, the classifier fix, the identity-collision rejection, the watchdog, the concurrent
> supervisor, per-host packing, PVC mounts, broker profiles, priority-class rendering and the advisory
> admission. Phase 2 added the JetStream adapter behind `NATSMessenger`, supersede without TERM,
> `ack_sync` settlement outcomes, owner-before-hydrate, the negotiated inline threshold, the
> obligation-keeping Redis store, the terminal log, the credit formula and stream replicas in
> provisioning, `VF-Replay-Target` and the routing-without-hydration replay. Until the runtime store
> (§9, Phase 3) exists the terminal log is per-process memory and a failed dead-letter publish keeps
> the delivery with a delayed NAK instead of a `pending_handoff` record; `max_deliver` stays the
> `STREAM-5` cap (decision D11); `dlq replay` relies on the `dlq/<flow>` pin rather than acquiring
> the target's reader obligations afresh (the replay bookkeeping lands with the ledger). The rest (`--gpu-policy`, `--gpu-nodes`, `--rollout-policy`,
> `--resources`, the DRA render adapter, UUID masks, readiness correlated by resourceVersion) ship
> with Phases 3–4 and are listed here so their non-RFC status is decided now.

These land in their plan phase without waiting for acceptance. None changes a wire
byte, a routing name or a stream configuration; each is listed so a reviewer can
check the classification.

- **Supersede without TERM.** A redelivered join half retires the buffered handle
  locally (`handle.supersede()`) instead of terminating the broker message the new
  attempt now holds; `settle` on a retired token returns `SettleStale`. The
  observable effect `JOIN-7`/`JOIN-13` promise — no duplicate group, a fresh ack
  deadline — is unchanged; their wording "terminate the stale handle" is corrected
  to "retire the stale handle locally" in the `PROTOCOL.md` edit that lands with
  this RFC (MSG-011, RUN-001).
- **`ack_sync` and `SettleUnknown`.** Acks use `msg.ack_sync(timeout)`; a lost
  reply is `SettleUnknown`, which releases no obligation (PAY-004).
- **Hand-back at retirement.** A receiver that shuts down gracefully NAKs every
  delivery it still holds — parked in its prefetch queue or received and never
  settled — so a scale-down or rollout returns that work to the survivors at
  once instead of after `ack_wait` on a replica that no longer exists; a crash
  hands nothing back and its leases lapse as before (MSG-017, RUN-030).
- **Off-loop hydration and owner-before-hydrate.** Payload fetches run on the
  receiving thread, off the adapter's event loop, after the ownership and
  replay-scope decisions on decoded metadata (`peek_envelope`,
  `decode_envelope(resolve_blobs = False)`), so a non-owner never fetches
  (PAY-018, PAY-019, RUN-034). Those two decisions run as the adapter's
  admission filter (`MessagingBackend.set_admission`) where the delivery
  arrives: a message this replica will never process is acked out of its ack
  window at once and its reader share released after the confirmed ack, instead
  of waiting behind the input being processed.
- **Negotiated inline threshold.** The offload threshold is measured on the encoded
  payload and bounded by the connection's `max_payload` less an envelope reserve
  (`safe_inline_threshold`): an unsafe `VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES` is
  lowered to what the broker carries, and with no store to offload to it is a
  `ConfigError` under the switch rather than a publish refused after the fact
  (PAY-001). No wire byte changes: a payload below the safe threshold is inline
  exactly as before.
- **Drops reported from below the seam.** `Messenger.take_drops()` hands the
  health seam the drops it cannot see from a disposition alone — `exhausted`,
  `undecodable`, `join_evicted`, `publish_discarded` — so
  `videoflow_messages_dropped_total{reason}` conserves `offered − processed −
  dropped` (RUN-026, RUN-045).
- **Replay routes from metadata and verifies bytes separately.** `dlq replay`
  decides every target from envelope metadata (`--dry-run` works while the store
  is down; offloaded entries are never skipped for lack of a store) and, given
  `--blob-redis-url`, reads every offloaded payload before publishing anything —
  a payload the store cannot return is `VF_RESOURCE_UNAVAILABLE` for the whole
  replay, not a message replayed into a second dead letter (PAY-015).
- **Watchdog.** `ProgressWatchdog` polls `ProgressDeadline.check()` from its own
  thread so a hung callback is noticed despite healthy heartbeats; `ERR-7` is
  unchanged (RUN-011, RUN-012).
- **Concurrent local supervisor.** One waiter thread per child feeding a queue,
  replacing the sequential `pending.pop(0)` (RUN-048).
- **Unknown-state preflight and cleanup.** `_kubectl_observed`,
  `gpu_units_in_use_observed`, `gpu_inventory_observed`, `_mig_manager_pods`,
  `_consumer_pending` and `pending_observation` return `Observation`s;
  `UnobservableState` replaces planning against zero; `ProgressDeadline` neither
  resets nor trips on `Unknown` and raises `BrokerUnavailable` after a grace period.
- **CAS ownership and readiness correlation.** Node owner labels are written with a
  `kubectl label --resource-version=<rv>` precondition (the API server's optimistic lock, verified live) plus `videoflow.io/gpu-owner-epoch`, a lost race
  raising `OwnershipConflict` (`VF_OWNERSHIP_CONFLICT`); MIG readiness
  is correlated with the operation (`resourceVersion` after the write,
  `mig.config` equal to the request, `state = success`, allocatable geometry)
  rather than read from a historical `success` (ALLOC-003, ALLOC-004, ALLOC-033).
- **ConfigMap tombstone** (decision D3). The shared `videoflow-mig-parted-config`
  is never deleted; last-one-out restores its contents by CAS and stamps
  `videoflow.io/mig-config-tombstone` — `kubectl` has no delete-with-precondition.
- **`--gpu-policy strict|shared`** (decision D4). Local default stays `shared`
  (today's wrap-around) until the flip; `strict` makes undersupply `Infeasible`
  before any launch (ALLOC-014, RUN-044).
- **UUID masks.** `CUDA_VISIBLE_DEVICES` entries may be GPU or MIG UUIDs; discovery
  resolves them through `nvidia-smi -L`; a failed discovery is `Unknown`, never "no
  GPUs" (ALLOC-015, RUN-039).
- **Blackwell MIG table** (`RTX-PRO-6000-Blackwell`: `1g.24gb`×4, `2g.48gb`×2,
  `4g.96gb`×1) and memory-aware `_Card.fits`.
- **Classifier fix.** `mig.strategy = single` with `mig.config = all-disabled`
  classifies as physical; explicit `mps`; mixed evidence is `unknown` (ALLOC-009).
- **Admission rejections.** `plan_composition` rejects, never downgrades
  (`VF_INCOMPATIBLE_PROFILE`, `VF_STATE_UNKNOWN`); `exactly_once_effects` are
  rejected unless the sink declares an external primitive (MSG-001, RUN-017,
  RUN-018, RUN-019, RUN-027).
- **Identity-collision rejection** — the mechanism (`_check_name_collisions`,
  `identity.collisions`). The *rule* is recorded in §10 because it changes which
  graphs compile.
- **PVC mounts** (`--mount-pvc claim:/path[:ro]`, `x-mounts: pvc:…`).
- **Broker and Redis profiles** (`--broker-profile dev|durable`,
  `--broker-replicas`, `--broker-storage-class`; `RedisProfile`).
- **Priority, requests, rollout and placement rendering** (`--priority-class`,
  `--resources`, `--rollout-policy drain|surge`, `--gpu-nodes`), all opt-in
  manifest fields; the DRA adapter renders only (decision D9).

## Alternatives considered

- **A suffix or hash scheme that makes broker and Kubernetes names reversible**
  (decision D2). Rejected: it renames every hyphenated flow id in use — including
  the k8s tests' own `flow_ids` — which is exactly the kind of change this process
  exists to avoid; exact-ownership metadata plus compile-time collision rejection
  fixes MSG-019 and MSG-020 with zero renames.
- **A Lua script for atomic release** (decision D10). Rejected: RFC 0002's
  Alternatives already rejected scripts — cross-slot on Redis Cluster, `EVAL`
  restricted on managed offerings. `WATCH`/`MULTI`/`EXEC` over keys hash-tagged onto
  the existing blob key is single-slot and keeps the wire `BlobRef.ref` unchanged.
- **`max_deliver = -1` unconditionally** (decision D11). Rejected: with a
  memory-only ledger, attempt counts reset on restart and a crashing worker would
  redeliver a poison message forever. The cap is lifted only where a durable shared
  ledger counts attempts.
- **New `NodeSpec` fields for requirements and grants** (decision D8). Rejected:
  `NodeSpec.to_dict()` is `asdict`, so any new field changes every specs ConfigMap
  byte. Requirements travel as a separate document beside the specs and as
  environment rows emitted only when set.
- **A permanent feature flag / dual path** (decision D1). Rejected: `VF_RFC0006`
  exists so each refactor step stays byte-identical on the default path while the
  suite exercises the new semantics; the off-path is deleted at acceptance.
- **Keep TERM-only for undecodable bytes.** Rejected: an unrecorded TERM under
  `reliable_work` is silent loss (MSG-010); the DLQ already exists and its record
  is what makes the disposition explicit.
- **Timestamp-only group ids, with a finer rounding.** Rejected: any rounding
  collides for some pair of member sets (RUN-016); the member hash makes identity a
  function of what was joined, and the `tw-{µs}` prefix keeps the ids sortable.
- **Two streams for a channel whose consumers request different retention**
  (MSG-002). Rejected: that is a routing change; the planner rejects mixed retention
  on one channel instead, and the remedy names the fix (move the reliable consumer
  behind its own producer, or drop the override).
- **A dedicated replay subject per target node.** Rejected for the reason RFC 0005
  rejected a side subject for ABORT: it would need its own stream, durables and
  interest anchor. A header the pull loop honours reuses everything.
- **A memory-only ledger with `restart_safe` "best effort".** Rejected: an
  unobservable or non-durable guarantee is not an available one; the planner
  rejects and says why.

## Open questions

1. **`window_id` for time-mode groups.** `group_identity` accepts a window
   namespace; this RFC fixes it to `None` for v1. If a per-node or per-policy
   namespace is wanted (RUN-016 A3 speaks of a "source/epoch/window namespace"),
   it must be settled before Phase 6 records `join/group_identity.json`, because
   it changes every id.
2. **Epoch form.** A random 12-hex token is specified; a CAS-incremented decimal
   from the runtime store would make epochs *ordered*, letting a receiver reject a
   pre-restart terminator by comparison, at the cost of a store dependency in the
   producer. Decide before the vectors are recorded.
3. **`VF_PARENT_REPLICAS` encoding.** Positional integers aligned with
   `VF_PARENT_NAMES` (as `ENV-3`) versus self-describing `name=count` pairs.
4. **Received-set growth.** The `EOS-7` barrier keeps one entry per distinct
   delivered id per `(parent, durable)` until the barrier closes. A BATCH run of
   millions of frames needs a compaction rule (fold committed, ack-confirmed ids
   into a count) that this RFC does not yet give.
5. **`derived_names()` and run-scoped names.** `identity.derived_names` enumerates
   the flow-scoped Kubernetes names; once `--run-scoped-names` is the default it
   must enumerate the run-scoped ones, or the collision check covers the wrong
   corpus.
6. **`STREAM-9`.** `STREAM-8` cites it ("MUST NOT be deleted by run teardown
   (`STREAM-9`)") but it is not defined anywhere. This RFC leaves the number
   unassigned; the `PROTOCOL.md` edit that lands with it should either define
   `STREAM-9` as that rule or drop the reference. Similarly `ABORT-1` cites
   `NAME-5` for the `_eos` subject where `NAME-4` is meant.
7. **Distinct codes for `Missing` and `Corrupt` payloads.** Both dead-letter under
   `VF_POISON_DECODE` with the distinction in `VF-Error`. Codes are permanent
   (`ERR-3`), so if dashboards need to separate "the blob expired" from "the bytes
   were wrong", new codes should be minted now rather than later.
8. **Fault schedules in production.** `ENV-16` is honoured whenever present. An
   explicit opt-in (a second variable, or refusing schedules unless
   `VF_FAULT_MARKER_DIR` is under a test-owned path) would make an accidental
   leak of a harness environment into a deployment inert.
