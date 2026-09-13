# Protocol requirement → coverage map

Phase 0 verification artifact. Every requirement ID in [`PROTOCOL.md`](PROTOCOL.md)
maps to one of:

- **✓ test** — an existing Python test in `tests/` already exercises it (the
  reference implementation is pinned here today).
- **~ partial** — exercised indirectly by an integration test but not asserted
  directly; a dedicated conformance scenario should assert it explicitly.
- **☐ conformance** — not yet directly tested; `TODO(conformance)` for Phase 4.
  These are the language-agnostic scenarios the Rust/C++ SDKs must pass.
- **P1** — concerns the v4 protobuf wire, delivered in Phase 1; tested there.

Paths are under `tests/` unless noted. This map is the source for the
scenario ↔ requirement cross-index the conformance suite (`tests/conformance/`)
carries; see [the backend conformance suite](#the-backend-conformance-suite-testsconformance)
at the end for the catalogued cases that are implemented today.

## §1 Environment contract

| ID | Status | Where |
|---|---|---|
| ENV-1 required vars / fail-fast | ~ partial | worker integration (`test_eos_replicas`, `examples_tests`) exercise the happy path; ☐ add missing-var fail-fast scenario |
| ENV-2 node name authoritative | ~ partial | `worker.py` sets `node._name`; ☐ scenario |
| ENV-3 parent order positional | ✓ test | `test_node_identity.py::test_multi_producer_graph`; `core/task.py` ordering |
| ENV-4 ignore unknown vars | ☐ conformance | trivial but unpinned |
| ENV-5 replica-id resolution (POD_NAME ordinal) | ☐ conformance | `_resolve_replica_id` has no direct unit test |

## §2 Naming

| ID | Status | Where |
|---|---|---|
| NAME-1 sanitize regex | ✓ test | `test_topology.py::test_names_sanitize_illegal_chars` |
| NAME-2 data subject | ✓ test | `test_topology.py::test_names_are_run_scoped` |
| NAME-3 stream name | ✓ test | `test_topology.py::test_names_are_run_scoped` |
| NAME-4 EOS subject | ~ partial | used throughout `test_eos_replicas`; ☐ assert format |
| NAME-5 data durable | ✓ test | `test_topology.py::test_durable_is_child_from_parent` |
| NAME-6 partitioned durable | ~ partial | `test_partitioning.py` uses it; ☐ assert `--p{n}` format |
| NAME-7 EOS durable per replica | ~ partial | `test_eos_replicas.py`; ☐ assert format |
| NAME-8 control subject | ✓ test | `test_topology.py::test_names_are_run_scoped` |
| NAME-9 DLQ stream/subject | ✓ test | `test_topology.py` (dlq_stream_name); `test_ack_semantics.py::test_dlq_on_exhausted_retries` |

## §3 Streams & consumers

| ID | Status | Where |
|---|---|---|
| STREAM-1 one stream/node, 2 subjects | ✓ test | `test_topology.py` stream config |
| STREAM-2 REALTIME LIMITS/OLD/max=1 | ✓ test | `test_topology.py::test_realtime_stream_config_drops_old`; `test_transport_semantics.py::test_realtime_keeps_only_freshest` |
| STREAM-3 BATCH INTEREST/NEW | ✓ test | `test_topology.py::test_batch_stream_config_is_interest_discard_new`; `test_transport_semantics.py::test_batch_interest_rejects_when_full_and_frees_on_ack` |
| STREAM-4 data consumer filter/ack_wait | ~ partial | exercised by transport tests; ☐ assert filter_subject |
| STREAM-5 max_deliver realtime=1 batch=r+1 | ✓ test | `test_ack_semantics.py::test_nak_redelivers_up_to_max_deliver` |
| STREAM-6 EOS consumer per replica | ~ partial | `test_eos_replicas.py` |
| STREAM-7 up-front provisioning idempotent | ✓ test | `test_topology.py::test_provision_is_idempotent` |
| STREAM-8 DLQ stream config | ~ partial | `test_ack_semantics.py::test_dlq_on_exhausted_retries` |

## §4 Envelope & wire (v4 = Phase 1)

| ID | Status | Where |
|---|---|---|
| WIRE-1 versioning/homogeneous run | ✓ test (v3) → P1 (v4) | `test_serialization.py::test_wrong_version_rejected`, `test_v2_envelope_decodes_without_event_ts` |
| WIRE-2 version pin / VF-Env header | P1 | `test_serialization_v4.py` |
| WIRE-3 reject unknown/legacy version | ✓ test | `test_serialization.py::test_legacy_msgpack_envelope_rejected`; `test_serialization_v4.py::test_legacy_msgpack_envelope_refused` |
| WIRE-4 carry-forward fields | ~ partial | `test_time_sync.py` (seq/event_ts min); ☐ metadata carry scenario |
| WIRE-5/6 data/eos types | ✓ test | `test_serialization.py::test_stop_signal_round_trip` |
| WIRE-7 Tensor codec | ✓ test (raw-ndarray) → P1 | `test_serialization.py::test_ndarray_round_trip` |
| WIRE-8 well-known payloads | P1 | `test_golden_vectors.py` |
| WIRE-9 vendor extension / opaque passthrough | P1 | `test_serialization_v4.py` |
| WIRE-10 Value for structured | P1 | `test_serialization_v4.py` |
| WIRE-11 *(withdrawn, RFC 0001)* | ✓ test | codec removed; `test_serialization_v4.py::test_arbitrary_payload_type_is_inert_on_decode` (unknown type stays opaque) |
| WIRE-12 Value int/double distinct | P1 | `test_serialization_v4.py` |
| WIRE-15 Value nests Tensor | ✓ test | `test_serialization_v4.py::test_nested_tensor_container_roundtrips` |

## §5 Message id & dedup

| ID | Status | Where |
|---|---|---|
| MSGID-1 derive_message_id `[:32]` | ✓ test | `test_serialization.py::test_message_id_is_deterministic_and_type_sensitive` |
| MSGID-2 content dedup on retry | ✓ test | `test_transport_semantics.py::test_publish_dedup_drops_duplicate_message_id` |
| MSGID-3 producer trace minting | ~ partial | `nats_messenger.publish_message`; ☐ assert format |

## §6 Task loop

| ID | Status | Where |
|---|---|---|
| LOOP-1 open/close lifecycle | ✓ test | `test_release_resources.py`, `test_ports_async_ctx.py` |
| LOOP-2 producer loop | ~ partial | `examples_tests`; ☐ scenario |
| LOOP-3 processor loop + poison-survive | ✓ test | `test_ack_semantics.py::test_realtime_failure_drops_without_dlq` (worker survives) |
| LOOP-4 consumer loop | ~ partial | `test_deployment_polish.py` idempotent sink |
| LOOP-5 publish-before-ack | ✓ test | `test_ack_semantics.py::test_unacked_message_survives_restart` |
| LOOP-6 no children ⇒ no output | ~ partial | `core/task.py` guards; ☐ scenario |
| LOOP-7 proctime metadata | ✓ test | `test_health.py::test_metrics_accumulate` |
| LOOP-8 set_output_partition_key | ~ partial | `test_ports_async_ctx.py`; ☐ assert `_partition_key` |
| LOOP-9 set_output_event_timestamp | ~ partial | `test_time_sync.py` relies on event_ts; ☐ ctx scenario |
| LOOP-10 event-ts propagation precedence | ~ partial | `nats_messenger.publish_message`; ☐ scenario |

## §7 Delivery / ack / retry / DLQ

| ID | Status | Where |
|---|---|---|
| DELIV-1 ack-after-process | ✓ test | `test_ack_semantics.py::test_unacked_message_survives_restart` |
| DELIV-2 ack on success | ✓ test | `test_ack_semantics.py` |
| DELIV-3 keepalive extend | ☐ conformance | `_keepalive_loop` untested directly |
| DELIV-4 realtime publish never blocks | ✓ test | `test_transport_semantics.py::test_realtime_keeps_only_freshest` |
| DELIV-5 batch publish backpressure | ✓ test | `test_transport_semantics.py::test_batch_interest_rejects_when_full_and_frees_on_ack` |
| DELIV-6 realtime fail drops | ✓ test | `test_ack_semantics.py::test_realtime_failure_drops_without_dlq` |
| DELIV-7 batch NAK redelivery | ✓ test | `test_ack_semantics.py::test_nak_redelivers_up_to_max_deliver` |
| DELIV-8 DLQ on exhaustion + headers | ✓ test | `test_ack_semantics.py::test_dlq_on_exhausted_retries` |
| DELIV-9 poison/undecodable term | ~ partial | decode-refusal tested (`test_serialization*.py` legacy-msgpack + inert unknown-type); `_pull_loop` term() wiring still integration-only |

## §8 Join / grouping

| ID | Status | Where |
|---|---|---|
| JOIN-1 policy validation | ✓ test | `test_time_sync.py::test_policy_validation`, `test_time_assembler_validation` |
| JOIN-2 defaults per flow type | ✓ test | `test_time_sync.py::test_policy_defaults_to_trace_mode`; `test_partitioning.py::test_join_policy_defaults_per_flow_type` |
| JOIN-3 single-parent passthrough | ~ partial | `test_node_identity.py::test_single_parent_processor_can_be_replicated` |
| JOIN-4 time+multiparent needs nb_tasks==1 | ~ partial | `nats_messenger.__init__` raises; ☐ scenario |
| JOIN-5 trace completeness | ✓ test | `test_time_sync.py::test_trace_group_completes_on_matching_trace_id` |
| JOIN-6 representative min seq/event_ts | ✓ test | `test_time_sync.py::test_time_group_seq_is_deterministic_for_dedup` (time); trace via grouping |
| JOIN-7 trace redelivery-supersede | ✓ test | `test_time_sync.py::test_trace_redelivery_supersedes_buffered_half` |
| JOIN-8 trace timeout eviction | ✓ test | `test_time_sync.py::test_trace_timeout_evicts_with_drop_and_error` |
| JOIN-9 max_pending evicts oldest | ✓ test | `test_time_sync.py::test_trace_max_pending_evicts_oldest` |
| JOIN-10 time validation | ✓ test | `test_time_sync.py::test_time_assembler_validation` |
| JOIN-11 group time = min; arrival fallback | ~ partial | `test_time_sync.py` time tests; ☐ assert no-event_ts fallback |
| JOIN-12 nearest-within-tolerance matching | ✓ test | `test_time_sync.py::test_time_group_picks_nearest_candidate`, `test_time_group_joins_within_tolerance`, `test_time_group_outside_tolerance_stays_separate` |
| JOIN-13 time redelivery-supersede | ✓ test | `test_time_sync.py::test_time_redelivery_supersedes_in_group` |
| JOIN-14 collect buffering + cap | ~ partial | `test_time_sync.py::test_time_collect_parent_delivers_window_as_list`; ☐ cap eviction |
| JOIN-15 settle window | ~ partial | collect test exercises it; ☐ assert hold |
| JOIN-16 ready emission order | ~ partial | implied by quorum/collect tests |
| JOIN-17 timeout/quorum staging | ✓ test | `test_time_sync.py::test_time_quorum_emits_partial_after_timeout`, `test_time_below_quorum_evicted_after_timeout` |
| JOIN-18 collect attach at emission | ✓ test | `test_time_sync.py::test_time_collect_parent_delivers_window_as_list` |
| JOIN-19 collect prune stale | ✓ test | `test_time_sync.py::test_time_collect_stale_samples_pruned` |
| JOIN-20 minted `tw-` identity + rounding | ✓ test | `test_time_sync.py::test_time_group_seq_is_deterministic_for_dedup` — **cross-language byte-identity is ☐ conformance (P3/P4)** |
| JOIN-21 eviction ack/nak mapping | ✓ test | `test_time_sync.py::test_trace_timeout_evicts_with_drop_and_error` |

## §9 EOS drain

| ID | Status | Where |
|---|---|---|
| EOS-1 per-replica EOS held un-acked | ✓ test | `test_eos_replicas.py::test_replicated_processor_all_replicas_terminate_and_deliver` |
| EOS-2 duplicate EOS ignored | ~ partial | `test_eos_replicas.py` |
| EOS-3 drain condition + quiescence | ~ partial | `test_eos_replicas.py::test_replicated_parent_child_drains_all_replicas`; ☐ quiescence-window timing scenario |
| EOS-4 ack EOS on stop | ~ partial | `test_eos_replicas.py` |
| EOS-5 all-stopped ⇒ loop terminates | ✓ test | `test_eos_replicas.py` |
| EOS-6 has_pending_from for time groups | ☐ conformance | `TimeGroupAssembler.has_pending_from`; ☐ EOS-while-collect-buffered scenario |

## §10 Partitioning

| ID | Status | Where |
|---|---|---|
| PART-1 enabled iff key & nb_tasks>1 | ✓ test | `test_node_identity.py::test_replicated_join_with_partition_by_is_accepted` |
| PART-2 key extraction | ~ partial | `nats_messenger._owns`; ☐ metadata-key scenario |
| PART-3 exact truncated-hash ownership | ☐ conformance | value not asserted; **critical cross-language vector** |
| PART-4 non-owned ack-and-skip | ✓ test | `test_partitioning.py::test_partitioned_processor_delivers_each_message_once` |

## §11 Control plane

| ID | Status | Where |
|---|---|---|
| CTRL-1 stop subject plain NATS | ~ partial | `engines/local.py::_publish_stop`; ☐ scenario |
| CTRL-2 producer honors stop | ~ partial | `core/task.py`; ☐ scenario |
| CTRL-3 consumer/processor hard stop | ~ partial | `nats_messenger.receive_message`; ☐ scenario |

## §12 Health & metrics

| ID | Status | Where |
|---|---|---|
| HEALTH-1 /readyz after activity | ✓ test | `test_health.py::test_not_ready_until_activity` |
| HEALTH-2 /healthz liveness stall | ✓ test | `test_health.py::test_liveness_stalls` |
| HEALTH-3 /metrics names | ✓ test | `test_health.py::test_metrics_accumulate` |
| HEALTH-4 unknown path 404 | ☐ conformance | handler returns 404; untested |

## §13 Blob store

| ID | Status | Where |
|---|---|---|
| BLOB-1 threshold → BlobRef | ✓ test | `test_serialization.py::test_large_payload_uses_blob_store` |
| BLOB-2 no store → error | ✓ test | `test_serialization.py::test_large_payload_without_blob_store_raises` |
| BLOB-3 resolve inner codec | ✓ test | `test_serialization.py::test_large_payload_uses_blob_store` (round-trip) |
| BLOB-4 Redis interop | ~ partial | `RedisBlobStore`; ☐ cross-language scenario |
| BLOB-5 reader-counted put | ✓ test | `test_blob_refcount.py::test_put_with_readers_writes_counter`; integration `test_blob_reclamation.py` |
| BLOB-6 release only on successful ack | ✓ test | `test_blob_refcount.py` (ack/nak/term discipline); integration `test_blob_reclamation.py` |
| BLOB-7 TTL backstop + flow-type default | ✓ test | `test_blob_refcount.py::test_release_without_counter_is_noop`; messenger TTL default test |

## §14 Idempotency

| ID | Status | Where |
|---|---|---|
| IDEM-1 check-then-mark skip | ✓ test | `test_deployment_polish.py::test_idempotent_sink_consumes_once_across_redelivery` |
| IDEM-2 key format (full hex) | ✓ test | `test_deployment_polish.py::test_idempotency_key_is_deterministic` |

---

## Summary

- **~68 requirement IDs.** The overwhelming majority are pinned by the existing
  ~99-test Python suite (`✓`/`~`).
- **Phase-1 (`P1`) items** are the v4 protobuf wire — tested when that code lands.
- **`☐ conformance` items** are the language-neutral gaps the Phase-4 kit closes,
  led by the two cross-language byte-identity vectors that most threaten drift:
  **PART-3** (truncated-hash ownership) and **JOIN-20** (minted `tw-` identity /
  rounding mode). These two, plus **EOS-6** (EOS-while-collect-buffered) and
  **DELIV-9** (poison message), are the highest-priority new scenarios.
- No requirement is unaccounted for.

## The backend conformance suite (`tests/conformance/`)

The requirement rows above pin the *protocol*. The backend conformance suite pins
the *backends* underneath it — transport, payload store, accelerator allocator,
runtime — against the contracts in `videoflow/backends/`. Its catalog,
`tests/conformance/catalog/test_catalog.json` (130 cases: 26 MSG, 22 PAY, 34 ALLOC,
48 RUN, at six levels — model, process, broker, kubernetes, gpu, benchmark), is
checked in verbatim from the validation kit and validated by
`tests/conformance/test_catalog_lint.py`, which also proves the ID ↔ test mapping
is 1:1: every case has exactly one primary `@pytest.mark.case(ID)` test, extra
tests for the same case carry `@pytest.mark.variant(name)`, and every
non-pending regression case has a `@pytest.mark.negative_control(of = ID)` test
that must *fail* against the reproduced defect (`tests/conformance/defects.py`).

Outcomes are five, not three: `PASS`, `FAIL`, `UNSUPPORTED` (the backend truthfully
declines the guarantee and the expected rejection was asserted), `NOT_RUN` (the
fixture is absent — no broker, no `videoflow-test` namespace, no GPU; evidence of
nothing) and `INVALID_TEST` (a scheduled fault never fired, or a negative control
missed its defect). Every case is always collected; a skeleton not yet written
carries `@pytest.mark.pending('phase N')` and reports `NOT_RUN: pending phase N`.

```bash
VF_RFC0006=1 uv run pytest tests/conformance -q -rs -p no:cacheprovider
uv run python tests/conformance/report.py      # tests/conformance/_out/run_results.json
```

Cases with a real oracle today (everything else is a pending skeleton), case id →
module. *Primary* is the test at the catalog's primary level whose status is the
case's; a case whose primary needs a broker (`nats_url`: `VF_TEST_NATS_URL`,
default `nats://localhost:4222`) or the k3s cluster (`k3s` / `k3s_admin`:
`scripts/k3s-test-up.sh`) reports `NOT_RUN` without it even when its model-level
variant passes.

| Case | Module | Primary level | Variants |
|---|---|---|---|
| MSG-001 capability negotiation rejects, never downgrades | `test_msg_capabilities.py` | model | — |
| MSG-019 broker identities are collision-resistant | `test_msg_capabilities.py` | model | — |
| MSG-020 teardown by exact ownership | `test_msg_capabilities.py` | broker (`nats_url` gate) | `memory` (model) |
| ALLOC-001 reject layouts exceeding physical MIG memory | `test_alloc_planning.py` | model | — |
| ALLOC-007 failed occupancy/ownership reads are Unknown | `test_alloc_ownership.py` | kubernetes (`k3s_admin` gate) | `memory-backend`, `mix-strategy` (model) |
| ALLOC-008 classification and capacity share the pool snapshot | `test_alloc_ownership.py` | kubernetes (**pending** phase 5: needs `VF_K8S_GPU_NODES` and the operator-applied pool label) | `kubectl-fake` (model) |
| ALLOC-009 recognise MPS and mixed/incomplete sharing evidence | `test_alloc_planning.py` | model | `memory-backend` |
| ALLOC-010 per-host fragmentation before claiming a whole GPU | `test_alloc_planning.py` | model | — |
| ALLOC-023 reject unsupported allocation features before any cluster call | `test_alloc_planning.py` | model | `mix-strategy-audit` |
| ALLOC-034 resolve `gpu_count`/memory defaults with explicit values | `test_alloc_planning.py` | model | — |
| RUN-011 independent watchdog detects a hung callback | `test_run_progress.py` | model | — |
| RUN-012 slow inference and idle sources do not trip the watchdog | `test_run_progress.py` | model | — |
| RUN-027 BATCH scaling targets a supported workload controller | `test_run_scaling.py` | kubernetes (`k3s` gate, plus KEDA installed on the cluster) | `admission` (model) |
| RUN-045 tail-latency objectives need histogram buckets | `test_run_telemetry.py` | model | — |
| RUN-046 multi-input demand observes every required parent | `test_run_scaling.py` | model | — |
| RUN-048 local supervision notices a later worker failure | `test_run_progress.py` | model | — |

The requirement IDs the suite will additionally validate once RFC 0006 lands
(`EOS-7`, `MSGID-5`/`6`, `JOIN-23`, `STREAM-14`/`15`, `DELIV-15`/`16`,
`BLOB-13`…`15`, `ENV-10`…`17`, `CTRL-4`) are mapped to cases in that RFC's
"Conformance impact" section; they join the tables above when it is accepted.
