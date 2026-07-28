# Error handling in videoflow: the whole picture

*A survey of how failures are classified, contained, and surfaced — from an
exception raised in a node's `process()` to the way a kubelet responds.*

Sources: `videoflow/core/`, `videoflow/runtime/`, `videoflow/messaging/`,
`videoflow/engines/`, `videoflow/deploy/`, and the normative contract in
[spec/PROTOCOL.md](../spec/PROTOCOL.md) §7.

---

## The organizing principle

Videoflow splits error handling across a hard line that mirrors its execution
model: **the graph is built on one machine and executed on many.**

| | Build/deploy time (one machine, one process) | Run time (N workers, no shared memory) |
|---|---|---|
| Philosophy | **Fail fast and loud**, with a message naming the fix | **Never crash on data**; quarantine the message, keep the worker alive |
| Vehicle | Exceptions → `SystemExit(str(e))` | Broker ack/nak/term + DLQ; exceptions are *caught*, not propagated |
| Audience | A human at a terminal | A log line, a metric counter, and a kubelet |
| Failure unit | The whole deploy | One message |

Everything below follows from that split.

---

## 1. Taxonomy of exceptions

There is exactly **one** custom exception class in the entire package
([deploy/mig.py:33](../videoflow/deploy/mig.py#L33), `LayoutError(ValueError)`).
Everything else is stdlib, used with strict conventions:

| Type | Meaning | Where | Example |
|---|---|---|---|
| `ValueError` | Graph/config/wire validation — the user built something invalid. Message must **name the fix**. | `core/graph.py`, `core/node.py`, `core/remote.py`, `deploy/images.py`, `wire/serialization.py` | [graph.py:66](../videoflow/core/graph.py#L66) — replicated join without `partition_by` tells you to set `partition_by='trace_id'` or `nb_tasks=1` |
| `RuntimeError` | Lifecycle misuse or **environment unusable** | [engine.py:159](../videoflow/core/engine.py#L159), [local.py:174](../videoflow/engines/local.py#L174), [kubernetes.py:220](../videoflow/engines/kubernetes.py#L220) | broker unreachable locally lists three ways to start one |
| `NotImplementedError` | Abstract method, **or** a node that structurally cannot run in-process | `core/engine.py`, `core/node.py`, [remote.py:85](../videoflow/core/remote.py#L85) | `RemoteProducer.next()` — "runs out-of-process in its own image" |
| `AttributeError` | The `get_params()` round-trip contract was violated | [node.py:138](../videoflow/core/node.py#L138) | a constructor arg not stored as `self._<name>` |
| `TypeError` | Payload/metadata cannot be encoded for the wire | [serialization.py:427](../videoflow/wire/serialization.py#L427) | ndarray put in metadata instead of payload |
| `KeyError` | Blob reference expired or never existed | [serialization.py:142](../videoflow/wire/serialization.py#L142) | |
| `SystemExit` | The CLI boundary — user sees a message, not a traceback | ~34 sites in [deploy/cli.py](../videoflow/deploy/cli.py) | always `raise SystemExit(str(e)) from e` |

The rule `SystemExit(str(e)) from e` appears everywhere in the CLI: internal code
raises a *typed* domain error, and exactly one layer converts it into an
operator-facing message.

---

## 2. The four boundaries where errors are handled

### Boundary 1 — Graph construction (in your process)

`Flow([...])` validates eagerly and refuses to build an invalid graph: parentless
non-producers ([flow.py:34](../videoflow/core/flow.py#L34)), cycles, unreachable
consumers, duplicate node names, and the replicated-join/`partition_by` rule
([graph.py:33-71](../videoflow/core/graph.py#L33-L71)). Node constructors validate
their own args (`device_type`, `gpu_count`, `gpu_memory_gib` mutual exclusion), and
`JoinPolicy.__init__` rejects incoherent combinations (`quorum` without
`timeout_seconds`, `tolerance_ms` outside `mode='time'`) at
[policies.py:71-92](../videoflow/core/policies.py#L71-L92).

The point: a graph that reaches the compiler is already structurally sound,
because on a cluster the same mistake would surface as an opaque hang.

### Boundary 2 — Deploy (`videoflow deploy` / `run-local`)

Pre-flight checks, in order, each converting a typed error into `SystemExit`:
image resolution, wire-version compatibility
([compiler.py:249](../videoflow/core/compiler.py#L249)), GPU capacity preflight
against *free* (not merely allocatable) cluster resources
([cluster.py:340-366](../videoflow/deploy/cluster.py#L340-L366)), and mount-spec
parsing.

Then a **two-phase apply** with a rollback discipline: the provisioning Job runs
first and is waited on
([kubernetes.py:328-355](../videoflow/engines/kubernetes.py#L328-L355)) so BATCH
interest-retention streams have consumer interest before any publish — otherwise a
fast producer's messages are dropped by the broker before anyone is listening. If
`allocate_and_run_tasks` throws, GPU cluster reconfiguration is undone —
deliberately catching `BaseException`, so a Ctrl-C during the multi-minute
provision wait also rolls back
([cli.py:316-325](../videoflow/deploy/cli.py#L316-L325)).

### Boundary 3 — Worker bootstrap (crash-fast, by design)

[runtime/worker.py](../videoflow/runtime/worker.py) validates its environment and
**crashes** on anything wrong: a `VF_NODE_KIND`/`VF_NODE_CLASS` disagreement
([worker.py:89-108](../videoflow/runtime/worker.py#L89-L108)), a missing
`VF_NODE_CLASS` on a remote component, an unemittable envelope version. The
`require_node_kind` docstring states the reasoning explicitly — without the check,
the mismatch surfaces later as an opaque `AttributeError` deep in the run loop.

`node.open()` is called **outside** the try in
[task.py:99-105](../videoflow/core/task.py#L99-L105), so an `open()` failure means
`close()` never runs and the process exits non-zero. That is the boundary: setup
failure = pod failure; per-message failure = quarantine.

### Boundary 4 — The run loop (the interesting one)

```
receive_message() → process() → publish() → ack()
                        ↓ any Exception
                    fail_inputs(e)   # worker survives
```

[ProcessorTask](../videoflow/core/task.py#L197-L218) and
[ConsumerTask](../videoflow/core/task.py#L246-L267) wrap process/publish/ack in
`try/except Exception`, log with `logger.exception`, and hand the failure to the
messenger. The comment says it plainly: *"a poison message never crashes the pod."*

**`ProducerTask` has no such path**
([task.py:119-146](../videoflow/core/task.py#L119-L146)) — only `StopIteration`
(normal end) and `KeyboardInterrupt` are caught. Any other exception from `next()`
kills the worker, *and skips the trailing `publish_stop_signal()`*, so downstream
nodes never see EOS. This is the asymmetry that matters most operationally (see §7).

Also note: `KeyboardInterrupt` in a processor/consumer is `continue`, not `break`.
Workers exit by *protocol* (EOS or the control-channel stop), never by signal.
`DelayedKeyboardInterrupt`
([generic_utils.py:206](../videoflow/utils/generic_utils.py#L206)) blocks SIGINT
across the critical section so Ctrl-C can't land between publish and ack.

---

## 3. The per-message failure ladder

This is the normative core, specified as `DELIV-1`…`DELIV-9` in
[spec/PROTOCOL.md](../spec/PROTOCOL.md) §7 and implemented in
[messaging/nats_messenger.py](../videoflow/messaging/nats_messenger.py).

**Ack-after-process** is the foundation: handles are queued *unacked*
([nats_messenger.py:396](../videoflow/messaging/nats_messenger.py#L396)) and
resolved only after the node processed and published. A crash mid-processing
therefore redelivers rather than loses. A keepalive loop extends ack deadlines
every `max(1s, ack_wait/3)` so a slow `process()` doesn't trigger spurious
redelivery.

`fail_inputs(exc)` then applies a ladder that **diverges entirely by flow type**
([nats_messenger.py:622-641](../videoflow/messaging/nats_messenger.py#L622-L641)):

| | REALTIME | BATCH |
|---|---|---|
| Stream policy | `LIMITS` + `discard=OLD`, `max_msgs=1` | `INTEREST` + `discard=NEW`, `max_msgs=10_000` |
| Publish when full | never blocks — oldest is evicted | **rejected** → retried with backoff `[0.05…1.0]s` = real backpressure |
| `max_deliver` | 1 (no redelivery) | `retries + 1` (default 4) |
| On node exception | `term()` — drop it, freshest wins | `nak(delay=min(2**n, 30))` → redeliver |
| Budget exhausted | n/a | **dead-letter**, then `term()` |
| Default join policy | timeout 10s, `missing='drop'` | wait forever, bounded by `max_pending` |

Two rules are load-bearing:

- **Never silently drop.** If the DLQ publish itself fails, the message is `nak`'d
  rather than terminated, so a later attempt can dead-letter it
  ([nats_messenger.py:635-638](../videoflow/messaging/nats_messenger.py#L635-L638)).
- **Poison ≠ failure.** A message that fails to *decode* is `term()`'d immediately
  in the pull loop
  ([nats_messenger.py:368-376](../videoflow/messaging/nats_messenger.py#L368-L376))
  — retrying an undecodable payload is pointless, and it must not be redelivered
  forever.

Dead letters carry the original raw bytes plus `VF-Origin-Node`, `VF-Error`
(truncated repr), `VF-Num-Delivered`, and an idempotent `Nats-Msg-Id` of
`dlq:{flow}:{run}:{node}:{stream_seq}`. They land in a per-run stream with a 7-day
`max_age`, inspectable without consuming them via `videoflow debug-decode --dlq`
([cli.py:840-880](../videoflow/deploy/cli.py#L840-L880)).

---

## 4. Failure modes that aren't exceptions

Three classes of failure never raise anything, and each has its own containment:

**Incomplete join groups.** A branch that dropped a message (REALTIME) or stalled
leaves a group half-assembled forever.
[messaging/grouping.py](../videoflow/messaging/grouping.py) resolves this per
`JoinPolicy.missing`: `drop` acks the partial handles (give up), `error` naks them
(redeliver — the missing half may still arrive, and eventually dead-letters),
`wait` never times out. `max_pending` (default 256) is the hard memory bound — the
oldest group is evicted as `drop` regardless of policy.

**Large payloads / blobs.** Refcounted release happens **only on ack success**,
never on nak/term, because a redelivery re-reads the blob
([nats_messenger.py:113-146](../videoflow/messaging/nats_messenger.py#L113-L146)).
A failed release is logged and swallowed — TTL (1h realtime / 24h batch) is the
deliberate backstop for everything acks can't cover.

**Termination stalls.** A node is stopped only once EOS is seen *and* its data
durable is drained, confirmed on two checks a quiescence window apart. When that
doesn't converge, `_log_drain_stall` prints per-parent diagnostics every ~15 idle
seconds
([nats_messenger.py:796-816](../videoflow/messaging/nats_messenger.py#L796-L816))
— turning what used to be a silent hang into a directly readable line naming which
parent, how many queued, and how many unacked.

**Duplicate effects.** At-least-once delivery is narrowed toward exactly-once from
both ends: content-derived `Nats-Msg-Id` gives broker-side dedup inside a 120s
window, and an opt-in `ConsumerNode(idempotent=True)` + Redis store skips
re-applying a sink effect on redelivery
([runtime/idempotency.py](../videoflow/runtime/idempotency.py),
[task.py:249-264](../videoflow/core/task.py#L249-L264)).

---

## 5. How Kubernetes is asked to behave

The manifest layer encodes the failure semantics into the workload kind itself
([manifests.py:447-530](../videoflow/deploy/manifests.py#L447-L530)):

| Node | Workload | Restart behaviour |
|---|---|---|
| Any node in a **BATCH** flow | `Job`, `restartPolicy: Never`, `backoffLimit: 3` | Fresh pod per retry; resumes from its durable |
| Finite producer in REALTIME | `Job` | same |
| Partitioned node (REALTIME) | `StatefulSet` | stable ordinals = stable replica ids; **not** autoscaled (rehashing would double/zero-process) |
| Everything else | `Deployment` | restarted forever |

Two choices are worth knowing:

- **`restartPolicy: Never`, not `OnFailure`** — because when an `OnFailure` Job
  exhausts its backoffLimit the controller *deletes the failed pod*, destroying the
  very logs `dump_failed_logs` exists to print. With `Never`, failed pods persist
  as evidence.
- **Probes are stripped from Job pods**
  ([manifests.py:476-479](../videoflow/deploy/manifests.py#L476-L479)). Long-running
  pods get `/readyz` (ready only after `open()` returns), `/healthz` (liveness,
  fails after 60s without a run-loop beat), and a startup probe with a **120s
  window** (`2s × 60`) covering slow model loads.

The rollout watchdog derives its own deadline from those probe constants, so
tuning the probe automatically retunes the check
([kubernetes.py:50-55](../videoflow/engines/kubernetes.py#L50-L55)).

**Detecting failure from outside the cluster** is where most of the engine's code
lives:

- `_FATAL_WAITING_REASONS` distinguishes kubelet *steady states reached only after
  failure* (`CrashLoopBackOff`, `ImagePullBackOff`, `InvalidImageName`,
  `CreateContainerConfigError`) from transient ones (`ErrImagePull`,
  `ContainerCreating`) that resolve on their own.
- `_is_failing` also catches `restart_count >= 2 and not ready` — the window
  between restarts where a container is briefly Running before the kubelet stamps
  CrashLoopBackOff.
- Both success and failure are **debounced over two consecutive polls**, symmetric
  so neither a blip nor an empty first snapshot decides anything.
- `_failure_detail` maps each state to a remedy: image pull → check the tag and
  `--image-pull-policy`; `OOMKilled` → raise the limit or use a smaller model;
  otherwise → the startup-probe window may be killing a slow `open()`.
- **Unschedulable is treated as fatal**, because such a pod never runs and
  therefore never consumes its `backoffLimit` — it would hang `wait_for_completion`
  forever. Guarded by a grace period and muted while a cluster-autoscaler scale-up
  is in flight *for those specific pods* (an unscoped Events query would let a
  stale hour-old event mute the watchdog indefinitely —
  [kubernetes.py:309-326](../videoflow/engines/kubernetes.py#L309-L326)).
- REALTIME deploys get `rollout_report()`, which exists precisely because a
  REALTIME deploy otherwise returns immediately and a broken node fails *silently*:
  producers keep publishing, frames get evicted, output never appears.

**Teardown is unconditional.** The BATCH path cleans up on success, failure,
watchdog stall, or Ctrl-C via `finally`, with logs dumped *before* deletion. The
broker stop is best-effort (the deploying host may not reach the in-cluster NATS
URL) and prints the exact `videoflow teardown` command to run manually; the
label-selector Kubernetes delete is the actual guarantee
([kubernetes.py:525-543](../videoflow/engines/kubernetes.py#L525-L543)).

---

## 6. Local vs Kubernetes: the same code, different supervision

The local engine runs the identical `videoflow.worker` entrypoint as subprocesses,
so per-message semantics are bit-identical. What differs is that **there is no
supervisor**: a crashed local worker stays dead. `wait_for_completion` just
collects return codes, excluding `-SIGINT`/`-SIGTERM` (that's Ctrl-C propagating,
not a failure), and `report_failures` prints an index — the tracebacks are already
on your terminal because workers inherit stdout/stderr.

So the same mid-flow crash resolves differently: **Kubernetes recovers** (new pod →
same durable → un-acked messages redeliver → drives to EOS → exits 0), while
**`run-local` hangs**, because the dead worker never publishes EOS and its children
block in `receive_message` until you Ctrl-C.

---

## 7. Sharp edges worth knowing

These are real consequences of the design, not bugs to rush to change — but
they're where the coherent picture has seams:

1. **A producer exception is uncontained.** No `fail_inputs` path exists (there's
   no input to nak), so it kills the worker *and* skips `publish_stop_signal()`.
   Downstream nodes then wait on an EOS that never comes. In Kubernetes the Job
   retries — but the producer restarts **from the beginning** with a fresh trace
   counter. Since message ids are content-derived, replayed messages dedup only
   within the broker's 120s `duplicate_window`; a producer that dies late in a long
   run replays as duplicates. There is no producer checkpointing.

2. **BATCH flows have no liveness backstop.** Every BATCH node is a Job, and Job
   pods have their probes stripped. Combined with no `activeDeadlineSeconds` and no
   overall timeout in `wait_for_completion`, a wedged BATCH worker hangs the run
   until Ctrl-C. The drain-stall log is the only diagnostic. (The unschedulable
   watchdog covers *never-started* pods, not *stuck* ones.)

3. **Teardown deletes the DLQ.** `delete_run_streams` explicitly removes the run's
   DLQ stream
   ([topology.py:321-336](../videoflow/messaging/topology.py#L321-L336)), and BATCH
   deploys tear down in a `finally`. So the 7-day retention only applies if you
   pass `--keep` — otherwise dead letters are gone before you can inspect them.

4. **The drain check fails open.** `_consumer_pending` returns `(0, 0)` when the
   broker query fails
   ([nats_messenger.py:830-833](../videoflow/messaging/nats_messenger.py#L830-L833)),
   and `(0, 0)` reads as "drained." A transient `consumer_info` failure across the
   quiescence window could declare a parent stopped while broker-side messages
   remain. Local buffers and pending join groups are checked independently, which
   narrows it — but the bias is toward terminating rather than hanging, and that
   trade isn't documented.

5. **The DLQ is inert.** Nothing consumes it, alerts on it, or reprocesses from it;
   `videoflow debug-decode --dlq` is a read-only inspector. `messages_failed` is a
   Prometheus counter, so alerting is possible but not wired.
