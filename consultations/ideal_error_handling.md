# A ground-up design for error handling in videoflow

*Companion to [error_handling.md](error_handling.md), which describes the system as
it is. This one describes the system I would build knowing what that survey found.*

---

## 0. The thesis

The current design gets the hardest thing right: **build time fails loud, run time
quarantines.** I would keep that line exactly where it is.

What it gets wrong is one level down. At run time, every failure is an
`except Exception`, and the retry ladder is chosen from `(flow_type,
num_delivered)` alone. That means the framework cannot distinguish:

- **this message is bad** — a malformed frame, a schema violation. Retrying it four
  times with exponential backoff is pure waste; it was never going to succeed.
- **the world blipped** — a socket reset, a throttled API, a lock timeout. Retrying
  is exactly right, and four attempts may be too few.
- **this worker is sick** — CUDA OOM, the model file vanished, the GPU fell off the
  bus. Retrying on *this* worker is worse than useless: it fails every message it
  touches, and each one dead-letters after four attempts. A single wedged GPU pod
  will quietly shred an entire stream into the DLQ, four attempts at a time, and
  nothing in the system notices that the failure rate is 100%.

That third case is the motivating bug for this whole design. So:

> **Design principle 1 — the retry decision is a function of the error's class,
> not only of the flow type.**
>
> **Design principle 2 — failure must be as propagable as success.** A flow has a
> protocol for "no more data, cleanly" (EOS). It needs a sibling for "no more data,
> because upstream died," or every failure at the head of a graph becomes a hang at
> the tail.
>
> **Design principle 3 — local and Kubernetes must fail identically.** They already
> share the worker; they must also share the supervision strategy, or the
> development environment is precisely the one where recovery bugs hide.

Everything below is those three principles worked out.

---

## 1. The taxonomy

### 1.1 What the taxonomy is for

An exception hierarchy earns its keep only if something *branches* on it. So each
level of this one exists to drive a specific decision:

| Level | Decides |
|---|---|
| Top-level class | Who is at fault, and therefore who sees it and at which boundary |
| Disposition | What the messenger does with the in-flight message |
| `code` | What the metric is labelled with and what the DLQ is queryable by |
| `remedy` | What the CLI prints under the error |

### 1.2 The hierarchy

```
VideoflowError                       # base; carries code, remedy, context
│
├── VideoflowUserError               # you built or configured something invalid
│   ├── GraphError                   # cycles, unreachable consumers, duplicate names
│   ├── NodeContractError            # get_params round-trip, kind/class mismatch
│   ├── ConfigError                  # flow_type, join policy, mounts, image refs
│   └── CapabilityError              # partition_by on a non-partitionable component,
│                                    #   quorum without accepts.missing
│
├── VideoflowEnvironmentError        # the world is not as required
│   ├── BrokerUnavailable            # NATS unreachable, stream missing
│   ├── ClusterError                 # kubectl absent, apply rejected, unschedulable
│   └── ResourceUnavailable          # GPU capacity, image pull, missing mount
│
└── VideoflowRuntimeError            # something failed while messages were flowing
    ├── PoisonMessage                # the DATA is bad     → DLQ now, never retry
    │   ├── DecodeError
    │   └── SchemaError
    ├── TransientFailure             # the WORLD blipped   → retry with backoff
    │   └── UpstreamUnavailable
    └── WorkerFatal                  # THIS WORKER is sick → hand off and exit
        ├── DeviceError              # CUDA OOM, device lost
        └── ResourceExhausted        # disk, host memory
```

Only the three leaves of `VideoflowRuntimeError` matter to the hot path; they are
the **dispositions**. The rest of the tree is for humans and exit codes.

### 1.3 The base class

Written in this repo's house style (space-before-colon annotations,
triple-single-quote docstrings with `- Arguments:` bullets):

```python
class VideoflowError(Exception):
    '''
    Base of every error videoflow raises on purpose.

    - Arguments:
        - message: what went wrong.
        - remedy: what the reader should *do* about it. The convention of naming \
            the fix in the message is the best thing about the current error \
            strings; this promotes it from prose to a field so the CLI, the DLQ \
            inspector and the Kubernetes termination log can all render it the \
            same way.
        - context: structured key/values (node, trace_id, replica, path). Never \
            interpolated into ``message`` — they are emitted as log fields and \
            proto fields so they stay queryable.

    Class attributes:
        - code: stable, greppable identifier (``VF_GRAPH_CYCLE``). The message may \
            be reworded freely; the code may not. Metrics and DLQ queries key on it.
        - exit_code: the process exit status when this error reaches the CLI (§3.2).
    '''
    code : str = 'VF_UNKNOWN'
    exit_code : int = 1

    def __init__(self, message : str, remedy : str | None = None,
                **context : Any) -> None:
        super().__init__(message)
        self.message = message
        self.remedy = remedy
        self.context = context

    def to_proto(self) -> error_pb2.Error:
        '''The wire form (§1.5) — for DLQ headers, ABORT markers, and the SDKs.'''
```

`VideoflowRuntimeError` adds one field:

```python
class VideoflowRuntimeError(VideoflowError):
    disposition : str = TRANSIENT       # POISON | TRANSIENT | WORKER_FATAL
```

### 1.4 Classifying exceptions you don't own

Users cannot subclass `torch.cuda.OutOfMemoryError`, and a node that raises a plain
`ValueError` must keep working. So classification is a registry, following the
extension-seam shape CLAUDE.md already prescribes (module-level registry seeded
with built-ins, explicit `register_*()`, lookup that raises `ValueError` naming
known values):

```python
_CLASSIFIERS : list[tuple[type[BaseException], str]] = [
    (UnicodeDecodeError, POISON),
    (json.JSONDecodeError, POISON),
    (ConnectionError, TRANSIENT),
    (TimeoutError, TRANSIENT),
    (MemoryError, WORKER_FATAL),
]

def register_error_classifier(exc_type : type[BaseException],
                            disposition : str) -> None:
    '''
    Map a third-party exception type onto a disposition, so the retry ladder can
    reason about failures from libraries videoflow does not own. Later
    registrations win, so a component may override a built-in mapping.

    - Raises:
        - ValueError: if ``disposition`` is not one of DISPOSITIONS.
    '''

def classify(exc : BaseException, default : str = TRANSIENT) -> str:
    '''Disposition of ``exc``: its own if typed, else the registry, else ``default``.'''
```

A CUDA-aware component registers `torch.cuda.OutOfMemoryError → WORKER_FATAL` once,
in its own package, and every flow that uses it inherits correct behaviour.

**The default matters more than the taxonomy.** An unclassified exception defaults
to `TRANSIENT`, which reproduces today's behaviour exactly — so adopting the
taxonomy changes nothing until someone opts in. Per-node override:
`ProcessorNode(on_unclassified_error = POISON)` for a node whose failures are known
to be data-shaped.

### 1.5 The wire form (this is not a Python feature)

Videoflow's contract is language-agnostic — components ship as OCI artifacts with
descriptors and may not be Python at all. A taxonomy that lives only in a Python
class hierarchy is invisible to half the system. So the *normative* artifact is a
proto message, and the Python classes are an ergonomic mapping onto it:

```protobuf
// spec/proto/videoflow/v1/error.proto
message Error {
  string code        = 1;   // VF_POISON_DECODE, VF_DEVICE_OOM, ...
  string message     = 2;
  string remedy      = 3;
  Disposition disposition = 4;
  string node        = 5;
  string trace_id    = 6;
  uint32 num_delivered = 7;
}
enum Disposition {
  DISPOSITION_UNSPECIFIED  = 0;
  DISPOSITION_POISON       = 1;
  DISPOSITION_TRANSIENT    = 2;
  DISPOSITION_WORKER_FATAL = 3;
}
```

This replaces `VF-Error: repr(exc)[:256]` — free text that cannot be aggregated,
counted, or alerted on — with something a dashboard can group by.

---

## 2. The failure ladder, driven by disposition

`fail_inputs` becomes a lookup rather than a branch on `flow_type`:

| Disposition | REALTIME | BATCH |
|---|---|---|
| `POISON` | sample to DLQ (§5.2), then `term()` | **DLQ immediately**, `term()` — zero retries |
| `TRANSIENT` | `term()` (freshest wins) | `nak` with jittered backoff up to the budget, then DLQ |
| `WORKER_FATAL` | `nak` (hand to another replica), trip the breaker | `nak`, trip the breaker |

Three changes from today, each fixing a concrete waste or hazard:

**Poison messages skip the ladder.** A message that will never parse currently
burns four delivery attempts and ~14s of backoff before reaching the DLQ it was
always destined for. Dead-letter it on the first failure.

**Transient backoff gets jitter.** `min(2**n, 30)` is deterministic, so N replicas
that fail together retry together — a thundering herd against whatever just
recovered. `min(2**n, 30) * uniform(0.5, 1.5)`.

**`WORKER_FATAL` never dead-letters.** The message is good; this worker is not.
NAK it so a healthy replica (or this pod's replacement) gets it, and let the
breaker take the worker out.

### 2.1 The circuit breaker

The missing feedback loop. Data failures are sparse and independent; worker
failures are dense and correlated. A consecutive-failure counter tells them apart
without any classification at all — it is the safety net for everything the
taxonomy fails to classify.

```python
class ConsecutiveFailureBreaker:
    '''
    Trips when a worker fails ``threshold`` messages in a row — the signature of a
    sick *worker* rather than bad *data*. Any successful ack resets the count, so
    an interleaved poison message never trips it.

    A tripped breaker stops the run loop and exits non-zero: every un-acked input
    redelivers to a healthy replica or to this pod's replacement, instead of being
    shredded into the DLQ one message at a time by a worker that cannot succeed.
    '''
    def __init__(self, threshold : int = 10) -> None: ...
```

The failure mode this exists for: one pod's GPU wedges at 03:00 and, by 03:20, ten
thousand perfectly good messages are in the DLQ with `CUDA out of memory` and the
flow looks healthy from the outside. With the breaker, the pod dies in ten
messages, Kubernetes replaces it, and the ten redeliver.

---

## 3. Boundaries

I would define **five**, one more than exists today. The new one is the seam where
user code becomes framework-legible.

### 3.1 Authoring — raise, never catch, and *collect*

Everything statically checkable is checked before the flow leaves the building
process. One change: **validation collects diagnostics instead of raising on the
first problem.** Today you fix the duplicate node name, rerun, and discover the
cycle; fix that, rerun, discover the join needs `partition_by`. A compiler would
have told you all three at once.

```python
@dataclass(frozen = True)
class Diagnostic:
    severity : str          # 'error' | 'warning'
    code : str
    node : str | None
    message : str
    remedy : str | None

def validate(flow : Flow) -> list[Diagnostic]:
    '''Every problem in one pass. Empty list ⇒ the flow is structurally sound.'''
```

`Flow.__init__` still raises `GraphError` on the first *error*-severity diagnostic
(keeping today's contract), but it raises one carrying the whole list, and the CLI
prints all of them. This also gives warnings a home — "this producer's output is
never read" is worth saying and is not worth failing on.

### 3.2 Deploy / CLI — one converter, meaningful exit codes

Replace ~34 scattered `raise SystemExit(str(e)) from e` with a single top-level
handler. The scattering is not a style problem: it means every new call site has to
remember, and one that forgets prints a traceback at a user.

```python
def main(argv : list[str] | None = None) -> int:
    try:
        return _dispatch(build_parser().parse_args(argv))
    except VideoflowError as e:
        _render(e)                              # code, message, remedy, context
        if os.environ.get('VF_DEBUG'):
            traceback.print_exc()
        return e.exit_code
    except KeyboardInterrupt:
        return 130
```

Exit codes carry the class, so CI and wrapper scripts can triage without parsing
stderr:

| Code | Meaning | Retry the command? |
|---|---|---|
| 0 | success | — |
| 2 | `VideoflowUserError` — the flow or config is wrong | no, fix the code |
| 3 | `VideoflowEnvironmentError` — cluster/broker/registry | maybe, after fixing infra |
| 4 | the flow ran and nodes failed | look at the DLQ |
| 5 | the flow stalled and was aborted | look at the stall reason |
| 130 | interrupted | — |

Today all of these are exit 1.

### 3.3 Worker bootstrap — crash fast, but *legibly*

Keep the crash-fast behaviour; make the reason machine-readable. Kubernetes reads
`terminationMessagePath` (default `/dev/termination-log`) and surfaces its contents
in `pod.status.containerStatuses[].state.terminated.message` — which the deploy
watchdog can read directly instead of scraping 80 lines of pod log:

```python
def _write_termination_reason(e : VideoflowError) -> None:
    '''
    Structured cause into /dev/termination-log so the k8s API carries it. Lets
    rollout_report say 'node detector: VF_DEVICE_OOM — model exceeded 16GiB'
    rather than 'crash-looping, see the logs above'.
    '''
```

Set `terminationMessagePolicy: FallbackToLogsOnError` in the pod spec so that even
an unstructured crash surfaces its last log lines through the same channel.

### 3.4 The node-call boundary (new)

This is the seam that does not exist today. `except Exception as e` in
[task.py:216](../videoflow/core/task.py#L216) catches the error and throws away
everything structural about it. Instead, one wrapper owns the transition from user
code to framework:

```python
def invoke_node(fn : Callable[..., Any], *args : Any,
                node : str, trace_id : str | None, replica : int) -> Any:
    '''
    Call a node method, converting whatever it raises into a classified
    ``VideoflowRuntimeError`` carrying the message's identity. The single place
    user exceptions become framework-legible: classification, enrichment and the
    structured log all happen here, so the task loop only ever sees typed errors
    and the messenger only ever sees dispositions.
    '''
```

The task loop then reads as policy, not as plumbing:

```python
try:
    output = invoke_node(self._processor.process, *inputs, node = ..., ...)
    self._messenger.publish_message(output, metrics)
    self._messenger.ack_inputs()
    self._breaker.record_success()
except VideoflowRuntimeError as e:
    self._messenger.fail_inputs(e)
    if self._breaker.record_failure(e).tripped:
        raise WorkerUnhealthy(...) from e
```

### 3.5 Run loop / messenger — mechanism only

`fail_inputs` stops making decisions and starts executing them: it receives a
disposition and a policy, and applies ack/nak/term/DLQ. All judgement moves up to
§3.4 and §2. This is what makes the ladder testable without a broker.

---

## 4. Local vs Kubernetes

### 4.1 The problem

Identical code, identical protocol, **opposite outcomes**: a mid-flow crash in
Kubernetes recovers (new pod → same durable → un-acked messages redeliver → drives
to EOS → exits 0); the same crash under `run-local` hangs forever, because nothing
restarts the worker and its children block on an EOS that will never come.

That is backwards. The development environment should be the *strictest* mirror of
production, because it is where recovery is supposed to be exercised.

### 4.2 Supervision becomes part of the engine contract

```python
@dataclass(frozen = True)
class SupervisionPolicy:
    '''
    How an engine responds to a worker exiting non-zero. Defaults mirror the
    Kubernetes Job semantics the manifests already encode (backoffLimit 3), so the
    two engines agree by construction rather than by coincidence.
    '''
    max_restarts : int = 3
    backoff_seconds : tuple[float, ...] = (10.0, 20.0, 40.0)
    restart_on : frozenset[str] = frozenset({TRANSIENT, WORKER_FATAL})
    # POISON is absent on purpose: a worker that died of bad data will die again.
```

`KubernetesExecutionEngine` renders it into `backoffLimit` / `restartPolicy`.
`LocalProcessEngine` grows a supervisor thread that implements it directly. The
policy is one object, defined once, honoured twice.

### 4.3 One event stream, two mechanisms

Both engines emit the same structured lifecycle events to one observer:

```
NodeStarted(node, replica, attempt)
NodeExited(node, replica, exit_code, reason : Error | None)
NodeRestarted(node, replica, attempt, delay)
NodeGaveUp(node, replica, attempts)          # → triggers §4.4
FlowStalled(reason, detail)
```

The CLI renders them identically whether they came from `kubectl get pods` polling
or from `waitpid`. `dump_failed_logs` and `report_failures` — two functions doing
the same job differently today — become one renderer over one event type.

### 4.4 Closing the EOS hole: `MSG_TYPE_ABORT`

The deepest bug in the current design: a producer exception kills the worker
*before* `publish_stop_signal()`, so downstream waits forever on an EOS that is
never coming. Fixing it needs a protocol addition, because a hang is not something
you can fix downstream — the information simply is not on the wire.

`MessageType` in [envelope.proto](../spec/proto/videoflow/v1/envelope.proto) has
room (`DATA = 1`, `EOS = 2`, so `ABORT = 3`). An ABORT marker rides the **existing
`_eos` subject**, which means it reuses the per-replica EOS consumers and the
provisioning interest anchor unchanged — no new topology.

Semantics: a node that receives ABORT from any parent finishes and acks its
in-flight group, propagates ABORT to its children, and exits non-zero with the
originating `Error` attached. Failure walks the graph the way EOS does.

**Three layers, because each covers the previous one's blind spot:**

1. **In-band ABORT**, published by a worker that is terminating on a known-fatal
   condition (breaker tripped, retries exhausted, `open()` failed). Fast and
   carries the cause — but a hard crash cannot publish anything.
2. **Supervisor control-abort** on `NodeGaveUp`, over the existing control subject.
   Covers SIGKILL and OOM — but requires the supervisor to be alive and able to
   reach the broker, which the k8s teardown path already documents as unreliable.
3. **Receiver-side progress deadline** (§5.3). Covers everything else, including a
   partitioned network. Slowest, but it cannot be defeated.

Per CLAUDE.md this is an observable wire change: it needs
`spec/rfcs/0005-abort-marker.md`, new golden vectors under `spec/vectors/`, and
`ABORT-1…n` requirement IDs in `PROTOCOL.md` cited from the commit message.

---

## 5. BATCH vs REALTIME

### 5.1 Unbundle the axes

`flow_type` is one string that currently decides seven things: retention policy,
discard policy, `max_deliver`, join defaults, workload kind, blob TTL, and failure
semantics. That bundling is *mostly* right — the correlations are real — but it
makes the common mixed case inexpressible.

I would keep the presets and make each axis individually overridable:

| Axis | REALTIME preset | BATCH preset |
|---|---|---|
| Loss tolerance | drop on full | block on full |
| Retry budget | 0 | 3 |
| DLQ | sampled (§5.2) | full |
| Join timeout / missing | 10s / drop | ∞ / wait |
| Restart | always | until `backoffLimit` |
| Stall detection | liveness probe | **progress deadline** (§5.3) |
| Completion | control stop | EOS drain |

**The case that forces this:** a REALTIME flow whose final sink writes alerts to a
database. The frame pipeline in front of it genuinely wants freshest-wins and no
retries. The sink genuinely wants at-least-once with a DLQ — a dropped alert is a
missed incident, not a stale frame. Today you cannot say that; the flow type
decides for every node. So:

```python
AlertSink(delivery = AT_LEAST_ONCE, name = 'alerts')   # overrides the flow preset
```

Loss tolerance is a property of *what a node does with a message*, not of the flow
it happens to live in.

### 5.2 REALTIME failures must stop being invisible

REALTIME failure today is `term()`: the message is gone, and the only trace is a
log line and an undimensioned counter. "It's realtime, we drop things" is true of
*load shedding* and false of *exceptions* — a node raising is a bug, and the
evidence is being deleted.

**Sampled dead-lettering**: the first N failures per (code, node) per minute go to
the DLQ; the rest increment a counter. Bounded storage, and every distinct failure
mode is represented by at least one inspectable specimen with its payload intact.

### 5.3 BATCH needs a progress deadline

Every BATCH node is a Job, Job pods have their probes stripped,
`wait_for_completion` has no overall timeout, and `activeDeadlineSeconds` is unset.
So a wedged BATCH worker hangs the run until somebody notices and hits Ctrl-C. The
liveness probe that would have caught this exists — it is simply not attached to
Jobs.

A wall-clock deadline is the wrong tool (a legitimately slow model looks identical
to a wedge). The right signal is **progress**:

```python
class ProgressDeadline:
    '''
    Trips when nothing has been acked for ``timeout`` while the node's durable
    still reports pending messages — work is available and none is being done.
    Immune to the slow-node false positive a wall-clock deadline suffers, because
    a slow node still acks, just rarely.
    '''
```

On trip: log the stall (reusing today's excellent `_log_drain_stall` diagnostics),
publish ABORT, exit non-zero. With `activeDeadlineSeconds` as a crude outer
backstop on the Job for the case where the worker is too wedged to run its own
deadline check.

### 5.4 The DLQ becomes a first-class object

Two problems, one root cause — the DLQ is run-scoped and write-only:

- `delete_run_streams` deletes it, and BATCH deploys tear down in a `finally`. The
  7-day retention is fiction unless you passed `--keep`. **Fix:** scope the DLQ to
  the *flow* (`vf-{flow}-dlq`), with `run_id` as an entry field. Teardown of a run
  no longer destroys the forensic record of that run.
- Nothing consumes it. **Fix:** make it a queue rather than a graveyard:

```
videoflow dlq ls     --flow-id F [--code VF_DEVICE_OOM] [--node detector]
videoflow dlq show   --flow-id F --id 42
videoflow dlq replay --flow-id F [--code ...] [--dry-run]
videoflow dlq purge  --flow-id F --older-than 7d
```

`replay` re-publishes the original bytes to the origin node's input subject with a
fresh message id and a `VF-Replay` header carrying the original run. That closes
the loop: fix the bug, redeploy, replay the DLQ, and the run is whole.

And it makes the metric alertable: `videoflow_dlq_depth{flow,node,code}`.

---

## 6. Observability: one taxonomy, three sinks

Every failure produces exactly three artifacts, all keyed by the same `code`:

| Sink | Content | Consumer |
|---|---|---|
| Structured log | `code`, `node`, `replica`, `trace_id`, `disposition`, `remedy` | a human, `kubectl logs`, a log aggregator |
| Metric | `videoflow_errors_total{code,node,disposition}` | alerting; `messages_failed` today has no dimensions, so "what is failing" is unanswerable |
| DLQ entry / termination log | the full `Error` proto plus the original payload bytes | forensics and replay |

The rule that makes this work: **the code is chosen when the exception is defined,
not when it is logged.** Free-text error strings cannot be aggregated, and
`repr(exc)[:256]` is free text.

---

## 7. What I would not change

Enough of the current design is right that listing it matters as much as the
critique:

- **Ack-after-process.** Correct, load-bearing, and stated as `DELIV-1`.
- **Poison-decode terminates immediately.** Already the right rule; §2 generalizes
  it from "undecodable" to "any poison."
- **"Never silently drop"** — nak when the DLQ publish itself fails. Exactly right.
- **Teardown in `finally`**, on success, failure, stall, or Ctrl-C.
- **Two-phase provision apply**, so BATCH interest exists before the first publish.
- **The probe-derived rollout deadline** — a change to the probe retunes the
  watchdog automatically. A small, excellent piece of design.
- **Unschedulable treated as fatal**, scoped per pod so a stale autoscaler event
  cannot mute the watchdog.
- **Two-poll debouncing**, symmetric for success and failure.
- **Error messages that name the fix.** The best convention in the repo. §1 makes
  it a field so it cannot be forgotten.
- **The drain-stall diagnostics.** The model for what a good "why is this stuck"
  log looks like; §5.3 reuses it verbatim.

---

## 8. Sequencing

Ordered so that nothing observable changes until the foundations are in place, and
each step is independently shippable:

| # | Step | Breaking? | Fixes |
|---|---|---|---|
| 1 | Taxonomy + classification registry + `invoke_node` | No — unclassified defaults to `TRANSIENT`, today's behaviour | foundation |
| 2 | Disposition-driven `fail_inputs`, jittered backoff | Behaviour-identical until nodes classify | wasted retries |
| 3 | Circuit breaker | No | the poison-worker shredder |
| 4 | Exit codes + single CLI converter + termination log | Exit code 1 → 2/3/4/5 | untriageable CI failures |
| 5 | Collect-all validation | No (still raises) | fix-one-rerun-repeat |
| 6 | `SupervisionPolicy` + local supervisor + one event stream | No | local/k8s divergence |
| 7 | Progress deadline | No | wedged BATCH hangs |
| 8 | Flow-scoped DLQ + `dlq` subcommands | Stream rename | DLQ deleted at teardown; write-only DLQ |
| 9 | `MSG_TYPE_ABORT` | **Wire change — RFC 0005 + golden vectors + `ABORT-n` IDs** | the EOS hole |
| 10 | Per-node `delivery=` override | No | mixed-criticality flows |

Steps 1–7 are pure additions to the Python layer. Only 8 and 9 touch the protocol,
and 9 is the one that needs an RFC, new vectors, and a version note — which is also
the honest reason it is last rather than first, despite being the deepest fix.
