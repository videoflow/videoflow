# Runtime failure handling audit — 2026-09-20

**Status: Partially resolved.** Reviewed against videoflow core commit `73e3b84`.
The redesign fixes the missing
circuit breaker, local restart supervision, and ordinary processor/consumer
hang detection. Keep this TODO open for the narrower retry-budget and hang
coverage gaps below; the historical description no longer describes the whole
runtime. This is a source review with local unit/model/process checks, not live
broker, Kubernetes, CUDA, or PyTorch verification.

## Claim-by-claim assessment

| Historical claim | Status | Current evidence and limits |
| --- | --- | --- |
| A poisoned-but-alive worker has no consecutive-failure circuit breaker. | **Fixed.** | The default threshold is 10, success resets it, and a trip raises `WorkerUnhealthy`: [supervision.py](../videoflow/core/supervision.py#L35). Workers install it for processors and consumers: [worker.py](../videoflow/runtime/worker.py#L667). Explicit `worker_fatal` errors stop the task immediately: [task.py](../videoflow/core/task.py#L229). |
| An OOM/exception in `open()` or producer `next()` exits the worker. | **Still true; supervised lifecycle behavior.** | Both calls cross the classification boundary and errors propagate: [open lifecycle](../videoflow/core/task.py#L261), [producer loop](../videoflow/core/task.py#L344). Contrary to the historical wording, failed `open()` deliberately does **not** call `close()`; an error after successful open does. Terminal deaths publish ABORT, while recoverable deaths defer the decision to supervision: [abort handling](../videoflow/core/task.py#L188). |
| Kubernetes REALTIME workloads restart, but default in-flight messages are not redelivered. | **Still true for default best-effort delivery; not a universal guarantee.** | Best-effort durables retain `max_deliver=1`, even with a durable ledger: [topology.py](../videoflow/messaging/topology.py#L301). Node delivery overrides can select at-least-once. Startup rollout now reports repeated container failures: [kubernetes.py](../videoflow/engines/kubernetes.py#L563). Finite REALTIME producers are Jobs: [manifests.py](../videoflow/deploy/manifests.py#L393). |
| BATCH nodes are Jobs with `restartPolicy: Never` and three retries. | **Still true by default, now policy-driven.** | Job backoff comes from `SupervisionPolicy.max_restarts`: [manifests.py](../videoflow/deploy/manifests.py#L803), [default policy](../videoflow/core/supervision.py#L254). Completion checks terminal Job conditions, not failed-pod counters, so one failed attempt does not prematurely fail a retrying Job: [kubernetes.py](../videoflow/engines/kubernetes.py#L299). |
| Crashes can exhaust the broker delivery cap without a DLQ record. | **Partially fixed; keep open.** | Durable/shared ledgers separate the application retry budget from the broker cap, but local provisioning does not enable that cap consistently. See the reproduced mismatch below. Non-durable configurations retain finite broker caps. |
| The local engine never restarts dead workers and waits forever for missing EOS. | **Fixed for exited workers.** | The supervisor observes child exits concurrently, restarts eligible failures, and repeatedly announces flow termination when it gives up: [local.py](../videoflow/engines/local.py#L461), [restart and abort](../videoflow/engines/local.py#L541). Default local policy allows three restarts: [supervision.py](../videoflow/core/supervision.py#L261). |
| BATCH callback hangs are unbounded because Job probes are stripped. | **Fixed for ordinary processor/consumer hangs; narrower gaps remain.** | A separate watchdog checks the pending-work deadline and exits the worker: [watchdog.py](../videoflow/runtime/watchdog.py#L94), [exit callback](../videoflow/runtime/worker.py#L203). BATCH Jobs also have a 24-hour outer deadline: [constant](../videoflow/deploy/manifests.py#L86), [rendering](../videoflow/deploy/manifests.py#L819). See remaining coverage gaps below. |

## Remaining work

### 1. Align local provisioning with the durable ledger retry budget

**Confirmed defect; high priority.** Local provisioning omits `ledger_budget`,
whose default is false, so an ordinary BATCH durable is provisioned with
`max_deliver=4`: [local provisioning](../videoflow/engines/local.py#L274),
[provisioning signature](../videoflow/messaging/topology.py#L573),
[cap selection](../videoflow/messaging/topology.py#L301).
The same engine gives workers a file runtime store:
[worker environment](../videoflow/engines/local.py#L318),
[store URL](../videoflow/engines/local.py#L335).
That store reports durable/shared capabilities:
[runtime_store.py](../videoflow/backends/memory/runtime_store.py#L169).
Workers therefore assume `max_deliver=-1`:
[nats_messenger.py](../videoflow/messaging/nats_messenger.py#L460).

The JetStream adapter binds an existing durable and returns the configuration
mismatch, but messenger initialization only checks join credit:
[adapter binding/read-back](../videoflow/messaging/jetstream_backend.py#L470),
[messenger initialization](../videoflow/messaging/nats_messenger.py#L554).
An isolated probe invoked the installed `nats-py` `pull_subscribe()` with mocked
API responses and confirmed that it does not update an existing consumer.
Capturing actual local-engine provisioning arguments and worker environments
produced:

```text
provision_ledger_budget: False
actual_provisioned_max_deliver: 4
runtime_store_scheme: file
runtime_durable_shared: True
worker_expected_max_deliver: -1
consumer_updated: False
config_mismatches: ('max_deliver: requested -1, effective 4',)
```

These probes used mocked process launches and APIs; they did not contact a
broker. The resulting crash-exhaustion risk follows from the verified cap
mismatch, rather than from a live crash/recovery experiment. There is no active
feature-switch guard around the local ledger assignment; the comment saying
"under the switch" is stale: [local.py](../videoflow/engines/local.py#L899).

The Kubernetes provisioner already derives the cap from the same runtime store
as workers: [provision.py](../videoflow/runtime/provision.py#L157).
Ledger attempt accounting excludes worker-fatal failures:
[runtime.py](../videoflow/backends/runtime.py#L492).
Without a durable/shared ledger, finite-cap exhaustion is only observed through
broker advisories; that observation does not itself create a DLQ record:
[advisory tracking](../videoflow/messaging/jetstream_backend.py#L507),
[unresolved work](../videoflow/messaging/jetstream_backend.py#L938).

Required follow-up: pass the local store's verified ledger-budget decision into
provisioning and enforce relevant subscription read-back mismatches. Add a
regression that exercises local provisioning plus worker binding, and a broker
test that crashes a worker at the former delivery limit and verifies completion
or an explicit terminal outcome.

### 2. State the limits of worker-fatal recovery and the breaker

`DeliveryPolicy.action_for(WORKER_FATAL)` returns NAK in either mode:
[policies.py](../videoflow/core/policies.py#L452).
However, default best-effort consumers have a broker cap of one; NAK does not
make redelivery possible past that cap. Preserve this distinction in recovery
documentation and tests rather than promising universal worker-fatal recovery.
Whether REALTIME should retain this loss policy is a product decision, not a
missing implementation of the default best-effort contract.

The breaker also runs **after** the failed input is settled:
[task.py](../videoflow/core/task.py#L447).
It prevents unlimited processing by a sick worker, but does not recover inputs
already dropped or dead-lettered before the threshold. Generic library errors
need classification or reach the breaker; this audit did not validate any
particular CUDA error against a real GPU.

### 3. Cover startup and producer hangs where a bounded run is required

The watchdog starts only after `open()` returns:
[task.py](../videoflow/core/task.py#L272).
Producers get no progress deadline/watchdog:
[worker.py](../videoflow/runtime/worker.py#L667).
Therefore a hung local `open()` or producer `next()` has no corresponding outer
supervisor deadline. A native call that indefinitely holds the GIL also prevents
the Python watchdog thread from running; this is a coverage inference, not a
reproduced CUDA hang. Kubernetes BATCH Jobs retain the 24-hour outer backstop,
and long-running REALTIME workloads have startup/liveness probes:
[manifests.py](../videoflow/deploy/manifests.py#L659).
Finite REALTIME producer Jobs have neither the BATCH outer deadline nor Job
probes: [Job rendering](../videoflow/deploy/manifests.py#L803).

Required follow-up: define a configurable startup/producer progress or outer
run deadline for the uncovered cases, with regression tests for a blocked
`open()` and `next()`. Do not reopen the already-fixed ordinary callback case.

## Validation performed

The following exact command completed with **209 passed, 1 failed**:

```sh
.venv/bin/python -m pytest -q tests/test_supervision.py tests/test_task_error_paths.py tests/test_watchdog.py tests/test_local_engine.py tests/test_batch_lifecycle.py tests/test_k8s_watchdog.py tests/test_health.py tests/test_error_taxonomy.py tests/test_unknown_states.py tests/test_backends_memory_messaging.py tests/test_obligation_ledger.py tests/test_runtime_policies.py
```

The failure is [test_workers_run_as_docker_containers_of_the_worker_image](../tests/test_local_engine.py#L538):
it expects the Linux URL `nats://localhost:4222` on this macOS host, where the
implementation rewrites it to `host.docker.internal`:
[platform handling](../videoflow/engines/local.py#L391).
The exact isolated rerun below also yielded **1 failed**, confirming the same
platform-dependent expectation rather than a passing full suite:

```sh
.venv/bin/python -m pytest -q tests/test_local_engine.py::test_workers_run_as_docker_containers_of_the_worker_image
```

The following exact command completed with **9 passed, 16 deselected**:

```sh
.venv/bin/python -m pytest -q tests/conformance/test_msg_recovery.py tests/conformance/test_run_progress.py -m 'level_model or level_process'
```

These checks include the breaker, callback watchdog, local restart/abort,
exhausted-work observation, and Job lifecycle behavior. Representative existing
regressions are [breaker task integration](../tests/test_task_error_paths.py#L201),
[blocked callback](../tests/test_watchdog.py#L222),
[outer BATCH deadline](../tests/test_batch_lifecycle.py#L131), and
[concurrent supervision conformance](../tests/conformance/test_run_progress.py#L279).
They do not substitute for live broker, Kubernetes, or GPU verification. No
application fixes were made during this audit.

## Historical notes (preserved)

1. A CUDA OOM in PyTorch is usually recoverable in-process, but a sticky CUDA error (illegal memory access, corrupted context) leaves the process alive and failing every message. Nothing detects that: /healthz stays green because the run loop is still beating (health.py only fails liveness on a stalled loop), and messages_failed is just a metric. A REALTIME flow silently drops everything; a BATCH flow dead-letters its whole stream. This is a real gap — there's no "N consecutive failures → unready/crash" circuit breaker.

OOM inside open() (model load) or a producer's next()
open() runs before the try block in NodeTask.run() (task.py:99), and ProducerTask only catches StopIteration/KeyboardInterrupt — so an OOM there propagates, close() runs via the finally, and the worker process exits non-zero. Recovery then depends on the engine and flow type (manifests.py:436-504):

- Kubernetes, REALTIME: processors/consumers are Deployments/StatefulSets (restartPolicy: Always) → the pod restarts indefinitely, going into CrashLoopBackOff if the OOM persists. In-flight messages the dead worker never acked are not recovered — max_deliver=1 means the broker won't redeliver them.
- Kubernetes, BATCH: every node is a Job with restartPolicy: Never and backoffLimit: 3 → up to 3 fresh pods, then the Job fails permanently (failed pods are kept so dump_failed_logs can print them). Unacked in-flight messages are redelivered after ack_wait (60s default) to a surviving replica or the restarted pod. One subtle nuance: each crash-death consumes a delivery attempt, and if a message exhausts max_deliver purely through crashes (never through a live fail_inputs), JetStream just stops delivering it — it never reaches the DLQ, because dead-lettering is done by a live worker.
- Local engine (run-local), either mode: there is no restart at all. local.py:239 just reaps processes and records non-zero exits for the end-of-run report. A dead mid-graph node means EOS never propagates past it, so downstream workers block forever and a BATCH run hangs until Ctrl-C.

One more GPU-specific failure shape: a CUDA call that hangs rather than raises. For long-running REALTIME pods the liveness probe catches this — /healthz returns 503 once the run loop stops beating and kubelet restarts the container. Job pods (all of BATCH) have probes deliberately stripped, so a hung BATCH worker sits until its messages' ack_wait redeliveries play out elsewhere or the run is killed.

Since I'm in plan mode but this was a question rather than a change request, I'll stop here. The two gaps that might interest you given the gap-analysis doc: no failure-rate circuit breaker for a poisoned-but-alive GPU worker, and crash-exhausted BATCH messages vanishing without a DLQ entry.
