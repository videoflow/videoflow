# Current validity audit — 2026-09-20

**Status: Partially resolved.** Reviewed against core commit `73e3b84` and contrib
commit `4573c4c`. The numbered findings below supersede the historical notes at
the end of this file; those notes are preserved verbatim. Duplicate item 5 is
distinguished by subject. “Resolved” means the requested implementation or
research exists; live infrastructure verification is reported separately.

## 3 — Kubernetes integration tests without GPUs

**Status: Resolved.** CPU-only cluster tests now exercise
[clean BATCH completion and failed nodes](../tests/integration/k8s/test_k8s_engine.py#L59),
[CLI failure reporting](../tests/integration/k8s/test_k8s_deploy_failure.py#L68),
and [worker-fatal recovery and poison messages](../tests/integration/k8s/test_k8s_solutions.py#L100).
The [CI Kubernetes job](../.github/workflows/ci.yml#L103) provisions kind and runs
this bucket. **Remaining work:** none for introducing the requested coverage;
these live tests were inspected, not rerun during this audit.

## 5 — Kubernetes Python client research

**Status: Resolved for the research request; migration is a design follow-up.**
The [existing comparison](../consultations/kubectl-vs-python-client.md) covers
read/watch paths, apply semantics, authentication, dependencies, and migration
tiers. The [engine](../videoflow/engines/kubernetes.py#L320) still uses kubectl
subprocesses. **Remaining work:** decide separately whether to migrate; continued
kubectl use does not itself make this a bug. External client/API claims in that
historical research were not revalidated here.

## 4 — Error-handling bugs and alignment with the design document

**Status: Partially resolved.** The relevant historical proposal is
[ideal_error_handling.md](../consultations/ideal_error_handling.md), rather than
a current normative `ideal.md`. Much of its design is implemented: the
[error taxonomy](../videoflow/core/errors.py),
[node-call boundary and supervision](../videoflow/core/task.py#L229),
[collected graph diagnostics](../videoflow/core/graph.py#L122), and
[structured error metrics](../videoflow/runtime/health.py#L334).
The focused tests listed below passed, but the proposal is not a blanket
statement of current behavior.

| Claim or discrepancy | Current evidence | Remaining work |
| --- | --- | --- |
| Worker-fatal errors should wait for the breaker | [RFC 0005](../spec/rfcs/0005-error-taxonomy-and-abort.md#L182) deliberately supersedes that part of the proposal: classified worker-fatal errors exit immediately; the breaker covers repeated unclassified failures. | Treat this as an intentional redesign, not a missing fix. |
| Both engines honor the same restart filtering | [Local supervision](../videoflow/engines/local.py#L541) consults the disposition. [Job rendering](../videoflow/deploy/manifests.py#L803) carries the restart count but no `podFailurePolicy` translating `restart_on`. The [maintained guide](../docs/source/user-documentation/error-handling-and-recovery.rst#L246) overstates poison-death parity. | Reconcile the Kubernetes contract and documentation; identical restart counts do not prove identical restart filtering. |
| Both engines emit one shared lifecycle event stream | The [proposal](../consultations/ideal_error_handling.md#L417) describes this. [Local events](../videoflow/engines/local.py#L549) exist, while the [Kubernetes engine](../videoflow/engines/kubernetes.py) does not emit the same `EventLog` lifecycle events. | Keep as an incomplete design capability, distinct from the implemented local restart fix. |
| DLQ-depth metric | The [proposal](../consultations/ideal_error_handling.md#L558) names `videoflow_dlq_depth`; the [health exporter](../videoflow/runtime/health.py#L338) implements error counters, but no implementation of that depth metric was found. | Keep the metric as a design follow-up, not an implemented guarantee. |
| Remaining correctness defects | The [local delivery-cap mismatch](01_small_bugs.md), [missing-Job/API-error completion hole](03_bug-2.md), and FlowStalled catches below remain. | Retain these concrete defects instead of leaving only an unbounded “review error handling” task. |

## 5 — Why BATCH and REALTIME differ under node failure

**Status: Resolved as an explanation; the difference is intentional policy.**
[Delivery defaults](../videoflow/core/policies.py#L507) select at-least-once/full
DLQ for BATCH and best-effort/sampled DLQ for REALTIME, with per-node overrides.
[Workload selection](../videoflow/deploy/manifests.py#L393) makes BATCH nodes Jobs;
long-running REALTIME workers use controllers that restart them, while finite
producers can still be Jobs.

Both delivery policies [NAK worker-fatal failures](../videoflow/core/policies.py#L452),
but default best-effort consumers retain
[`max_deliver=1`](../videoflow/messaging/topology.py#L301). Replacing the worker
therefore does not guarantee redelivery of its input. **Remaining work:** none
to make both presets identical; retain the delivery-cap qualification when
documenting recovery, and track the separate BATCH residual in
[01_small_bugs.md](01_small_bugs.md).

## 6 — DeviceError and fatal-worker replacement

**Status: Resolved for worker exit/replacement, with policy limits.**
[`DeviceError`](../videoflow/core/errors.py#L350) is worker-fatal;
[`_record_failure`](../videoflow/core/task.py#L246) immediately ends that worker
after failure handling. [Local supervision](../videoflow/engines/local.py#L541)
restarts recoverable deaths, bounded by its policy; Kubernetes
[Jobs](../videoflow/deploy/manifests.py#L812) have the configured restart budget.
Terminal deaths and exhausted budgets are not promises of unlimited recovery.
The [task regression](../tests/test_task_error_paths.py#L168) verifies handoff and
exit; the [cluster recovery test](../tests/integration/k8s/test_k8s_solutions.py#L100)
exercises replacement. **Remaining work:** no missing replacement mechanism for
the original claim; preserve the best-effort and local retry-budget caveats above.

## 8 — mypy versus editor diagnostics

**Status: Unverified for the original symptom.** The original editor diagnostics
and editor type-checker configuration are unavailable. Current
[mypy configuration](../pyproject.toml#L169) deliberately ignores missing imports,
allows implicit optionals, and disables checking untyped function bodies;
override checking is enabled. The [hook](../.pre-commit-config.yaml#L34) runs
mypy in the project environment. The actual audit command passed for 110 source
files with mypy 2.3.0. A passing run does not establish equivalence with an editor
using different rules. **Remaining work:** compare a concrete editor diagnostic,
interpreter, and checker settings before deciding whether stricter configuration
is wanted. Do not mark the unexplained discrepancy fixed.

## 9 — Release tagging and Docker publishing workflows

**Status: Resolved.** The [publish workflow](../.github/workflows/publish.yml#L242)
builds and pushes versioned CPU/CUDA base images. Its
[release step](../.github/workflows/publish.yml#L297) uses `gh release create`
with the target SHA, creating the tag and release. **Remaining work:** none for
adding these workflows. Hosted runs, credentials, package visibility, and registry
availability were not checked; workflow source is the evidence for this verdict.

## 9.1 — Contrib documentation using published images

**Status: Resolved.** The contrib
[quickstart](../../videoflow-contrib/README.md#L20) explains pulling the published
base, and its [development section](../../videoflow-contrib/README.md#L294)
distinguishes source-checkout builds from released installs. Core
[build selection](../videoflow/deploy/build.py#L126) derives and pulls the published
base reference. **Remaining work:** none for the requested documentation change.
Building the solution-specific image remains intentional; the prebuilt image is
its base, not every user's solution.

## 10 — Redis deletion and retention

**Status: Partially resolved.** Default Redis workers select the redesigned
[`RedisPayloadStore`](../videoflow/runtime/worker.py#L573). Its
[final-reader release](../videoflow/wire/redis_payload_store.py#L553) removes
payload and bookkeeping with generation fencing and idempotent reader identity.
[Reconciliation](../videoflow/wire/redis_payload_store.py#L693), when the worker has
a durable shared runtime ledger, is invoked at
[startup](../videoflow/messaging/nats_messenger.py#L587) and
[periodically](../videoflow/messaging/nats_messenger.py#L865).
Outstanding obligations, TTL-only objects, and
[DLQ pins](../tests/test_redis_payload_store.py#L340) intentionally retain data;
their presence does not by itself prove a leak.

**Remaining work:**

- **Legacy release race — still valid:** direct compatibility callers and the
  registered `unix://` path can still reach
  [`RedisBlobStore.release`](../videoflow/wire/serialization.py#L220), whose
  separate EXISTS/DECR operations permit counter-expiry races. The existing
  [negative control](../tests/test_redis_payload_store.py#L311) demonstrates the
  old defect while verifying the new store's protection. Do not attribute this
  residual to the default redesigned `redis://`/`rediss://` worker path.
- **Runtime-ledger retention — design follow-up:**
  [CAS and append](../videoflow/runtime/redis_runtime_store.py#L95) do not set TTLs.
  Some records are explicitly deleted, but no general run-ledger pruning was
  found. [Effect retention](../videoflow/backends/runtime.py#L819) checks logical
  age rather than physically expiring all stored records. A fake-store probe
  confirmed physical presence and TTL `-1` after logical expiry. Define the
  intended retention policy separately; this is not proof that retained recovery
  history violates an existing contract.

## Unnumbered — FlowStalled caught as RuntimeError

**Status: Still valid.** The watchdog
[raises FlowStalled](../videoflow/engines/kubernetes.py#L515), which
[inherits VideoflowEnvironmentError](../videoflow/core/errors.py#L278), not
RuntimeError. The deploy path still
[catches RuntimeError](../videoflow/deploy/cli.py#L744), so the intended
`Flow aborted:` wrapping at line 766 is bypassed. Cleanup remains in `finally`,
and the typed CLI exit code remains 5.

The same stale catch in
[`join_task_processes`](../videoflow/engines/kubernetes.py#L661) lets FlowStalled
escape despite its documented log-and-return contract. A mocked probe confirmed
that propagation. **Remaining work:** align both exception handlers and their
regressions with the actual exception type. The current implementation is in
`deploy/cli.py`; the historical top-level CLI line references are stale.

## Verification recorded during the review

These are separate selections, some overlapping; their counts must not be summed
as unique coverage. Tests preceded this documentation-only update.

```sh
.venv/bin/python -m pytest tests/test_error_policy.py tests/test_error_taxonomy.py tests/test_error_reporting.py tests/test_graph_diagnostics.py tests/test_supervision.py tests/test_task_error_paths.py -q
# 116 passed

.venv/bin/python -m pytest -q tests/test_redis_payload_store.py tests/test_obligation_ledger.py tests/test_deploy_cli.py tests/test_k8s_watchdog.py tests/test_build.py
# 93 passed

.venv/bin/python -m pytest -q tests/test_flow_runtime.py tests/test_backends_memory_payload.py tests/test_store_backpressure.py
# 75 passed

.venv/bin/python -m mypy videoflow
# Success: no issues found in 110 source files (mypy 2.3.0)
```

The separate [runtime selection](01_small_bugs.md) had 209 passes and one
macOS-specific Docker-URL assertion failure: it expected Linux `localhost`, while
the implementation rendered `host.docker.internal`. It is recorded there, not
silently treated as a passing suite. Mocked probes also checked the FlowStalled
join behavior, broker-teardown failure path, and Redis ledger retention.

**Not verified live:** GPU hardware/Operator behavior, Kubernetes deployments,
NATS/Redis integration, or hosted release execution. Existing integration tests
and workflow definitions were inspected; their presence is not evidence of a
successful live run during this audit.

## Historical notes (preserved)

3. Figure out how to create a Kubernetes integration test for non gpu stuff (errors, etc.)
5. Do research on if we should use kubernetes python api vs what we are using now.
4. Ask the agent to threview the code and (1) find bugs in error handling, (2) find inconsistencies with the documentation in ideal.md file compared to the tests.
5. Ask from ideal_error_handling.md file, why is it that BATCH and REALTIME behave differently under node failure.
6. Confirm that DeviceErrors and other kind of fatal errors reintroduce the worker back.
8. Check why mypy errors is not detecting things that the editor detects in the ui.
9. Introduce github workflows that: (a) tag the repo with a new tag (b) compile a docker image
9.1. Update videoflow-contrib documentation to use published image instead compiling from scratch.
10. Check that Redis data is getting deleted properly as indicated by code.

A latent bug I did not fix, since it's outside this scope: cli.py:373 catches RuntimeError around wait_for_completion, but FlowStalled is a VideoflowError, not a RuntimeError. Cleanup still runs and the exit code is still 5, but the Flow aborted: message at line 395 is unreachable. Worth its own change.
