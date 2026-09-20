# Flow ids and run ids: what namespaces a run, and who assigns it

*Two questions about identity — whether the broker's JetStream names are unique per
run, and where `flow_id` actually comes from.*

Sources: `videoflow/messaging/topology.py`, `videoflow/core/flow.py`,
`videoflow/deploy/cli.py`, `videoflow/deploy/manifests.py`,
`videoflow/deploy/compile.py`, `videoflow/wire/serialization.py`,
`videoflow/runtime/idempotency.py`, `solutions/`, `tests/integration/k8s/`.

Nothing here has been changed; this is the finding set.

---

## Q1 — Do the NATS JetStream channel names have randomization, so two different runs of the same flow don't clash?

Yes. The run scoping is there, and it is random by default.

**Every JetStream name is scoped by `flow_id` *and* `run_id`**
([topology.py:39-107](../videoflow/messaging/topology.py#L39-L107)):

| Thing | Format |
|---|---|
| data subject | `vf.{flow}.{run}.{node}` |
| stream | `vf-{flow}-{run}-{node}` |
| EOS subject | `vf.{flow}.{run}.{node}._eos` |
| control subject | `vf.{flow}.{run}._control.stop` |

**`run_id` defaults to a fresh random id** — `uuid.uuid4().hex[:12]`, in
[flow.py:123](../videoflow/core/flow.py#L123) (the `run-local` / in-process path) and in
[cli.py:199](../videoflow/deploy/cli.py#L199) (`videoflow deploy`). So two runs of the same
flow get entirely disjoint streams.

Durable consumer names deliberately carry **no** run id
([topology.py:54-59](../videoflow/messaging/topology.py#L54-L59)) — they don't need one, since
a durable lives on a stream that is already run-scoped. Teardown exploits the prefix:
`stream_label_selector` → `delete_run_streams` drops everything named `vf-{flow}-{run}-*`.

### Four places where it is intentionally *not* randomized

1. **The DLQ stream is flow-scoped, not run-scoped**: `vf-{flow}-dlq`
   ([topology.py:76](../videoflow/messaging/topology.py#L76)). Runs share it on purpose —
   teardown runs in a `finally` and would otherwise destroy the forensic record of the run that
   just failed. Attribution is preserved because the run id is in the *subject*:
   `vf.{flow}._dlq.{run}.{node}`.
2. **`--run-id` pins it.** Passing it explicitly makes two runs share streams. That is the
   escape hatch (resume / rejoin), not a bug.
3. **`flow_id` itself** auto-generates a uuid only if the graph module doesn't pass one
   ([flow.py:82](../videoflow/core/flow.py#L82)). See Q2 — this cuts both ways.
4. **Kubernetes object *names* are flow-scoped, not run-scoped**
   ([manifests.py:536](../videoflow/deploy/manifests.py#L536)); `run_id` is only a label
   ([manifests.py:850](../videoflow/deploy/manifests.py#L850)). So two *concurrent* deploys of
   the same `flow_id` into one namespace would collide at the Deployment/StatefulSet level even
   though their broker streams would not. Sequential redeploys are fine, and that is the
   intended behaviour — a redeploy replaces rather than accumulates.

### The two adjacent keyspaces

The other cross-run clash surfaces both check out. Blob refs are `vf-blob-{uuid4}`, random per
put. Idempotency keys are `sha256(flow:node:message_id)`
([idempotency.py:35](../videoflow/runtime/idempotency.py#L35)) — not visibly run-scoped, but
`message_id` comes from
[`derive_message_id`](../videoflow/wire/serialization.py#L133), which folds `run_id` into the
hash, so a re-run's messages are never mistaken for already-seen ones.

---

## Q2 — How is `flow_id` assigned?

By the **graph module**, not the runtime. Unlike `run_id`, it is meant to be stable rather than
fresh per run.

**The chain, in order:**

1. **The `Flow` constructor is the source of truth** —
   [flow.py:75-82](../videoflow/core/flow.py#L75-L82). You pass it:
   `Flow([sink], flow_type='batch', flow_id='toy-recovery')`. Omit it and you get
   `uuid.uuid4().hex[:12]` — random **per call to `build_flow()`**, i.e. per process that builds
   the graph, not per run.
2. **`videoflow deploy --flow-id` overrides it**, applied right after compile at
   [cli.py:197-198](../videoflow/deploy/cli.py#L197-L198). `run-local` has no `--flow-id` flag
   (only `--run-id`), so on the local path whatever the graph module set is final.
3. **It crosses the compile process boundary as JSON** when the graph can't be imported on the
   host: `python -m videoflow.compile` emits `{"flow_id": ..., "flow_type": ..., "specs": [...]}`
   ([compile.py:106](../videoflow/deploy/compile.py#L106)) and deploy reads it back via
   `specs_from_document` ([compile.py:112-115](../videoflow/deploy/compile.py#L112-L115)).
4. **It reaches workers as `VF_FLOW_ID`** — [manifests.py:159](../videoflow/deploy/manifests.py#L159)
   (k8s) / [local.py:576](../videoflow/engines/local.py#L576) (subprocess), read at
   [worker.py:190](../videoflow/runtime/worker.py#L190).
5. **Two different sanitizers mangle it downstream**, so the string passed in is not necessarily
   what appears anywhere: broker names go through `sanitize()`
   ([topology.py:34](../videoflow/messaging/topology.py#L34), `[^A-Za-z0-9_-]` → `_`),
   Kubernetes names through `k8s_name()`
   ([manifests.py:92-96](../videoflow/deploy/manifests.py#L92-L96)) — lowercased, DNS-1123,
   truncated to 63 chars.

### What that means in practice

**Only one of the four toy solutions actually sets it.** `toy_recovery` hardcodes
`flow_id='toy-recovery'` ([toy_recovery.py:72](../solutions/toy_recovery/toy_recovery.py#L72));
[toy_calculator.py:80](../solutions/toy_calculator/toy_calculator.py#L80),
[toy_router.py:69](../solutions/toy_router/toy_router.py#L69) and
[toy_fusion.py:77](../solutions/toy_fusion/toy_fusion.py#L77) don't, so each build mints a fresh
random one. Two consequences follow directly from the flow-scoped naming in Q1:

- A redeploy is **never an in-place update** unless you pass `--flow-id` — new id, new Deployment
  names, and the previous run's resources aren't replaced by name.
- The DLQ stream `vf-{flow}-dlq` is per-flow, so a random flow_id gives every deploy its own DLQ,
  and old ones linger until the 7-day retention ages them out.

This is exactly why the k8s tests don't rely on the graph's value: the `flow_ids` fixture mints
`{test-name}-{uuid8}` per test and passes it via `--flow-id`
([conftest.py:155-167](../tests/integration/k8s/conftest.py#L155-L167)). Its docstring names the
failure mode — resource names are flow-scoped, so re-applying a leftover Job of the same name
fails on immutable fields, and one test's dead letters would surface in the next test's DLQ.

### One sharp edge

`k8s_name('vf', flow_id, spec.name)` truncates the **joined** string to 63 chars with no
validation anywhere — [manifests.py:96](../videoflow/deploy/manifests.py#L96) is the only mention
of 63 in the codebase. A long `--flow-id` therefore eats into the node name, and two nodes of the
same flow can silently render to the same resource name. A length check at deploy time would
close this; it has not been added.
