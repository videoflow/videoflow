# Replacing `kubectl` subprocess calls with the Kubernetes Python client

Reading the question as *kubectl*, not *kubelet*: nothing in this repo talks to a kubelet. Every
Kubernetes interaction goes through the `kubectl` binary as a subprocess, and the proposal is to
replace those with the official `kubernetes` Python client (`kubernetes-client/python`).

Research pass over `deploy/cluster.py`, `deploy/gpu.py`, `deploy/infra.py`, `deploy/manifests.py`,
`deploy/cli.py`, `engines/kubernetes.py`, the unit tests that mock them, and the client library's
own source. Nothing has been changed; this is the finding set and a recommendation.

**Short answer: yes for the read and watch paths, no for "across the whole codebase," and not as
one change.** The reads are where the payoff is concentrated and the risk is lowest. `apply` is a
semantic change, not a port. And there is one verified failure mode — exec-credential token
refresh — that decides the whole question and can only be answered against a real cloud cluster.

---

## 1. What is actually there today

21 `subprocess.run` call sites invoke `kubectl`, spread across five modules:

| Module | `subprocess.run` sites | `kubectl` mentions | Character |
|---|---:|---:|---|
| [deploy/gpu.py](../videoflow/deploy/gpu.py) | 4 | 104 | Mutations: label/annotate nodes, patch `clusterpolicies.nvidia.com`, publish ConfigMaps, wait for MIG state |
| [deploy/cluster.py](../videoflow/deploy/cluster.py) | 3 | 45 | Pure reads: `get nodes -o json`, `get pods -A -o json`, `config current-context` |
| [engines/kubernetes.py](../videoflow/engines/kubernetes.py) | 6 | 22 | `apply -f -`, jsonpath status polling, `get events`, `logs`, delete |
| [deploy/infra.py](../videoflow/deploy/infra.py) | 6 | 14 | Namespace create, `apply -f -`, `rollout status`, label-scoped delete |
| [deploy/manifests.py](../videoflow/deploy/manifests.py) | 2 | 6 | `delete_resources` by label, per-CRD-kind deletes |

Verbs used, by frequency: `get` (26), `label` (8), `annotate` (7), `delete` (4), `apply` (2), and
one each of `patch`, `rollout`, `logs`, `create`, `config`, `version`.

Four facts that shape the answer:

- **No `exec`, no `port-forward`, no `cp`.** The two operations that are genuinely painful in the
  Python client (they need the `stream` module and a websocket, and the README notes you must
  recreate the API client when alternating between stream and normal calls) are simply absent
  here. That removes the usual hardest third of a kubectl→client migration.
- **Nothing inside a pod talks to the API server.** [deploy/manifests.py](../videoflow/deploy/manifests.py)
  renders no ServiceAccount, Role or RoleBinding. The dependency would be operator-side only —
  it belongs in the `deploy` extra, and worker images stay exactly as lean as they are.
- **`kubernetes` is not a dependency and is not installed.** It appears nowhere in
  `pyproject.toml`. Note that [CLAUDE.md](../CLAUDE.md)'s third-party lookup table lists
  `kubernetes` as used in `engines/kubernetes.py`, `deploy/cluster.py` and `deploy/manifests.py`.
  That row describes an intention, not the code. It should be corrected regardless of what is
  decided here.
- **Removing kubectl does not remove subprocess from the deploy path.**
  [cluster.py:240-263](../videoflow/deploy/cluster.py#L240-L263) shells out to `kind load
  docker-image`, `minikube image load` and `sudo k3s ctr images import`; `deploy/build.py` shells
  to docker. "One fewer external binary" is only true for remote clusters.

---

## 2. Pros

### 2.1 Typed reads delete the jsonpath-and-`split('|')` parsing layer

This is the strongest technical argument. Three methods in the engine encode field paths as
strings inside a jsonpath template, run kubectl, then re-parse a pipe-delimited line by position:

- [`_job_states`](../videoflow/engines/kubernetes.py#L267) — reads the `Complete`/`Failed`
  conditions, and its docstring explains at length why reading those instead of the pod counters
  is load-bearing for `solutions/toy_recovery`.
- [`_pod_states`](../videoflow/engines/kubernetes.py#L303) — `PodScheduled` reason and message.
- [`_container_states`](../videoflow/engines/kubernetes.py#L331) — readiness, restart count,
  waiting reason, and the worker's JSON death note out of `lastState.terminated.message`.

The code already documents the hazard it lives with: the termination message is "free text as far
as this parser is concerned, so a `|` inside it must not shift the enum-ish fields before it,"
which is why it is read last with `maxsplit`. That is a correct fix for a problem that would not
exist against `V1Pod`.

The sharper issue is that **the jsonpath expressions themselves are untested**. A typo in
`{.status.containerStatuses[0].lastState.terminated.reason}` produces empty output, not an error —
and the unit tests feed canned stdout, so they exercise the parser, never the query. A wrong field
path degrades silently to "no state," which the watchdog reads as "nothing wrong yet." With typed
objects, mypy checks the field names.

### 2.2 Watch instead of poll

[`wait_for_completion`](../videoflow/engines/kubernetes.py#L431),
[`rollout_report`](../videoflow/engines/kubernetes.py#L481) and
[`_wait_provision`](../videoflow/engines/kubernetes.py#L394) poll on 2–3 second intervals, and
each tick can fan out to several kubectl invocations (`_scaleup_in_flight` is explicitly bounded
at "one kubectl call per stuck pod," capped at 8). A `deploy --wait` on a long rollout spawns
thousands of processes, each paying a fork, a TLS handshake and API discovery.

`watch.Watch().stream(...)` (or the dynamic client's `watch`) collapses that into one long-lived
HTTP stream: sub-second reaction to a CrashLoopBackOff instead of up-to-3-seconds, no
poll-interval-versus-deadline tuning, and `resourceVersion` resumption for free. For a framework
whose whole error story is "notice the sick worker fast and say something useful," this is the
biggest functional win on the list.

### 2.3 Structured errors map onto the error taxonomy

Today a failure is a return code plus stderr text.
[`_kubectl_out`](../videoflow/deploy/cluster.py#L36) swallows everything to `''`, and
[`_kubectl_json`](../videoflow/deploy/gpu.py#L378) returns `None` on "kubectl missing, non-zero
exit, or unparseable output" — three very different worlds collapsed into one sentinel. So
[`_cluster_policy`](../videoflow/deploy/gpu.py#L395) returns `None` both for *the NVIDIA GPU
operator is not installed* and for *your kubeconfig is broken*.

`ApiException.status` separates them: 401 auth, 403 RBAC, 404 CRD-not-installed, 409 conflict,
422 invalid object. Those map one-to-one onto the taxonomy in
[core/errors.py](../videoflow/core/errors.py) — 403 becomes a `CapabilityError` whose `remedy`
names the missing RBAC verb, a 404 on `clusterpolicies.nvidia.com` becomes "install the GPU
operator" instead of a silent degrade. Given how much this repo invests in `remedy = ...` being a
first-class field, this is an architectural fit, not a nicety.

### 2.4 Smaller wins

- **Field selectors and pagination come free.** `_scaleup_in_flight`'s per-pod loop becomes one
  call with a field selector.
- **No PATH dependency and no CLI-output contract.** `kubectl rollout status` output and exit
  semantics are a CLI contract; `-o json` is stable but `--kubectl` currently trusts whatever
  binary is on PATH, of whatever version.
- **Connection reuse.** The MIG path in `gpu.py` makes dozens of sequential calls per node in
  `prepare`/`cleanup`; a persistent client removes the per-call handshake.

---

## 3. Cons

### 3.1 `apply` is not a method on the client — and SSA is a behavior change

Verified against the client's source: there is no client-side three-way-merge `apply`.
`kubernetes.utils.create_from_yaml` is create-only. The dynamic client
(`kubernetes/base/dynamic/client.py`) does provide:

```python
def server_side_apply(self, resource, body=None, name=None, namespace=None,
                      force_conflicts=None, **kwargs)   # forces 'application/apply-patch+yaml'
```

Server-side apply is the right modern answer, but swapping it in for
[`_kubectl_apply`](../videoflow/engines/kubernetes.py#L252) and
[`ensure_infra`](../videoflow/deploy/infra.py#L162) is **a semantics change, not a port**: field
ownership moves to the API server, a second manager touching the same field returns a 409 that
you must decide whether to force, and re-deploying a flow a human has since edited by hand behaves
differently than it does today. Under this repo's own rules, a change to what a re-deploy does to
running workloads is closer to RFC territory than to a refactor.

(The manifests themselves are fine — they are full objects with `apiVersion`/`kind`/
`metadata.name`, and the client accepts plain dicts, so `dump_manifests` and the `render_*`
functions are untouched. [CLAUDE.md](../CLAUDE.md) is right that Kubernetes objects should stay
dicts; migrating the transport does not change that.)

### 3.2 Exec-credential auth is where this bites in production — and it is the go/no-go

`kubernetes/base/config/exec_provider.py` carries, verbatim, a TODO listing what is missing from
its implementation: **TLS cert support and caching.** It does no expiry handling of its own; the
plugin is executed once and the token written onto the `Configuration`. Refresh depends on the
config loader's `refresh_api_key_hook` / `expiry` path being re-entered, which is a code path
built primarily for auth-provider bearer tokens.

kubectl, by contrast, re-execs `aws eks get-token` / `gke-gcloud-auth-plugin` / the Azure plugin
on *every invocation*. That is why a multi-hour `videoflow deploy --wait` cannot currently outlive
its token — each poll re-authenticates by construction. A client-based watch holds one connection
and one token.

This is the failure that would never appear in kind and would appear in every user's EKS cluster.
**Test it before committing to anything: a >1 hour watch against a real EKS or GKE cluster with an
exec-based kubeconfig.** If exec tokens do not refresh, the watch path needs an explicit
reload-on-401 wrapper, which is doable but must be designed in, not discovered.

A second, milder version of the same problem: subprocess-per-call picks up `KUBECONFIG` changes
and context switches between calls; a long-lived client caches them.

### 3.3 `--kubectl` is public CLI contract

[cli.py:1094](../videoflow/deploy/cli.py#L1094) and [cli.py:1293](../videoflow/deploy/cli.py#L1293)
expose `--kubectl <binary>`, threaded through 25 call sites. If the binary is no longer used,
keeping the flag is a lie and removing it breaks operator scripts — so a deprecation cycle. And
kubeconfig/context selection, which today comes free from kubectl's own resolution, needs new
`--kubeconfig` / `--context` flags to replace it.

### 3.4 About 2,100 lines of unit tests are built on mocking `subprocess.run`

| Test file | subprocess refs | lines |
|---|---:|---:|
| `tests/test_mix_strategy.py` | 28 | 967 |
| `tests/test_cluster.py` | 48 | 562 |
| `tests/test_k8s_watchdog.py` | 7 | 309 |
| `tests/test_gpu_strategies.py` | 4 | 194 |
| `tests/test_infra.py` | 6 | 87 |

Every one of these monkeypatches `subprocess.run` and serves canned stdout keyed by a substring of
the command line. All of them get rewritten. That is the bulk of the migration cost, and it buys
no user-visible behavior.

The mitigating half: the replacements are better tests. Asserting against `V1Pod` objects checks
field names; asserting that a command line contained `'get'` and `'nodes'` checks almost nothing.

### 3.5 A heavy new dependency, and a version-skew burden that becomes ours

`kubernetes` pulls `urllib3`, `requests`, `oauthlib`, `requests-oauthlib`, `google-auth`,
`websocket-client`, `durationpy`, `certifi` and `pyyaml` — roughly doubling the `deploy` extra of
a package whose `pyproject.toml` comment says core is "deliberately lean." Contained (workers are
unaffected), but real, and it becomes another module that `deploy/cli.py`'s function-level import
discipline has to cover.

Version skew is the subtler cost. The client's own compatibility matrix pairs client `36.y.z` with
Kubernetes 1.36 (exact match ✓, ±1 minor marked partial). Pinning a floor in `pyproject.toml`
entangles videoflow's release cadence with Kubernetes'. Today the operator's kubectl version is
the operator's problem, and videoflow's users run whatever they have — kind, k3s, EKS, GKE, all at
different minors.

### 3.6 Two conveniences you reimplement

- **`rollout status`.** [`wait_infra_ready`](../videoflow/deploy/infra.py#L192) gets Deployment
  readiness plus a timeout for free. The client has no equivalent; you reimplement
  `observedGeneration >= generation and updatedReplicas == replicas == availableReplicas`. About
  20 lines, and `rollout_report` already contains most of it — but it looks free and isn't.
- **Comma-separated multi-kind deletes.**
  [`delete_resources`](../videoflow/deploy/manifests.py#L900) does one
  `kubectl delete deployment,service,configmap,... -l selector`. The client has no comma form: one
  `delete_collection_namespaced_*` per kind, plus `CustomObjectsApi` or the dynamic client for the
  CRD kinds. Same semantics, more code — though the existing per-CRD loop already anticipates that
  shape for exactly the same reason (a missing CRD must not abort the batch).

---

## 4. Recommendation: four tiers, in this order

Do it behind **one narrow seam** — a new `videoflow/deploy/kube.py` exposing the dozen operations
the rest of the code actually needs (`list_pods`, `list_jobs`, `watch_pods`, `label_node`,
`annotate_node`, `patch_cr`, `delete_by_selector`, `apply`, …). This satisfies
[CLAUDE.md](../CLAUDE.md)'s "no abstraction until there is a second caller" rule, because during
the migration there genuinely are two implementations, and it means Tier 1 can land without
touching `gpu.py` at all.

| Tier | Scope | Why here | Risk |
|---|---|---|---|
| **1. Do it** | `engines/kubernetes.py` reads: `_job_states`, `_pod_states`, `_container_states`, `_scaleup_in_flight`, and a watch-based `rollout_report` | All of §2.1, §2.2 and §2.3 land here at once. ~200 lines, one test file | Low — read-only. A bug is a wrong diagnosis, not a wrong mutation |
| **2. Do it** | `deploy/cluster.py` reads: `gpu_units_in_use`, `gpu_availability`, `gpu_inventory`, `classify_gpu_resource`, `allocatable_gpus`, `current_context` | Already `-o json` + `json.loads`; becomes typed list calls with a label selector. `current_context` → `config.list_kube_config_contexts()` | Low. Leave the `kind`/`minikube`/`k3s` image-load handlers on subprocess |
| **3. Judgment call** | `deploy/gpu.py` mutations | Real wins (structured 403/404, no stderr parsing) but the largest blast radius: cluster-scoped labels and CRD patches on a shared multi-tenant cluster. `todos/00_gpu_allocation_bugs.md` says this area isn't settled | High. Defer until Tiers 1–2 have proved the auth story, or until the GPU work lands |
| **4. Last, or never** | `_kubectl_apply`, `ensure_infra` | SSA is a behavior change (§3.1), needs its own decision and probably an RFC note on redeploy semantics | Highest, and nothing forces it to move with the reads |

The honest framing of the cost/benefit: **Tiers 1–2 are worth doing on their merits** — they
remove a silent-failure class, replace polling with watching, and feed the error taxonomy real
signal. Tiers 3–4 are mostly consistency, paid for with the riskiest code in the deploy path. A
codebase that ends up with a typed client for reads and kubectl for `apply` is not an
embarrassment; it is roughly where most operator tooling lands.

---

## 5. Verify before writing code

The context7 MCP tools are **not available in this session**, and `kubernetes` is not installed in
the venv — so the API-surface claims above come from the client's own source on GitHub, fetched
over the web, not from an installed package. Per [CLAUDE.md](../CLAUDE.md)'s third-party rule,
confirm each of these against the installed library before relying on it:

1. `uv add --optional deploy kubernetes`, then read `.venv/.../kubernetes/base/dynamic/client.py`
   (`server_side_apply` signature) and `.venv/.../kubernetes/base/config/exec_provider.py` (the
   caching TODO) directly.
2. **Exec-credential token refresh over a >1 hour watch against EKS or GKE.** This is the go/no-go
   for Tier 1, because Tier 1's whole point is one long-lived connection.
3. Whether `server_side_apply` handles the two-phase provision/worker split
   ([kubernetes.py:243-250](../videoflow/engines/kubernetes.py#L243-L250)) without a field-manager
   conflict on the second phase — only relevant if Tier 4 is ever attempted.

## 6. Docs that go stale

Per the repo's same-commit rule, whichever tier lands takes these with it:

- [CLAUDE.md](../CLAUDE.md) — the third-party table row claiming `kubernetes` is already used
  (wrong today, independently of this decision), and the `deploy` extra description.
- [engines/kubernetes.py:3-8](../videoflow/engines/kubernetes.py#L3-L8) — the module docstring's
  "applies them with `kubectl`" and "Requires `kubectl` on PATH."
- [README.md](../README.md) — prerequisites and the GPU walkthrough (16 kubectl references; most
  are operator-facing commands that stay, but the prerequisite line changes).
- `docs/source/` — 8 references.
- `tests/integration/README.md` — what the `k8s` bucket needs on the host.

Note the integration tests under `tests/integration/k8s/` use kubectl directly in 34 places as a
*test harness*. Those are independent of this decision and should stay: a test that verifies
cluster state through the same client the code under test uses is not verifying much.
