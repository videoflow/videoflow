# CLAUDE.md — videoflow core

Videoflow is a Python framework for building video/stream processing applications as a
**directed graph of nodes**, plus a runtime that executes that graph either as local
subprocesses or as one Kubernetes workload per node, and a `videoflow` CLI that deploys it.

The single idea that explains most of the design: **the graph is built on one machine and
executed on many.** Each node is serialized to `(class path, params)` and reconstructed inside
its own worker process, which has no access to the object that built the graph. Nearly every
convention below follows from that.

## Repo map

| Path | What lives there |
|---|---|
| `videoflow/core/` | The abstractions: `node.py` (Node hierarchy), `flow.py` (Flow), `graph.py` (validation), `task.py` (per-node run loop), `engine.py` (Messenger/ExecutionEngine interfaces), `policies.py` (JoinPolicy), `compiler.py` (Flow → `NodeSpec`s), `remote.py`, `constants.py` |
| `videoflow/runtime/` | Runs **inside a worker container**: `worker.py` (the one-node entrypoint), `provision.py`, `health.py`, `idempotency.py`, `logging_config.py` |
| `videoflow/deploy/` | Runs on the **operator's machine**: `cli.py`, `compile.py`, `manifests.py`, `images.py`, `build.py`, `cluster.py`, `gpu.py`, `solution.py`, `infra.py`, `localinfra.py` |
| `videoflow/wire/` | `serialization.py` — the transport-independent envelope format (msgpack v2/v3, protobuf v4) |
| `videoflow/components/` | `descriptor.py` (component.yaml loading/validation), `oci.py` (descriptors as OCI artifacts) |
| `videoflow/engines/` | `local.py` (subprocess per node), `kubernetes.py` (pod per node) |
| `videoflow/messaging/` | NATS JetStream transport. `topology.py` is the single source of truth for subject/stream/durable naming |
| `videoflow/producers/`, `processors/`, `consumers/` | Built-in nodes |
| `videoflow/v1/` | **Generated** protobuf modules — never hand-edit (see hard rules) |
| `videoflow/utils/` | Graph algorithms, model downloader, parsers, transforms |
| `videoflow/*.py` (root) | Only `version.py` and five frozen compatibility shims — see below |
| `spec/` | Language-agnostic protocol contract: `PROTOCOL.md`, `proto/`, golden `vectors/`, `rfcs/` |
| `docker/`, `k8s/` | Base images (CPU + CUDA) and dev broker manifests |
| `docs/` | Sphinx site (`docs/source/`) |
| `tests/`, `tests/integration/`, `examples/` | Unit tests, broker-backed tests, runnable examples |

Sibling repo: `../videoflow-contrib` — community components and end-to-end solutions. It has its
own `CLAUDE.md`.

## Commands

```bash
uv sync                                        # install (uv is the tool of record)
uv run pytest --ignore=tests/integration       # unit tests — what the pre-push hook runs
uv run pytest tests/integration                # needs a live NATS; auto-skipped if absent
uv run mypy                                    # type check (files = ["videoflow"])
uv run ruff check --fix .                      # lint + import sort
docker compose up -d                           # dev NATS (:4222) + Redis (:6379)
./scripts/gen-proto.sh                         # regenerate videoflow/v1/ from spec/proto/
```

First-time setup also needs **both** pre-commit hooks — the pytest hook is on pre-push, so
installing only the commit hook silently skips tests:

```bash
uv tool install pre-commit                    # needs >= 3.2.0; see below
pre-commit install && pre-commit install --hook-type pre-push
```

**pre-commit must be >= 3.2.0.** The hook repos pin `pre-commit-hooks` v5, which declares the
renamed stages (`pre-commit`, `pre-push`). An older binary fails with
`InvalidManifestError: ... Expected one of commit, ... but got: 'pre-commit'`. Distro packages are
often far too old (Ubuntu ships 2.17.0 at `/usr/bin/pre-commit`), so install it into
`~/.local/bin` via `uv tool install` or `pipx` and make sure that directory precedes `/usr/bin` on
`PATH`. Note the git hooks pin their own interpreter at install time, so commits can keep working
while a manual `pre-commit run --all-files` fails — check `pre-commit --version`, not the hook.

Integration tests probe `VF_TEST_NATS_URL` (default `nats://localhost:4222`) with a raw socket
connect and skip if nothing is listening. Don't "fix" that probe to use `nats.connect` — an
import-time nats probe made test collection take ~14 minutes.

## Python conventions

Python 3.12. The guidance below is split deliberately: the codebase predates some of these
rules, so **what to write** and **what to leave alone** are not the same list.

### Rules for new code

**Type-hint every parameter and every return.** Existing code annotates returns almost
everywhere but parameters only sparsely. That is legacy, not the target. Every function you add
or meaningfully touch gets full annotations, including `self`-less helpers and private
functions. Prefer `X | None` over an unannotated `= None` default.

`mypy` is configured leniently (`implicit_optional = true`, `check_untyped_defs = false`) so the
old code passes. That is a compatibility shim, not permission to omit hints in new code.

**Liskov/override checking is on** (`enable_error_code = ["override"]`). The framework has a
handful of deliberate violations — a node's `process()`/`consume()` takes one positional arg per
parent, so those subclasses can't match the base signature. Each carries a local
`# type: ignore[override]` with a comment naming the reason. Add the local ignore; do not
re-disable the code repo-wide. The blanket disable it replaced was also hiding unrelated real
bugs (`get_params()` stubs typed `-> None` instead of `NoReturn`, and `ModuleNode.__call__`
silently returning `None` where callers expect a `Node` for chaining).

**Prefer `Any` over a wrong narrowing.** Arbitrary user data flows through the graph, so the
payload surfaces — `process`/`consume`/`next`/`publish_message`/`encode_payload`, and
`set_output_partition_key` — are `Any` *by contract*, not by neglect. Two traps when tightening
types: narrowing a param that a subclass legitimately re-narrows breaks it (`Any` is
bidirectionally compatible, `object` is not), and typing a value against a third-party or
duck-typed object (`topology.specs`, `grouping.handle`) turns a working seam into a lie.

**Import at module scope by default.** A function-level import needs a reason, and the reason
goes in a comment next to it. There are exactly two good reasons here:

1. A genuinely optional dependency — `nats`, `cv2`, `redis`, `yaml`, `kubernetes` are installed
   via extras, so the core must import cleanly without them. This is why `deploy/cli.py` defers so much.
2. Breaking a real circular import.

"It's slow to import" and "it's only used in one branch" are not reasons. The audit of existing
function-level imports is **done** — every one of the 44 that remain is deliberate and carries a
reason comment. Don't add an undocumented one.

Two things that audit learned, worth knowing before you defer or hoist anything:

- **Watch for transitive pulls.** `from .solution import ...` looks like a cheap intra-package
  import, but `solution.py` imports `yaml` at module scope, so hoisting it breaks a bare install.
  Most of the legitimate deferrals in `deploy/` and `engines/` are this shape, not direct
  imports of an extra. Verify by importing the module with the extras blocked — don't eyeball it.
- **To hoist a plugin-registry import, import the module, not the symbol.** Tests monkeypatch
  `plugins.load_plugin_group`, which only works while the name resolves at call time. Use
  `from ..utils import plugins` at module scope and call `plugins.load_plugin_group(...)`; that
  satisfies this rule and keeps the patch seam. See `wire/serialization.py` and `deploy/gpu.py`.

**Don't use `getattr`/`hasattr` to dodge a type.** `getattr(node, 'partition_by', None)` and
`node._join_policy if hasattr(node, '_join_policy') else None` read as defensive, but they say
"I don't know what this object is" in a codebase where the checker does. They silently return the
default when an attribute is *renamed*, turning a refactor into a wrong answer at runtime instead
of an error at check time. Use the type system:

- The attribute lives on a subclass → `isinstance`. `partition_by`/`_join_policy` are on
  `ProcessorNode`/`ConsumerNode` but not `ProducerNode`, so
  `node.partition_by if isinstance(node, (ProcessorNode, ConsumerNode)) else None` — which is
  what the surrounding lines in `core/compiler.py` already do.
- The attribute is always there → just read it. `image` is a property on `Node`, so
  `getattr(node, 'image', None)` was pure noise. Same for every field of a `NodeSpec` dataclass.
- Duck-typing a foreign object → `isinstance` against the real base, or a `Protocol` /
  `TypeGuard` if there's no importable base. `_is_proto_message` went from probing for
  `DESCRIPTOR`/`SerializeToString` to `TypeGuard[Message]`, which also narrows for callers.
- A loosely-typed parameter → fix the parameter. `topology.provision_flow(specs : list)` used
  `getattr` defaults for `has_children`/`nb_tasks`; every caller passes `list[NodeSpec]`, so
  typing it removed all three.

`getattr`/`hasattr` are correct only for **genuine reflection**, where the attribute name is a
variable rather than a literal: `Node.get_params()` walking `'_' + pname`, importing a class named
by `VF_NODE_CLASS`, a log field named by config, or an object that legitimately may not have the
attribute at all (a module without `__file__`). Those are the only ones left in `videoflow/` —
if you add another, it should be that kind.

**Abstraction practices.**
- Prefer small pure functions taking explicit arguments over methods that reach into `self._*`.
  The graph-validation and topology-naming code is written this way and is the easiest code in
  the repo to test.
- Node `__init__` does no I/O. Open files, sessions, and models in `open()`; release in
  `close()`. `__init__` runs on the machine that builds the graph; `open()` runs in the worker.
- Raise a domain error with an actionable message rather than returning a sentinel. Validation
  errors are `ValueError` and should name the fix, not just the problem — see the messages in
  `videoflow/deploy/images.py` and `videoflow/core/graph.py` for the house standard.
- Use `@dataclass` for plain records instead of passing dicts around as structs — but only for
  records **we own**. The line that matters is who defines the shape:

  | Stays a `dict` | Why |
  |---|---|
  | Kubernetes objects in `deploy/manifests.py` | External API schema. A typed mirror is a second source of truth that drifts, and still has to become a dict at the YAML boundary. |
  | `decode_envelope()`'s return | Re-exported through the frozen `videoflow/serialization.py` shim; golden vectors subscript it. |
  | `component.yaml` descriptors | Mirrors the jsonschema-validated file format. |
  | `solution.py` template config | Arbitrary user YAML. |

  Also leave it a dict when it is genuinely a **mapping** — arbitrary keys rather than fixed
  fields. `gpu_demand()` returns cluster-defined extended-resource names (`nvidia.com/gpu`, MIG
  profiles, vendor-specific), so `dict[str, int]` is the truthful type; a dataclass would have to
  invent a field set that doesn't exist.

  Current dataclasses: `NodeSpec` (`core/compiler.py`), `Mount` + `ProvisionSplit`
  (`deploy/manifests.py`), `EnvelopeEntry` + `CollectEntry` (`messaging/grouping.py`),
  `_MetricAggregate` (`runtime/health.py`).

  Two things that bite:
  - **Keep `to_dict()`/`from_dict()` explicit where a dict crosses a boundary.**
    `dataclasses.asdict()` recurses and rebuilds nested values; `NodeSpec.to_dict()` passes
    `params`/`descriptor` through by reference. Swapping in `asdict()` silently changes what
    lands in `VF_FLOW_SPECS_JSON`.
  - **A behaviour-identity check, not a green suite, is the acceptance test** for this kind of
    refactor: byte-compare the rendered YAML / emitted JSON before and after.
- Don't introduce an abstraction layer until there is a second caller.
- When something *is* worth making pluggable, follow the established shape: a module-level
  registry seeded with the built-ins, an explicit `register_*()`, and a lookup that raises
  `ValueError` naming the known values and the fix. Stdlib only — entry-point discovery goes
  through `utils/plugins.py`. See the extension-seam table in
  [.claude/docs/ARCHITECTURE.md](.claude/docs/ARCHITECTURE.md), which also records what was
  deliberately *not* abstracted and why.
- One reason to change per module. The top-level deploy modules are deliberately narrow
  (`deploy/images.py` only resolves images, `messaging/topology.py` only names things) — keep them that way.

### Existing house style — match it, don't "fix" it

- `from __future__ import absolute_import, division, print_function` header on most modules.
- Space-before-colon annotations and spaced kwargs: `def f(self, node : Node) -> None:`,
  `IntProducer(0, 40, name = 'producer')`. Ruff's `select` deliberately omits whitespace rules
  so it won't fight this. **Do not reformat surrounding code** — it makes diffs unreviewable.
- Docstrings use triple **single** quotes with `- Arguments:` / `- Returns:` / `- Raises:`
  bullets and `\` line continuations.
- Every module gets a substantial module-level docstring explaining *why* it exists. This is the
  strongest and most useful convention in the repo; keep it up.
- `logger = logging.getLogger(__package__)` per module.

## Hard rules

**Store every constructor argument verbatim as `self._<name>`.** `Node.get_params()`
([core/node.py:103](videoflow/core/node.py#L103)) walks the MRO's `__init__` signatures and looks
up `self._<param>` (then `self.<param>`) so a worker can rebuild the node via
`type(node)(**get_params())`. A missing attribute raises `AttributeError` at build time. If your
argument names don't match stored attributes, override `get_params()`. All params must be
JSON-serializable.

**The five root shims are frozen public contract.** `videoflow/{worker,provision,compile,cli,
serialization}.py` are thin re-export modules left behind by the package reorganization. Do not
delete them, do not add code to them, and do not repoint the things that reference them:

| Shim | Why it can't move |
|---|---|
| `videoflow.worker` | ENTRYPOINT of the published base images, inherited by every contrib component image |
| `videoflow.provision` | Rendered into every flow's init Job, including manifests already applied in clusters |
| `videoflow.compile` | Spawned inside solution images by the host CLI, which may be a different version |
| `videoflow.cli` | Backs the `videoflow` console script; installed entry points outlive the source tree |
| `videoflow.serialization` | The module path a pickled payload records for its class — a DLQ'd message must still decode |

`tests/test_shims.py` enforces this. Edit the real modules under `runtime/`, `deploy/` and
`wire/` instead.

**Never hand-edit `videoflow/v1/`.** It is generated from `spec/proto/` by `scripts/gen-proto.sh`
and excluded from both ruff and mypy — ruff's F401 would strip the cross-proto imports that
register dependencies in the descriptor pool, breaking a cold import of the wire format.

**A node's `name` is its identity everywhere outside the building process** — broker subjects,
Kubernetes resource names, logs. Renaming a node changes its wire identity.

**Errors:** `ValueError` for graph/config validation with a message naming the fix;
`SystemExit(str(e)) from e` in CLI paths so users see a message rather than a traceback;
`RuntimeError` for lifecycle misuse; `NotImplementedError` for abstract methods and for nodes
that can't work in distributed mode. Use `raise ... from e` consistently.

**Observable wire or routing changes require an RFC** under `spec/rfcs/` plus updated golden
vectors in `spec/vectors/`. `spec/PROTOCOL.md` has stable requirement IDs (`ENV-1`, `EOS-3`,
`WIRE-11`) — cite them in commit messages when you touch protocol behaviour.

## Keep docs in sync with code

A change isn't done until the docs describing it are updated **in the same commit**. Before
finishing, check each of these and update the ones your change invalidates:

- `README.md` — behaviour, CLI flags, installation, or extras changed.
- `docs/source/` — a public API or user-facing concept changed.
- `spec/PROTOCOL.md` + `spec/vectors/` — the wire format or routing changed (also needs an RFC).
- `CLAUDE.md` and `.claude/docs/*.md` — conventions, layout, or commands changed.
- `examples/` — an example is now wrong or misleading.
- `../videoflow-contrib` — the node contract changed in a way components must follow.

## Where to look next

- [.claude/docs/ARCHITECTURE.md](.claude/docs/ARCHITECTURE.md) — how a flow goes from
  `Flow([...])` to running workers; messaging topology; wire format.
- [.claude/docs/NODE_CONTRACT.md](.claude/docs/NODE_CONTRACT.md) — writing a node: the four
  types, the `get_params()` round trip, lifecycle, scaling knobs.
- [.claude/docs/DEPLOYMENT.md](.claude/docs/DEPLOYMENT.md) — the `deploy` / `run-local`
  pipeline, solutions, manifests, GPU.
- [README.md](README.md) — the user-facing guide, including the GPU cluster walkthrough.
- [spec/PROTOCOL.md](spec/PROTOCOL.md) — normative protocol contract.
