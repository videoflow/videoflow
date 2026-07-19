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
| `videoflow/deploy/` | Runs on the **operator's machine**: `cli.py`, `compile.py`, `manifests.py`, `images.py`, `build.py`, `cluster.py`, `solution.py`, `infra.py`, `localinfra.py` |
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

**Import at module scope by default.** A function-level import needs a reason, and the reason
goes in a comment next to it. There are exactly two good reasons here:

1. A genuinely optional dependency — `nats`, `cv2`, `redis`, `yaml`, `kubernetes` are installed
   via extras, so the core must import cleanly without them. This is why `deploy/cli.py` defers so much.
2. Breaking a real circular import.

"It's slow to import" and "it's only used in one branch" are not reasons. `TODO.md` item 7
tracks auditing the existing function-level imports; don't add to the pile.

**Abstraction practices.**
- Prefer small pure functions taking explicit arguments over methods that reach into `self._*`.
  The graph-validation and topology-naming code is written this way and is the easiest code in
  the repo to test.
- Node `__init__` does no I/O. Open files, sessions, and models in `open()`; release in
  `close()`. `__init__` runs on the machine that builds the graph; `open()` runs in the worker.
- Raise a domain error with an actionable message rather than returning a sentinel. Validation
  errors are `ValueError` and should name the fix, not just the problem — see the messages in
  `videoflow/deploy/images.py` and `videoflow/core/graph.py` for the house standard.
- Use `@dataclass` for plain records instead of passing dicts around as structs.
- Don't introduce an abstraction layer until there is a second caller.
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
