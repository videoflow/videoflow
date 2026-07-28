# Integration tests — what to stand up, and how

Everything here runs against real infrastructure. There are three buckets and they
need different things:

| Bucket | What it exercises | What it needs |
|---|---|---|
| [broker/](broker/) | The transport contract, driven straight against JetStream — acks, redelivery, the dead-letter queue, blob reclamation. No flow ever runs. | NATS (+ Redis for one module) |
| [local/](local/) | Whole flows through `LocalProcessEngine` and `videoflow run-local` — one worker *subprocess* per node replica. | NATS (+ Redis, optionally) |
| [k8s/](k8s/) | The same flows deployed to a **kind cluster** via `videoflow deploy` — one Job or Deployment per node. | a kind cluster (`./scripts/kind-up.sh`) |

> **A green run is not evidence.** Each bucket skips itself when its infrastructure
> is missing, so `pytest tests/integration` passes cheerfully with nothing running.
> Use `-rs` to see the skip reasons.

`broker/` and `local/` share one broker; `k8s/` has its own inside the cluster, on a
different port. Nothing is shared between the two halves, so they run at the same
time — which is how CI runs them.

---

## broker/ and local/ — the local broker

### What you need running

| Service | Where the tests look | Override | Gates |
|---|---|---|---|
| NATS with JetStream | `nats://localhost:4222` | `VF_TEST_NATS_URL` | **everything** in `broker/` and `local/` ([conftest.py](conftest.py)) |
| Redis | `redis://localhost:6379/0` | `VF_TEST_REDIS_URL` | `broker/test_blob_reclamation.py`, and two tests elsewhere |

Redis is genuinely optional. `test_blob_reclamation.py` hard-skips without it;
`local/test_toy_solutions.py` uses it opportunistically, passing `--blob-redis-url`
only when Redis answers, because `toy_router`'s idempotent sink must produce the
same answer either way. The Python clients come from the `dev` dependency group, so
a plain `uv sync` is enough.

### Start it

**Docker Compose (recommended)**

```bash
docker compose up -d
```

[docker-compose.yml](../../docker-compose.yml) runs `nats:2.10` with `-js -m 8222`
(4222 for clients, 8222 for monitoring) and `redis:7` with persistence off.

**Bare binaries (what CI does)**

```bash
nats-server -js -sd /tmp/nats-jetstream &
redis-server --save '' --appendonly no &          # only for the blob tests
```

### Verify

```bash
docker compose ps
curl -s localhost:8222/varz | head          # compose only; needs -m 8222
uv run pytest tests/integration/broker tests/integration/local -q -rs
```

`-rs` is the important part: without it a down broker reads as a clean pass. The
unit-only command, which the pre-push hook runs, is
`uv run pytest --ignore=tests/integration`.

---

## k8s/ — the kind cluster

### Stand it up

```bash
./scripts/kind-up.sh          # ~5 min the first time, seconds afterwards
uv run pytest tests/integration/k8s -q -rs
./scripts/kind-down.sh        # add --purge to also delete the work root
```

[scripts/kind-up.sh](../../scripts/kind-up.sh) is idempotent — re-run it any time,
and **do** re-run it after changing anything under `videoflow/`. It creates the
cluster from [k8s/kind-cluster.yaml](../../k8s/kind-cluster.yaml), builds
`videoflow-base:py3.12`, the [fixture image](k8s/Dockerfile) the engine tests run,
and one image per toy solution, side-loads them all with `kind load docker-image`,
installs NATS and Redis into the `videoflow-test` namespace through videoflow's own
`deploy.infra` manifests, publishes NATS on the host, and then checks that the work
root round-trips.

It rebuilds `videoflow-base` every time rather than skipping when the tag exists.
That is deliberate: a tag says nothing about which source it was built from, and a
stale base image fails in a genuinely baffling way — every pod starts, imports a
videoflow from weeks ago, and dies with a `ModuleNotFoundError` for a module that
plainly exists in your checkout. Docker's layer cache makes the no-change rebuild
cheap.

You need `docker`, `kind` and `kubectl` on `PATH`. The tests never create a cluster
and never change your kubectl context — they check and skip, naming `kind-up.sh` as
the fix, because a test that silently retargeted kubectl would deploy into whatever
cluster you were actually working on.

### The two things that make it work

**The work root is mounted at the same absolute path everywhere.** A solution's
`work_dir` is resolved against its config file's directory and baked into the node
parameters at compile time; `videoflow deploy` hostPath-mounts it into the worker
pods; `prepare.py` writes into it from a container on the host. A kind node is a
container with its own filesystem, so unless host, node and pod all agree on the
path, the pods write somewhere nothing can read them and every artifact assertion
fails with a missing file. `kind-cluster.yaml` bind-mounts `/tmp/videoflow-k8s` into
the node at `/tmp/videoflow-k8s`, and `kind-up.sh` proves the round trip before you
get as far as a test.

Override it with `VF_K8S_WORK_ROOT` — but `extraMounts` bind once, at node creation,
so changing it means recreating the cluster.

**The broker is reachable from both sides.** Workers use the in-cluster name
`nats://nats.videoflow-test.svc:4222`. The host needs its own way in, to read the
dead-letter queue and run `videoflow teardown`, so a NodePort
([k8s/nats-nodeport.yaml](../../k8s/nats-nodeport.yaml)) plus a kind
`extraPortMapping` publishes it on `127.0.0.1:4223`. Port 4223 rather than 4222 so a
docker-compose broker can keep 4222 and both halves of the suite can run at once.

### Environment

| Variable | Default | What it is |
|---|---|---|
| `VF_KIND_CLUSTER` | `videoflow` | cluster name; the kubectl context is `kind-<name>` |
| `VF_K8S_NAMESPACE` | `videoflow-test` | namespace holding the broker and every test flow |
| `VF_K8S_WORK_ROOT` | `/tmp/videoflow-k8s` | bind-mounted into the node at the same path |
| `VF_K8S_NATS_URL` | `nats://127.0.0.1:4223` | the broker as reached from the host |

kind has no supported GPU passthrough, so this bucket is CPU-only. The GPU paths are
covered by unit tests and by a real cluster — see the walkthrough in the top-level
[README.md](../../README.md).

---

## Pointing at a different broker

Both local URLs are read from the environment at import time, by `conftest.py`,
`support_broker.py` and each test module:

```bash
export VF_TEST_NATS_URL=nats://10.0.0.5:4222
export VF_TEST_REDIS_URL=redis://10.0.0.5:6379/0
uv run pytest tests/integration/broker tests/integration/local -q -rs
```

---

## Troubleshooting

**Everything skips.** Nothing is listening. The probe is a plain TCP connect, so
`-rs` tells you exactly which URL it tried.

**Tests fail during provisioning.** The server started without JetStream. A TCP
probe cannot tell `nats-server` from `nats-server -js`, so a plain server passes the
check and then fails when a stream is created. Restart it with `-js`.

**`address already in use` on 4222.** Something else has the port — often a stray
`nats-server` from an earlier run, or a compose stack you forgot about. `docker
compose down`, or `pkill nats-server`.

**Leftover streams.** Harmless. Every test mints a fresh `flow_id`/`run_id` (see
[support_broker.py](support_broker.py)), so old streams are clutter, not
interference. `docker compose down -v` clears them.

**The k8s bucket skips with a reason.** The reason is the fix, and almost all of
them end in `run ./scripts/kind-up.sh`. The exception is the work-root message,
which means the cluster was created before the directory existed or with a different
`VF_K8S_WORK_ROOT` — recreate it.

**A k8s test fails with "report.json was not written".** The flow ran and the pods
exited cleanly, but their output never reached the host. Check the mount:

```bash
echo hello > /tmp/videoflow-k8s/.check
docker exec videoflow-control-plane cat /tmp/videoflow-k8s/.check
```

**Do not change the probe to use `nats.connect`.** It looks like the obvious
improvement and it is a two-minute trap: nats-py retries a refused connection
internally, so a real handshake against a dead port ignores `connect_timeout`
entirely. An import-time version of that made mere *collection* of this directory
take about fourteen minutes with no server running.

---

## What the tests actually do

**`broker/`** — `provision_flow_sync` plus an in-process `NATSMessenger`, or raw
JetStream. No worker, no flow: these pin the transport contract, and the answers are
the same regardless of where the workers filling the broker happen to run. That is
why they are not filed under `local/`.

**`local/`** — real flows through `LocalProcessEngine`, plus the four toy solutions
under `solutions/` driven by `videoflow run-local`.

**`k8s/`** — the same four solutions driven by `videoflow deploy`, using the same
config dicts and the same assertions, imported from
[support_solutions.py](support_solutions.py). That sharing is the point: the
framework's promise is that a graph built on one machine runs unchanged on many, so
the two engines must produce the same answer from the same input, and anything
asserted in only one of them is a claim nobody is checking. Alongside them, `k8s/`
covers the machinery the local path has no counterpart for — infra provisioning, Job
completion, and the `deploy` exit codes.
