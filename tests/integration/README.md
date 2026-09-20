# Integration tests — what to stand up, and how

Everything here runs against real infrastructure. There are three buckets and they
need different things:

| Bucket | What it exercises | What it needs |
|---|---|---|
| [broker/](broker/) | The transport contract, driven straight against JetStream — acks, redelivery, the dead-letter queue, blob reclamation. No flow ever runs. | NATS (+ Redis for one module) |
| [local/](local/) | Whole flows through `LocalProcessEngine` and `videoflow run-local` — one worker *subprocess* per node replica. | NATS (+ Redis, optionally) |
| [k8s/](k8s/) | The same flows deployed to a **Kubernetes cluster** via `videoflow deploy` — one Job or Deployment per node. | the shared k3s cluster (`./scripts/k3s-test-up.sh`), or a kind cluster (`./scripts/kind-up.sh`, what CI uses) |

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
only when the **durable** instance answers (`--profile redis-durable`,
`VF_TEST_REDIS_DURABLE_URL`), because `toy_router`'s idempotent sink must produce
the same answer either way and `run-local` reads the live store's persistence
back: a BATCH flow on the evictable dev cache is refused rather than admitted to
a guarantee the store cannot keep. The Python clients come from the `dev`
dependency group, so
a plain `uv sync` is enough.

### Start it

**Docker Compose (recommended)**

```bash
docker compose up -d
```

[docker-compose.yml](../../docker-compose.yml) runs `nats:2.10` with `-js -m 8222`
(4222 for clients, 8222 for monitoring) and `redis:7` with the dev profile's settings
(`--appendonly yes --maxmemory-policy noeviction`, a 4 GB cap): the same shape
`videoflow deploy` and `run-local` provision, and one a BATCH flow is admitted on —
admission reads a bring-your-own store back and refuses an evictable cache for
`reliable_work`, so the `redis-small` profile below is where eviction is exercised.

**Optional compose profiles** — the fixtures the *broker-level conformance cases*
(`tests/conformance`, `-m level_broker`) need beyond the default pair. Each is gated
by its own variable and reports `NOT_RUN` naming the profile when it is absent;
nothing is assumed from a default URL:

```bash
docker compose --profile cluster --profile toxiproxy --profile redis-durable \
               --profile redis-small --profile restricted up -d
export VF_TEST_NATS_CLUSTER_URLS=nats://localhost:4231,nats://localhost:4232,nats://localhost:4233
export VF_TEST_TOXIPROXY_URL=http://localhost:8474 VF_TEST_NATS_PROXIED_URL=nats://localhost:4242
export VF_TEST_REDIS_PROXIED_URL=redis://localhost:6380/0
export VF_TEST_REDIS_DURABLE_URL=redis://localhost:6381/0 VF_TEST_REDIS_SMALL_URL=redis://localhost:6382/0
export VF_TEST_NATS_RESTRICTED_URL=nats://restricted:restricted@localhost:4224
uv run pytest tests/conformance -q -rs -m level_broker
```

| Profile | Service | Port(s) | Used by |
|---|---|---|---|
| `cluster` | three-server JetStream cluster (`nats-1/2/3`, file store) | 4231 / 4232 / 4233 | replication and quorum cases (MSG-016, MSG-018) |
| `toxiproxy` | `toxiproxy` in front of `nats` (4242) and `redis` (6380); control API | 8474 | lost acks, stalled publishes, severed connections (MSG-013/014/015, PAY-004, RUN-013) |
| `redis-durable` | Redis with `--appendonly yes` on a named volume | 6381 | the durable profile's local twin: payload obligations and the run ledger across restarts (MSG-018, PAY-006, PAY-011) |
| `redis-small` | Redis with `--maxmemory 64mb --maxmemory-policy volatile-lru` | 6382 | eviction under pressure (PAY-010) |
| `restricted` | NATS with a `restricted` user denied `$JS.API.CONSUMER.CREATE.>` | 4224 | provisioning without permission (MSG-005) |

The proxies are seeded from the compose file, so a case only adds and removes
toxics ([`tests/conformance/_toxiproxy.py`](../conformance/_toxiproxy.py)); every
fixture resets them afterwards. The default services are unchanged by the profiles.

Every test names its run uniquely and tears its streams down, but a run killed
mid-way (a timeout, a Ctrl-C) leaves them, and a few cases deliberately leave a
flow's DLQ behind to prove it survives a run's teardown. Over weeks a dev broker
accumulates hundreds of `vf-*` streams, and one `$JS.API.STREAM.LIST` reply carries
256 of them — the product pages (`topology._list_streams`), and so must a test that
lists streams itself. Sweep the compose broker when it fills up (every stream on it
is a test's; nothing else uses it):

```bash
uv run python -c "
import asyncio, nats
from videoflow.messaging.topology import _list_streams
async def go():
    nc = await nats.connect('nats://localhost:4222'); js = nc.jetstream()
    for s in await _list_streams(js): await js.delete_stream(s.config.name)
    await nc.close()
asyncio.run(go())"
```

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
`uv run pytest --ignore=tests/integration --ignore=tests/conformance`.

---

## k8s/ — a Kubernetes cluster: k3s (the default) or kind

The bucket runs against whichever of two flavors the environment names. Nothing
in the tests differs between them: `support_k8s.py` resolves the facts that do
(where images come from, where the work root is, how the broker is reached, which
flags `deploy` needs) once, from the environment, and `deploy_solution` adds the
flavor's flags itself.

| | k3s (default) | kind (`VF_KIND_CLUSTER` set; CI) |
|---|---|---|
| stand it up | `./scripts/k3s-test-up.sh` | `./scripts/kind-up.sh` |
| expected context | `VF_K8S_CONTEXT` (default `default`) | `kind-$VF_KIND_CLUSTER` |
| images | pushed to `VF_K8S_IMAGE_REGISTRY` with crane, pulled by the nodes | side-loaded with `kind load` |
| work root | the backing directory of the RWX claim `VF_K8S_PVC` on the NFS server (this host), reached in the pods through `--mount-pvc` | `/tmp/videoflow-k8s`, bind-mounted into the node |
| broker from the host | NodePort `30422` on this node (`nats://127.0.0.1:30422`) | `127.0.0.1:4223` via an extraPortMapping |
| every test pod | `priorityClassName: cluster-batch`, `--image-pull-policy Always` | cluster defaults |

### The k3s cluster is shared — what the tests and the script will and will not do

The k3s cluster is production infrastructure other people run GPU jobs on. The
rules, all enforced by `scripts/k3s-test-up.sh` and `k8s/conftest.py`:

- **Verify, never provision a cluster.** The script checks the kubeconfig, that the
  current context *equals* `VF_K8S_CONTEXT`, and that `videoflow.deploy.cluster.detect_cluster()`
  says `k3s`; it stops otherwise. It never runs `kubectl config use-context` — a
  silent retarget would push test workloads into whatever cluster you were
  actually pointed at. The gate does the same and skips with the reason.
- **Namespaced objects only, all labelled `app.kubernetes.io/managed-by=videoflow`:**
  the namespace `VF_K8S_NAMESPACE`, the RWX claim ([k8s/test-pvc.yaml](../../k8s/test-pvc.yaml)),
  NATS + Redis from videoflow's own `deploy.infra` manifests, the NodePort
  ([k8s/nats-nodeport.yaml](../../k8s/nats-nodeport.yaml)), one short-lived probe pod,
  and the per-test Jobs/Deployments/ConfigMaps every test deletes. `kubectl delete ns
  videoflow-test` removes all of it.
- **Never:** label a node, change the GPU Operator `ClusterPolicy`, enable MIG,
  install anything cluster-wide (the StorageClass `nfs-shared` and the
  PriorityClass `cluster-batch` must already exist), restart docker or edit its
  daemon config, or create a pod that requests a GPU. This bucket is CPU-only on
  both flavors.
- **Yield to training work.** Every pod the tests create carries
  `priorityClassName: cluster-batch` (never `cluster-training`, which preempts),
  so a training job that needs the capacity evicts the test rather than the other
  way round.

### Stand it up (k3s)

```bash
./scripts/k3s-test-up.sh             # verify + prepare; ~5 min the first time (image builds)
./scripts/k3s-test-up.sh --durable   # also the durable broker profile, in videoflow-test-ha
                                     # (NodePort 30423; exports VF_K8S_HA_NAMESPACE / VF_K8S_HA_NATS_URL,
                                     #  which gate the conformance durability cases MSG-021/022/023, PAY-011)
# The conformance kubernetes level runs only with VF_K8S_NAMESPACE (and, for the
# durability cases, VF_K8S_HA_NAMESPACE) exported explicitly: those cases delete
# broker pods (PAY-011 restarts the dev namespace's Redis too, to show its emptyDir
# loses what its claim-backed twin keeps) and scale the durable StatefulSet, so a
# plain `pytest` on a host that is a cluster node reports them NOT_RUN instead of
# touching the cluster by itself. The script is idempotent: rerun it after pulling
# a tree that changes worker-side code (the images) or the dev profile (it replaces
# a Redis whose arguments no longer match the profile and stamps the profile
# record every deploy reads onto Services created before the record existed).
uv run pytest tests/integration/k8s -q -rs
kubectl delete ns videoflow-test     # when you are done (and videoflow-test-ha)
```

The script builds `videoflow-base:py3.12`, the [fixture image](k8s/Dockerfile) and
one image per toy solution exactly as `kind-up.sh` does, then pushes them with
[scripts/push-images.sh](../../scripts/push-images.sh): `docker save` to a tarball
under `$HOME/.cache/videoflow/images` (never `/`, which is tiny on the nodes) and
`crane push --insecure` to the registry. crane runs in user space; if it is not on
`PATH` the script prints the one-line install into `~/.local/bin` and stops. The
registry speaks plain HTTP and the node's docker does not trust it — that is the
whole reason for crane: teaching docker about the registry means restarting it,
which kills every container on the host, including other people's. Each image is
also tagged locally under the registry prefix so `deploy` can run a solution's
`prepare.py` in it with `docker run` without pulling.

It then applies the claim and waits for it to bind, installs the dev broker
through `deploy.infra.ensure_infra` (so the tests run against exactly what
`videoflow deploy` would install, and each deploy's own `ensure_infra` finds the
Services present and owns nothing), applies the NodePort, and — the check that
matters — writes a sentinel into the claim's backing directory on this host and
reads it back from a pod that mounts the claim, scheduled on a *different* node
where possible (a pod on the NFS server would see the directory even with the
export unreachable). It ends by printing the exports the tests read.

### The two things that make it work

**The work root is the same directory on every node and on this host.** A
solution's `work_dir` is resolved against its config file's directory and baked
into the node parameters at compile time; `prepare.py` writes into it from a
container on this host; the pods write their artifacts there; the test reads them
here. On kind that is a bind mount into the single node. On k3s no node's own
filesystem holds the directory, so the tests stage everything inside the RWX
claim's backing directory — `/opt/data/cluster-share/<pv subdir>/` on the NFS
server, found from the bound PersistentVolume's `spec.csi.volumeAttributes` — and
deploy with `--mount-pvc vf-test-share:/opt/data/cluster-share/<pv subdir>`. The
`x-mounts` hostPath the solution declares for its `work_dir` falls under that
path, so `deploy` drops it from the pods (the claim serves it there) and keeps it
for the prepare container on the host: the deterministic rule in
`manifests.pod_mounts`. Because the artifacts are read from the export's backing
directory, **the k3s bucket runs on the NFS server** (node 02 today) or on a host
that mounts the export at the same path.

**The broker is reachable from both sides.** Workers use the in-cluster name
`nats://nats.videoflow-test.svc:4222`. The host needs its own way in, to read the
dead-letter queue and run `videoflow teardown`: a NodePort on `30422`, which a
cluster node reaches at `127.0.0.1:30422`; from elsewhere,
`kubectl -n videoflow-test port-forward svc/nats 30422:4222` gives the same URL.
kind pairs the same NodePort with an `extraPortMapping` onto `127.0.0.1:4223`
(4223 rather than 4222 so the docker-compose broker keeps its port and both halves
of the suite can run at once).

### Environment

| Variable | Default | What it is |
|---|---|---|
| `VF_KIND_CLUSTER` | unset | **set it to use kind**: the cluster name; the expected context is then `kind-<name>` |
| `VF_K8S_CONTEXT` | `default` | the expected kubectl context on k3s; never switched to, only checked |
| `VF_K8S_NAMESPACE` | `videoflow-test` | namespace holding the broker, the claim and every test flow |
| `VF_K8S_PVC` | `vf-test-share` | the RWX claim the k3s work root lives on |
| `VF_K8S_IMAGE_REGISTRY` | `10.128.81.10:5000` on k3s, unset on kind | prefix for every image ref; the gate checks the images are there (crane when installed, else the registry's v2 API) |
| `VF_K8S_WORK_ROOT` | kind: `/tmp/videoflow-k8s`; k3s: derived from the claim | where solutions are staged; on k3s it must lie inside the claim's directory |
| `VF_K8S_NATS_URL` | `nats://127.0.0.1:30422` (k3s), `nats://127.0.0.1:4223` (kind) | the broker as reached from the host |
| `VF_K8S_NATS_NODEPORT` | `30422` | the NodePort the script publishes (script only) |
| `VF_K8S_PRIORITY_CLASS` | `cluster-batch` on k3s | the PriorityClass every test pod carries; empty disables |

The GPU paths are covered by unit tests, by the conformance suite's own fixtures,
and by the walkthrough in the top-level [README.md](../../README.md); this bucket
never requests a GPU on either flavor.

### The conformance suite's GPU levels (`tests/conformance/`)

The conformance suite has two more gates for accelerators, both opt-in by an
explicit export and both refusing devices that are not idle:

| Variable | What it opens | The gate checks |
|---|---|---|
| `VF_TEST_GPU_UUIDS` | the `gpu` level on **this host** (ALLOC-014/015/016, RUN-039/043): `run-local`-style grants on real devices with a CUDA-runtime probe as the independent witness | `cuda-python` installed (`uv sync --group gpu-test`), every UUID present per `nvidia-smi -L`, **no compute process on any of them** (`nvidia-smi --query-compute-apps`, re-checked at teardown), and never GPUs 0/1 of `lnmcltappgke02` (a service runs there) |
| `VF_K8S_GPU_NODES` | in-cluster GPU pods (ALLOC-004/008/017/029/030/031, RUN-028/029/032): inert `sleep` holders of the base image on the pool nodes named | each node carries `videoflow.io/gpu-pool=true` — applied by the operator (`kubectl label node <n> videoflow.io/gpu-pool=true`), never by a test or script — none is `lnmcltappgke02`, and the allocated-GPU read is Known |
| `VF_K8S_MIG_NODE` | managed-MIG lifecycle on hardware (ALLOC-003/005/006/012/013/033) — geometry applied and restored through the GPU Operator | one node from `VF_K8S_GPU_NODES` with zero allocated GPUs and `nvidia.com/mig.capable=true`; the cases additionally need the operator's *mixed* MIG strategy on it (managed MIG names `nvidia.com/mig-<profile>` resources) and report NOT_RUN under `single` |
| `VF_TEST_MIG_GPU_UUID` | host-side MIG placement (ALLOC-002) and profile qualification (ALLOC-032): MIG mode switched on one of the `VF_TEST_GPU_UUIDS` devices, every accepted layout instantiated through `nvidia-smi mig -cgi` and read back, mode restored at the end | one of the gated devices; passwordless `sudo` (instance creation is privileged); **no driver client holding `/dev/nvidia<N>` open** (`fuser` — a container started with every card visible blocks instance creation with "In use by another client", and is never ours to stop) |
| `VF_BENCH_THRESHOLDS_JSON` | the `benchmark` level (MSG-026, PAY-017, PAY-022, ALLOC-032): a path to, or the inline JSON of, *your* SLOs and workload parameters keyed by case id — [tests/conformance/bench/thresholds.example.json](../conformance/bench/thresholds.example.json) is the shape and what one developer box sustains | the case's entry present; the infrastructure each benchmark measures (the compose broker's monitoring port `VF_TEST_NATS_MONITOR_URL`, default `http://localhost:8222`; Redis; a MIG-capable device). A benchmark passes only against supplied thresholds and always writes what it measured with the environment it measured on — it is never a universal scale claim |

**A green GPU test is not evidence a model ran.** The gpu-level cases prove what
the *runtime* does with a grant — enumeration, masks, memory budgets, peer access,
MIG placement — with a CUDA-runtime probe as the witness; none of them loads a
model. The component cases that do (RUN-037/038/040/041/042) live with the
components in `../videoflow-contrib` and report NOT_RUN here, on purpose.

The DRA cases (ALLOC-018..028, kubernetes/gpu levels) gate on a DRA driver
publishing GPU `ResourceSlices` and report NOT_RUN without one; their model-level
variants (the version/feature-gate matrix, the rendered claims, the readiness
state machine on the reference allocator) run everywhere. The process-level
local-GPU cases use a fake `nvidia-smi` on `PATH`
([tests/conformance/tools/fake_nvidia_smi.py](../conformance/tools/fake_nvidia_smi.py)),
so a host with one, zero or unobservable GPUs is a fixture, not a machine. The
cluster cases that run a real worker pod (RUN-018/019/026/027/047, RUN-030's
cluster variant, RUN-033) need the base and fixtures images rebuilt from the
current tree — `scripts/k3s-test-up.sh` does it — because the fixture nodes they
use and the worker-side code they exercise (the partition lease, the obligation
reconciler) live in the images. RUN-018/019 deploy into the test namespace and
start their extra pods by hand from the Job's template (a Job's parallelism is
bounded by its completions, so it cannot be "scaled"): the run ledger that refuses
a scaled-out singleton at bind time needs a Redis that persists and never evicts,
which the dev profile's append-only, `noeviction` Redis is — its emptyDir is what
separates it from the durable profile, and that only matters for surviving a pod
loss (PAY-011, MSG-021).

### kind, for CI

`scripts/kind-up.sh` and `scripts/kind-down.sh` are unchanged and remain what CI
runs: they create a throwaway cluster from [k8s/kind-cluster.yaml](../../k8s/kind-cluster.yaml),
side-load the images, install the broker and prove the bind-mounted work root
round-trips. Export `VF_KIND_CLUSTER=videoflow` (the script prints it) and the
bucket takes the kind path. kind has no supported GPU passthrough, so it is
CPU-only, like the k3s path.

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
them end in `run ./scripts/k3s-test-up.sh` (or `kind-up.sh`). A context message
means kubectl points somewhere other than the cluster the environment names — the
tests never switch it for you; export `VF_K8S_CONTEXT` or `VF_KIND_CLUSTER` to say
which one you mean. The kind work-root message means the cluster was created before
the directory existed or with a different `VF_K8S_WORK_ROOT` — recreate it.

**A k8s test fails with "report.json was not written".** The flow ran and the pods
exited cleanly, but their output never reached the host. Check the mount — on kind:

```bash
echo hello > /tmp/videoflow-k8s/.check
docker exec videoflow-control-plane cat /tmp/videoflow-k8s/.check
```

On k3s, re-run `./scripts/k3s-test-up.sh`: its last step is exactly this round trip
through the claim, and it prints what the probe pod saw when it fails.

**`ImagePullBackOff` on k3s.** The image is not in the registry, or was pushed under
another tag: `curl --noproxy '*' http://10.128.81.10:5000/v2/_catalog`, then
`./scripts/push-images.sh <name:tag>`.

**A k3s deploy stops at `sudo k3s ctr images import`.** `deploy` found the
registry-qualified image tag locally (the push script tags it so `prepare.py` can
run in it) and tried to side-load it; the k3s flavor must skip registry-qualified
refs, which the nodes pull themselves.

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

**`k8s/`** — the same four solutions driven by `videoflow deploy` on k3s or kind, using
the same config dicts and the same assertions, imported from
[support_solutions.py](support_solutions.py). That sharing is the point: the
framework's promise is that a graph built on one machine runs unchanged on many, so
the two engines must produce the same answer from the same input, and anything
asserted in only one of them is a claim nobody is checking. Alongside them, `k8s/`
covers the machinery the local path has no counterpart for — infra provisioning, Job
completion, and the `deploy` exit codes.
