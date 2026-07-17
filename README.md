# Videoflow

![Videoflow](assets/videoflow_logo_small.png)

[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/videoflow/videoflow/blob/master/LICENSE)

**Videoflow** is a Python framework for building **distributed** video and stream
processing pipelines. You describe your pipeline once as a directed acyclic graph
of producers, processors and consumers, and Videoflow runs it as a set of
independent workers that communicate over a [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
message broker.

The same graph runs two ways with no code changes:

- **Locally**, as one OS subprocess per node — for fast development and testing.
- **On Kubernetes**, as one container per node — with per-node scaling, GPU
  scheduling, health probes and autoscaling for production.

It ships with off-the-shelf components for object detection, tracking, pose
estimation, segmentation and video I/O, and is easy to extend with your own.

---

## How it works

```
   ┌──────────┐      ┌───────────┐      ┌───────────┐      ┌──────────┐
   │ producer │─────▶│ processor │─────▶│ processor │─────▶│ consumer │
   └──────────┘      └───────────┘      └───────────┘      └──────────┘
        │                  │                  │                  │
        └──────────────────┴───── NATS JetStream ───────────────┘
                        (one stream per node)
```

- Each **node** is identified by a stable, unique `name` and runs in its own
  worker (subprocess locally, pod on Kubernetes).
- Every node publishes its output to its own broker subject; each node subscribes
  to the subjects of its real parents and reassembles its inputs. This makes
  arbitrary DAGs — multi-parent joins, multiple independent producers, fan-out —
  work naturally.
- A node's constructor arguments must be **JSON-serializable** so a worker can
  reconstruct just its one node from configuration. Expensive or stateful setup
  (opening a camera, loading a model) belongs in the node's `open()` method, not
  its `__init__`.

---

## Installation

Requires **Python 3.6+** and a running NATS JetStream server at runtime.

```bash
pip install "videoflow[distributed]"   # core + broker client + wire format
pip install "videoflow[vision]"        # + OpenCV for vision processors
pip install "videoflow[video]"         # + ffmpeg/OpenCV for video I/O
pip install "videoflow[deploy]"        # + Kubernetes manifest generation
pip install "videoflow[all]"           # everything
```

From a clone: `pip install ".[all]"`

Start a local broker for development (a `docker-compose.yml` with NATS + Redis is
included):

```bash
docker compose up -d          # NATS JetStream on :4222, Redis on :6379
# or, without Docker:
nats-server -js
```

---

## Quickstart

A pipeline is defined inside a `build_flow()` factory that returns a `Flow`. The
same factory is used to run locally and to deploy to Kubernetes.

```python
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

def build_flow():
    producer  = IntProducer(0, 40, 0.1, name='producer')
    identity  = IdentityProcessor(name='identity')(producer)
    identity1 = IdentityProcessor(name='identity1')(identity)
    joined    = JoinerProcessor(name='joined')(identity, identity1)
    printer   = CommandlineConsumer(name='printer')(joined)
    # Producers are discovered automatically from the consumers — list only the leaves.
    return Flow([printer], flow_type=BATCH)

if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())   # one subprocess per node, talking to NATS
    flow.join()
```

Run it (with the broker up):

```bash
python my_flow.py
# or via the CLI:
videoflow run-local my_flow.py:build_flow --nats nats://localhost:4222
```

---

## Deploying to Kubernetes

The CLI imports your `build_flow()`, compiles the graph, and renders one
Deployment (or a Job, for finite producers) plus a ConfigMap per node, choosing
the correct per-family image for each node.

```bash
# 1. Broker in the cluster (for dev clusters; use the NATS Helm chart in prod)
kubectl create namespace videoflow
kubectl apply -n videoflow -f k8s/nats.yaml

# 2. Build & push the per-component images
./docker/build-images.sh ghcr.io/acme v1
docker push ghcr.io/acme/videoflow-base:v1   # ...and each family image

# 3. Render and apply the manifests for your graph
videoflow deploy my_flow.py:build_flow \
    --nats nats://nats.videoflow.svc:4222 \
    --namespace videoflow \
    --registry ghcr.io/acme --image-tag v1 \
    --autoscaling                             # optional KEDA scalers
kubectl apply -k ./manifests
```

Use `--dry-run` to print the manifests to stdout without writing files.

### How graph concepts map onto the broker and Kubernetes

| Concept | Behavior |
| --- | --- |
| `flow_type=REALTIME` | broker keeps only the freshest message per edge — stale frames are dropped, producers never block |
| `flow_type=BATCH` | at-least-once delivery with a deep buffer and explicit acks |
| `ProcessorNode(nb_tasks=N)` | N competing-consumer replicas (Deployment replicas). A **multi-parent join must keep `nb_tasks=1`** |
| `device_type=GPU` | pod requests `nvidia.com/gpu` plus a GPU-pool nodeSelector/toleration |
| finite `ProducerNode` (`is_finite=True`) | Kubernetes **Job**; infinite/streaming producers and all other nodes are **Deployments** |
| `flow.stop()` | publishes on a control channel every worker subscribes to, then tears the workloads down |
| observability | each worker exposes `/metrics` (Prometheus) and `/readyz` + `/healthz` probes |

---

## The three node types

| Type | Base class | Implements | Role |
| --- | --- | --- | --- |
| Producer | `ProducerNode` | `next()` | Creates data from an external source (camera, file, stream). Set `is_finite=False` for unbounded sources. |
| Processor | `ProcessorNode` | `process(*inputs)` | Transforms inputs into an output. Supports `nb_tasks` (parallel replicas) and `device_type` (`cpu`/`gpu`). |
| Consumer | `ConsumerNode` | `consume(item)` | Terminal sink — writes to a file, pushes to a REST API/S3, etc. Produces no output. |

Every node also has `open()`/`close()` lifecycle hooks for acquiring and releasing
resources.

### Writing a custom node

```python
from videoflow.core.node import ProcessorNode

class Threshold(ProcessorNode):
    def __init__(self, cutoff, **kwargs):   # args must be JSON-serializable
        self._cutoff = cutoff               # store them so get_params() can find them
        super().__init__(**kwargs)

    def open(self):
        ...                                 # heavy/stateful setup goes here

    def process(self, value):
        return value if value >= self._cutoff else 0
```

Always accept and forward `**kwargs` to `super().__init__()` (that's how `name`,
`nb_tasks`, `device_type`, etc. are passed through), and store each constructor
argument on `self` under the same name so it can be captured for reconstruction in
a worker.

---

## Per-component Docker images

Each node family has its own image so a pod carries only the dependencies its node
needs:

| Image | For |
| --- | --- |
| `videoflow-base` | framework + broker client + wire format (foundation for the others) |
| `videoflow-basic` | producers/processors/consumers with no extra deps |
| `videoflow-vision` | `videoflow.processors.vision.*` (OpenCV, DL frameworks) |
| `videoflow-video-io` | `videoflow.producers.video` / `videoflow.consumers.video` (ffmpeg) |

```bash
./docker/build-images.sh                    # local images
./docker/build-images.sh ghcr.io/acme v1    # tagged for a registry
```

The compiler maps each node to an image family automatically from its module path;
override per node with `videoflow deploy --image-override <node-name>=<family>`.

---

## Contributing

A tentative [roadmap](ROADMAP.md) of where we are headed, and the
[contribution rules](CONTRIBUTING.md).

New processors, producers or consumers that pull in additional third-party
dependencies belong in the [videoflow-contrib](https://github.com/videoflow/videoflow-contrib)
project — we keep the core framework lean.

## Citing Videoflow

If you use Videoflow in your research please use the following BibTeX entry.

```
@misc{deArmas2019videoflow,
  author =       {Jadiel de Armas},
  title =        {Videoflow},
  howpublished = {\url{https://github.com/videoflow/videoflow}},
  year =         {2019}
}
```
