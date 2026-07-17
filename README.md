# Videoflow

![Videoflow](assets/videoflow_logo_small.png)

[![Build Status](https://travis-ci.org/videoflow/videoflow.svg?branch=master)](https://travis-ci.org/videoflow/videoflow)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/videoflow/videoflow/blob/master/LICENSE)

**Videoflow** is a Python framework for video stream processing. The library is designed to facilitate easy and quick definition of computer vision stream processing pipelines. It empowers developers to build applications and systems with self-contained Deep Learning and Computer Vision capabilities using simple and few lines of code.  It contains off-the-shelf reference components for object detection, object tracking, human pose estimation, etc, and it is easy to extend with your own.

The complete documentation to the project is located in [**docs.videoflow.dev**](https://docs.videoflow.dev)

[1.2]: http://i.imgur.com/wWzX9uB.png
[1]: http://www.twitter.com/videoflow_py
<!--Follow us on [![alt text][1.2]][1]-->


> **Videoflow is now a distributed framework.** A flow is a directed acyclic
> graph of nodes that communicate over a [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
> message broker instead of in-process queues. You can run the exact same graph
> two ways: as local subprocesses for development, or as one container per node on
> Kubernetes for production. See [Distributed execution](#distributed-execution) below.

## Installing the framework
### Requirements
Python 2 is **NOT SUPPORTED**.  Requires Python 3.6+. A running NATS JetStream
server is required at runtime (`nats-server -js`, or `docker compose up -d` using
the provided `docker-compose.yml`).

### Installation
Install the core plus the distributed runtime:

```bash
pip3 install "videoflow[distributed]"      # broker client + wire format
pip3 install "videoflow[vision]"           # + OpenCV for vision processors
pip3 install "videoflow[video]"            # + ffmpeg/OpenCV for video I/O
pip3 install "videoflow[deploy]"           # + Kubernetes manifest generation
pip3 install "videoflow[all]"              # everything
```

Or from a clone: `pip3 install ".[all]" --user`

### Building the per-component Docker images
Each node family has its own image (base, basic, vision, video-io) so a pod only
carries the dependencies its node needs:

```bash
./docker/build-images.sh                    # local images
./docker/build-images.sh ghcr.io/acme v1    # tagged for a registry
```
## Contributing:
A tentative [roadmap](ROADMAP.md) of where we are headed.

[Contribution rules](CONTRIBUTING.md).

If you have new processors, producers or consumers that you can to create, check the [videoflow-contrib](https://github.com/videoflow/videoflow-contrib) project.  We want 
to keep videoflow succinct, clean, and simple, with as minimal dependencies to third-party libraries as necessaries. [videoflow-contrib](https://github.com/videoflow/videoflow-contrib) is better suited for adding new components that require new library 
dependencies.

## Sample Videoflow application:
Below a sample videoflow application that detects automobiles in an intersection. For more examples see the [examples](examples/) folder. It uses detection model published by [tensorflow/models](https://github.com/tensorflow/models/tree/master/research/object_detection)

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/TYGMllb7fHM/0.jpg)](https://www.youtube.com/watch?v=TYGMllb7fHM)

A graph is defined inside a `build_flow()` factory that returns a `Flow`. The same
factory is used both to run locally and to deploy to Kubernetes.

```python
import videoflow
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow_contrib.detector_tf import TensorflowObjectDetector
from videoflow.processors.vision.annotators import BoundingBoxAnnotator
from videoflow.utils.downloader import get_file

URL_VIDEO = "https://github.com/videoflow/videoflow/releases/download/examples/intersection.mp4"

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    def __init__(self, **kwargs):
        super(FrameIndexSplitter, self).__init__(**kwargs)

    def process(self, data):
        index, frame = data
        return frame

def build_flow():
    input_file = get_file("intersection.mp4", URL_VIDEO)
    reader = VideofileReader(input_file, name='reader')
    frame = FrameIndexSplitter(name='frame')(reader)
    detector = TensorflowObjectDetector(name='detector')(frame)
    annotator = BoundingBoxAnnotator(name='annotator')(frame, detector)
    writer = VideofileWriter("output.avi", fps=30, name='writer')(annotator)
    # Producers are discovered automatically from the consumers; you only list leaves.
    return Flow([writer], flow_type=BATCH)

if __name__ == "__main__":
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())   # one subprocess per node, talking to NATS
    flow.join()
```

## Distributed execution

Every node is identified by a **stable `name`** (unique within a flow) rather than
its in-process object id, and each node's constructor arguments must be
JSON-serializable so a worker can reconstruct just its one node from
environment variables. Two things follow: expensive/stateful setup belongs in
`open()` (not `__init__`), and any node runs identically as a local subprocess or
a Kubernetes pod.

**Run locally** (needs a NATS server — `docker compose up -d`):

```bash
python examples/simple_example1.py
# or via the CLI:
videoflow run-local examples/simple_example1.py:build_flow --nats nats://localhost:4222
```

**Deploy to Kubernetes** — the CLI imports your `build_flow()`, compiles the graph,
and renders one Deployment (or Job, for finite producers) + ConfigMap per node,
picking the right per-family image for each:

```bash
kubectl apply -n videoflow -f k8s/nats.yaml            # in-cluster broker
videoflow deploy examples/simple_example1.py:build_flow \
    --nats nats://nats.videoflow.svc:4222 \
    --namespace videoflow --registry ghcr.io/acme --image-tag v1 \
    --autoscaling                                       # optional KEDA scalers
kubectl apply -k ./manifests
```

How graph concepts map onto the broker and Kubernetes:

| Concept | Behavior |
| --- | --- |
| `flow_type=REALTIME` | broker keeps only the freshest message per edge — stale frames are dropped, producers never block |
| `flow_type=BATCH` | at-least-once delivery with a deep buffer and explicit acks |
| `ProcessorNode(nb_tasks=N)` | N competing-consumer replicas (Deployment replicas); a **multi-parent join must keep `nb_tasks=1`** |
| `device_type=GPU` | pod requests `nvidia.com/gpu` + GPU-pool nodeSelector/toleration |
| finite `ProducerNode` (`is_finite=True`) | Kubernetes **Job**; infinite/streaming producers and all other nodes are **Deployments** |
| `flow.stop()` | publishes on a control channel every worker subscribes to, then tears the workloads down |
| metrics | each worker exposes `/metrics` (Prometheus) and `/readyz` + `/healthz` probes |

## The Structure of a flow application

A flow application usually consists of three parts:

1. Define a directed acyclic graph of computation nodes: producers (create data), processors (transform data), and consumers (terminal sinks — write to a file, push to a REST API/S3, etc.). Give each node a unique `name`.

2. Wrap the graph in a `build_flow()` factory that returns `Flow(consumers, flow_type=...)`. Producers are discovered by walking parents back from the consumers, so you only pass the leaves. Multiple independent producers (e.g. several cameras) are fully supported.

3. Run the flow through an execution engine — `LocalProcessEngine` for development or `KubernetesExecutionEngine` / the `videoflow deploy` CLI for production. `flow.stop()` signals termination on the broker control channel; producers stop first and the rest drain and exit in turn.

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
