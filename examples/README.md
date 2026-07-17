# Examples

Runnable example flows. Each script is self-contained and documented in its module
docstring. Most run locally against a NATS JetStream server (start one with
`docker compose up -d` from the repo root, or `nats-server -js`) and deploy to
Kubernetes unchanged via `videoflow deploy <script>:build_flow --nats ...`.

## Getting started

| Script | What it shows |
| --- | --- |
| [simple_example1.py](simple_example1.py) | Minimal distributed flow — a single-producer diamond (fan-out + join). |
| [simple_example2.py](simple_example2.py) | A second small linear/graph variant. |
| [custom_nodes.py](custom_nodes.py) | Subclassing the three base node types with `open()`/`close()` lifecycle hooks. |
| [example_nodes.py](example_nodes.py) | Importable custom node classes shared by the other scripts (why custom nodes must live in a real module). |
| [stopping_flow.py](stopping_flow.py) | Starting a flow and stopping it after a fixed time via `flow.stop()`. |

## Parallelism and routing

| Script | What it shows |
| --- | --- |
| [simple_mp_example1.py](simple_mp_example1.py) | `nb_tasks > 1` competing-consumer replicas (joiner stays `nb_tasks=1`). |
| [simple_mp_example2.py](simple_mp_example2.py) | A second multi-processor variant. |
| [partitioned_processing.py](partitioned_processing.py) | `partition_by='trace_id'` pins each key to one replica so a replicated processor can keep per-key state. |
| [aggregators.py](aggregators.py) | Running aggregators (`OneTaskProcessorNode`) fanned out and rejoined. |

## I/O and vision

| Script | What it shows |
| --- | --- |
| [file_output.py](file_output.py) | Writing flow output to a file with `FileAppenderConsumer`. |
| [reading_livestream.py](reading_livestream.py) | Reading an RTSP stream (infinite producer) in REALTIME mode. |
| [object_detector.py](object_detector.py) | Object detection over a video ([demo](https://www.youtube.com/watch?v=TYGMllb7fHM)). Requires `videoflow_contrib`. |
| [object_tracking.py](object_tracking.py) | Detector + tracker writing an annotated video. Requires `videoflow_contrib`. |
| [object_detector.ipynb](object_detector.ipynb) | The detector example as a notebook. |

## Time-synchronized fusion

| Script | What it shows |
| --- | --- |
| [multicamera_time_sync.py](multicamera_time_sync.py) | Fusing several independent camera streams plus a high-rate sensor by **event time** (`JoinPolicy(mode='time', ...)`) — the shape of a multi-view / offside pipeline. |
