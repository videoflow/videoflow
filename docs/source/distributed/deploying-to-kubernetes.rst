Deploying to Kubernetes
=======================

The ``videoflow deploy`` CLI imports your ``build_flow()`` factory, compiles the
graph, and renders one Kubernetes workload plus a ConfigMap per node — each running
the container image you provide with ``--image`` (or a node's own ``image=``). The
same graph you run locally deploys unchanged.

Prerequisites
-------------

- A Kubernetes cluster and ``kubectl`` configured against it.
- A NATS JetStream broker reachable from inside the cluster. For dev clusters
  (kind, minikube) apply the bundled manifest::

      kubectl create namespace videoflow
      kubectl apply -n videoflow -f k8s/nats.yaml

  For production, use the official NATS Helm chart.
- Your container image built and pushed to a registry your cluster can pull from
  (see below).

Building the image
------------------

Videoflow ships a single ``videoflow-base`` image (framework + broker client + the
built-in nodes' dependencies: OpenCV, ffmpeg, Redis). You build **your** image on top
of it, adding your dependencies and your node package so the worker can import your
node classes by their module path::

    # Dockerfile (see docker/user-image.example.Dockerfile)
    FROM videoflow-base:latest
    RUN pip install torch my-libs        # your dependencies
    COPY . . && RUN pip install .        # your package

::

    ./docker/build-images.sh ghcr.io/acme v1     # build+tag videoflow-base
    docker push ghcr.io/acme/videoflow-base:v1
    docker build -t ghcr.io/acme/app:v1 .        # your image, FROM videoflow-base
    docker push ghcr.io/acme/app:v1

A pure built-in flow can just deploy with ``--image videoflow-base:latest``.

Deploying
---------

::

    videoflow deploy my_flow.py:build_flow \
        --nats nats://nats.videoflow.svc:4222 \
        --namespace videoflow \
        --image ghcr.io/acme/app:v1 \
        --flow-id my-flow \
        --autoscaling
    kubectl apply -k ./manifests

Useful options:

``--image`` / ``--image-override NAME=REF``
    ``--image`` is the default container image for every node that didn't declare its
    own ``image=``. ``--image-override`` sets the image for one node and wins over both
    the default and the node's own image (repeatable).

``--dry-run``
    Print the manifests to stdout instead of writing files — handy for review or
    piping into ``kubectl apply -f -``.

``--flow-id``
    A stable identifier used to name all resources. Reuse the same value to
    redeploy/update the same logical flow.

``--autoscaling`` / ``--max-replicas``
    Emit a KEDA ``ScaledObject`` per processor that scales on broker backlog, using
    ``nb_tasks`` as the minimum replica count.

``--blob-redis-url``
    Enable the external blob store for large payloads.

``--run-id``
    Per-run id that scopes this run's broker streams (auto-generated otherwise). A
    new run id gives fresh streams; reuse it to target the same run.

Other CLI commands
------------------

``videoflow explain my_flow.py``
    Print a human-readable summary of the compiled graph — nodes, replicas, image
    families, partitioning, subjects, and the DLQ stream — without touching a cluster.

``videoflow provision my_flow.py --nats ...``
    Create the flow's broker streams and durable consumers up front. This normally
    happens automatically (a generated init Job on Kubernetes, or the local engine
    before it spawns workers), but is exposed for manual/debug use.

``videoflow teardown --flow-id ... --run-id ... --nats ... [--namespace ...]``
    Stop a run (control-channel signal) and delete its broker streams; with
    ``--namespace`` it also ``kubectl delete``\ s the flow's workloads.

How graph concepts map onto Kubernetes
--------------------------------------

+---------------------------------------+-------------------------------------------------------------+
| Concept                               | Behavior                                                    |
+=======================================+=============================================================+
| ``flow_type=REALTIME``                | broker keeps only the freshest message per edge             |
+---------------------------------------+-------------------------------------------------------------+
| ``flow_type=BATCH``                   | at-least-once, loss-free delivery (interest retention +     |
|                                       | backpressure); failures retry then dead-letter to a DLQ     |
+---------------------------------------+-------------------------------------------------------------+
| ``ProcessorNode(nb_tasks=N)``         | N Deployment replicas (competing consumers)                 |
+---------------------------------------+-------------------------------------------------------------+
| ``ProcessorNode(..., partition_by=)`` | N StatefulSet replicas, partitioned by key (scales joins);  |
|                                       | not autoscaled                                              |
+---------------------------------------+-------------------------------------------------------------+
| ``device_type='gpu'``                 | pod requests ``nvidia.com/gpu`` + GPU-pool nodeSelector     |
+---------------------------------------+-------------------------------------------------------------+
| finite producer (``is_finite=True``)  | a Kubernetes **Job**                                        |
+---------------------------------------+-------------------------------------------------------------+
| infinite producer / processor /       | a Kubernetes **Deployment**                                 |
| consumer                              |                                                             |
+---------------------------------------+-------------------------------------------------------------+
| ``flow.stop()``                       | control-channel signal, then the workloads are torn down    |
+---------------------------------------+-------------------------------------------------------------+

Observability
-------------

Every worker pod exposes an HTTP endpoint (port 8080) with:

- ``/readyz`` — readiness: turns healthy only after the node's ``open()`` completes,
  so a pod whose model is still loading is not sent traffic.
- ``/healthz`` — liveness: a heartbeat updated on every loop iteration; a stalled
  worker is restarted.
- ``/metrics`` — Prometheus metrics for per-node processing time.

The generated Deployments/Jobs reference the readiness and liveness probes
automatically. See :doc:`../user-documentation/debugging-flow-applications`.
