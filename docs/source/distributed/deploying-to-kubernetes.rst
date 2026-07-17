Deploying to Kubernetes
=======================

The ``videoflow deploy`` CLI imports your ``build_flow()`` factory, compiles the
graph, and renders one Kubernetes workload plus a ConfigMap per node — choosing the
correct per-family container image for each node. The same graph you run locally
deploys unchanged.

Prerequisites
-------------

- A Kubernetes cluster and ``kubectl`` configured against it.
- A NATS JetStream broker reachable from inside the cluster. For dev clusters
  (kind, minikube) apply the bundled manifest::

      kubectl create namespace videoflow
      kubectl apply -n videoflow -f k8s/nats.yaml

  For production, use the official NATS Helm chart.
- The per-component images built and pushed to a registry your cluster can pull
  from (see below).

Building the images
-------------------

Each node family has its own image so a pod carries only the dependencies its node
needs:

+-----------------------+----------------------------------------------------------+
| Image                 | For                                                      |
+=======================+==========================================================+
| ``videoflow-base``    | framework + broker client + wire format (the foundation) |
+-----------------------+----------------------------------------------------------+
| ``videoflow-basic``   | producers/processors/consumers with no extra deps        |
+-----------------------+----------------------------------------------------------+
| ``videoflow-vision``  | ``videoflow.processors.vision.*`` (OpenCV, DL frameworks)|
+-----------------------+----------------------------------------------------------+
| ``videoflow-video-io``| ``videoflow.producers/consumers.video`` (ffmpeg)         |
+-----------------------+----------------------------------------------------------+

::

    ./docker/build-images.sh ghcr.io/acme v1
    docker push ghcr.io/acme/videoflow-base:v1
    # ...and each family image

The compiler maps each node to a family from its module path automatically; override
per node with ``--image-override <node-name>=<family>``.

Deploying
---------

::

    videoflow deploy my_flow.py:build_flow \
        --nats nats://nats.videoflow.svc:4222 \
        --namespace videoflow \
        --registry ghcr.io/acme --image-tag v1 \
        --flow-id my-flow \
        --autoscaling
    kubectl apply -k ./manifests

Useful options:

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

How graph concepts map onto Kubernetes
--------------------------------------

+---------------------------------------+-------------------------------------------------------------+
| Concept                               | Behavior                                                    |
+=======================================+=============================================================+
| ``flow_type=REALTIME``                | broker keeps only the freshest message per edge             |
+---------------------------------------+-------------------------------------------------------------+
| ``flow_type=BATCH``                   | at-least-once delivery with a deep buffer and acks          |
+---------------------------------------+-------------------------------------------------------------+
| ``ProcessorNode(nb_tasks=N)``         | N Deployment replicas (competing consumers)                 |
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
