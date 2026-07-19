Deploying to Kubernetes
=======================

On a dev cluster, deploying a flow is one command::

    videoflow deploy my_flow.py

``deploy`` is a one-stop pipeline: every step below runs automatically by
default, and every step has an explicit flag to do it manually instead. The
same graph you run locally deploys unchanged.

What ``videoflow deploy`` does, step by step
--------------------------------------------

1. **Config** — if the solution ships a ``config.template.yaml`` and there is no
   ``config.yaml`` next to the graph, deploy asks the template's ``x-questions``
   interactively on the terminal and writes ``config.yaml``. An existing
   ``config.yaml`` (or ``--config PATH``) is used as-is. In a non-interactive
   session (``--non-interactive``, or stdin is not a TTY) deploy fails with the
   full list of missing inputs instead of hanging on a prompt.

2. **Image build** — with no ``--image``, deploy looks for a Dockerfile next to
   the graph: ``gpu.Dockerfile`` when the local docker daemon has the NVIDIA
   runtime, else ``Dockerfile`` (falling back to whichever exists). The build
   context is the **git root** enclosing the graph (solution Dockerfiles COPY
   sibling packages from the repo root); override with ``--build-context``. If
   the Dockerfile is ``FROM`` a ``videoflow-base:*`` image that is not built
   locally, deploy builds it first from the videoflow source checkout (this
   requires an editable/source install — a wheel-only install gets an error
   with the exact manual commands). The image is tagged
   ``videoflow-<solution-dir>:latest``. Docker's layer cache makes unchanged
   rebuilds take about a second. ``--no-build`` disables all of this.

3. **Prepare hook** — if the solution ships a ``prepare.py``, deploy runs it
   *inside the built image* (``docker run``, with ``--gpus all`` when
   available) before compiling, because its outputs (calibration files, model
   weights, ...) get baked into the compiled node parameters. The solution
   directory and every resolved mount (see step 5) are volume-mounted into the
   container at their host paths, so all paths in the config resolve
   identically. Skip with ``--no-prepare``. Hooks are expected to be
   idempotent (skip finished steps), so re-running deploy is cheap.

4. **Compile** — deploy calls your ``build_flow()`` factory and compiles the
   graph to node specs. If the graph's dependencies import on the operator
   machine this happens in-process; otherwise deploy runs
   ``python -m videoflow.compile`` inside the solution image and reads the
   specs back as JSON (the same serialization the provision Job uses). Either
   way the operator machine only needs ``videoflow[deploy]`` — never the ML
   stack.

5. **Mounts** — hostPath volumes from the repeatable ``--mount
   /abs/path[:/container/path][:ro]`` flag plus the solution template's
   ``x-mounts`` are added to *every* node workload (Jobs, Deployments,
   StatefulSets — not the provision Job). The single-path form mounts the same
   absolute path on both sides, which is what a flow compiled against local
   files needs: the paths baked into node params must resolve identically
   inside the pods.

6. **Cluster mechanics** — deploy classifies the cluster kubectl points at
   (``k3s`` / ``kind`` / ``minikube`` / ``docker-desktop`` / generic remote)
   from the kubectl context and node labels, then:

   - loads every locally-built image into the cluster with the right mechanism
     (``kind load docker-image`` / ``minikube image load`` /
     ``docker save | k3s ctr images import``; docker-desktop needs nothing).
     A remote cluster with a locally-built image is a hard error with push
     instructions — pods there can never see your local docker daemon.
   - warns when hostPath mounts will not see your local filesystem (kind and
     minikube nodes are VMs/containers with their own filesystem) and what to
     do about it.
   - for flows with GPU nodes, preflights what the generated GPU manifests need —
     a node labeled ``videoflow.io/gpu-pool=true``, **enough allocatable units of
     each requested GPU resource to cover the whole flow's demand** (every replica
     claims its own devices exclusively; a partially-schedulable flow stalls), and
     a ``--gpu-runtime-class`` where the NVIDIA runtime is an opt-in RuntimeClass —
     and prints copy-pasteable fix commands. These are warnings by default;
     ``--strict-preflight`` turns them into a non-zero exit before anything is
     applied. See :doc:`gpu-sharing` for running more GPU nodes than you have
     GPUs.

7. **Broker infra** — with no ``--nats``, deploy creates the namespace if
   needed and applies a dev NATS JetStream (and, when ``--blob-redis-url`` is
   also omitted, a dev Redis for the large-payload blob store) into it, waits
   for the rollout, and derives the in-cluster URLs
   (``nats://nats.<ns>.svc:4222``, ``redis://redis.<ns>.svc:6379/0``). A
   pre-existing ``nats``/``redis`` Service in the namespace is **reused and
   never owned**; only components deploy itself created are labeled
   ``videoflow.io/infra`` and torn down later. For production, bring your own
   broker (the official NATS Helm chart) and pass ``--nats``.

8. **Apply & run** — the manifests are applied in two phases (broker
   provisioning Job first, then workers). A BATCH flow then runs to
   completion: deploy waits, dumps the logs of any failed node, and tears down
   the run's workloads, broker streams, *and* the infra it created in step 7
   (``--keep`` keeps everything for debugging; ``--keep-infra`` keeps just
   NATS/Redis so the next deploy reuses them). A REALTIME flow is left running
   and deploy prints the matching ``videoflow teardown`` command.

``--dry-run`` prints all manifests to stdout — including the dev-infra
manifests whenever the broker would have been auto-provisioned — and
``--render-only`` writes them plus a ``kustomization.yaml`` to ``--output``
for a later ``kubectl apply -k``. Neither touches the cluster.

Prerequisites
-------------

- ``docker`` and ``kubectl`` on the operator machine, kubectl configured
  against the target cluster.
- ``pip install "videoflow[deploy]"`` — the graph's own dependencies are *not*
  required on the operator machine (see step 4).
- For GPU flows: cluster nodes with the NVIDIA device plugin and the
  ``videoflow.io/gpu-pool=true`` label (deploy tells you the exact commands if
  they are missing).

Building the image manually
---------------------------

Videoflow ships a single ``videoflow-base`` image (framework + broker client +
the built-in nodes' dependencies: OpenCV, ffmpeg, Redis). Solution images build
**on top of it**, adding your dependencies and your node package so the worker
can import your node classes by their module path::

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

Option reference
----------------

``--config PATH`` / ``--non-interactive``
    Explicit solution config; never prompt (fail listing missing inputs).

``--image`` / ``--image-override NAME=REF`` / ``--no-build`` / ``--build-context PATH``
    ``--image`` is the default image for every node that didn't declare its own
    ``image=`` and disables auto-build. ``--image-override`` sets the image for
    one node and wins over both (repeatable). ``--build-context`` overrides the
    git-root build context.

``--no-prepare``
    Skip the solution's ``prepare.py`` hook.

``--mount HOST[:CONTAINER][:ro]``
    hostPath volume added to every node workload and to the prep/compile
    containers. Absolute paths; single-path form mounts the same path on both
    sides. Repeatable; solution ``x-mounts`` are added automatically.

``--nats`` / ``--blob-redis-url``
    Bring-your-own broker / blob store; omitting them auto-provisions dev
    equivalents in ``--namespace`` (see step 7).

``--keep`` / ``--keep-infra``
    After a BATCH run, keep everything / keep just the auto-provisioned
    NATS+Redis.

``--flow-id``
    A stable identifier used to name all resources. Reuse the same value to
    redeploy/update the same logical flow.

``--run-id``
    Per-run id that scopes this run's broker streams (auto-generated
    otherwise). A new run id gives fresh streams; reuse it to target the same
    run.

``--autoscaling`` / ``--max-replicas``
    Emit a KEDA ``ScaledObject`` per processor that scales on broker backlog,
    using ``nb_tasks`` as the minimum replica count.

``--dry-run`` / ``--render-only`` / ``--output``
    Manifest generation without touching the cluster (see above).

Other CLI commands
------------------

``videoflow explain my_flow.py``
    Print a human-readable summary of the compiled graph — nodes, replicas, image
    families, partitioning, subjects, and the DLQ stream — without touching a cluster.

``videoflow provision my_flow.py --nats ...``
    Create the flow's broker streams and durable consumers up front. This normally
    happens automatically (a generated init Job on Kubernetes, or the local engine
    before it spawns workers), but is exposed for manual/debug use.

``videoflow teardown --flow-id ... --run-id ... --nats ... [--namespace ...] [--infra]``
    Stop a run (control-channel signal) and delete its broker streams; with
    ``--namespace`` it also ``kubectl delete``\ s the flow's workloads, and with
    ``--infra`` it deletes auto-provisioned NATS/Redis (only resources labeled
    ``videoflow.io/infra`` — a bring-your-own broker is never touched). This is
    the escape hatch for REALTIME flows deployed with auto-infra.

``python -m videoflow.compile graph.py[:factory]``
    Compile a graph to a JSON specs document on stdout — what deploy runs inside
    the solution image when the graph can't be imported on the operator machine.

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
| ``device_type='gpu'``                 | pod requests ``gpu_count`` x ``nvidia.com/gpu`` (or         |
|                                       | ``gpu_resource_name``) + GPU-pool nodeSelector; exclusive — |
|                                       | see :doc:`gpu-sharing` (``--gpu-mode shared`` omits the     |
|                                       | request so pods share the physical GPUs)                    |
+---------------------------------------+-------------------------------------------------------------+
| finite producer (``is_finite=True``)  | a Kubernetes **Job**                                        |
+---------------------------------------+-------------------------------------------------------------+
| infinite producer / processor /       | a Kubernetes **Deployment**                                 |
| consumer                              |                                                             |
+---------------------------------------+-------------------------------------------------------------+
| ``flow.stop()``                       | control-channel signal, then the workloads are torn down    |
+---------------------------------------+-------------------------------------------------------------+
| ``--mount`` / solution ``x-mounts``   | hostPath volume + volumeMount on every node workload        |
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
