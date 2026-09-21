Deploying to Kubernetes
=======================

On a dev cluster, deploying a flow is one command::

    videoflow deploy my_flow.py

A graph with no Dockerfile next to it — the Quickstart's ``my_flow.py`` — runs
its built-in nodes in the videoflow base image for your version (deploy says
so); a ``Dockerfile`` next to the graph, your nodes and their dependencies on
top of that base, is built and used instead, and ``--image`` overrides either.
The argument names a solution directory in one of three ways; see
*Prerequisites* below.

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
   the graph: ``gpu.Dockerfile`` when the flow needs a GPU, else ``Dockerfile``
   (falling back to whichever exists). Whether the flow needs a GPU is read from
   the config keys the template names in ``x-gpu`` (``x-gpu: ['{device}']``),
   else from the compiled graph's device placement when the graph imports on
   this machine, else it is the CPU image with a note — never from whether this
   machine's docker daemon has the NVIDIA runtime. The build context is the
   **git root** enclosing the graph (solution Dockerfiles COPY sibling packages
   from the repo root); override with ``--build-context``. If the Dockerfile is
   ``FROM`` a ``videoflow-base:*`` image that is not built locally, deploy
   pulls ``ghcr.io/videoflow/videoflow-base:<version>[-cuda]`` first and tags
   it under that name — or, on a source install, builds it from the checkout
   (a development version with no published image gets an error with the exact
   manual commands). The image is built as ``videoflow-<solution-dir>:latest``
   and deployed under a **content-addressed tag** (``:<12 hex of its content digest>``),
   so a changed image is always a new tag. Docker's layer cache makes unchanged
   rebuilds take about a second. With no Dockerfile at all, the image is the
   base image itself — built from a source checkout (under a content-addressed
   tag, side-loaded or pushed like any built image), or
   ``ghcr.io/videoflow/videoflow-base:<version>[-cuda]`` on a wheel install,
   which the pods pull — after a check that every Python node is one of
   videoflow's built-ins, since the base image holds nothing else (a graph with
   its own node classes, or one that does not import on this machine, is
   refused with the fix: add a Dockerfile or pass ``--image``). ``--no-build``
   disables all of this; ``VF_DOCKER_BUILD_ARGS`` adds arguments to every
   ``docker build`` (a proxy). With ``--registry`` (usually from the cluster
   profile, below) a locally built image is then pushed and the pods pull the
   registry-qualified ref.

3. **Prepare hook** — if the solution ships a ``prepare.py``, deploy runs it
   *inside the built image* (``docker run``, with ``--gpus all`` when
   available, plus ``VF_DOCKER_RUN_ARGS``) before compiling, because its
   outputs (calibration files, model weights, ...) get baked into the compiled
   node parameters. The solution directory and every resolved mount (see step
   5) are volume-mounted into the container at their host paths, so all paths
   in the config resolve identically, and the resolved config path travels as
   ``VF_SOLUTION_CONFIG`` (a ``build_flow`` that reads it honours ``--config``
   kept elsewhere). Skip with ``--no-prepare``. Hooks are expected to be
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
   inside the pods. Data that lives in the cluster rather than on your machine
   — a shared model cache, an RWX work directory on a multi-node cluster where
   no node's own filesystem holds it — is mounted from an existing
   PersistentVolumeClaim with ``--mount-pvc claim:/path[:ro]`` (or an
   ``x-mounts`` entry ``pvc:claim:/path[:ro]``). The two compose by one rule: a
   ``--mount`` or ``x-mounts`` host path at or under a claim's mount path is
   served by the claim in the pods (a hostPath there would shadow it with an
   empty directory on every node but one) and still by the host in the
   prepare/compile containers — at the same path when the mount is a same-path
   one, or, for an entry that remaps a directory onto the container's home
   (``~/.videoflow:/root/.videoflow``), as a ``subPath`` of the claim.
   ``--mount-home DIR`` is what makes those cache entries land inside the
   claim's directory: it is what ``~`` resolves to on the host side. Host paths
   are resolved to their real location (symlinks followed) before any of this.

6. **Cluster mechanics** — deploy classifies the cluster kubectl points at
   (``k3s`` / ``kind`` / ``minikube`` / ``docker-desktop`` / generic remote)
   from the kubectl context and node labels, then:

   - loads every locally-built image into the cluster with the right mechanism
     (``kind load docker-image`` / ``minikube image load`` /
     ``docker save | k3s ctr images import``; docker-desktop needs nothing).
     A registry-qualified image (``--registry``, or an ``--image`` naming one)
     is never side-loaded — the nodes pull it. A remote cluster with a
     locally-built image and no ``--registry`` is a hard error with push
     instructions — pods there can never see your local docker daemon.
   - warns when hostPath mounts will not see your local filesystem (kind and
     minikube nodes are VMs/containers with their own filesystem) and what to
     do about it.
   - for flows with GPU nodes, preflights what the generated GPU manifests need —
     a node labeled ``videoflow.io/gpu-pool=true``, **enough free units of each
     requested GPU resource to cover the whole flow's demand** (every replica
     claims its own devices exclusively; a partially-schedulable flow stalls),
     **a placement for every pod on the per-node free counts** (the total and
     the largest node are necessary, not sufficient: two nodes with 3 free GPUs
     each hold only two of three ``gpu_count = 2`` replicas), and
     a RuntimeClass where the NVIDIA runtime is an opt-in one (deploy puts the
     ``nvidia`` class the cluster registers on the GPU pods by itself, and
     announces it; ``--gpu-runtime-class NAME`` chooses, ``none`` opts out) —
     and prints copy-pasteable fix commands. These are warnings by default;
     ``--strict-preflight`` turns them into a non-zero exit before anything is
     applied. A pod listing the API refused is reported as ``unobservable GPU
     state`` rather than read as an idle pool — a warning for exclusive claims,
     fatal under ``--gpu-mode mix``. See :doc:`gpu-sharing` for running more GPU
     nodes than you have GPUs.
   - checks that the broker and payload store it is about to use can provide
     what every channel asks for — ``reliable_work`` for a BATCH flow,
     ``live_latest`` for REALTIME, or whatever ``--require-profile
     CHANNEL=PROFILE`` names. A broker or store this deploy provisions is judged
     by the profile it renders; one the namespace already runs (an earlier
     ``--keep-infra``, a shared dev cluster) by the profile recorded on its
     Service (``videoflow.io/profile``), and the provision Job reads it back
     in-cluster before creating a stream; a bring-your-own ``--nats`` /
     ``--blob-redis-url`` is read back live here (a short probe, before any
     infrastructure exists), and what the probe cannot read is reported as
     unknown, never assumed. A composition that definitely cannot provide a
     guarantee — an evictable cache brought as the store of a BATCH flow, a
     server without JetStream — stops the deploy before anything is applied
     (exit 2); an *unobserved* guarantee is a warning unless the operator named
     the profile with ``--require-profile``, when it is refused too (exit 3).
     Nothing is quietly downgraded. Both shipped profiles admit BATCH and
     REALTIME flows: the dev Redis persists and never evicts (see step 7).

7. **Broker infra** — with no ``--nats``, deploy creates the namespace if
   needed and applies a dev NATS JetStream (and, when ``--blob-redis-url`` is
   also omitted, a dev Redis for the large-payload blob store) into it, waits
   for the rollout, and derives the in-cluster URLs
   (``nats://nats.<ns>.svc:4222``, ``redis://redis.<ns>.svc:6379/0``). A
   pre-existing ``nats``/``redis`` Service in the namespace is **reused and
   never owned**; only components deploy itself created are labeled
   ``videoflow.io/infra`` and torn down later. Each Service records the profile
   that rendered it (``videoflow.io/profile``); a deploy that reuses it adopts
   that profile — its stream copies follow the real broker — and a
   ``--broker-profile`` that contradicts the record is refused rather than
   silently served the other shape. The dev profile is one emptyDir server
   each: the NATS file store and the Redis append-only file both live for the
   pod (a container restart replays them, a pod loss does not), and the Redis
   runs ``noeviction`` — a blob is never dropped while a reader still holds it,
   which is what a BATCH flow's ``reliable_work`` channels require; a full store
   refuses a write instead (every key still carries a TTL, and the reconciler
   reclaims orphans, so that is what bounds memory). ``--broker-profile
   durable`` renders a NATS StatefulSet with cluster routes and a
   PersistentVolumeClaim per pod plus the same Redis on a claim, sized with
   ``--broker-replicas N`` (odd, default 3) and ``--broker-storage-class NAME``
   (default ``local-path``); its claims are kept at teardown. ``--priority-class NAME`` puts every pod the deploy creates —
   workers, provision Job and this broker — in that PriorityClass. For
   production, bring your own broker (the official NATS Helm chart) and pass
   ``--nats``.

8. **Apply & run** — the manifests are applied in two phases (broker
   provisioning Job first, then workers). A BATCH flow then runs to
   completion: deploy waits, dumps the logs of any failed node, and tears down
   the run's workloads, broker streams, *and* the infra it created in step 7
   (``--keep`` keeps everything for debugging; ``--keep-infra`` keeps just
   NATS/Redis so the next deploy reuses them). A REALTIME flow is left running
   and deploy prints the matching ``videoflow teardown`` command — but only
   after a bounded rollout check: deploy waits for every pod to become Ready
   (i.e. ``open()`` completed), and if a pod crash-loops, is OOM-killed, cannot
   pull its image, or sits unschedulable past a grace period, it dumps the pod
   logs and exits non-zero, leaving the flow running for inspection. A pod that
   is merely still loading when the check's deadline (~150 s, sized to the
   startup-probe window) expires is reported as a warning, not a failure.

``--dry-run`` prints all manifests to stdout — including the dev-infra
manifests whenever the broker would have been auto-provisioned — and
``--render-only`` writes them plus a ``kustomization.yaml`` to ``--output``
for a later ``kubectl apply -k``. Neither touches the cluster.

Prerequisites
-------------

- ``docker`` and ``kubectl`` on the operator machine, kubectl configured
  against the target cluster (deploy never switches contexts).
- videoflow installed (``pip install 'videoflow[all]'``, see
  :doc:`../first-steps/installing-videoflow`). The graph's own dependencies
  are *not* required on the operator machine (see step 4).
- The graph: the ``build_flow()`` module of a **solution directory** (the
  module plus whatever the solution ships next to it — ``config.template.yaml``,
  ``prepare.py``, ``[gpu.]Dockerfile``, ``requirements.txt``), named in one of
  three ways that differ only in where the directory comes from:

  .. list-table::
     :header-rows: 1
     :widths: 18 30 22 30

     * -
       - A solution shipped with videoflow
       - A solution in another git repository
       - A solution on your disk
     * - Command
       - ``videoflow deploy videoflow://toy_calculator``,
         ``videoflow deploy videoflow-contrib://human_tracking``
       - ``git clone <url>``, then the local form
       - ``videoflow deploy path/to/my_solution/my_solution.py[:factory]``, or
         the directory itself: ``videoflow deploy path/to/my_solution``
     * - The directory
       - ``solutions/<name>/`` of ``github.com/videoflow/<repo>``, fetched at
         the tag of your installed version into
         ``~/.videoflow/solutions/<repo>@v<version>/`` (``$VF_SOLUTION_REF``
         picks another ref); ``<repo>://`` reaches the videoflow repositories
         only
       - the clone
       - the one you named; the directory form expects ``<name>/<name>.py``,
         the convention the shipped solutions follow
     * - The image
       - built from the solution's ``[gpu.]Dockerfile`` with the clone root as
         build context
       - as local
       - built from the ``[gpu.]Dockerfile`` next to the graph, with the
         enclosing git root (else the directory, or ``--build-context``) as
         context — your ``COPY`` paths are relative to it. No Dockerfile: the
         base image for your version, for a flow of built-in nodes. ``--image``
         or a node's own ``image=`` wins over both
     * - ``config.yaml``, outputs
       - next to the graph, in the clone (``--config`` keeps them elsewhere)
       - next to the graph
       - next to the graph

  Where the deploy goes is never part of the argument: the current kubectl
  context, plus the cluster profile that names it (next section).
- For GPU flows: cluster nodes with the NVIDIA device plugin and the
  ``videoflow.io/gpu-pool=true`` label (deploy tells you the exact commands if
  they are missing).
- For a multi-node or shared cluster: a cluster profile (next section).

Multi-node and shared clusters
------------------------------

A laptop cluster (kind, minikube, k3s, Docker Desktop) needs nothing beyond the
prerequisites. A cluster with several nodes has two things a single node hides:
a locally built image side-loaded into one node's containerd is invisible to
the others, and a hostPath is a different directory on every node. A shared
cluster usually adds a namespace, a PriorityClass and a set of GPU nodes that
are yours. The flags that address them — ``--registry`` (+ ``--push-tool
crane`` for a plain-HTTP registry the docker daemon does not trust),
``--mount-pvc`` with ``--mount-home``, ``--priority-class``, ``--gpu-nodes``,
``--namespace`` — describe the *cluster*, not a solution or a run, so they live
in a cluster profile keyed by the kubectl context::

    # ~/.config/videoflow/clusters.yaml  (or $VF_CLUSTERS_FILE, or --clusters-file PATH)
    docker:                                # machine-level; every docker build / run
      build_args: '--build-arg http_proxy=http://proxy:3128'
      run_args: ''
    clusters:
      lab:                                 # `--cluster lab`, or matched by `context`
        context: default                   # the kubectl context this profile belongs to
        namespace: videoflow
        registry: 10.0.0.1:5000
        push_tool: crane
        mount_pvc: ['work-share:/shared/videoflow']
        mount_home: /shared/videoflow/home
        priority_class: cluster-batch
        gpu_nodes: [gpu-01]                # optional
        broker_profile: durable            # optional: broker + payload store on claims of
        broker_storage_class: nfs-shared   #   this class, not on the nodes' disks
        broker_replicas: 1                 #   (NATS servers; default 3)
        nats: null                         # optional bring-your-own broker (+ blob_redis_url)

Every cluster key is a ``deploy`` flag with underscores; lists stand for
repeatable flags (``mount_pvc``, ``mount``, ``gpu_nodes``). ``deploy`` takes
all of them, ``teardown`` the ones it has (``namespace``, ``nats``,
``kubectl``, ``gpu_mode``), ``run-local`` only the
``docker`` section. The profile is chosen by ``--cluster NAME``, else by the
profile whose ``context`` is the current kubectl context; deploy prints which
one it took and what it contributed. Precedence, everywhere: an explicit flag,
then an environment variable (``VF_DOCKER_BUILD_ARGS`` / ``VF_DOCKER_RUN_ARGS``
for the ``docker`` keys), then the profile, then the built-in default.

The claim: an RWX PersistentVolumeClaim in the namespace, mounted at the
directory it is served at on your machine (an NFS export, say — the same
absolute path on every node and on the host). Put the solution's ``work_dir``
and its inputs under that directory when the config Q&A asks (``work_dir`` is
one of the questions), and point ``mount_home`` inside it so the caches the
prepare hook fills (``~/.videoflow``, ``~/.torch``, ``~/.cache``) are the ones
the pods mount. A hostPath under the claim's directory is then served by the
claim in every pod (step 5), and the prepare container on your machine writes
to the same files.

A disposable cluster
--------------------

``./scripts/kind-up.sh`` builds a local kind cluster set up exactly the way this
page describes — images side-loaded, NATS and Redis installed in a namespace, the
broker also published on the host — and ``./scripts/kind-down.sh`` deletes it. It
is what the ``tests/integration/k8s`` suite deploys against on every CI build, so
it is also the shortest way to try a deploy without a real cluster. See
``tests/integration/README.md``.

The same suite runs against a shared, multi-node k3s cluster through
``./scripts/k3s-test-up.sh``, which *verifies* rather than creates: it checks the
kubeconfig and that the current context is the expected one (it never switches
it), then prepares only namespaced objects — the test namespace, an RWX claim the
pods and the host share (``k8s/test-pvc.yaml``), the dev broker, a NodePort — and
pushes the images to the cluster's registry with ``scripts/push-images.sh``
(crane, from user space, no docker restart). Every pod it creates carries
``priorityClassName: cluster-batch`` so it yields to other tenants' work.

One thing it has to arrange is worth knowing before you point a solution at any
kind cluster: a solution's ``work_dir`` is hostPath-mounted into the worker pods at
the absolute path baked in at compile time, and a kind node has its own filesystem.
The cluster config bind-mounts the work root into the node at the *same* path, so
host, node and pod agree. Without that the flow runs, every pod exits zero, and the
artifacts are nowhere to be found.

Building the image manually
---------------------------

Videoflow publishes one base image per release (framework + broker client +
the built-in nodes' dependencies: OpenCV, ffmpeg, Redis):
``ghcr.io/videoflow/videoflow-base:<version>`` and ``:<version>-cuda``.
Solution images build **on top of it**, adding your dependencies and your node
package so the worker can import your node classes by their module path::

    # Dockerfile (see docker/user-image.example.Dockerfile)
    FROM ghcr.io/videoflow/videoflow-base:1.0.2   # pin the version you installed
    RUN pip install torch my-libs        # your dependencies
    COPY . .
    RUN pip install .                    # your package

::

    docker build -t ghcr.io/acme/app:v1 .        # your image, FROM videoflow-base
    docker push ghcr.io/acme/app:v1

A pure built-in flow needs none of this: with no Dockerfile and no ``--image``
it deploys in ``ghcr.io/videoflow/videoflow-base:<version>``. From a source checkout,
``./docker/build-images.sh`` builds both bases locally (as
``videoflow-base:py3.12`` and ``:py3.12-cuda``) from the code you are editing.

Option reference
----------------

``--config PATH`` / ``--non-interactive``
    Explicit solution config; never prompt (fail listing missing inputs).

``--image`` / ``--image-override NAME=REF`` / ``--no-build`` / ``--build-context PATH``
    ``--image`` is the default image for every node that didn't declare its own
    ``image=`` and disables auto-build. ``--image-override`` sets the image for
    one node and wins over both (repeatable). ``--build-context`` overrides the
    git-root build context.

``--registry HOST[:PORT][/PREFIX]`` / ``--push-tool {docker,crane}``
    Push the built image (or a local ``--image``) to this registry and have the
    pods pull the registry-qualified ref — the way onto a multi-node cluster,
    where side-loading reaches one node only. ``docker`` pushes through the
    daemon (which must trust the registry); ``crane`` pushes from user space
    with ``--insecure``, for a plain-HTTP registry the daemon does not trust,
    and needs ``crane`` on PATH (the error tells you how to install it). Both
    usually come from the cluster profile.

``--image-pull-policy {Always,IfNotPresent,Never}``
    ``imagePullPolicy`` for every container, workers and the provision Job alike.
    Defaults to ``IfNotPresent``, which is what lets a locally built image run:
    deploy loads it into the cluster itself, so there is nothing to pull — and
    because an auto-built image is deployed under a content-addressed tag, it
    stays right on a registry too: a changed image is a new tag. Use ``Always``
    only for a registry image under a mutable tag you re-push yourself.

``--cluster NAME`` / ``--clusters-file PATH``
    The cluster profile to take defaults from (see *Multi-node and shared
    clusters*), and where the profiles file is (default ``$VF_CLUSTERS_FILE``,
    else ``~/.config/videoflow/clusters.yaml``). Without ``--cluster`` the
    profile whose ``context`` is the current kubectl context applies.

``--no-prepare``
    Skip the solution's ``prepare.py`` hook.

``--mount HOST[:CONTAINER][:ro]``
    hostPath volume added to every node workload and to the prep/compile
    containers. Absolute paths; single-path form mounts the same path on both
    sides. Repeatable; solution ``x-mounts`` are added automatically.

``--mount-pvc CLAIM:PATH[:ro]``
    An existing PersistentVolumeClaim (in ``--namespace``) mounted at ``PATH``
    in every node workload. A ``--mount`` or ``x-mounts`` host path at or under
    ``PATH`` is served by the claim in the pods (as a ``subPath`` when the entry
    remaps it) and by the host in the prep/compile containers. Repeatable;
    solution ``x-mounts`` of the form ``pvc:CLAIM:PATH`` are added automatically.

``--mount-home DIR``
    What ``~`` in the solution's ``x-mounts`` resolves to on the host side
    (default: your home directory). On a multi-node cluster point it inside the
    directory passed to ``--mount-pvc``, so the caches the pods mount are the
    ones the prepare hook filled.

``--gpu-runtime-class NAME``
    ``runtimeClassName`` for the GPU pods. Default: the NVIDIA RuntimeClass the
    cluster registers (``nvidia`` on k3s, where the NVIDIA container runtime is
    opt-in and a GPU pod without it starts device-less), detected at deploy time
    and announced; a name chooses one, ``none`` sets no class.

``--priority-class NAME``
    ``priorityClassName`` for every pod this deploy creates — workers, the
    provision Job and any broker it provisions. The PriorityClass must exist.

``--broker-profile {dev,durable}`` / ``--broker-replicas N`` / ``--broker-storage-class NAME``
    Shape of the auto-provisioned NATS/Redis when ``--nats`` is omitted (step 7).
    Omitted, the deploy renders ``dev`` for what is missing and adopts whatever
    the namespace already runs. ``teardown --infra`` reads the profile from the
    record on the ``nats`` Service, or takes the same ``--broker-profile``, so it
    deletes the right workload kinds.

``--require-profile CHANNEL=PROFILE``
    Require a messaging profile (``live_latest``, ``reliable_work``,
    ``durable_control``, ``replay_archive``) on the named channel — the output of
    that node. The composition check is always binding for a definite
    incompatibility; naming a profile additionally refuses an *unobserved*
    guarantee (a bring-your-own store whose configuration could not be read
    back), and refuses a profile the flow type's own streams cannot carry
    (``reliable_work`` on a REALTIME channel) before anything is applied.
    Repeatable; also on ``run-local``, against the dev containers. The
    requests reach the provision Job and the workers as
    ``VF_PROFILE_REQUESTS_JSON``: the Job admits the composition against the
    live broker before creating any stream and verifies the streams it created
    carry the requested profiles, and each worker verifies its own channel and
    its parents' before it opens — a contradiction ends the worker with exit 2,
    an unreadable stream with exit 3. ``VF_ADMISSION_TIMEOUT_SECONDS`` (default
    60) bounds those read-backs.

    The render also carries the run ledger: the ``VF_NATS_URL`` ConfigMap
    carries ``VF_RUNTIME_STORE_URL`` (the blob Redis) and every node's ConfigMap
    ``VF_PARENT_REPLICAS``. The provision Job reads the ledger's persistence back
    like the store's: only a Redis with ``appendonly yes`` and ``noeviction``
    (both shipped profiles; not an evictable cache brought as
    ``--blob-redis-url``) makes the ledger durable, and only then are
    at-least-once durables provisioned with an unbounded broker cap and their
    retry budget kept in the ledger, a singleton node's partition leased to the
    one pod that holds it (a second pod started by hand is refused at bind time
    instead of splitting the work), and payload obligations reconciled from the
    ledger at start and periodically; against a cache the broker cap stays and
    the ledger is process-local.

    Every object of a run is named for it — ``vf-<flow>-<run>-<node>`` and the
    run-wide ``-broker`` / ``-specs`` / ``-provision`` ConfigMaps and Job, with
    selectors carrying the ``videoflow.io/run-id`` label — so two runs of one
    flow coexist in a namespace without applying over each other; only the
    NetworkPolicy is shared by the flow's runs.

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
    using ``nb_tasks`` as the minimum replica count. Only a processor that renders
    as a Deployment can be scaled: a BATCH flow's nodes are Jobs, whose
    parallelism is fixed at creation, so ``--autoscaling`` on a BATCH flow is
    refused at render time (a ``CapabilityError``) instead of emitting a scaler
    that would dangle on a Deployment that never exists. Partitioned and, without
    ``--gpu-autoscaling``, GPU nodes keep their fixed scale. A scaler carries one
    trigger per parent and KEDA scales on the highest, so a join whose second
    input backs up is scaled too. A multi-parent join at one replica and a
    node that declares ``partition_by`` at ``nb_tasks = 1`` also keep their
    declared scale: scaled by KEDA, the first would split every group's halves
    across competing replicas and the second would split one key's history
    across replicas bound to the same competing durable. Redeploy such a node at
    the replica count it should own its keys at instead.

``--single-run``
    Refuse to start this run while another run of the same flow holds workloads
    in the namespace — decided before anything of the new run is created, so the
    active run is never reconfigured (exit 3, ``VF_ACTIVE_RUN``; a namespace
    that cannot be listed is not a free one). Without it runs of one flow
    coexist under their run-scoped names.

``--rollout-policy {drain,surge}``
    How a node's Deployment replaces its pods on an update. ``drain`` renders
    ``strategy: Recreate`` — every old replica stops before a new one starts,
    which is what a GPU node needs when its devices cannot be held by two
    generations at once. ``surge`` renders a rolling update with one extra
    replica and none unavailable, and is admitted against the GPU pool's free
    devices: with nothing spare it is refused before anything is applied,
    because the replacement would wait forever behind the old pod. Omitted, the
    Kubernetes default rolling update stays — deploy warns when the pool is
    full, since that default stalls the same way.

``--gpu-nodes HOST[,HOST...]``
    Pin every GPU pod to these hosts: a required ``kubernetes.io/hostname``
    node-affinity term on top of the pool label, for a shared cluster where only
    some GPU nodes are yours to use.

``--resources NODE=key:quantity[,key:quantity...]``
    Host requests and limits for a node's worker container — ``cpu`` and
    ``memory`` are requests, ``cpu_limit`` and ``memory_limit`` limits;
    ``NODE=*`` applies to every node, a node entry overrides it, and both
    override a component descriptor's ``spec.resources.cpu`` / ``memory``.
    Repeatable. Host memory is a scheduler request, never a GPU memory
    declaration: a node whose replicas fit the GPUs but not a node's RAM stays
    Pending with the scheduler's reason instead of being admitted on GPU
    capacity alone.

``--gpu-mode dra``
    Render Dynamic Resource Allocation claims instead of an extended-resource
    limit (see :doc:`gpu-sharing`); needs a GPU DRA driver in the cluster.

``--dry-run`` / ``--render-only`` / ``--output``
    Manifest generation without touching the cluster (see above). With a
    ``--registry``, ``--render-only`` pushes the image (its output is meant to
    be applied) while ``--dry-run`` only names the registry ref it would push.

Other CLI commands
------------------

``videoflow explain my_flow.py``
    Print a human-readable summary of the compiled graph — nodes, replicas, image
    families, partitioning, subjects, and the DLQ stream — without touching a cluster.
    A solution's config is resolved exactly as ``deploy`` does it (``--config``,
    else ``config.yaml`` beside the graph, else the template's questions —
    ``--non-interactive`` lists them instead of asking), so ``explain`` works
    before the first deploy has written one.

``videoflow --version``
    The installed version — also the tag ``<repo>://<name>`` references are
    fetched at, and the ``ghcr.io/videoflow/videoflow-base`` tag a wheel install pulls.

``videoflow provision my_flow.py --nats ...``
    Create the flow's broker streams and durable consumers up front. This normally
    happens automatically (a generated init Job on Kubernetes, or the local engine
    before it spawns workers), but is exposed for manual/debug use.

``videoflow teardown --flow-id ... --run-id ... --nats ... [--namespace ...] [--infra]``
    Stop a run (control-channel signal) and delete its broker streams; with
    ``--namespace`` it also ``kubectl delete``\ s the flow's workloads, and with
    ``--infra`` it deletes auto-provisioned NATS/Redis (only resources labeled
    ``videoflow.io/infra`` — a bring-your-own broker is never touched). This is
    the escape hatch for REALTIME flows deployed with auto-infra. Streams are
    matched by exact ownership, never by name prefix; if the stream listing failed
    or a delete did not land, teardown prints ``WARNING: broker cleanup
    incomplete ...`` naming what remains and carries on — re-run it once the
    broker answers.

``python -m videoflow.compile graph.py[:factory]``
    Compile a graph to a JSON specs document on stdout — what deploy runs inside
    the solution image when the graph can't be imported on the operator machine.

``videoflow run-local graph.py [--in-image] [--mount ...] [--mount-home DIR]``
    The local twin of ``deploy``: the same config Q&A and prepare hook, a dev
    NATS/Redis started in docker when nothing is listening, one worker
    subprocess per node replica. When the graph's dependencies are not installed
    on this machine — the ML solutions of videoflow-contrib — the prepare hook
    and every worker run inside the solution image instead (built exactly as
    deploy would, and reused by it), with the solution's ``x-mounts`` and any
    ``--mount`` as bind mounts and, when the docker daemon has the NVIDIA
    runtime, each worker's granted devices passed with ``--gpus``. ``--in-image``
    forces that even for a graph that imports here.

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
| ``ProcessorNode(nb_tasks=N)``         | N Deployment replicas (competing consumers), each claiming  |
|                                       | a replica slot through the run ledger at start; in a BATCH  |
|                                       | flow an Indexed Job of N completions (index = replica id)   |
+---------------------------------------+-------------------------------------------------------------+
| ``ProcessorNode(..., partition_by=)`` | N StatefulSet replicas, partitioned by key (scales joins);  |
|                                       | not autoscaled                                              |
+---------------------------------------+-------------------------------------------------------------+
| ``device_type='gpu'``                 | pod requests ``gpu_count`` x ``nvidia.com/gpu`` (or         |
|                                       | ``--gpu-resource-name``) + GPU-pool nodeSelector; whole     |
|                                       | physical devices — see :doc:`gpu-sharing`                   |
|                                       | (``--gpu-mode mix`` packs ``gpu_memory_gib`` nodes onto     |
|                                       | solver-chosen exclusive MIG slices)                         |
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
| ``--mount-pvc`` / ``x-mounts``        | ``persistentVolumeClaim`` volume + volumeMount on every     |
| ``pvc:...``                           | node workload; shadowed hostPaths dropped from the pods     |
+---------------------------------------+-------------------------------------------------------------+

Observability
-------------

Every worker pod exposes an HTTP endpoint (port 8080) with:

- ``/readyz`` — readiness: turns healthy only after the node's ``open()`` completes,
  so a pod whose model is still loading is not sent traffic.
- ``/healthz`` — liveness: a heartbeat updated on every loop iteration; a stalled
  worker is restarted.
- ``/metrics`` — Prometheus metrics: per-node processing-time histograms
  (``_bucket{le=...}`` plus ``_count``/``_sum``), throughput and drop counters,
  and errors by code and disposition.

The generated Deployments/Jobs reference the readiness and liveness probes
automatically. See :doc:`../user-documentation/debugging-flow-applications`.
