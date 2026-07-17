Scaling and node allocation
===========================

Each node in a flow runs in its own worker — a subprocess locally, a pod on
Kubernetes. This page covers how to scale a node across workers and machines, and
how to request a GPU.

Replicating a processor with ``nb_tasks``
-----------------------------------------

If a **processor** is a bottleneck, run several copies of it by passing
``nb_tasks``::

    node = SomeProcessor(name='detector', nb_tasks=4)

The replicas are **competing consumers** of the node's input: each incoming message
is delivered to exactly one replica, so the work is spread across them. Locally each
replica is a separate subprocess; on Kubernetes it is a Deployment replica.

- Producers and consumers are not replicated with ``nb_tasks`` (a producer is a
  single source; a consumer is a single sink).
- Nodes that subclass ``OneTaskProcessorNode`` (trackers, aggregators — anything
  stateful) always run as a single worker regardless of ``nb_tasks``.

- A **join** (a processor with more than one parent) may only be replicated if it
  partitions its input (see below); otherwise it must keep ``nb_tasks=1``, because
  the two halves of a single event could be delivered to different replicas.

Partitioned scale-out (stateful nodes and joins)
------------------------------------------------

Pass ``partition_by`` alongside ``nb_tasks`` to make replicas **partition** the
input by a key instead of competing for it — each message is owned by exactly one
replica, chosen by ``hash(key) % nb_tasks``::

    tracker = MyTracker(name='tracker', nb_tasks=4, partition_by='trace_id')

The key is either the special value ``'trace_id'`` (the per-event id, which
co-locates both halves of a join on the same replica) or the name of a metadata
field set upstream via ``ctx.set_partition_key(...)``. This is what lets **stateful
nodes and joins scale horizontally**: a replicated join *requires* ``partition_by``
(``partition_by='trace_id'`` is the usual choice). On Kubernetes a partitioned node
becomes a **StatefulSet** (stable replica ordinals) and is **not** autoscaled, since
changing the replica count would rehash ownership mid-flight.

Requesting a GPU
----------------

Instantiate a processor with ``device_type='gpu'`` to request GPU scheduling::

    detector = ObjectDetector(name='detector', device_type='gpu')

On Kubernetes this makes the node's pod request one ``nvidia.com/gpu`` and adds a
GPU-pool ``nodeSelector`` and toleration, so the pod lands on a GPU node. The NVIDIA
device plugin then exposes the GPU to the container through
``CUDA_VISIBLE_DEVICES``; your node code is responsible for actually placing its
model/computation on the GPU.

You can combine GPU scheduling with ``nb_tasks`` to run several GPU replicas — each
replica pod requests its own GPU.

Autoscaling
-----------

On Kubernetes, ``nb_tasks`` is the **minimum** replica count. Pass ``--autoscaling``
to ``videoflow deploy`` to also generate a `KEDA <https://keda.sh>`_ ``ScaledObject``
per processor that scales replicas up (toward ``--max-replicas``) based on the
node's input backlog on the broker, and back down when the backlog clears. See
:doc:`../distributed/deploying-to-kubernetes`.

Running across multiple machines
--------------------------------

Because nodes communicate over the broker rather than shared memory, a flow already
spans as many machines as its workers are scheduled onto. Locally that is one host;
on Kubernetes the scheduler places pods across the cluster automatically, honoring
each node's resource requests (CPU, memory, GPU).
