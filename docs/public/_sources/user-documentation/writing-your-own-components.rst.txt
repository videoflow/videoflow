Writing your own components
===========================

Videoflow ships with a collection of producers, processors and consumers, but you
will often want your own — a custom data source, a specific model, or a bespoke
sink. This page shows how to write each kind of node so that it runs correctly both
locally and in a distributed deployment.

The golden rules
----------------

Every custom node must follow two rules so a worker can reconstruct it from
configuration:

1. **Accept and forward** ``**kwargs`` **to** ``super().__init__()``. That is how
   framework parameters (``name``, ``nb_tasks``, ``device_type``, ``is_finite``,
   ``partition_by``, ``join_policy``, ``idempotent``, ``metadata``) reach the base
   class.
2. **Constructor arguments must be JSON-serializable, and each must be stored on**
   ``self`` **under the same name** (``self._cutoff = cutoff`` for a ``cutoff``
   argument). This lets ``get_params()`` capture them automatically. Put any
   non-serializable or expensive setup (opening files, loading models) in
   ``open()`` instead.

Async methods and the runtime context
-------------------------------------

Any lifecycle or processing method (``open``/``next``/``process``/``consume``/
``close``) may be:

- **an** ``async def`` — the worker awaits it without blocking broker I/O, so a node
  can ``await`` network calls or async model inference::

      class AsyncDetector(ProcessorNode):
          async def process(self, frame):
              return await self._model.infer(frame)

- **given a runtime context** — declare a final ``ctx`` (or ``context``) parameter
  and the worker passes a ``RuntimeContext`` with ``ctx.flow_id`` / ``ctx.run_id`` /
  ``ctx.node_name`` / ``ctx.replica_id`` / ``ctx.logger``, plus
  ``ctx.set_partition_key(k)`` to route a downstream partitioned node by a business
  key. Methods that don't declare ``ctx`` are called exactly as before::

      class Tagger(ProcessorNode):
          def process(self, record, ctx=None):
              if ctx is not None:
                  ctx.set_partition_key(record['customer_id'])
              return record

Writing producers
-----------------

Subclass ``videoflow.core.node.ProducerNode`` and implement ``next()``. You may also
implement ``open()`` and ``close()``.

- ``open()`` is called once before production begins — open your data source here.
- ``next()`` is called repeatedly; each call returns the next item. Raise
  ``StopIteration`` when the source is exhausted.
- ``close()`` is called once after ``next()`` raises ``StopIteration`` (or the flow
  is stopped) — release your resources here.

Set ``is_finite=False`` for unbounded sources (e.g. a live stream) so the deploy
tooling schedules them as long-running services rather than run-to-completion jobs.

::

    import cv2
    from videoflow.core.node import ProducerNode

    class VideoFileReader(ProducerNode):
        def __init__(self, video_file: str, nb_frames=-1, **kwargs):
            self._video_file = video_file        # serializable; stored by name
            self._nb_frames = nb_frames
            self._video = None                    # the live handle is created in open()
            self._frame_count = 0
            super().__init__(**kwargs)

        def open(self):
            self._video = cv2.VideoCapture(self._video_file)

        def close(self):
            if self._video is not None:
                self._video.release()

        def next(self):
            if not self._video.isOpened():
                raise StopIteration()
            success, frame = self._video.read()
            self._frame_count += 1
            if not success or self._frame_count == self._nb_frames:
                raise StopIteration()
            return frame

Writing processors
------------------

Subclass ``videoflow.core.node.ProcessorNode`` and implement ``process()``. The
number of parameters of ``process()`` equals the number of parents of the node, in
the **same order** you pass them to the node's call. For example, a processor that
takes two parents::

    from videoflow.core.node import ProcessorNode

    class ComparisonProcessor(ProcessorNode):
        def process(self, inp1, inp2):
            return 0 if inp1 > inp2 else 1

.. note::
    The order of parents matters. When wiring the graph, pass parents in the order
    ``process()`` expects: ``ComparisonProcessor(name='cmp')(left, right)``.

Parallel processors and ``nb_tasks``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A stateless processor can be replicated to keep up with load by passing
``nb_tasks=N``. The N replicas act as competing consumers of the same input — each
message is handled by exactly one replica. See :doc:`task-allocation`.

Stateful processors and ``OneTaskProcessorNode``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a processor keeps internal state that depends on the order or completeness of
the stream (a tracker, an aggregator), it must not be replicated by plain
competing consumers. Subclass ``videoflow.core.node.OneTaskProcessorNode``, which
fixes ``nb_tasks`` to 1::

    from videoflow.core.node import OneTaskProcessorNode

    class MinAggregator(OneTaskProcessorNode):
        def __init__(self, **kwargs):
            self._min = float('inf')
            super().__init__(**kwargs)

        def process(self, inp):
            self._min = min(self._min, inp)
            return self._min

To scale a stateful processor or a join, use ``partition_by`` instead: replicas
then partition the stream by a key so each key is always handled by the same
replica (see :doc:`task-allocation`).

Running on the GPU
^^^^^^^^^^^^^^^^^^

Instantiate a processor with ``device_type='gpu'`` to request GPU scheduling::

    detector = ObjectDetector(name='detector', device_type='gpu')(frame)

On Kubernetes this makes the node's pod request an ``nvidia.com/gpu`` resource and
be scheduled onto a GPU node pool; the container runtime exposes the GPU to your
code via ``CUDA_VISIBLE_DEVICES``. Your ``process()``/``open()`` code is responsible
for actually using the GPU (loading the model onto it, etc.). Keep in mind the
resource is claimed exclusively per replica — a graph with more GPU nodes than the
cluster has devices cannot fully schedule (see :doc:`task-allocation` and
:doc:`/distributed/gpu-sharing`).

Writing consumers
-----------------

Subclass ``videoflow.core.node.ConsumerNode`` and implement ``consume()``. As with
processors, ``consume()`` receives one argument per parent, in order. Consumers are
leaves and return nothing::

    from videoflow.core.node import ConsumerNode

    class CommandlineConsumer(ConsumerNode):
        def consume(self, item):
            print(item)

Use ``open()``/``close()`` for a consumer that writes to an external resource (a
file handle, a socket, an API client).

Because delivery is at-least-once, a consumer may occasionally be handed the same
message twice (after a redelivery or restart). If the side effect must not be
duplicated, either make ``consume`` naturally idempotent, or pass
``idempotent=True`` and give the flow a Redis URL (``--blob-redis-url``): the worker
then records each processed message id and skips re-applying it::

    writer = MyApiWriter(name='writer', idempotent=True)(result)

Choosing a container image
--------------------------

On Kubernetes each node runs in a container image **you** provide — there is no
inference from the node's module path. The normal pattern is to build one image with
your dependencies and your node package on top of the shipped ``videoflow-base``
image, and point the whole deploy at it::

    # Dockerfile (see docker/user-image.example.Dockerfile)
    FROM videoflow-base:latest
    RUN pip install torch my-libs        # your dependencies
    COPY . . && RUN pip install .        # your package (importable by module path)

::

    docker build -t ghcr.io/me/app:v1 .
    videoflow deploy my_flow.py:build_flow --nats ... --image ghcr.io/me/app:v1

Image resolution per node, first match wins:

1. a deploy-time override — ``--image-override <node-name>=<ref>``;
2. the node's own ``image=`` kwarg, declared in graph code when a node intrinsically
   needs a specific environment (``MyDetector(name='det', image='ghcr.io/me/gpu:v1')``);
3. the deploy default — ``--image <ref>``.

If a node matches none of these, the deploy fails with a message naming the node —
nothing is guessed. (The local engine ignores images entirely; it runs workers in
your current Python environment.) See :doc:`../distributed/deploying-to-kubernetes`.
