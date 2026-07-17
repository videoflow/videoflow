Under the hood
==============

This page describes what happens behind the scenes when a graph is defined,
compiled and run.

Node creation and graph definition
----------------------------------

Consider a small linear graph::

    from videoflow.core import Flow
    from videoflow.producers import IntProducer
    from videoflow.processors import IdentityProcessor
    from videoflow.consumers import CommandlineConsumer

    A = IntProducer(0, 40, 0.1, name='A')
    B = IdentityProcessor(name='B')(A)
    C = CommandlineConsumer(name='C')(B)

For a processor or consumer, two things happen. The call to ``__init__`` **creates**
the node; the subsequent call — ``IdentityProcessor(name='B')(A)`` — **wires the
edge** ``A -> B``, recording that ``B`` takes ``A``'s output as input. Calling a node
twice to set its parents raises a ``RuntimeError``.

Each node also captures its constructor arguments (via ``get_params()``) so that a
worker can later rebuild it from configuration alone.

Flow creation and validation
----------------------------

``Flow`` is created from the consumers::

    flow = Flow([C], flow_type=BATCH, flow_id='demo')

At construction the flow discovers the producers (the parentless ancestors of the
consumers) and validates the graph:

- it must be acyclic;
- every consumer must be reachable from a producer;
- all node names must be unique;
- a replicated multi-parent join (``nb_tasks > 1``) must set ``partition_by``.

Any violation raises a ``ValueError`` before anything is launched.

Compilation
-----------

When you call ``flow.run(engine)`` (or ``videoflow deploy``), a fresh **run id** is
minted and the graph is compiled into one **node specification** per node — a flat,
JSON-serializable record holding the node's class, its captured parameters, its
parents' names, its kind (producer/processor/consumer), replica count, device type,
partitioning and join policy, and its declared container image (if any). The engine
provisions the run's broker streams/consumers, then turns those specs into workers.

Routing and message assembly
----------------------------

Each node owns one broker stream, named from its ``flow_id``, ``run_id`` and node
name. A node publishes only its own output there. To assemble its inputs, a node
subscribes to a durable consumer on **each parent's** stream. Three delivery modes
coexist:

- **Broadcast**: distinct child nodes of the same parent each get their own durable
  consumer, so each child receives a full copy of every message.
- **Competing consumers**: replicas of one node (``nb_tasks > 1``) share a single
  durable, so each message goes to exactly one replica.
- **Partitioned**: with ``partition_by``, each replica has its own durable and keeps
  only the messages it owns (by key hash), so a key is always handled by the same
  replica — this is what lets stateful nodes and joins scale.

Every message carries a **trace id** that originates at the producer and is carried
forward through the graph. A join buffers incoming messages by trace id and releases
a set to ``process()`` only once one message from every parent with a matching trace
id has arrived; a :doc:`join policy <task-allocation>` bounds how long it waits.

Delivery and acknowledgement
----------------------------

A worker acknowledges each input to the broker only **after** the node has processed
it (and published its output). If it crashes in between, the un-acked message is
redelivered — at-least-once delivery. To keep that from double-emitting, each
message's id is derived from its content, so a re-run republishes the same id and
the broker de-duplicates it. In BATCH mode a message that keeps failing is retried up
to a limit and then dead-lettered to a per-run DLQ stream; REALTIME drops it.

Termination
-----------

A flow stops after any of:

1. all producers raise ``StopIteration``;
2. ``flow.stop()`` is called;
3. an interrupt (``Ctrl-C``) reaches the workers.

When a producer finishes it publishes an **end-of-stream** marker on a dedicated
subject of its stream. Every replica of each downstream node has its own consumer for
that marker, so all replicas observe it; a node then keeps draining its input until
the broker reports nothing pending before propagating end-of-stream to its own
children and exiting. This makes shutdown lossless even for replicated nodes.
``flow.stop()`` instead publishes on a dedicated **control channel** every worker
subscribes to, for an immediate flow-wide stop. Either way, each worker runs its
node's ``close()`` before exiting.
