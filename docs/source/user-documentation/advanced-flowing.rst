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
- a multi-parent join must have ``nb_tasks=1``.

Any violation raises a ``ValueError`` before anything is launched.

Compilation
-----------

When you call ``flow.run(engine)`` (or ``videoflow deploy``), the graph is compiled
into one **node specification** per node — a flat, JSON-serializable record holding
the node's class, its captured parameters, its parents' names, its kind
(producer/processor/consumer), replica count, device type, and resolved image
family. The engine turns those specs into workers.

Routing and message assembly
----------------------------

Each node owns one broker stream, named from its node name. A node publishes only
its own output there. To assemble its inputs, a node subscribes to a durable
consumer on **each parent's** stream. Two mechanisms coexist:

- **Broadcast**: distinct child nodes of the same parent each get their own durable
  consumer, so each child receives a full copy of every message.
- **Competing consumers**: replicas of one node (``nb_tasks > 1``) share a single
  durable consumer, so each message goes to exactly one replica.

Every message carries a **trace id** that originates at the producer and is carried
forward through the graph. A join buffers incoming messages by trace id and releases
a set to ``process()`` only once one message from every parent with a matching trace
id has arrived. This is how multi-parent nodes stay correctly synchronized without
any user-visible coordination.

Termination
-----------

A flow stops after any of:

1. all producers raise ``StopIteration``;
2. ``flow.stop()`` is called;
3. an interrupt (``Ctrl-C``) reaches the workers.

Termination propagates two ways. Producers emit an explicit **stop marker** on their
data streams, which flows through the graph like any other message so consumers
drain in order. In parallel, ``flow.stop()`` publishes on a dedicated **control
channel** every worker subscribes to, so long-idle workers stop waiting promptly.
Each worker runs its node's ``close()`` before exiting.
