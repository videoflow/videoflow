Advanced Flowing
================
This section describes in a more detailed way what happens behind the scenes
when nodes and flows are created, and when flows are started and stopped.

Node creation and graph definition
----------------------------------
Consider the following simple example that defines a linear graph with one
producer, one processor and one consumer::

    from videoflow.core import Flow
    from videoflow.producers import IntProducer
    from videoflow.processors import IdentityProcessor
    from videoflow.consumers import CommandlineConsumer

    A = IntProducer(0, 40, 0.1)
    B = IdentityProcessor()(A)
    C = CommandlineConsumer()(B)

In the case of the **processor** and the **consumer**, two calls happen: one call to the ``__init__``
function, and another one to the ``__call__`` function of the just created object. 
The call to ``__init__`` creates a node.  The call to ``__call__`` defines the edges between the
nodes of the graph.

For example, ``A = IntProducer(0, 40, 0.1)`` creates node **A**. 
``B = IdentityProcessor()(producer)`` creates node **B** and creates edge **A -> B**, indicating
that **B** takes **A**'s output as its input.

Calling ``__call__`` twice in an object will raise a ``RuntimeError``.

Flow creation
-------------
A flow is created passing to it the list of **producers**, the list of **consumers**, an optional 
**flow_type**, and an optional **flow_options** parameter::

    flow = Flow([A], [C])

When the flow is created, the constructor checks that there are no cycles in the graph, otherwise
it raises a ``ValueError`` exception.  Also, only flows with exactly one **producer** are supported
for now. 

.. note:: 
    In the future:
        - Graphs with more than one **producer** will be supported.

flow.run() and the Execution Engine
-----------------------------------
Once the flow is built, when ``flow.run()`` is called, a topological sort of the nodes in the 
graph is created, and the topological sort of nodes is passed to the execution engine, 
whose function is: (1) to wrap each node as a task, (2) to create queues for communication between tasks,
and (3) to allocate each task to run in an independent operating system **process**.  If at node creation time it
was specified that more than one task (OS process) should be used for it, then more than one task is allocated
for that node.

A **flow** eventually stops running after any of the following events happen:
    1. All **producers** of the graph have raised an ``StopIteration`` exception.
    2. A ``KeyboardInterruption`` is received, such as ``Ctrl-C``.
    3. ``flow.stop()`` is explicitly called on the **flow**.

For any of the three cases above, the **flow** stops naturally: **producers** stop
emitting data and emit a ``STOP_FLOW`` signal.  ``STOP_FLOW`` is propagated through
the graph in the same way the rest of the data has been propagated.  Each time a task receives the
``STOP_FLOW`` signal, it closes any resources its corresponding node might have been using, passes
the ``STOP_FLOW`` to its "children" (only one child, since now the graph has been linearized
into a topological sort), and stops itself from running.

Multiple producers
------------------
To be supported in later versions of the framework.