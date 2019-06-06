Batch versus realtime mode
==========================

Flows in **Videoflow** can be of one of two types: ``realtime`` or ``batch``.
The difference in the type of the flow determines the behaviour of the
execution engine when passing data from one flow node to another.

The default mode is ``realtime``.  The flow type is set at flow creation time::

    from videoflow.core import Flow
    from videoflow.core.constants import BATCH

    # Other code to build the graph here...

    fl = Flow([producer], [consumer], flow_type = BATCH)

Batch mode
----------
If the flow is in ``batch`` mode, when the task of a **node A** finishes computing and produces an
output `i`, it passes it to the task of its child, **node B**.  Task of **node A** will block
until task of **node B** has finished computing on output `i-1` and is ready to receive output `i`.

In essence, this means that in ``batch`` mode, the processing (the flow) moves at the speed of the 
slowest node in the graph.  If the slowest node is not a **producer**, then you have a bottleneck
in the flow.  One way to solve this problem is to allocate more processes to the bottleneck
node.

.. note:: A **node** in a flow is a **bottleneck** node if its processing speed is slower
    than the production speed of the **producer** node of the flow.

Realtime mode
-------------
If the flow is in ``realtime`` mode, when the task of a **node A** finishes computing and produces
an output `i`, if task of child **node B** has not finished processing the previous entry `i-1`,
then task of **node A** drops output `i` and moves on to compute output `i+1`.

In essence, this means that in ``realtime`` mode the processing (the flow) moves at the speed
of the **producer** node of the graph.  If for some reason there are bottlenecks in the flow, this
means that frames will be dropped along the way so that the processing in the flow can keep up
with the speed of the **producer**.  As in the Batch Mode, one way to solve the issue of frames
being dropped is to allocate more processes to the bottleneck node(s).
