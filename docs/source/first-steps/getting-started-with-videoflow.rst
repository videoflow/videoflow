Getting started with Videoflow
==============================

The main datastructure of **Videoflow** is a **Flow**. A **Flow** is defined as 
a directed acyclic graph (DAG) of nodes that can be of three types: **producers**,
**processors** and **consumers**. Directed edges in the graph represent dependency relationships:
A **node B** as a child of **node A** in the computation graph means that
**node B** receives as its input(s) the output(s) of **node A**.

Producer Node
    Producer nodes generate data and "place it" on the flow. They have no parents.  
    Examples of producers are nodes that randomly generate sequences of numbers,
    or nodes that read data from a file, or nodes that consume a video stream. 
    Notice that **producer nodes** usually `consume` data from an external data source,
    but they are called **producers** because they are the nodes that 
    originate or `produce` the data and place it in the flow.

Processor Node
    Processor nodes receive data as input. They compute or `process` on it,
    and return the result of the computation as output to be used by its child nodes.

Consumer Node
    Consumer nodes receive or `consume` data. They have no children.  They usually
    publish the data to sources external to the flow, but they are called **consumer nodes**
    because they are the `sinks` of the flow since no node in the flow receives data
    from them.

A first Videoflow application
-----------------------------

The sample application that we are going to create does the following:
It produces integers from 0 to 40 inclusive, at 0.01 second intervals.
It computes the aggregate sum of the produced integers and it prints
the result to the command line.  You can find the complete example 
`here <https://github.com/jadielam/videoflow/blob/master/examples/simple_example2.py>`_.

**The first section of the example imports the nodes to be used**::

    from videoflow.core import Flow
    from videoflow.producers import IntProducer
    from videoflow.processors import IdentityProcessor, JoinerProcessor
    from videoflow.consumers import CommandlineConsume

**After the imports, the example defines the computation graph of nodes**::

    producer = IntProducer(0, 40, 0.01)
    sum_agg = SumAggregator()(producer)
    printer = CommandlineConsumer()(sum_agg)

``producer`` is a **producer node**.  ``sum_agg`` is a **processor node**.
And ``printer`` is a **consumer node**.  ``producer`` does
not have parents, and ``printer`` does not have children.  **processors** and
**consumers** are **callable** objects.  They accept as arguments
the parents that they depend on.  In this simple example the computation
graph of nodes is very simple, a linear one:

.. image:: ../assets/first-steps/getting-started-with-videoflow/linear_graph.png

**The next lines of code create, start and wait for the the flow to finish**::

    flow = Flow([producer], [printer])
    flow.run()
    flow.join()

``flow = Flow([producer], [printer])`` creates the flow.  To create the flow you need at least a list of producers.  You
can also specify a list of consumers and the type of flow (if it is a
**realtime** flow or a **batch processing** flow).  By default a flow is
a **realtime** flow.

.. warning:: Videoflow currently supports flows with only
    one producer.  If you pass to the flow a list of more than one producer, 
    an exception is raised.

``flow.run()`` creates tasks for each node in the 
computation graph. Each task runs in an independent CPU process.  These tasks
communicate and coordinate between each other using queues, but the
user of **Videoflow** does not need to be aware of how this happens. 

``flow.join()`` blocks until all the tasks of the flow finish running.

When you run this example, you should see a sequence of monotonically
increasing numbers being printed on your screen.

.. note::
    You can write more complex flows with **Videoflow** really easily:
    
    .. image:: ../assets/first-steps/getting-started-with-videoflow/videoflow_logo_annotated.png
    
    The graph shown above can be easily translated to code as::

        from videoflow.producers import IntProducer
        from videoflow.processors import IdentityProcessor
        from videoflow.consumers import CommandlineConsumer

        A = IntProducer()
        B = IdentityProcessor()(A)
        C = IdentityProcessor()(B)
        D = IdentityProcessor()(C)
        E = IdentityProcessor()(D)
        F = CommandlineConsumer()(E)
        G = CommandLineConsumer()(D)
        H = CommandlineConsumer()(D)
        