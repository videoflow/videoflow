Node Allocation
===============

Allocating to multiple processes
--------------------------------
If your **processor** node is a bottleneck of the graph, you might
want to ask the framework to allocate it to more than one task.

.. note::
    In the current version of **Videoflow** there is a one to one correspondence
    between a task and an operating system process.

The allocation will not happen immediately; it will happen when ``flow.run()`` 
is called.  **Producer** and **consumer** nodes cannot be allocated to more than one process.

For example, to allocate a node to *n* processes, simply do at node definition time::
    
    node = Node(nb_tasks = n)

.. warning::
    Certain kind of processing nodes cannot be allocated to more than one process
    because they inherit from the ``videoflow.core.node.OneTaskProcessorNode``. In
    this case, the framework will ignore the `nb_tasks` parameter passed to the node
    at creation time. 

    If you are creating your own nodes, and if they are not stateless, it is highly likely
    that you will want to implement them inheriting from ``videoflow.core.node.OneTaskProcessorNode``.

.. warning::
    If your **processing** node is CPU-bound and the number of CPU cores in the physical
    system is less than the number CPU-bound tasks in your graph, you may not win a speed processing
    advantage by allocating a node to more than one task (or process).

Allocating to gpu
-----------------
When instantiating a node, you can ask the framework to run that node in a GPU like this:
``node = Node(device_type = 'gpu')``.  This does not mean that the node will run in a GPU.  First, the
source code of the node needs to support GPU allocation. Secondly, the machine where the flow is being
ran may not have a GPU.  Thirdly, even if the machine has a GPU, it might be in use by another node 
of the flow. 

You can both allocate to GPU and allocate to more than one process. If the machine where the flow is
running has enough GPUs, all the tasks (processes) will make use, each, of a GPU, otherwise, some of them 
will run in a GPU and some in the CPU.

Beware that some nodes have been defined to only be run in the GPU, and if no GPU is available at 
allocation time, a ``ValueError`` exception will be raised.  See the section **Using the GPU 
and the change_device method** under the heading **Writing your own components**
for more details.


Allocating to a different machine
---------------------------------
.. note::
    Currently **Videoflow** is designed for a multiprocessor setting.
    In the future **Videoflow** will have the capability to be deployed in a distributed setting.