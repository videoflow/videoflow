Writing your own components
===========================

One of the best features of **Videoflow** is that it can be easily extended.
Videoflow has a good collection of components (producers, processors and consumers)
that can help you quickly bootstrap a computer vision application.  Sometimes the components
provided are not enough because you need to bring your own models, or you need to 
implement a complex algorithm, or you are dealing with a non-standard data source. 

In this tutorial we will show you how to write your own components, and how to 
integrate them to the **Videoflow** application.

Writing producers
-----------------

To write your own custom producer, you need to write a class that extends 
``videoflow.core.ProducerNode``.  You must write your own implementation of the ``next()`` method,
and you may write implementations for the ``open()`` and ``close()`` methods that ``videoflow.core.ProducerNode``
inherits from ``videoflow.core.Node``.

The ``open()`` method will be called by **Videoflow**'s execution engine before the producer task
begins to run.  You should use it whenever you need to open access to resources such as file system
resources, etc.

Once the task begins to run, the task runner will continuously call the ``next()`` method of the producer.
Each time the ``next()`` method is called, it should return the next produced element.  To indicate that
no more elements will be produced and returned, the method should raise the ``StopIteration()`` exception.

After the task finishes running because the producer has raised a ``StopIteration()``, the **Videoflow** execution engine
calls the ``close()`` method.  The method should close any resources that were opened by the ``open()`` or ``next()`` methods.
Examples of this resources are files and tensorflow sessions.

See below a sample implementation of ``videoflow.producers.VideofileReader``::

    import cv2
    from ..core.node import ProducerNode

    class VideofileReader(ProducerNode):
        '''
        Opens a video capture object and returns subsequent frames
        from the video each time ``next`` is called
        '''
        def __init__(self, video_file : str, nb_frames = -1):
            '''
            - Arguments:
                - video_file: path to video file
                - nb_frames: number of frames to process. -1 means all of them
            '''
            self._video_file = video_file
            self._video = None
            self._nb_frames = nb_frames
            self._frame_count = 0
            super(VideofileReader, self).__init__()
    
        def open(self):
            '''
            Opens the video stream
            '''
            if self._video is None:
                self._video = cv2.VideoCapture(self._video_file)

        def close(self):
            '''
            Releases the video stream object
            '''
            self._video.release()

        def next(self):
            '''
            - Returns:
                - frame: np.array of shape (h, w, 3)
        
            - Raises:
                - StopIteration: after it finishes reading the videofile \
                    or when it reaches the specified number of frames to \
                    process.
            '''
        
            if self._video.isOpened():
                success, frame = self._video.read()
                self._frame_count += 1
                if not success or self._frame_count == self._nb_frames:
                    raise StopIteration()
                else:
                    return frame
            else:
                raise StopIteration()

Writing processors
------------------
Processors are the nodes that perform computations, transformations or filtering of data. 
In general, processors receive data as input and return data as output.

To write your own custom processor, you need to write a class that extends 
``videoflow.core.Processor``.  You must write your own implementation of the ``process()`` method,
and you may write implementations for ``change_device()`` and for the ``open()`` and ``close()`` methods that ``videoflow.core.ProcessorNode``
inherits from ``videoflow.core.Node``.

Read **Writing producers** section above for a good explanation of how to implement the ``open()`` and
``close()`` methods.

Implementing the ``process`` method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The process method receives as parameters as many items as the number of parents the processor
node receives input from.  For example, consider implementing a processor that takes as input the
outputs of two parents, and returns as output `0` if the minimum came from the first parent, and `1`
if it came from the second one.  To implement it, simply do::

    from ..core.node import ProcessorNode

    class ComparisonProcessor(ProcessorNode):
        def process(self, inp1, inp2):
            if inp1 > inp2:
                return 0
            return 1

Notice that the order in which the inputs are received is important.  At flow definition time,
be sure to pass the parents to the ``__call__`` method in the same order that the ``process`` 
method expects them.

Using the GPU and the ``change_device`` method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
When a processor node is instantiated, the instantiator can pass a ``device_type`` parameter to
indicate its preference of whether the ``process`` method should be run in the ``cpu`` or the ``gpu``.
As the writer of a processor, you are responsible to write code that reads this parameter and acts 
accordingly.  For an example, see ``videoflow.processors.vision.detectors.TensorflowObjectDetector``.

The **Videoflow** execution engine keeps track of the number of gpus in the system, and of 
the number of processors in the flow that were instantiated with ``device_type`` being ``gpu``
(regardless of if the processor actually implements gpu allocation or not).  At task allocation time
(tasks are allocated in topological-sort order of the computation graph (which is not unique)), 
if there are no gpus left, the execution engine will call the ``change_device`` method of the **processor**
to change the device_type to ``cpu``.  

If for some reason you want to force the process to run on a gpu or make the flow process fail, 
you need to reimplement the ``change_device()`` method and raise a ``ValueError`` exception to make
the allocation process fail.

When to extend ``OneTaskProcessorNode``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**Videoflow** supports the parallelization of a processor in multiple
processes.  That functionality is very useful whenever to have whenever the processor is or
may become a bottleneck in the flow.

But there are certain processor nodes for one reason or another should not be parallelized. 
This usually happens if the processor node keeps an internal state.
In this case this processors should subclass the ``videoflow.core.node.OneTaskProcessorNode`` class.
A simple example is given below.  Another example are all the subclasses of ``videoflow.processors.vision.BoundingBoxTracker``::

    class MinAggregator(OneTaskProcessorNode):
        def __init__(self):
            self._min = float("inf")
            super(MinAggregator, self).__init__()
    
        def process(self, inp):
            if inp < self._min:
                self._min = inp
            return self._min

Writing consumers
-----------------
Consumers are the sinks of the flow.  They are leafs in the computation graph, so they do not produce
output, hence they are not parents to any node.  A common use of consumers is to publish results
to sources external to the flow, such as the file system, the command line, or a remote endpoint, etc.

To write your own custom consumer, you need to write a class that extends 
``videoflow.core.Consumer``.  You must write your own implementation of the ``consume()`` method,
and you may write implementations for the ``open()`` and ``close()`` methods that ``videoflow.core.ConsumerNode``
inherits from ``videoflow.core.Node``.

The ``consume`` method receives as parameters as many items as the number of parents the consumer
node receives input from.  Notice that the order in which the inputs are received is important.  At flow definition time,
be sure to pass the parents to the ``__call__`` method of the **consumer** in the same order that the ``consume`` 
method expects them.

See below an example of a simple consumer that writes its input to the command line::

    from ..core.node import ConsumerNode

    class CommandlineConsumer(ConsumerNode):
        def consume(self, item):
            print(item)
