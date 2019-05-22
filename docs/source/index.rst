videoflow's documentation
=========================

.. meta::
   :description lang=en: video processing, video analytics framework, object detection, object tracking.

**Videoflow** is a Python framework that facilitates the quick development of complex video analysis applications and other series-processing based applications in a multiprocessing environment.

It can be used for any kind of streaming data, but we designed it with video in mind.  For example, in the computer vision domain it can be used to quickly develop video streaming analytics applications such as people detection, people tracking, people counting.

Developer friendly
    Even complex flow applications can be defined in a simple file with less than 20 lines of code.

Automatic synchronization of resources
    You only have to define flow as a computation graph. The framework automatically allocates the resources for the computation and the synchronization and communication among the resources.

Easy to extend
    It is very easy to create your own components and add them to the **flow**. 

Free and open source
    **Videoflow** is free and open source.  It uses the MIT License, which means you can mostly do anything with it.

.. toctree::
    :maxdepth: 2
    :hidden:
    :caption: First steps

    first-steps/installing-videoflow
    first-steps/getting-started-with-videoflow
    first-steps/how-to-contribute

.. toctree::
    :maxdepth: 2
    :hidden:
    :caption: Tutorials

    user-documentation/writing-your-own-components
    user-documentation/object-tracking-sample-application
    user-documentation/batch-versus-realtime-mode
    user-documentation/debugging-flow-applications
    user-documentation/task-allocation
    user-documentation/advanced-flowing
    user-documentation/common-patterns

.. toctree:: 
    :maxdepth: 2
    :hidden:
    :caption: Api documentation

    apidocs/videoflow.core
    apidocs/videoflow.environments
    apidocs/videoflow.producers
    apidocs/videoflow.processors
    apidocs/videoflow.consumers
    apidocs/videoflow.utils
    


