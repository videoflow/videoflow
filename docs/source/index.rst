videoflow's documentation
==========================

.. meta::
   :description lang=en: video processing, video analytics framework, object detection, object tracking.

**videoflow** is a Python framework for video stream processing. The library is designed to facilitate easy and quick definition of streaming applications.  Yet, it can be also very efficient.

It can be used for any kind of streaming data, but we designed it with video in mind.  For example, in the computer vision domain it can be used to quickly develop video streaming analytics applications such as people detection, people tracking, people counting.

Developer friendly
    Even complex flow applications can be defined in a simple file with less than 20 lines of code.

Automatic synchronization of resources
    You only have to define flow as a computation graph. The framework automatically allocates the resources for the computation and the synchronization and communication among the resources.

Easy to extend
    It is very easy to create your own components and add them to the pipeline. 

Free and open source
    **videoflow** is free and open source.  It uses the MIT License, which means you can mostly do anything with it.

.. toctree::
    :maxdepth: 2
    :hidden:
    :caption: First steps

    first-steps/getting-started-with-videoflow
    first-steps/what-is-a-flow
    first-steps/how-to-contribute

.. toctree::
    :maxdepth: 2
    :hidden:
    :caption: User documentation

    user-documentation/creating-a-flow-application
    user-documentation/writing-your-own-components
    user-documentation/debugging-flow-applications
    user-documentation/task-allocation
    user-documentation/advanced-flowing

.. toctree:: 
    :maxdepth: 2
    :hidden:
    :caption: Api documentation

    apidocs/modules


