Installing Videoflow
==============================

Before installing **Videoflow**, please install **Tensorflow** and **OpenCV**.
Also, **Python 2** is not supported. You need to be running on **Python 3.6+**.

There are two ways to install **Videoflow**:

- Install **Videoflow** from PyPI (recommended)::

    sudo pip3 install videoflow

If you are using ``virtualenv``, you may want to avoid using sudo::

    pip3 install videoflow

- Alternatively: Install **Videoflow** from the Github source:

First clone Videoflow using `git`::

    git clone https://github.com/videoflow/videoflow.git

Then, `cd` to the **Videoflow** folder and run the install command::

    cd videoflow
    sudo python setup.py install
