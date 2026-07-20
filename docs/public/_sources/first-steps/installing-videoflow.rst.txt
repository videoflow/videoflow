Installing Videoflow
==============================

**Python 2** is not supported. You need to be running on **Python 3.8+**. A running
**NATS JetStream** server is required at runtime (``nats-server -js``, or
``docker compose up -d`` using the ``docker-compose.yml`` in the repository root).

There are two ways to install **Videoflow**:

- Install **Videoflow** from PyPI (recommended). Pick the extras your nodes need::

    pip install "videoflow[distributed]"   # core + broker client + wire format
    pip install "videoflow[vision]"         # + OpenCV for vision processors
    pip install "videoflow[video]"          # + ffmpeg/OpenCV for video I/O
    pip install "videoflow[deploy]"         # + Kubernetes manifest generation
    pip install "videoflow[all]"            # everything

- Alternatively, install **Videoflow** from the Github source with `uv
  <https://docs.astral.sh/uv/>`_:

First clone Videoflow using `git`::

    git clone https://github.com/videoflow/videoflow.git

Then, `cd` to the **Videoflow** folder and sync the environment::

    cd videoflow
    uv sync          # creates .venv with all dependencies (including dev tools)
    uv run pytest    # optional: run the test suite (needs a NATS server)
