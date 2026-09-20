Installing Videoflow
==============================

You need **Python 3.12+** and docker. A **NATS JetStream** server is required
at runtime; ``videoflow run-local`` starts one in docker when none is
listening, or run your own (``nats-server -js``, or ``docker compose up -d``
using the ``docker-compose.yml`` in the repository root).

Videoflow is not on PyPI yet (the ``videoflow`` published there is an older,
unrelated generation), so install it **from a clone** — next to
`videoflow-contrib <https://github.com/videoflow/videoflow-contrib>`_ if you
want its solutions, the layout both repositories' docs assume::

    git clone https://github.com/videoflow/videoflow
    git clone https://github.com/videoflow/videoflow-contrib      # optional, side by side

Then pick one of:

- A tool install with `uv <https://docs.astral.sh/uv/>`_, which puts the
  ``videoflow`` command on your PATH for every shell and directory::

    uv tool install --editable './videoflow[all]'

- A virtual environment of your own::

    python3 -m venv .venv && .venv/bin/pip install -e './videoflow[all]'

- The development environment, to work on videoflow itself (dev tools
  included; run the command through ``uv run``)::

    cd videoflow
    uv sync
    uv run videoflow --help
    uv run pytest    # optional: the unit suite

``--editable`` (``-e``) matters: ``deploy`` and ``run-local`` build the
``videoflow-base`` image from this checkout the first time they need it, and
only a source install knows where the checkout is.

The extras are the same in every form: ``distributed`` (core + broker client +
wire format), ``vision`` / ``video`` (OpenCV, ffmpeg), ``deploy`` (Kubernetes
manifests, component descriptors), ``blob`` (the Redis payload store), or
``all``.

A solution whose dependencies you do not want on your machine — the ML
solutions in videoflow-contrib — needs nothing more than this: ``run-local``
builds its image and runs it there (see :doc:`../distributed/deploying-to-kubernetes`).
