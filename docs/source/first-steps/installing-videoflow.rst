Installing Videoflow
==============================

You need **Python 3.12+** and docker. A **NATS JetStream** server is required
at runtime; ``videoflow run-local`` starts one in docker when none is
listening, or run your own (``nats-server -js``, or ``docker compose up -d``
using the ``docker-compose.yml`` in the repository).

Videoflow is on `PyPI <https://pypi.org/project/videoflow/>`_. Pick one of:

- A virtual environment of your own::

    python3 -m venv .venv && .venv/bin/pip install 'videoflow[all]'

- A tool install with `uv <https://docs.astral.sh/uv/>`_, which puts the
  ``videoflow`` command on your PATH for every shell and directory::

    uv tool install 'videoflow[all]'

The extras are the same in every form: ``distributed`` (core + broker client +
wire format), ``vision`` / ``video`` (OpenCV, ffmpeg), ``deploy`` (Kubernetes
manifests, component descriptors), ``blob`` (the Redis payload store), or
``all``.

That is the whole install. The two other things a run may need arrive on
their own:

- The ``videoflow-base`` container image that solutions build on is pulled
  from ``ghcr.io/videoflow/videoflow-base:<your version>`` the first time
  ``deploy`` or ``run-local`` needs it (``VF_BASE_IMAGE_REGISTRY`` names a
  mirror instead).
- The solutions shipped in the videoflow repositories are fetched at your
  version when you name them as ``<repo>://<name>`` — ``videoflow
  run-local videoflow://toy_calculator``, ``videoflow deploy
  videoflow-contrib://human_tracking`` — into ``~/.videoflow/solutions/``
  (see :doc:`getting-started-with-videoflow` and
  :doc:`../distributed/deploying-to-kubernetes`).

A solution whose dependencies you do not want on your machine — the ML
solutions in `videoflow-contrib <https://github.com/videoflow/videoflow-contrib>`_
— needs nothing more than this: ``run-local`` builds its image and runs it
there.

To work on videoflow itself, install it from a clone instead; see
:doc:`how-to-contribute` (*Development setup*). A source install builds the
base image from the checkout rather than pulling it, so the code you edit is
what runs in the workers.
