How to contribute
=================

Found a bug? Have a feature to suggest? Want to contribute code? Read this first.

Bug reporting
-------------

1. Make sure your bug is not already fixed — update to the current ``master`` branch.

2. Search existing issues (including closed ones) for the same problem. If it is
   new, open an issue on GitHub.

3. Give us the information we need to reproduce it: your OS, Python version, how you
   ran the flow (locally with ``LocalProcessEngine`` or on Kubernetes), your NATS
   server version, and — if GPU-related — your CUDA/driver versions and GPU model.

4. Provide a **minimal, runnable** script that reproduces the issue. It should not
   require downloading external data — use ``IntProducer`` or randomly generated
   arrays where possible. A small flow that fails is far easier to diagnose than a
   full application.

The more information you provide, the faster we can help.

Requesting a feature
--------------------

Open a GitHub issue with a clear explanation of the feature and why it is broadly
useful, and code snippets demonstrating the API you have in mind. If it targets a
narrow use case, consider an add-on in
`videoflow-contrib <https://github.com/videoflow/videoflow-contrib>`_ instead.

Development setup
-----------------

Videoflow uses `uv <https://docs.astral.sh/uv/>`_ for packaging and environments::

    git clone https://github.com/videoflow/videoflow.git
    cd videoflow
    uv sync              # creates .venv with all dependencies, including dev tools

The test suite needs a running NATS JetStream server::

    docker compose up -d          # or: nats-server -js
    uv run pytest tests/

Pull requests
-------------

Improvements and bug fixes go to the ``master`` branch. A good PR:

1. Adds proper docstrings to any new function or class, and keeps touched code's
   docstrings up to date. Docstrings use sections for **Arguments**, **Returns** and
   **Raises** where applicable.
2. Includes tests. Pure logic (serialization, compilation, manifest generation) is
   covered by unit tests; end-to-end behavior is covered by integration tests that
   run small flows against a local NATS server.
3. Passes the full suite locally: ``uv run pytest tests/``.
4. Uses clear, descriptive commit messages.
5. Updates the documentation, including runnable snippets for new features.

Adding examples
---------------

Even without touching the core, if you have a concise, powerful Videoflow
application, consider adding it to the
`examples <https://github.com/videoflow/videoflow/tree/master/examples>`_ folder.
Each example should expose a ``build_flow()`` factory so it can be both run locally
and deployed with ``videoflow deploy``.
