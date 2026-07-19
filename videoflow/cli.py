'''
Moved to ``videoflow.deploy.cli``.

This module remains as a permanent compatibility shim: the ``videoflow``
console script resolves ``videoflow.cli:main``, and an installed entry point
outlives the source tree it was generated from. Do not add code — edit
``videoflow/deploy/cli.py`` instead.
'''
from videoflow.deploy.cli import *  # noqa: F401,F403
from videoflow.deploy.cli import main  # noqa: F401

if __name__ == '__main__':
    main()
