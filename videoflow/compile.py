'''
Moved to ``videoflow.deploy.compile``.

This module remains as a permanent compatibility shim: ``videoflow deploy``
spawns ``python -m videoflow.compile`` *inside the solution image*, so the host
CLI and the image can be different videoflow versions in either direction and
the old path must keep resolving. Do not add code — edit
``videoflow/deploy/compile.py`` instead.
'''
from videoflow.deploy.compile import *  # noqa: F401,F403
from videoflow.deploy.compile import main  # noqa: F401

if __name__ == '__main__':
    main()
