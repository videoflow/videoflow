'''
Moved to ``videoflow.runtime.worker``.

This module remains as a permanent compatibility shim, and is the most frozen
path in the package: ``python -m videoflow.worker`` is the ENTRYPOINT baked into
the published base images, inherited by every contrib component image, and
spawned by the local engine. Already-built images cannot be retargeted. Do not
add code — edit ``videoflow/runtime/worker.py`` instead.
'''
from videoflow.runtime.worker import *  # noqa: F401,F403
from videoflow.runtime.worker import main  # noqa: F401

if __name__ == '__main__':
    main()
