'''
Moved to ``videoflow.runtime.provision``.

This module remains as a permanent compatibility shim: ``python -m
videoflow.provision`` is rendered into the provision init Job of every deployed
flow, and those manifests may already be applied in a cluster. Do not add code
— edit ``videoflow/runtime/provision.py`` instead.
'''
from videoflow.runtime.provision import *  # noqa: F401,F403
from videoflow.runtime.provision import main  # noqa: F401

if __name__ == '__main__':
    main()
