'''
Moved to ``videoflow.wire.serialization``.

This module remains as a permanent compatibility shim: the wire format is the one
thing a message outlives its producer on, so external importers may have pinned this
module path, and it must keep resolving to the wire codec. Do not add code — edit
``videoflow/wire/serialization.py`` instead.
'''
from videoflow.wire.serialization import *  # noqa: F401,F403
