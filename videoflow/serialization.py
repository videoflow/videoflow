'''
Moved to ``videoflow.wire.serialization``.

This module remains as a permanent compatibility shim: the wire format is the
one thing a message outlives its producer on, and a dead-lettered pickle payload
recorded under the old module path must still decode here. Do not add code —
edit ``videoflow/wire/serialization.py`` instead.
'''
from videoflow.wire.serialization import *  # noqa: F401,F403
