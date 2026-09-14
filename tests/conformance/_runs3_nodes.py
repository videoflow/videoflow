'''
Node classes a conformance worker *subprocess* imports (``VF_NODE_CLASS``).

Kept apart from ``_runs3.py`` on purpose: a real ``videoflow.runtime.worker``
process reconstructs its node from ``(class path, params)`` and must not pull the
drivers, the broker helpers or pytest along with it. Every constructor argument
is stored verbatim as ``self._<name>`` so ``get_params()`` round-trips the node
into the worker exactly as the framework requires.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import time
from typing import Any, Optional

from videoflow.backends import faults
from videoflow.core.node import ConsumerNode


class PausingSink(ConsumerNode):
    '''
    A sink that reaches the ``sink.effect.before`` barrier on every input and
    then appends one JSON line per consumed input to ``log_path`` (pid, item,
    wall time) — the effect record a rollout test reads back to show which
    process committed which input, and that nothing was committed twice or
    never (RUN-030). Under a ``Pause`` schedule the first input is held inside
    ``consume`` for as long as the test wants the worker busy.
    '''
    def __init__(self, log_path : str, name : Optional[str] = None, **kwargs : Any) -> None:
        self._log_path = log_path
        super().__init__(name = name, **kwargs)

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    def consume(self, item : Any) -> None:
        faults.barrier('sink.effect.before', item = item)
        with open(self._log_path, 'a') as f:
            f.write(json.dumps({'pid': os.getpid(), 'item': item, 'at': time.time()}) + '\n')
            f.flush()
            os.fsync(f.fileno())
