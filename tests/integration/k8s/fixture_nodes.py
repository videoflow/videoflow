'''
Nodes for the engine-level cluster tests, written to be importable *inside a pod*.

A worker rebuilds its node from ``<module>.<Class>``, so this module has to be on
the worker's ``sys.path`` — which in a pod means inside the image or on a mounted
volume. ``stage_fixture_nodes`` in test_k8s_engine.py copies this file into a
directory that is hostPath-mounted at the base image's ``/app`` workdir; ``python
-m videoflow.worker`` puts the cwd first on ``sys.path``, so ``fixture_nodes``
resolves there. That is also why it imports nothing but videoflow: whatever it
needs has to already be in videoflow-base.

Failure is injected through constructor params rather than the environment,
matching tests/support_errors.py: params already round-trip ``get_params()`` →
``VF_NODE_PARAMS_JSON`` → worker, which is the only channel that reaches a pod.
'''
from __future__ import absolute_import, division, print_function

import os
from typing import Any

from videoflow.core.errors import DeviceError
from videoflow.core.node import ConsumerNode, ProcessorNode


class BoomProcessor(ProcessorNode):
    '''
    Fails on every message, from the first one, with a worker-fatal error.

    Worker-fatal rather than poison on purpose: it hands the message back instead
    of dead-lettering it, and stops the worker. The pod exits non-zero, the Job
    retries it, and every retry fails the same way — so the Job eventually
    exhausts its backoffLimit and reaches the terminal Failed condition. That is
    the signal ``wait_for_completion`` is supposed to report, and the reason the
    test that uses this sets ``max_restarts = 0``: with the default 3 the same
    outcome takes an extra ~70s of Job backoff to arrive at.
    '''
    def __init__(self, **kwargs : Any) -> None:
        super(BoomProcessor, self).__init__(**kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        raise DeviceError(f'the accelerator fell over on {item!r}',
                        remedy = 'Nothing to fix — this node exists to fail.')

class LineWriterConsumer(ConsumerNode):
    '''
    Appends each item to a file, one per line, creating it in ``open()``.

    The stdlib FileAppenderConsumer would do, but this keeps the fixture module
    self-contained and — more usefully — makes the file the *proof* that a pod
    wrote through the hostPath mount back onto the host filesystem.

    - Arguments:
        - path: absolute path inside the container, which the mount makes the \
            same absolute path on the host.
    '''
    def __init__(self, path : str, **kwargs : Any) -> None:
        self._path = path
        self._file = None
        super(LineWriterConsumer, self).__init__(**kwargs)

    def open(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok = True)
        self._file = open(self._path, 'a')

    def consume(self, item : Any) -> None:     # type: ignore[override]
        self._file.write(f'{item}\n')
        self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
