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
from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode


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


class SlowLineWriterConsumer(LineWriterConsumer):
    '''
    ``LineWriterConsumer`` that takes ``delay_seconds`` per item — long enough for
    a test to delete its pod mid-consume (conformance RUN-030) and watch the
    replacement finish the work the first pod was holding.
    '''
    def __init__(self, path : str, delay_seconds : float = 1.0, **kwargs : Any) -> None:
        self._delay_seconds = delay_seconds
        super(SlowLineWriterConsumer, self).__init__(path, **kwargs)

    def consume(self, item : Any) -> None:     # type: ignore[override]
        import time
        time.sleep(self._delay_seconds)
        super(SlowLineWriterConsumer, self).consume(item)


class AssetLineWriterConsumer(LineWriterConsumer):
    '''
    ``LineWriterConsumer`` that depends on one asset by content identity
    (conformance RUN-033): the worker verifies ``asset_path`` has ``asset_sha256``
    before ``open()``; ``portable`` says whether the file is expected on every
    host (a claim) or only where it was written (a hostPath).
    '''
    def __init__(self, path : str, asset_path : str, asset_sha256 : str, portable : bool = False,
                 **kwargs : Any) -> None:
        self._asset_path = asset_path
        self._asset_sha256 = asset_sha256
        self._portable = portable
        super(AssetLineWriterConsumer, self).__init__(path, **kwargs)

    def required_assets(self) -> list:
        from videoflow.core.node import AssetRequirement
        return [AssetRequirement(self._asset_path, self._asset_sha256, self._portable)]

    def consume(self, item : Any) -> None:     # type: ignore[override]
        with open(self._asset_path, 'rb') as f:
            head = f.read(8)
        super(AssetLineWriterConsumer, self).consume(f'{item}:{head.hex()}')


class CameraFrameProducer(ProducerNode):
    '''
    ``frames`` items per camera, round-robin over ``cameras`` cameras, each a
    ``{'camera_id': 'cam<i>', 'n': <k>}`` dict — a partitionable stream for the
    scaling cases (conformance RUN-019): a tracker declaring
    ``partition_by = 'camera_id'`` keys on it.
    '''
    def __init__(self, cameras : int = 2, frames : int = 20, delay_seconds : float = 0.05, **kwargs : Any) -> None:
        self._cameras = cameras
        self._frames = frames
        self._delay_seconds = delay_seconds
        self._emitted = 0
        super(CameraFrameProducer, self).__init__(**kwargs)

    def next(self) -> Any:
        import time
        if self._emitted >= self._cameras * self._frames:
            raise StopIteration()
        camera, n = self._emitted % self._cameras, self._emitted // self._cameras
        self._emitted += 1
        time.sleep(self._delay_seconds)
        return {'camera_id': f'cam{camera}', 'n': n}


class PairProcessor(ProcessorNode):
    '''A two-parent processor: the trace join's output as a ``(left, right)`` tuple (conformance RUN-018).'''
    def __init__(self, **kwargs : Any) -> None:
        super(PairProcessor, self).__init__(**kwargs)

    def process(self, left : Any, right : Any) -> Any:      # type: ignore[override]
        return (left, right)


class TaggingProcessor(ProcessorNode):
    '''
    Stamps each item with the process that handled it (``handler``: hostname and
    pid) and a per-process running count — the membership trace a scaling case
    reads back from the sink's lines (conformance RUN-018/019).
    '''
    def __init__(self, **kwargs : Any) -> None:
        self._count = 0
        super(TaggingProcessor, self).__init__(**kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        import socket
        self._count += 1
        return {'item': item, 'handler': f'{socket.gethostname()}:{os.getpid()}', 'count': self._count}


class SleepProcessor(ProcessorNode):
    '''Takes ``seconds`` per item and passes it through — a node whose capacity a source can exceed (conformance RUN-026).'''
    def __init__(self, seconds : float = 0.05, **kwargs : Any) -> None:
        self._seconds = seconds
        super(SleepProcessor, self).__init__(**kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        import time
        time.sleep(self._seconds)
        return item
