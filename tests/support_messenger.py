'''
A ``Messenger`` stand-in for driving task loops without a broker.

The task loop is where the framework decides what a failure costs, and until now
it had no unit tests at all — every assertion about it needed a live NATS. This
records what the loop asked the messenger to do, in order, so the interesting
properties (ack *after* publish, fail rather than crash, abort rather than hang)
are checkable in milliseconds.

Grown out of ``_FakeInner`` in ``test_health.py``, which needed exactly this and
had to grow its own.
'''
from __future__ import absolute_import, division, print_function

from typing import Any, Dict, List, Optional

from videoflow.core.engine import Messenger
from videoflow.core.errors import error_to_dict


class RecordingMessenger(Messenger):
    '''
    - Arguments:
        - inputs: the input groups ``receive_message`` hands out, in order. Each is \
            a ``{parent: entry}`` dict. When they run out, an all-parents-stopped \
            result is returned, so a task loop always terminates.
        - parents: parent names, used to build the terminal result.
        - pending: what ``pending_count`` reports (feeds the progress deadline).

    - Attributes:
        - calls: every messenger method the task invoked, in order, as \
            ``(name, arg)``. Order is the point: ``publish`` must precede ``ack``.
    '''
    def __init__(self, inputs : Optional[List[Dict[str, dict]]] = None,
                parents : Optional[List[str]] = None, pending : int = 0) -> None:
        self._inputs = list(inputs or [])
        self._parents = list(parents or ['p'])
        self._pending = pending
        self.calls : List[tuple] = []
        self.published : List[tuple] = []
        self.failures : List[BaseException] = []
        self.aborts : List[Any] = []
        self.acks = 0
        self.stop_signals = 0
        self.terminated = False

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def data(message : Any, parent : str = 'p', metadata : Optional[dict] = None) -> dict:
        '''One ordinary input group.'''
        return {parent: {'message': message, 'metadata': metadata or {},
                        'is_stop_signal': False, 'is_abort': False, 'event_ts': None}}

    @staticmethod
    def eos(parent : str = 'p') -> dict:
        return {parent: {'message': None, 'metadata': None, 'is_stop_signal': True,
                        'is_abort': False}}

    @staticmethod
    def abort(parent : str = 'p', error : Optional[dict] = None) -> dict:
        return {parent: {'message': None, 'metadata': None, 'is_stop_signal': True,
                        'is_abort': True, 'abort_origin': parent,
                        'abort_error': error or {'code': 'VF_DEVICE',
                                                'message': 'card fell over'}}}

    # -- Messenger ----------------------------------------------------------

    def receive_message(self) -> dict:
        self.calls.append(('receive', None))
        if self._inputs:
            return self._inputs.pop(0)
        return {name: {'message': None, 'metadata': None, 'is_stop_signal': True,
                    'is_abort': False} for name in self._parents}

    def publish_message(self, message : Any, metadata : Optional[dict] = None) -> None:
        self.calls.append(('publish', message))
        self.published.append((message, metadata))

    def publish_stop_signal(self) -> None:
        self.calls.append(('stop', None))
        self.stop_signals += 1

    def publish_abort(self, error : Any) -> None:
        # Normalized here exactly as the real messenger does: the interface accepts
        # a typed error, a bare exception or an already-normalized dict, and every
        # implementation is responsible for flattening them to one shape.
        record = error_to_dict(error)
        self.calls.append(('abort', record))
        self.aborts.append(record)

    def check_for_termination(self) -> bool:
        return self.terminated

    def ack_inputs(self) -> None:
        self.calls.append(('ack', None))
        self.acks += 1

    def fail_inputs(self, exc : BaseException) -> None:
        self.calls.append(('fail', exc))
        self.failures.append(exc)

    def pending_count(self) -> int:
        return self._pending

    def last_input_key(self) -> Optional[str]:
        return None

    def close(self) -> None:
        self.calls.append(('close', None))

    # -- assertions ---------------------------------------------------------

    def call_names(self) -> List[str]:
        return [name for name, _arg in self.calls]
