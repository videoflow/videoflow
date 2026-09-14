'''
Runtime context optionally handed to a node's lifecycle/processing methods.

A node method (``open``/``next``/``process``/``consume``/``close``) may declare a
final ``ctx`` (or ``context``) parameter; if it does, the task passes a
``RuntimeContext`` so the node can read run identity and set a partition key on its
output without depending on any global state. Methods that don't declare it are
called exactly as before, so this is fully backward compatible with existing nodes.
'''
from __future__ import absolute_import, division, print_function

import logging
from typing import Any, Dict, Optional

from .engine import Messenger

#: Reserved metadata key under which a task hands the node's pending checkpoint to
#: ``Messenger.publish_message`` (RFC 0006 ``CTRL-4``; RUN-003/RUN-022): the state
#: rides with the output it belongs to, so the messenger commits both in one
#: ledger write and the key never reaches the wire. Like ``_partition_key``, a
#: leading underscore marks it as the runtime's, not the node's.
CHECKPOINT_METADATA_KEY = '_checkpoint'


class RuntimeContext:
    '''
    - Attributes:
        - flow_id / run_id / node_name / replica_id: identity of this running node.
        - logger: a standard library logger scoped to the node.
        - emits_output: whether an output publication follows each processed input \
            (set by the task from ``has_children``). It decides when a checkpoint \
            becomes durable: with an output, together with that output; without \
            one, at once.
    '''
    def __init__(self, flow_id : str, run_id : str, node_name : str, replica_id : int,
                logger : logging.Logger, messenger : Optional[Messenger] = None) -> None:
        self.flow_id = flow_id
        self.run_id = run_id
        self.node_name = node_name
        self.replica_id = replica_id
        self.logger = logger
        self._messenger = messenger
        self.emits_output : bool = False
        self._pending_checkpoint : Optional[bytes] = None

    def set_partition_key(self, value : Any) -> None:
        '''
        Set the partition key carried on this node's *next* published output, so a
        downstream partitioned node can route by a business key. Applied to the
        message metadata under the reserved field ``_partition_key``.
        '''
        if self._messenger is not None:
            self._messenger.set_output_partition_key(value)

    def set_event_timestamp(self, value : float) -> None:
        '''
        Set the event time (epoch seconds) stamped on this node's *next* published
        output — when the underlying real-world event was captured (a frame's
        capture time, a sensor sample's timestamp). Producers of time-sensitive
        data should call this from ``next()``; downstream nodes inherit their
        input group's event time automatically, so they rarely need to. Time-
        aligned joins (``JoinPolicy(mode='time')``) group on this value.
        '''
        if self._messenger is not None:
            self._messenger.set_output_event_timestamp(value)

    @property
    def input_key(self) -> Optional[str]:
        '''
        A stable identity for the input group being processed (the same across a
        redelivery or a restart): what an idempotent sink keys its external
        effect on (``effect_guarantee = 'idempotent_key'``). ``None`` for producers.
        '''
        if self._messenger is None:
            return None
        return self._messenger.last_input_key()

    def checkpoint(self, state : bytes) -> None:
        '''
        Record ``state`` — the node's own serialized state — in the run ledger,
        together with the identity of the input group being processed, in one
        write (RFC 0006 ``CTRL-4``; RUN-022): whatever a replacement restores
        with ``restore_checkpoint`` describes exactly the inputs up to and
        including that group, and the runtime acknowledges that group without
        handing it to the node again. Call it from ``process``/``consume`` after
        the state is updated and before returning. A no-op without a messenger.

        For a node whose input produces an output (``emits_output``), the write
        is deferred and committed by the messenger in the same ledger write as
        the output it belongs to (the task passes it under
        ``CHECKPOINT_METADATA_KEY``): a crash between the state and the output
        can then never leave one without the other (RUN-003). A sink or a leaf
        node has no output to wait for, so its checkpoint is written at once.
        '''
        if self._messenger is None:
            return
        if self.emits_output:
            self._pending_checkpoint = state
            return
        self._messenger.checkpoint(state)

    def take_pending_checkpoint(self) -> Optional[bytes]:
        '''The state a deferred ``checkpoint`` left for the next output publication, cleared once taken (the task's call).'''
        state, self._pending_checkpoint = self._pending_checkpoint, None
        return state

    def restore_checkpoint(self) -> Optional[bytes]:
        '''The state bytes of the last ``checkpoint`` of this node in the run, or ``None``. Call it from ``open()``.'''
        if self._messenger is None:
            return None
        return self._messenger.restore_checkpoint()

    @property
    def input_info(self) -> Optional[Dict[str, Any]]:
        '''
        Per-parent envelope info for the input group currently being processed:
        ``{parent_name: {'event_ts': ..., 'metadata': ..., 'trace_id': ..., 'seq': ...}}``
        (``None`` values for parents missing from a quorum emission; lists for
        collect parents). ``None`` for producers. Lets fusion code read each
        input's exact event time without changing ``process()`` signatures.
        '''
        if self._messenger is None:
            return None
        return self._messenger.last_input_info()
