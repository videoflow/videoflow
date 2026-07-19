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


class RuntimeContext:
    '''
    - Attributes:
        - flow_id / run_id / node_name / replica_id: identity of this running node.
        - logger: a standard library logger scoped to the node.
    '''
    def __init__(self, flow_id : str, run_id : str, node_name : str, replica_id : int,
                logger : logging.Logger, messenger : Optional[Messenger] = None) -> None:
        self.flow_id = flow_id
        self.run_id = run_id
        self.node_name = node_name
        self.replica_id = replica_id
        self.logger = logger
        self._messenger = messenger

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
