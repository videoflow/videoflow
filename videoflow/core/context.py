'''
Runtime context optionally handed to a node's lifecycle/processing methods.

A node method (``open``/``next``/``process``/``consume``/``close``) may declare a
final ``ctx`` (or ``context``) parameter; if it does, the task passes a
``RuntimeContext`` so the node can read run identity and set a partition key on its
output without depending on any global state. Methods that don't declare it are
called exactly as before, so this is fully backward compatible with existing nodes.
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

class RuntimeContext:
    '''
    - Attributes:
        - flow_id / run_id / node_name / replica_id: identity of this running node.
        - logger: a standard library logger scoped to the node.
    '''
    def __init__(self, flow_id, run_id, node_name, replica_id, logger, messenger = None):
        self.flow_id = flow_id
        self.run_id = run_id
        self.node_name = node_name
        self.replica_id = replica_id
        self.logger = logger
        self._messenger = messenger

    def set_partition_key(self, value):
        '''
        Set the partition key carried on this node's *next* published output, so a
        downstream partitioned node can route by a business key. Applied to the
        message metadata under the reserved field ``_partition_key``.
        '''
        if self._messenger is not None:
            self._messenger.set_output_partition_key(value)
