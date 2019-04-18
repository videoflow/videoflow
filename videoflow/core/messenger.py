from .execution import ExecutionContext
from .node import Node

from ..brokers import zeromq as broker

def socket_from_task_id(task_id : int):
    return 5000 + task_id

class Messenger:
    def __init__(self, computation_node : Node, task_id, parent_task_id):
        self._computation_node = computation_node
        self._parent_nodes_ids = [a.id for a in self._computation_node.parents]
        self._task_id = task_id
        self._parent_task_id = parent_task_id
        self._output_socket_address = port_from_task_id(self._task_id)
        self._input_socket_address = port_from_task_id(self._parent_task_id)
        self._last_message_received = None

    def publish_message(self, message):
        if self._last_message_received is None:
            broker.publish_next_message(self._output_socket_address, {
                self._computation_node.id: message
            })
        else:
            self._last_message_received[self._computation_node.id] = message
            broker.publish_next_message(self._output_socket_address, self._last_message_received)
        
    def receive_message(self):
        '''
        Blocking method
        '''
        input_message_dict = broker.get_next_message(self._input_channel_id)
        self._last_message_received = input_message_dict
        inputs = [input_message[a] for a in self._parent_nodes_ids]
        return inputs
        