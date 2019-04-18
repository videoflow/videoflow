from .node import Node

from ..brokers import zeromq as broker

REFERENCE_SOCKET_NUMBER = 5000

def termination_socket_from_task_id(task_id : int):
    return REFERENCE_SOCKET_NUMBER - task_id

def socket_from_task_id(task_id : int):
    return REFERENCE_SOCKET_NUMBER + task_id

def task_id_from_socket(socket_number : int):
    return socket_number - REFERENCE_SOCKET_NUMBER

def task_id_from_termination_socket(socket_number : int):
    return REFERENCE_SOCKET_NUMBER - socket_number

class Messenger:
    def __init__(self, computation_node : Node, task_id, parent_task_id):
        self._computation_node = computation_node
        self._parent_nodes_ids = [a.id for a in self._computation_node.parents]
        self._task_id = task_id
        self._parent_task_id = parent_task_id
        self._output_socket_address = socket_from_task_id(self._task_id)
        self._input_socket_address = socket_from_task_id(self._parent_task_id)
        self._termination_socket_address = termination_socket_from_task_id(self.task_id)
        self._last_message_received = None

    def publish_message(self, message):
        if self._last_message_received is None:
            broker.publish_next_message(self._output_socket_address, {
                self._computation_node.id: message
            })
        else:
            self._last_message_received[self._computation_node.id] = message
            broker.publish_next_message(self._output_socket_address, self._last_message_received)
    
    def passthrough_message(self):
        broker.publish_next_message(self._output_socket_address, self._last_message_received)

    def check_for_termination_message(self):
        '''
        IMPORTANT:
        This cannot be a blocking method. If message does not exists,
        it needs to keep going.
        '''
        termination_message = broker.pick_next_message(self._termination_socket_address)
        return termination_message

    def receive_message(self):
        '''
        Blocking method
        '''
        input_message_dict = broker.get_next_message(self._input_channel_id)
        self._last_message_received = input_message_dict
        inputs = [input_message[a] for a in self._parent_nodes_ids]
        return inputs