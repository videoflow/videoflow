
_execution_context = {}

class ExecutionContext:
    def __init__(self):
        self._node_parentnodes_d = {}
    
    def add_parent_nodes(self, node_id, parent_nodes_ids):
        self._node_parentnodes_d[node_id] = parent_nodes_ids
    
    def get_parent_nodes(self, node_id):
        self._node_parentnodes_d.get(node_id, None)
    
    def get_input_channel_address(self, node_id):
        pass
    
    def get_output_channel_address(self, node_id):
        pass

