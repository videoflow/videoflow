from ..core.node import Node

class Producer(Node):
    def __iter__(self):
        raise NotImplemented('Method needs to be implemented by subclass')
    
