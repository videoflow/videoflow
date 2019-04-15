from ..core.node import Node

class Producer(Node):
    def __init__(self):
        super(Producer, self).__init__()

    def __iter__(self):
        raise NotImplemented('Method needs to be implemented by subclass')
    
