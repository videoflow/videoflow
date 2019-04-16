from .node import Leaf

class Consumer(Leaf): 
    def __init__(self):
        super(Consumer, self).__init__()
    
    def consume(self, item):
        raise NotImplemented('consume function needs to be implemented\
                            by subclass')
