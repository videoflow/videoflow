from .node import Node

class Processor(Node):
    def __init__(self):
        self._parents = None
        super(Processor, self).__init__()

    def process(self, input):
        raise NotImplemented("process function needs to be \
            implemented by subclass")
