from ..core.node import Node

class Processor(Node):
    def __init__(self):
        self._parents = None

    def process(input):
        raise NotImplemented("process function needs to be \
            implemented by subclass")
