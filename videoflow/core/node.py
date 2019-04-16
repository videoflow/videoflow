from .processor import Processor

class Node:
    def __init__(self):
        self._parents = None
        self._children = set()
    
    def __repr__(self):
        return self.__class__.__name__
    
    def __eq__(self, other):
        return self is other
    
    def __hash__(self):
        return id(self)
    
    @property
    def id(self):
        return self.__hash__()
    
    def __call__(self, *parents):
        if self._parents is None:
            self._parents = set()
        for parent in parents:
            assert isinstance(parent, Node) and not isinstance(parent, Leaf),
                    '%s is not a non-leaf node' % str(parent)
            self._parents.add(parent)
            parent.add_child(self)
        return self
        
    def add_child(self, child):
        self._children.add(child)
    
    @property
    def parents(self):
        return self._parents
    
    @property
    def children(self):
        return self._children

class Leaf(Node):
    def __init__(self):
        super(Leaf, self).__init__()

class ConsumerNode(Leaf): 
    def __init__(self):
        super(ConsumerNode, self).__init__()
    
    def consume(self, item):
        raise NotImplemented('consume function needs to be implemented\
                            by subclass')

class ProcessorNode(Node):
    def __init__(self, processor : Processor):
        self._processor = processor
        super(ProcessorNode, self).__init__()

    def process(self, inp):
        return self._processor.process(inp)

class ProducerNode(Node):
    def __init__(self):
        super(ProducerNode, self).__init__()

    def __iter__(self):
        raise NotImplemented('Method needs to be implemented by subclass')
