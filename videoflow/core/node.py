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
        if self._parents is not None:
            raise RuntimeError('This method has already been called. It can only be called once.')
        self._parents = list()
        for parent in parents:
            assert isinstance(parent, Node) and not isinstance(parent, Leaf), '%s is not a non-leaf node' % str(parent)
            self._parents.append(parent)
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
        self._children = None
        super(Leaf, self).__init__()

class ConsumerNode(Leaf): 
    def __init__(self):
        super(ConsumerNode, self).__init__()
    
    def consume(self, item):
        raise NotImplemented('consume function needs to be implemented\
                            by subclass')

class ProcessorNode(Node):
    def __init__(self):
        super(ProcessorNode, self).__init__()

    def process(self, inp):
        raise NotImplemented('process function needs to be implemented\
                            by subclass')

class ExternalProcessorNode(ProcessorNode):
    def __init__(self, processor : Processor):
        self._processor = processor
        super(ExternalProcessorNode, self).__init__()
    
    def process(self, inp):
        return self._processor.process(inp)

class FunctionProcessorNode(ProcessorNode):
    def __init__(self, processor_function):
        self._fn = processor_function
        super(FunctionProcessorNode, self).__init__()
    
    def process(self, inp):
        return self._fn(inp)

class ProducerNode(Node):
    def __init__(self):
        super(ProducerNode, self).__init__()

    def next(self):
        raise NotImplemented('Method needs to be implemented by subclass')
