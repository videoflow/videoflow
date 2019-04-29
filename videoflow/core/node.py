from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from .processor import Processor
import allocation

class ContextNode:
    '''
    Used to defined a computation node interface that allows the 
    computation node to be used within a context.  It is useful
    for computation nodes that need to open and close resources
    when a tasks begins and ends.
    '''
    def __enter__(self):
        raise NotImplemented('Subclass must implement __enter__')
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        raise NotImplemented('Subclass must implement __exit__')

class Node:
    '''
    Represents a computational node in the graph. It is also a callable object. \
        It can be call with the list of parents on which it depends.
    '''
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
        '''
        The id of the node.  In this case the id of the node is produced by calling
        ``id(self)``.
        '''
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
        '''
        Adds child to the set of childs that depend on it.
        '''
        self._children.add(child)
    
    @property
    def parents(self):
        '''
        Returns a list with the parent nodes
        '''
        return list(self._parents)
    
    @property
    def children(self):
        '''
        Returns a set of the child nodes
        '''
        return set(self._children)

class Leaf(Node):
    '''
    Node with no children.
    '''
    def __init__(self):
        self._children = None
        super(Leaf, self).__init__()

class ConsumerNode(Leaf):
    def __init__(self):
        super(ConsumerNode, self).__init__()
    
    def consume(self, item):
        '''
        Method definition that needs to be implemented by subclasses. 

        - Arguments:
            - item: the item being received as input (or consumed).
        '''
        raise NotImplemented('consume function needs to be implemented\
                            by subclass')

class AllocatableConsumerNode(ConsumerNode):
    def __init__(self, allocation : allocation.Allocation):
        self._allocation = allocation
        super(ConsumerNode, self).__init__()
    
    def _consume_gpu(self, item):
        raise NotImplemented('_consume_gpu needs to be implemented by subclass')
    
    def _consume_cpu(self, item):
        raise NotImplemented('_consume_cpu needs to be implemented by sublcass')

    def consume(self, item):
        if self._allocation.device_type == allocation.GPU:
            self._consume_gpu(item)
        elif self._allocation.device_type == allocation.CPU:
            self._consume_cpu(item)
        else:
            
            self._consume_cpu(item)

class ProcessorNode(Node):
    def __init__(self):
        super(ProcessorNode, self).__init__()

    def process(self, inp : any) -> any:
        '''
        Method definition that needs to be implemented by subclasses.

        - Arguments:
            - inp: object or list of objects being received for processing \
                from parent nodes.
        
        - Returns:
            - the output being consumed by child nodes.
        '''
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
    '''
    The `producer node` does not receive input, and produces input. \
        Each time the ``next()`` method is called, it produces a new input.
    
    It would have been more natural to implement the ``ProducerNode`` as a generator, \
        but generators cannot be pickled, and hence you cannot easily work with generators \
        in a multiprocessing setting.
    '''
    def __init__(self):
        super(ProducerNode, self).__init__()

    def next(self) -> any:
        '''
        Returns next produced element.

        Raises ``StopIteration`` after the last element has been produced
        and a call to self.next happens.
        '''
        raise NotImplemented('Method needs to be implemented by subclass')
