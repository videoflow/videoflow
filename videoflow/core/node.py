from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from .processor import Processor

GPU = 'gpu'
CPU = 'cpu'
device_types = [CPU, GPU]

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
    
    def open(self):
        '''
        This method is called by the task runner before doing any consuming,
        processing or producing.  Should be used to open any resources
        that will be needed during the life of the task, such as opening files,
        tensorflow sessions, etc.
        '''
        pass
    
    def close(self):
        '''
        This method is called by the task running after finishing doing all
        consuming, processing or producing because of and end signal receival.
        Should be used to close any resources
        that were opened by the open() method, such as files,
        tensorflow sessions, etc.
        '''
        pass
    
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
        if self._parents is None:
            return None
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
        raise NotImplementedError('consume function needs to be implemented\
                            by subclass')

class ProcessorNode(Node):
    def __init__(self, nb_tasks : int = 1, device_type = CPU):
        self._nb_tasks = nb_tasks
        if device_type not in device_types:
            raise ValueError('Device is not one of {}'.format(",".join(device_types)))
        self._device_type = device_type
        super(ProcessorNode, self).__init__()

    @property
    def nb_tasks(self):
        '''
        Returns the number of tasks to allocate to this processor
        '''
        return self._nb_tasks
    
    @property
    def device_type(self):
        '''
        Returns the preferred device type to use to run the processor's code
        '''
        return self._device_type
    
    def change_device(self, device_type):
        if device_type not in device_types:
            raise ValueError('Device is not one of {}'.format(",".join(device_types)))
        self._device_type = device_type
    
    def process(self, inp : any) -> any:
        '''
        Method definition that needs to be implemented by subclasses.

        - Arguments:
            - inp: object or list of objects being received for processing \
                from parent nodes.
        
        - Returns:
            - the output being consumed by child nodes.
        '''
        raise NotImplementedError('process function needs to be implemented\
                            by subclass')

class OneTaskProcessorNode(ProcessorNode):
    '''
    Used for processes that keep internal state so they are easily parallelizable.
    The main use of this class if for processes that can only run one
    task, such as trackers and aggregators.
    '''
    def __init__(self):
        super(OneTaskProcessorNode, self).__init__(nb_tasks = 1)

class ExternalProcessorNode(ProcessorNode):
    def __init__(self, processor : Processor, nb_proc : int = 1, device = CPU):
        self._processor = processor
        super(ExternalProcessorNode, self).__init__(nb_proc, device)
    
    def process(self, inp):
        return self._processor.process(inp)

class FunctionProcessorNode(ProcessorNode):
    def __init__(self, processor_function, nb_proc : int = 1, device = CPU):
        self._fn = processor_function
        super(FunctionProcessorNode, self).__init__(nb_proc, device)
    
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
        raise NotImplementedError('Method needs to be implemented by subclass')
