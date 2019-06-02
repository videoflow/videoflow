from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
logger = logging.getLogger(__package__)

from ..utils.graph import has_cycle, topological_sort
from .constants import LOGGING_LEVEL
from .processor import Processor
from .constants import GPU, CPU, DEVICE_TYPES

class Node:
    '''
    Represents a computational node in the graph. It is also a callable object. \
        It can be call with the list of parents on which it depends.
    '''
    def __init__(self):
        self._parents = None
        self._id = id(self)
        self._children = set()
        self._logger = self._configure_logger()
        self._logger.debug(f'Created Node with id {self._id}')
    
    def _configure_logger(self):
        logger = logging.getLogger(f'{self.__repr__()}_{self._id}')
        logger.setLevel(LOGGING_LEVEL)
        ch = logging.StreamHandler()
        ch.setLevel(LOGGING_LEVEL)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def __repr__(self):
        return self.__class__.__name__
    
    def __eq__(self, other):
        return self is other
    
    def __hash__(self):
        return self._id
    
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
        # ****IMPORTANT********
        # Must not be changed. Computing the id at call time will likely introduce errors
        # because the Python built-in `id` function might return different ids for the same
        # graph node if called from different processes.  So is better to compute it once
        # from the process where all constructors are called, and then save it for later.
        return self._id
    
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
        if device_type not in DEVICE_TYPES:
            raise ValueError('Device is not one of {}'.format(",".join(DEVICE_TYPES)))
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
        if device_type not in DEVICE_TYPES:
            raise ValueError('Device is not one of {}'.format(",".join(DEVICE_TYPES)))
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

class SequenceProcessorNode(ProcessorNode):
    '''
    Processor node that wraps a sequence of processor nodes. This has the effect
    that the natural effect that instead of allocating one task per processor node in
    the sequence, only one task is allocated for the entire sequence.
    - Arguments:
        - processor_nodes: [ProcessorNode] The sequence of processor nodes
        - nb_tasks (int) The number of parallel tasks to allocate
    - Raises:
        - ``ValueError`` if:
            - There is at least one node in the sequence that is not instance of ``ProcessorNode``
            - ``nb_tasks`` parameter is greater than one and there is at least one node in the sequence \
                that derives from ``OneTaskProcessorNode``.
            - There is at least one node in the sequence that has device_type GPU
            - The sequence length is less than 1.
    '''
    def __init__(self, processor_nodes : [ProcessorNode], nb_tasks : int = 1):
        if any([not isinstance(p, ProcessorNode) for p in processor_nodes]):
            raise ValueError('There is at least one node that is not instance of ProcessorNode')
        if any([isinstance(p, OneTaskProcessorNode) for p in processor_nodes]) and nb_tasks > 1:
            raise ValueError('Cannot have nb_tasks > 1 if one of the processor nodes is derived from OneTaskProcessorNode')
        if any([p.device_type == GPU for p in processor_nodes]):
            raise ValueError('Cannot have nodes with device type GPU as part of the sequence')
        if len(processor_nodes) < 1:
            raise ValueError('Must pass a list of at least one processor node')
        self._processor_nodes = processor_nodes
        # TODO: Check what to do with a call to __call__ that might have happened in processor 
        # nodes that are part of the sequence.
        super(SequenceProcessorNode, self).__init__(nb_tasks, device_type = CPU)
    
    def process(self, *inp):
        to_return = self._processor_nodes[0].process(*inp)
        for p in self._processor_nodes[1:]:
            to_return = p.process(to_return)
        return to_return

class ModuleProcessorNode(SequenceProcessorNode):
    def __init__(self, tsort_sequence : [ProcessorNode]):
        super(ModuleProcessorNode, self).__init__(tsort_sequence, nb_tasks = 1)

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

class ModuleNode(Node):
    '''
    Module node that wraps a subgraph of computation. Each node of the Module must be a ``ProcessorNode``
    or a ``ModuleNode`` itself.  For simplicity, a module node has exaclty one node as entry point, 
    and exactly one node as exit point. 
    If for some reason a ModuleNode has flag ``one_process`` set to ``True``:
        - Then any module within the subgraph must also be of that type, or an exception will be thrown. 
        - No process inside the module can be allocated to a gpu, or an exception will be thrown

    - Arguments:
        - entry_node (Node): The node that sits at the top of the subgraph
        - exit_node (Node): The node that sits at the top of the subgraph
        - one_process (boolean) If ``True``, the entire module will execute in one process. \
            Otherwise, each node of the module will execute on its own process

    - Raises:
        - ``ValueError`` if:
            - There is at least one node in the sequence that is not instance of ``ProcessorNode`` or of ``ModuleNode``
            - There is a cycle in the subgraph
            - The ``exit_node`` is not reachable from the ``entry_node``
            - The flag ``one_process`` is set to ``True``, and any of the following conditions is true:
                - There is a ModuleNode within the subgraph that does not have that flag set to true too.
                - There is at least one node in the sequence that has device_type GPU
    '''
    def __init__(self, entry_node : Node, exit_node : Node, one_process : boolean = False):
        self._entry_node = entry_node
        self._exit_node = exit_node
        self._one_process = one_process

        if has_cycle([entry_node]):
            logger.error('Cycle detected in module graph. Exiting now...')
            raise ValueError('Cycle found in module graph')
        
        temp_tsort = topological_sort([entry_node])
        
        if exit_node not in temp_sort:
            logger.error(f'{exit_node} is not descendant of entry node. Exiting now...')
            raise ValueError(f'{exit_node} is not descendant of entry node')
        
        if any([((not isinstance(p, ProcessorNode)) and (not isinstance(p, ModuleNode))) for p in self._tsort]):
            raise ValueError('There is at least one node in the module graph that is not instance of ProcessorNode or of ModuleNode')
        
        if one_process:
            for n in temp_sort:
                if isinstance(n, ModuleNode):
                    if not n.one_process:
                        logger.error(f'Cannot have ModuleNode {n} without one_process flag inside'
                                        ' a module node with one_process flag set')
                        raise ValueError('Cannot have ModuleNode {n} without one_process flag inside' 
                                        ' a module node with one_process flag set')
                elif isinstance(n, ProcessorNode):
                    if n.device_type == GPU:
                        logger.error(f'Cannot have node {n} with device type GPU as part of a ModuleNode')
                        raise ValueError(f'Cannot have node {n} with device type GPU as part of a ModuleNode')

        # Create valid tsort here.
        self._tsort = []
        for n in temp_sort:
            if isinstance(n, ProcessorNode):
                self._tsort.append(n)
            elif isinstance(n, ModuleNode):
                self._tsort.extend(n.nodes)       

        super(ModuleNode, self).__init__()
    
    @property
    def one_process(self):
        return self._one_process

    @property
    def nodes(self):
        if self.one_process:
            return [ModuleProcessorNode(list(self._tsort))]
        else:
            return list(self._tsort)

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
