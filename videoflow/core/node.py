from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import inspect
import logging
import re

logger = logging.getLogger(__package__)

from ..utils.graph import has_cycle, topological_sort
from .constants import LOGGING_LEVEL
from .constants import GPU, CPU, DEVICE_TYPES

_SLUG_RE = re.compile(r'[^a-z0-9]+')

def _slugify(value : str) -> str:
    return _SLUG_RE.sub('-', value.lower()).strip('-')

class Node:
    '''
    Represents a computational node in the graph. It is also a callable object. \
        It can be call with the list of parents on which it depends.

    - Arguments:
        - name (str): a unique, stable identifier for this node within a flow. \
            Used as the node's identity everywhere outside the process that built \
            the graph: message broker subject names, Kubernetes resource names, and \
            logging. If not given, one is auto-generated from the class name and a \
            per-class construction counter; uniqueness within a flow is validated when \
            the flow is built (see ``videoflow.core.graph.GraphEngine``), not at \
            construction time, since a collision can only be detected once the whole \
            graph is known.
    '''
    _name_counters = {}

    def __init__(self, name = None):
        if name is None:
            cls_name = type(self).__name__
            Node._name_counters[cls_name] = Node._name_counters.get(cls_name, 0) + 1
            name = f'{_slugify(cls_name)}-{Node._name_counters[cls_name]}'
        self._name = name
        self._parents = None
        self._children = set()
        self._is_part_of_taskmodule_node = False
        self._logger = self._configure_logger()
        self._logger.debug(f'Created Node with name {self._name}')

    def _configure_logger(self):
        logger = logging.getLogger(f'{self.__repr__()}')
        logger.setLevel(LOGGING_LEVEL)
        ch = logging.StreamHandler()
        ch.setLevel(LOGGING_LEVEL)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def __repr__(self):
        return self._name

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
    def name(self) -> str:
        '''
        The stable, unique-within-a-flow string identity of the node. This is the \
            identifier used for broker subjects, Kubernetes resource names, and node \
            reconstruction in worker processes.
        '''
        return self._name

    def get_params(self) -> dict:
        '''
        Returns a JSON-serializable dict of keyword arguments sufficient to \
            reconstruct an equivalent node via ``type(node)(**node.get_params())``. \
            This is what lets a worker process (running in its own container, with no \
            access to the object that built the graph) recreate the exact node it is \
            responsible for running.

        Default implementation: walks every class in this node's MRO that declares \
            its own ``__init__``, inspects that ``__init__``'s named parameters \
            (skipping ``*args``/``**kwargs``), and for each parameter name looks up \
            ``self._<name>`` and then ``self.<name>``. This matches the prevailing \
            convention in this codebase of storing constructor arguments verbatim as \
            an underscore-prefixed attribute of the same name.

        Subclasses whose constructor arguments aren't stored under a matching \
            attribute name, or that accept non-serializable arguments (e.g. \
            references to other nodes, as ``TaskModuleNode`` does), must override \
            this method.
        '''
        params = {}
        for klass in reversed(type(self).__mro__):
            if '__init__' not in klass.__dict__:
                continue
            sig = inspect.signature(klass.__dict__['__init__'])
            for pname, p in sig.parameters.items():
                if pname == 'self' or p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
                    continue
                if hasattr(self, '_' + pname):
                    params[pname] = getattr(self, '_' + pname)
                elif hasattr(self, pname):
                    params[pname] = getattr(self, pname)
                else:
                    raise AttributeError(
                        f"Cannot auto-capture constructor parameter '{pname}' for "
                        f"{type(self).__name__}: no attribute 'self._{pname}' or "
                        f"'self.{pname}' found. Store the parameter under one of "
                        f"those names, or override get_params() on this class."
                    )
        return params

    def __call__(self, *parents):
        if self._parents is not None:
            raise RuntimeError('This method has already been called. It can only be called once.')
        if self._is_part_of_taskmodule_node:
            raise RuntimeError('Cannot make a node that belongs to a TaskModuleNode to be'
                                ' a child of another node in the graph. Use the TaskModuleNode for the edge'
                                ' if possible.')
        for parent in parents:
            if parent._is_part_of_taskmodule_node:
                raise RuntimeError('Cannot make a node that belongs to a TaskModuleNode to be'
                                ' a parent of another node. Use the TaskModuleNode as parent'
                                ' if possible.')
        self._parents = list()
        for parent in parents:
            assert isinstance(parent, Node), '%s is not a node' % str(parent)
            self._parents.append(parent)
            parent.add_child(self)
        return self

    def add_child(self, child):
        '''
        Adds child to the set of childs that depend on it.
        '''
        self._children.add(child)

    def remove_child(self, child):
        self._children.remove(child)

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
    def __init__(self, *args, **kwargs):
        self._children = None
        super(Leaf, self).__init__(*args, **kwargs)

class ConsumerNode(Leaf):
    '''
    - Arguments:
        - metadata (boolean): By default is False. If True, instead of receiving \
            output of parent nodes, receives metadata produced by parent nodes.
        - name (str): see ``Node``.
    '''
    def __init__(self, metadata = False, name = None, **kwargs):
        self._metadata = metadata
        super(ConsumerNode, self).__init__(name = name, **kwargs)

    @property
    def metadata(self):
        return self._metadata

    def consume(self, item):
        '''
        Method definition that needs to be implemented by subclasses.

        - Arguments:
            - item: the item being received as input (or consumed).
        '''
        raise NotImplementedError('consume function needs to be implemented\
                            by subclass')

class ProcessorNode(Node):
    '''
    - Arguments:
        - nb_tasks (int): number of parallel replicas to allocate for this processor.
        - device_type (str): ``videoflow.core.constants.CPU`` or ``GPU``.
        - name (str): see ``Node``.
    '''
    def __init__(self, nb_tasks : int = 1, device_type = CPU, name = None, **kwargs):
        self._nb_tasks = nb_tasks
        if device_type not in DEVICE_TYPES:
            raise ValueError('Device is not one of {}'.format(",".join(DEVICE_TYPES)))
        self._device_type = device_type
        super(ProcessorNode, self).__init__(name = name, **kwargs)

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
    def __init__(self, device_type = CPU, name = None, **kwargs):
        # nb_tasks is always 1 for this class; discard it if present (e.g. when
        # reconstructing from get_params(), which captures it from the ProcessorNode
        # level of the MRO) so it doesn't collide with the positional 1 below.
        kwargs.pop('nb_tasks', None)
        super(OneTaskProcessorNode, self).__init__(1, device_type = device_type, name = name, **kwargs)

class TaskModuleNode(ProcessorNode):
    '''
    Processor node that wraps a graph of processor nodes. This has the effect
    that instead of allocating one task per processor node in
    the graph, only one task process is allocated for the entire subgraph.

    - Arguments:
        - entry_node (Node): The node that sits at the top of the subgraph
        - exit_node (Node): The node that sits at the top of the subgraph
        - nb_tasks (int) The number of parallel tasks to allocate

    - Raises:
        - ``ValueError`` if:
            - There is at least one node in the subgraph that is not instance of ``ProcessorNode``
            - ``nb_tasks`` parameter is greater than one and there is at least one node in the sequence \
                that derives from ``OneTaskProcessorNode``.
            - There is at least one node in the sequence that has device_type GPU
            - The subgraph has less than one node.
            - There is a node of type ``TaskModuleNode`` among the nodes of the \
                subgraph.

    Note: because it wraps live references to other nodes, ``TaskModuleNode`` does \
        not support ``get_params()``-based reconstruction and is not yet supported \
        by the distributed Kubernetes execution path (see ``videoflow.compiler``).
    '''
    def __init__(self, entry_node : ProcessorNode, exit_node: ProcessorNode, nb_tasks = 1, name = None, **kwargs):
        super(TaskModuleNode, self).__init__(nb_tasks = nb_tasks, device_type = CPU, name = name, **kwargs)
        self._entry_node = entry_node
        self._exit_node = exit_node

        #3. Relinking entry and exit nodes
        #3.1 Adopt entry_node parents if any
        if entry_node.parents is not None:
            self._parents = entry_node.parents
            entry_node._parents = None
            for parent in self._parents:
                parent.remove_child(entry_node)
                parent.add_child(self)

        #3.2 Relink things with exit_node
        for child in exit_node.children:
            self.add_child(child)
            if child._parents is not None:
                pos_idx = child._parents.index(exit_node)
                child._parents[pos_idx] = self
        exit_node._children = set()

        #1. Building topological sort
        if has_cycle([entry_node]):
            logger.error('Cycle detected in module graph. Exiting now...')
            raise ValueError('Cycle found in module graph')
        self._tsort = topological_sort([entry_node])
        for node in self._tsort:
            node._is_part_of_taskmodule_node = True

        #2. Checking for correctness
        if exit_node not in self._tsort:
            logger.error(f'{exit_node} is not descendant of entry node. Exiting now...')
            raise ValueError(f'{exit_node} is not descendant of entry node')
        if any([not isinstance(p, ProcessorNode) for p in self._tsort]):
            raise ValueError(f'There is at least one instance in the module graph that is not instance of ProcessorNode')
        if any([isinstance(p, TaskModuleNode) for p in self._tsort]):
            raise ValueError('TaskModuleNode type of nodes cannot be nested.')
        if any([isinstance(p, OneTaskProcessorNode) for p in self._tsort]) and nb_tasks > 1:
            raise ValueError('Cannot have nb_tasks > 1 if one of the processor nodes is derived from OneTaskProcessorNode')
        if any([p.device_type == GPU for p in self._tsort]):
            raise ValueError('Cannot have nodes with device type GPU as part of the sequence')
        if len(self._tsort) < 1:
            raise ValueError('Must pass a list of at least one processor node')
        for node in self._tsort[1:]:
            for p in node.parents:
                if p not in self._tsort:
                    raise ValueError('There is a node whose parents are not part'
                                        ' of the list of processor nodes')


    def process(self, *inp):
        intermediate_results = {}
        result = self._tsort[0].process(*inp)
        intermediate_results[self._tsort[0]] = result
        for p in self._tsort[1:]:
            inp_values = [intermediate_results[a] for a in p.parents]
            result = p.process(*inp_values)
            intermediate_results[p] = result
        return result

    def get_params(self):
        raise NotImplementedError(
            'TaskModuleNode wraps live node references and is not supported by the '
            'distributed execution path yet. Deploy the fused nodes as separate, '
            'unfused nodes instead of wrapping them in a TaskModuleNode.'
        )

class FunctionProcessorNode(ProcessorNode):
    def __init__(self, processor_function, nb_tasks : int = 1, device_type = CPU, name = None, **kwargs):
        self._processor_function = processor_function
        super(FunctionProcessorNode, self).__init__(nb_tasks, device_type, name = name, **kwargs)

    def process(self, inp):
        return self._processor_function(inp)

    def get_params(self):
        raise NotImplementedError(
            'FunctionProcessorNode wraps an arbitrary Python callable, which is not '
            'JSON-serializable. Define a ProcessorNode subclass instead if this node '
            'needs to run in a distributed/Kubernetes flow.'
        )

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

    - Raises:
        - ``ValueError`` if:
            - There is at least one node in the sequence that is not instance of ``ProcessorNode`` or of ``ModuleNode``
            - There is a cycle in the subgraph
            - The ``exit_node`` is not reachable from the ``entry_node``
            - The flag ``one_process`` is set to ``True``, and any of the following conditions is true:
                - There is a ModuleNode within the subgraph that does not have that flag set to true too.
                - There is at least one node in the sequence that has device_type GPU
    '''
    def __init__(self, entry_node : Node, exit_node : Node, *args, **kwargs):
        self._entry_node = entry_node
        self._exit_node = exit_node

        if has_cycle([entry_node]):
            logger.error('Cycle detected in module graph. Exiting now...')
            raise ValueError('Cycle found in module graph')

        temp_tsort = topological_sort([entry_node])

        if exit_node not in temp_tsort:
            logger.error(f'{exit_node} is not descendant of entry node. Exiting now...')
            raise ValueError(f'{exit_node} is not descendant of entry node')

        if any([((not isinstance(p, ProcessorNode)) and (not isinstance(p, ModuleNode))) for p in temp_tsort]):
            raise ValueError('There is at least one node in the module graph that is not instance of ProcessorNode or of ModuleNode')

        # Create valid tsort here.
        self._tsort = []
        for n in temp_tsort:
            if isinstance(n, ProcessorNode):
                self._tsort.append(n)
            elif isinstance(n, ModuleNode):
                self._tsort.extend(n.nodes)

        super(ModuleNode, self).__init__(*args, **kwargs)

    def __call__(self, *parents):
        #TODO
        pass

    @property
    def nodes(self):
        return list(self._tsort)

    def get_params(self):
        raise NotImplementedError(
            'ModuleNode wraps live node references and is not supported by the '
            'distributed execution path.'
        )

class ProducerNode(Node):
    '''
    The `producer node` does not receive input, and produces input. \
        Each time the ``next()`` method is called, it produces a new input.

    It would have been more natural to implement the ``ProducerNode`` as a generator, \
        but generators cannot be pickled, and hence you cannot easily work with generators \
        in a multiprocessing setting.

    - Arguments:
        - is_finite (bool): True (the default) if a call to ``next()`` will eventually \
            raise ``StopIteration`` on its own (e.g. reading a fixed video file). Set to \
            ``False`` for producers that read from an unbounded/live source (e.g. an \
            RTSP stream) and only stop when told to. The Kubernetes execution engine \
            uses this to decide whether to deploy the producer as a ``Job`` (finite) or \
            a ``Deployment`` (infinite).
        - name (str): see ``Node``.
    '''
    def __init__(self, is_finite : bool = True, name = None, **kwargs):
        self._is_finite = is_finite
        super(ProducerNode, self).__init__(name = name, **kwargs)

    @property
    def is_finite(self) -> bool:
        return self._is_finite

    def next(self) -> any:
        '''
        Returns next produced element.

        Raises ``StopIteration`` after the last element has been produced
        and a call to self.next happens.
        '''
        raise NotImplementedError('Method needs to be implemented by subclass')
