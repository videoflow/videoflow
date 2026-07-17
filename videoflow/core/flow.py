from __future__ import absolute_import, division, print_function

import logging
import uuid
from typing import List, Optional

from .constants import FLOW_TYPES, REALTIME
from .engine import ExecutionEngine
from .graph import GraphEngine
from .node import ProducerNode

logger = logging.getLogger(__package__)

def _discover_producers(consumers) -> list:
    '''
    Walks the ``.parents`` chain backwards from each consumer to find the set of \
        root ``ProducerNode``s that feed it. A flow no longer needs producers to be \
        passed explicitly: they're always exactly the parentless ancestors of the \
        consumers.
    '''
    producers = []
    seen = set()
    stack = list(consumers)
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        parents = node.parents
        if not parents:
            if isinstance(node, ProducerNode):
                producers.append(node)
            else:
                raise ValueError(
                    f'{node} has no parents but is not a ProducerNode. '
                    'Every root of the graph must be a ProducerNode.'
                )
        else:
            stack.extend(parents)
    return producers

def build_tasks_data(graph_engine : GraphEngine) -> list:
    '''
    Turns a validated ``GraphEngine`` into the list of ``(node, parent_names, is_last)`` \
        tuples that both the local execution engine and the Kubernetes compiler \
        (``videoflow.compiler``) use to allocate one task per node.

    - Returns:
        - tasks_data: list of tuples ``(node, parent_names : [str], is_last : bool)``
    '''
    tsort = graph_engine.topological_sort()
    tasks_data = []
    for node in tsort:
        parents = node.parents or []
        parent_names = [p.name for p in parents]
        is_last = len(node.children) == 0
        tasks_data.append((node, parent_names, is_last))
    return tasks_data

class Flow:
    '''
    Represents a flow of data from producer nodes to consumer nodes, over the \
        directed acyclic graph formed by however you've wired up ``Node`` instances \
        via ``child(*parents)``.

    - Arguments:
        - consumers: a list of consumer nodes of type ``videoflow.core.node.ConsumerNode``. \
            Producers are discovered automatically by walking parents back from these.
        - flow_type: one of 'realtime' or 'batch'. Controls the message broker's \
            retention/discard policy for every edge in the flow (drop-when-full vs. \
            block/at-least-once).
        - flow_id: a stable identifier for this flow, used to namespace broker \
            subjects and Kubernetes resources. Auto-generated if not given.
    '''
    def __init__(self, consumers, flow_type = REALTIME, flow_id = None) -> None:
        producers = _discover_producers(consumers)
        self._graph_engine = GraphEngine(producers, consumers)
        if flow_type not in FLOW_TYPES:
            raise ValueError('flow_type must be one of {}'.format(','.join(FLOW_TYPES)))
        self._flow_type = flow_type
        self._flow_id = flow_id or uuid.uuid4().hex[:12]
        self._run_id: Optional[str] = None
        self._execution_engine: Optional[ExecutionEngine] = None

    @property
    def flow_id(self) -> str:
        return self._flow_id

    @property
    def flow_type(self) -> str:
        return self._flow_type

    @property
    def run_id(self) -> Optional[str]:
        '''The id of the most recent (or in-progress) run, or None before ``run()``.'''
        return self._run_id

    def topological_sort(self) -> list:
        '''
        Returns the topologically-sorted list of nodes in this flow. Exposed so the \
            Kubernetes manifest-generation CLI (``videoflow.cli``) can inspect the \
            graph without needing to call ``.run()``.
        '''
        return self._graph_engine.topological_sort()

    def tasks_data(self) -> List[tuple]:
        return build_tasks_data(self._graph_engine)

    def run(self, execution_engine : ExecutionEngine, run_id = None) -> None:
        '''
        Starts the flow using the given ``ExecutionEngine`` (e.g. \
            ``videoflow.engines.local.LocalProcessEngine`` or \
            ``videoflow.engines.kubernetes.KubernetesExecutionEngine``). \
            Non-blocking: returns once tasks have been allocated/started.

        - Arguments:
            - run_id: optional stable per-run id (auto-generated if omitted). Every \
                broker stream/subject for this run is namespaced by it, so re-running \
                the same flow never collides with a previous run's streams.
        '''
        self._execution_engine = execution_engine
        self._run_id = run_id or uuid.uuid4().hex[:12]
        tasks_data = self.tasks_data()
        self._execution_engine.allocate_and_run_tasks(
            tasks_data, self._flow_id, self._flow_type, self._run_id)
        logger.info('Allocated {} tasks for flow {} run {}'.format(
            len(tasks_data), self._flow_id, self._run_id))
        logger.info('Started running flow.')

    def join(self) -> None:
        '''
        Blocking method. Will make the process that calls this method block until the flow finishes
        running naturally.
        '''
        assert self._execution_engine is not None, 'join() called before run()'
        self._execution_engine.join_task_processes()
        logger.info('Flow has stopped.')

    def stop(self) -> None:
        '''
        Blocking method. Stops the flow.  Makes the execution environment send a flow termination signal.
        '''
        assert self._execution_engine is not None, 'stop() called before run()'
        logger.info('Stop termination signal placed on flow.')
        self._execution_engine.signal_flow_termination()
        self.join()
