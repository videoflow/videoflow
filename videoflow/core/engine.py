from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

class Messenger:
    '''
    Utility class that tasks use to receive input and write output, over a message \
        broker (see ``videoflow.messaging.nats_messenger.NATSMessenger`` for the \
        concrete implementation). A ``Messenger`` is bound to exactly one node in the \
        graph. It knows that node's own broker subject (for publishing) and its real \
        parents' subjects (for receiving) — routing is by node ``name``, not by \
        position in a topological sort, so it works correctly for arbitrary DAGs \
        (multi-parent joins, multi-producer graphs).
    '''
    def publish_message(self, message, metadata = None):
        '''
        Publishes this node's own output message. Depending on the flow's \
            configured retention policy (REALTIME vs BATCH), this may drop the \
            message if downstream consumers are behind.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def publish_stop_signal(self):
        '''
        Publishes a termination marker on this node's own subject. Unlike \
            ``publish_message``, this is never dropped regardless of retention \
            policy — every downstream consumer must observe it exactly once.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def check_for_termination(self) -> bool:
        '''
        Returns true if a flow-wide termination signal has been received on the \
            control channel. Used by ``videoflow.core.task.ProducerTask`` to stop \
            pulling new input even before it naturally reaches ``StopIteration``.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def receive_message(self) -> dict:
        '''
        Blocks until this node has received a complete input: one message from \
            every real parent, all derived from the same upstream originating event \
            (see the ``trace_id`` propagation scheme in the concrete implementation) \
            — or until every parent has signaled termination.

        - Returns:
            - a dict ``{parent_name: {"message": ..., "metadata": ..., "is_stop_signal": bool}}`` \
                with exactly one entry per real parent of this node.
        '''
        raise NotImplementedError('Messenger subclass must implement method.')

class ExecutionEngine:
    '''
    Defines the interface of the `execution environment` — how tasks are physically \
        started (as local OS processes for development, or as Kubernetes pods in \
        production) and how flow-wide termination and completion are observed.
    '''
    def __init__(self):
        self._allocation_called = False

    def _al_create_and_start_processes(self, tasks_data, flow_id : str, flow_type : str):
        '''
        - Arguments:
            - tasks_data: list of tuples ``(node, parent_names : [str], is_last : bool)``, \
                as produced by ``videoflow.core.flow.build_tasks_data``.
            - flow_id: stable identifier for this flow run.
            - flow_type: 'realtime' or 'batch' — the broker retention policy to use \
                for every edge.
        '''
        raise NotImplementedError('Subclass of ExecutionEngine must implement')

    def signal_flow_termination(self):
        '''
        Signals the execution environment that the flow needs to stop.
        '''
        raise NotImplementedError('Subclass of ExecutionEngine must implement')

    def join_task_processes(self):
        '''
        Blocking method.  It is supposed to make the calling process sleep until all task \
        processes have finished processing.
        '''
        raise NotImplementedError('Subclass of ExecutionEngine must implement')

    def allocate_and_run_tasks(self, tasks_data, flow_id : str, flow_type : str):
        '''
        Defines a template with the order of methods that need to run in order to \
            allocate and run tasks.

        - Arguments:
            - tasks_data: list of tuples ``(node, parent_names : [str], is_last : bool)``.
            - flow_id: stable identifier for this flow run.
            - flow_type: 'realtime' or 'batch'.
        '''
        if self._allocation_called:
            raise RuntimeError('This method has already been called. It can only be called once.')
        self._al_create_and_start_processes(tasks_data, flow_id, flow_type)
        self._allocation_called = True
