from __future__ import absolute_import, division, print_function

from typing import Optional


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
    def publish_message(self, message, metadata = None) -> None:
        '''
        Publishes this node's own output message. Depending on the flow's \
            configured retention policy (REALTIME vs BATCH), this may drop the \
            message if downstream consumers are behind.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def publish_stop_signal(self) -> None:
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

    def ack_inputs(self) -> None:
        '''
        Acknowledge the input group last returned by ``receive_message`` — called by \
            the task only *after* the node has processed it (and, for a processor, \
            published its output). This ack-after-process ordering is what makes a \
            crash mid-processing safe: the un-acked message is redelivered instead \
            of lost.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def fail_inputs(self, exc) -> None:
        '''
        Report that the node raised while processing the last input group. The \
            messenger decides whether to redeliver (BATCH, up to a retry limit, then \
            dead-letter) or drop (REALTIME).
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def set_output_partition_key(self, value) -> None:
        '''
        Set the partition key attached to this node's next published output (via \
            ``RuntimeContext.set_partition_key``), so a downstream partitioned node \
            can route by a business key. Default: no-op.
        '''
        pass

    def last_input_key(self) -> Optional[str]:
        '''
        A stable identity for the input group last returned by ``receive_message``, \
            used as a sink idempotency key. Default: None (no idempotency).
        '''
        return None

    def close(self) -> None:
        '''Release any broker resources held by the messenger. Default: no-op.'''
        pass

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
    def __init__(self) -> None:
        self._allocation_called = False

    def _al_create_and_start_processes(self, tasks_data, flow_id : str, flow_type : str, run_id : str) -> None:
        '''
        - Arguments:
            - tasks_data: list of tuples ``(node, parent_names : [str], is_last : bool)``, \
                as produced by ``videoflow.core.flow.build_tasks_data``.
            - flow_id: stable identifier for this flow (constant across runs).
            - flow_type: 'realtime' or 'batch' — the broker retention policy to use \
                for every edge.
            - run_id: per-run identifier that scopes this execution's broker streams.
        '''
        raise NotImplementedError('Subclass of ExecutionEngine must implement')

    def signal_flow_termination(self) -> None:
        '''
        Signals the execution environment that the flow needs to stop.
        '''
        raise NotImplementedError('Subclass of ExecutionEngine must implement')

    def join_task_processes(self) -> None:
        '''
        Blocking method.  It is supposed to make the calling process sleep until all task \
        processes have finished processing.
        '''
        raise NotImplementedError('Subclass of ExecutionEngine must implement')

    def allocate_and_run_tasks(self, tasks_data, flow_id : str, flow_type : str, run_id : str) -> None:
        '''
        Defines a template with the order of methods that need to run in order to \
            allocate and run tasks.

        - Arguments:
            - tasks_data: list of tuples ``(node, parent_names : [str], is_last : bool)``.
            - flow_id: stable identifier for this flow (constant across runs).
            - flow_type: 'realtime' or 'batch'.
            - run_id: per-run identifier that scopes this execution's broker streams.
        '''
        if self._allocation_called:
            raise RuntimeError('This method has already been called. It can only be called once.')
        self._al_create_and_start_processes(tasks_data, flow_id, flow_type, run_id)
        self._allocation_called = True
