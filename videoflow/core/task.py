from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import asyncio
import inspect
import logging
import time

from .node import Node, ProducerNode, ProcessorNode, ConsumerNode
from ..utils.generic_utils import DelayedKeyboardInterrupt

logger = logging.getLogger(__package__)

class Task:
    def run(self):
        '''
        Starts the task in an infinite loop.
        '''
        raise NotImplementedError('Subclass needs to implement run')

class NodeTask(Task):
    '''
    A ``NodeTask`` is a wrapper around a ``videoflow.core.node.Node`` that \
        is able to interact with the execution environment through a messenger. \
        Nodes receive input and/or produce output, but tasks are the ones \
        that run in infinite loops, receiving inputs from the environment and passing them to the \
        computation node, and taking outputs from the computation node and passing \
        them to the environment.

    - Arguments:
        - computation_node
        - messenger (Messenger): the messenger that will communicate between nodes.
        - has_children (bool): True if this node has at least one downstream child \
            in the graph — used to skip publishing when nothing would ever consume it.
    '''
    def __init__(self, computation_node : Node, messenger, has_children : bool, ctx = None):
        self._messenger = messenger
        self._computation_node = computation_node
        self._has_children = has_children
        self._ctx = ctx
        self._async_loop = None

    @property
    def computation_node(self):
        '''
        Returns the current computation node
        '''
        return self._computation_node

    def _assert_messenger(self):
        assert self._messenger is not None, 'Task cannot run if messenger has not been set.'

    def _ctx_kwarg(self, method):
        '''Returns 'ctx'/'context' if the method declares that parameter, else None.'''
        try:
            params = inspect.signature(method).parameters
        except (TypeError, ValueError):
            return None
        if 'ctx' in params:
            return 'ctx'
        if 'context' in params:
            return 'context'
        return None

    def _call(self, method, *args):
        '''
        Invoke a node method, passing ``ctx`` only if it declares it, and awaiting
        the result if the method is a coroutine (async ``def``). Async node methods
        run on a task-owned event loop kept off the messenger's I/O loop, so a
        node's async work never blocks broker fetches/acks.
        '''
        kw = self._ctx_kwarg(method) if self._ctx is not None else None
        result = method(*args, **{kw: self._ctx}) if kw else method(*args)
        if inspect.isawaitable(result):
            if self._async_loop is None:
                self._async_loop = asyncio.new_event_loop()
            return self._async_loop.run_until_complete(result)
        return result

    def _run(self):
        raise NotImplementedError('Sublcass needs to implement _run')

    def run(self):
        '''
        Starts the task in an infinite loop.  If this method is called and the \
            ``set_messenger()`` method has not been called yet, an assertion error \
            will happen.
        '''
        self._assert_messenger()
        self._call(self._computation_node.open)
        try:
            self._run()
        finally:
            self._call(self._computation_node.close)
            if self._async_loop is not None:
                self._async_loop.close()


class ProducerTask(NodeTask):
    '''
    It runs forever calling the ``next()`` method in the producer node. \
    At each iteration it checks for a termination signal, and if so it \
    sends a termination message to its child task and breaks the infinite loop.
    '''
    def __init__(self, producer : ProducerNode, messenger, has_children : bool, ctx = None):
        self._producer = producer
        super(ProducerTask, self).__init__(producer, messenger, has_children, ctx)

    def _run(self):
        previous_end_t = time.time()
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    if self._messenger.check_for_termination():
                        break
                    start_t = time.time()
                    a = self._call(self._producer.next)
                    end_t = time.time()
                    proc_time = end_t - start_t
                    actual_proc_time = end_t - previous_end_t
                    previous_end_t = end_t
                    if self._has_children:
                        self._messenger.publish_message(
                            a,
                            {
                                'proctime': proc_time,
                                'actual_proctime': actual_proc_time
                            }
                        )
            except StopIteration:
                break
            except KeyboardInterrupt:
                logger.info('Interrupt signal received. Sending signal to stop flow.')
                break
        if self._has_children:
            self._messenger.publish_stop_signal()

class ProcessorTask(NodeTask):
    '''
    It runs forever, first blocking until it receives a message from every parent \
    node through the messenger. Then it passes the merged inputs to the processor \
    node and, when it gets back the output, uses the messenger to publish it down \
    the flow. If every parent has signaled termination, it passes termination \
    message down the flow and breaks from infinite loop.
    '''
    def __init__(self, processor : ProcessorNode, messenger, has_children : bool, parent_names, ctx = None):
        '''
        - Arguments:
            - parent_names ([str]): names of this node's real parents, in the exact \
                order ``process()`` expects its positional arguments. Passed \
                explicitly rather than read off ``processor.parents`` because a \
                worker process only reconstructs the one node it's responsible for \
                (via ``get_params()``) — it never has the live parent ``Node`` \
                objects the way the single-process local graph-building step does.
        '''
        self._processor = processor
        self._parent_names = list(parent_names)
        super(ProcessorTask, self).__init__(processor, messenger, has_children, ctx)

    @property
    def device_type(self):
        return self._processor.device_type

    def change_device(self, device_type : str):
        self._processor.change_device(device_type)

    def _run(self):
        previous_end_t = time.time()
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs_d = self._messenger.receive_message()
                    # Order matters: process(*inputs) is positional, so entries must
                    # follow the node's own declared parent order, not dict iteration
                    # order (the messenger may assemble the join in arrival order).
                    entries = [inputs_d[name] for name in self._parent_names]
                    if any(e['is_stop_signal'] for e in entries):
                        if self._has_children:
                            self._messenger.publish_stop_signal()
                        break

                    # Process (and publish) then ack. If process/publish raises, the
                    # inputs are failed (redelivered for BATCH up to the retry limit,
                    # then dead-lettered; dropped for REALTIME) and the worker keeps
                    # running — a poison message never crashes the pod.
                    try:
                        inputs = [e['message'] for e in entries]
                        if self._has_children:
                            start_2_t = time.time()
                            output = self._call(self._processor.process, *inputs)
                            end_t = time.time()
                            proc_time = end_t - start_2_t
                            actual_proc_time = end_t - previous_end_t
                            previous_end_t = end_t
                            self._messenger.publish_message(
                                output,
                                {
                                    'proctime': proc_time,
                                    'actual_proctime': actual_proc_time
                                }
                            )
                        else:
                            self._call(self._processor.process, *inputs)
                        self._messenger.ack_inputs()
                    except Exception as e:
                        logger.exception(f'{self._processor} failed to process a message: {e}')
                        self._messenger.fail_inputs(e)
            except KeyboardInterrupt:
                continue

class ConsumerTask(NodeTask):
    '''
    It runs forever, blocking until it receives a message from every parent node \
    through the messenger. It consumes the message and does not publish anything \
    back down the pipe — consumers are the leaves of the graph.
    '''
    def __init__(self, consumer : ConsumerNode, messenger, has_children : bool, parent_names,
                ctx = None, idempotency_store = None):
        self._consumer = consumer
        self._parent_names = list(parent_names)
        # Sink-effect dedup (opt-in via ConsumerNode(idempotent=True) + a store).
        self._idem_store = idempotency_store if getattr(consumer, 'idempotent', False) else None
        super(ConsumerTask, self).__init__(consumer, messenger, has_children, ctx)

    def _run(self):
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs_d = self._messenger.receive_message()
                    entries = [inputs_d[name] for name in self._parent_names]
                    if any(e['is_stop_signal'] for e in entries):
                        break

                    try:
                        # Idempotent sink: if we've already applied this exact input's
                        # effects (a redelivery/restart), skip re-consuming.
                        key = self._messenger.last_input_key() if self._idem_store else None
                        if key is not None and self._idem_store.seen(key):
                            self._messenger.ack_inputs()
                            continue

                        if not self._consumer.metadata:
                            inputs = [e['message'] for e in entries]
                            self._call(self._consumer.consume, *inputs)
                        else:
                            metadatas = [e['metadata'] for e in entries]
                            self._call(self._consumer.consume, *metadatas)

                        if key is not None:
                            self._idem_store.mark(key)
                        self._messenger.ack_inputs()
                    except Exception as e:
                        logger.exception(f'{self._consumer} failed to consume a message: {e}')
                        self._messenger.fail_inputs(e)
            except KeyboardInterrupt:
                continue
