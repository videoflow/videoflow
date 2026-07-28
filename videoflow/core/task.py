from __future__ import absolute_import, division, print_function

import asyncio
import inspect
import logging
import time
from typing import TYPE_CHECKING, Any, Callable, List, Optional

from ..utils.generic_utils import DelayedKeyboardInterrupt
from .context import RuntimeContext
from .engine import Messenger
from .errors import (
    DEFAULT_DISPOSITION,
    WORKER_FATAL,
    UpstreamAborted,
    as_runtime_error,
    classify,
)
from .node import ConsumerNode, Node, ProcessorNode, ProducerNode
from .supervision import is_terminal

if TYPE_CHECKING:
    # Type-only: the store is constructed in the worker (videoflow.runtime) and
    # handed down to the task, so a real import here would invert the
    # core <- runtime dependency direction for nothing but an annotation.
    from ..runtime.idempotency import IdempotencyStore
    from .supervision import ConsecutiveFailureBreaker, ProgressDeadline

logger = logging.getLogger(__package__)

def _ctx_kwarg(method : Callable[..., Any]) -> Optional[str]:
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

def invoke_node(method : Callable[..., Any], *args : Any, node : str = '',
                replica : int = 0, trace_id : Optional[str] = None,
                seq : Optional[int] = None, ctx : Optional[RuntimeContext] = None,
                run_async : Optional[Callable[[Any], Any]] = None,
                on_error : str = DEFAULT_DISPOSITION) -> Any:
    '''
    The one seam where user code becomes framework-legible.

    Calls a node method, passing ``ctx`` only if it declares it and awaiting the
    result if it is a coroutine, then converts whatever it raised into a
    classified ``VideoflowRuntimeError`` carrying the message's identity. Without
    this, the task loop's ``except Exception`` catches the error and throws away
    everything structural about it: the retry ladder cannot tell a bad message
    from a sick worker, and the DLQ records free text nothing can aggregate.

    ``BaseException`` — ``KeyboardInterrupt``, ``SystemExit`` — and
    ``StopIteration`` deliberately pass through untouched: the first two are not
    message failures, and the third is a producer's normal end of stream.

    - Arguments:
        - method: the node's ``next``/``process``/``consume``/``open``/``close``.
        - node, replica, trace_id, seq: identity stamped into the error's context.
        - ctx: the ``RuntimeContext``, passed only to methods that declare it.
        - run_async: runs an awaitable to completion. Injected because the event \
            loop belongs to the task, which keeps node coroutines off the \
            messenger's I/O loop so a node's async work never blocks broker \
            fetches and acks.
        - on_error: disposition for an exception nothing classifies.

    - Raises:
        - VideoflowRuntimeError: whatever the node raised, classified and enriched.
    '''
    try:
        kw = _ctx_kwarg(method) if ctx is not None else None
        result = method(*args, **{kw: ctx}) if kw else method(*args)
        if inspect.isawaitable(result):
            if run_async is None:
                raise RuntimeError(
                    f'{node}: {getattr(method, "__name__", method)} is a coroutine '
                    'function but this task has no event loop to run it on.')
            return run_async(result)
        return result
    except StopIteration:
        raise
    except Exception as e:
        raise as_runtime_error(e, default = on_error, node = node, replica = replica,
                            trace_id = trace_id, seq = seq) from e

class Task:
    def run(self) -> None:
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
        - breaker: trips when the worker fails too many messages in a row \
            (see ``videoflow.core.supervision``). Constructed by the worker and \
            handed down, like ``idempotency_store``.
        - deadline: trips when the node stops acking while work is pending.
        - on_error (str): disposition for exceptions nothing classifies.
    '''
    def __init__(self, computation_node : Node, messenger : Messenger, has_children : bool,
                ctx : Optional[RuntimeContext] = None,
                breaker : Optional["ConsecutiveFailureBreaker"] = None,
                deadline : Optional["ProgressDeadline"] = None,
                on_error : str = DEFAULT_DISPOSITION) -> None:
        self._messenger = messenger
        self._computation_node = computation_node
        self._has_children = has_children
        self._ctx = ctx
        self._breaker = breaker
        self._deadline = deadline
        self._on_error = on_error
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def computation_node(self) -> Node:
        '''
        Returns the current computation node
        '''
        return self._computation_node

    def _assert_messenger(self) -> None:
        assert self._messenger is not None, 'Task cannot run if messenger has not been set.'

    def _run_async(self, awaitable : Any) -> Any:
        '''Runs a node coroutine on a task-owned loop, kept off the messenger's I/O loop.'''
        if self._async_loop is None:
            self._async_loop = asyncio.new_event_loop()
        return self._async_loop.run_until_complete(awaitable)

    def _call(self, method : Callable[..., Any], *args : Any,
            trace_id : Optional[str] = None, seq : Optional[int] = None) -> Any:
        '''Invokes a node method through the classification boundary (``invoke_node``).'''
        return invoke_node(
            method, *args,
            node = self._computation_node.name,
            replica = self._ctx.replica_id if self._ctx is not None else 0,
            trace_id = trace_id, seq = seq, ctx = self._ctx,
            run_async = self._run_async, on_error = self._on_error,
        )

    def _abort_downstream(self, error : BaseException) -> None:
        '''
        Tells this node's children that its stream has ended abnormally — but only
        when the death is one no restart can fix.

        The restraint is the important part. A worker that announces every death
        would kill its children while its own replacement was still starting,
        turning a recoverable crash into a flow-wide failure. So a worker speaks
        only for deaths that are final by nature (see
        ``supervision.is_terminal``); every other death is announced by the
        supervisor, once it has actually given up.

        Best-effort even then: a dying worker may not reach the broker at all,
        which is why the supervisor's control-abort and the receiver-side progress
        deadline sit behind this.
        '''
        if not self._has_children:
            return
        if not is_terminal(classify(error, self._on_error)):
            return
        try:
            self._messenger.publish_abort(error)
        except Exception:
            logger.debug('could not publish ABORT marker', exc_info = True)

    def _record_failure(self, exc : BaseException) -> None:
        '''
        Decides whether the worker survives the failure it just reported.

        A ``worker_fatal`` error ends it immediately. That disposition *is* the
        node saying "this worker cannot process anything" — the message has
        already been naked back to the broker for a healthy replica, and staying
        alive would only nak the next nine messages the same way before the
        breaker reached the same conclusion with no more information than it had
        at the first one.

        The breaker is for everything that never says so: a dense run of
        failures the taxonomy could not classify, which is the signature of a
        sick worker whether or not anything labelled it one.
        '''
        if self._deadline is not None:
            self._deadline.record_progress()
        if classify(exc, self._on_error) == WORKER_FATAL:
            raise exc
        if self._breaker is not None:
            self._breaker.record_failure(exc)
            self._breaker.check()

    def _record_success(self) -> None:
        if self._deadline is not None:
            self._deadline.record_progress()
        if self._breaker is not None:
            self._breaker.record_success()

    def _run(self) -> None:
        raise NotImplementedError('Sublcass needs to implement _run')

    def run(self) -> None:
        '''
        Starts the task in an infinite loop.  If this method is called and the \
            ``set_messenger()`` method has not been called yet, an assertion error \
            will happen.

        A failure in ``open()`` or in the run loop publishes an ABORT marker
        before propagating, so the graph downstream of this node terminates
        instead of hanging on an end-of-stream that will never arrive.
        '''
        self._assert_messenger()
        try:
            self._call(self._computation_node.open)
        except BaseException as e:
            # open() failed: close() is deliberately not called (there is no
            # half-open state to release), but downstream must still be told.
            self._abort_downstream(e)
            raise
        try:
            self._run()
        except UpstreamAborted:
            # Already relayed by raise_if_aborted, which forwards the *origin's*
            # error rather than this node's casualty report. Aborting again here
            # would publish a second marker and overwrite the real cause with
            # "someone upstream died", one hop further from the truth each time.
            raise
        except BaseException as e:
            if not isinstance(e, KeyboardInterrupt):
                self._abort_downstream(e)
            raise
        finally:
            try:
                self._call(self._computation_node.close)
            except Exception:
                # A close() failure must not mask the error that got us here.
                logger.exception(f'{self._computation_node} failed to close cleanly')
            if self._async_loop is not None:
                self._async_loop.close()


class ProducerTask(NodeTask):
    '''
    It runs forever calling the ``next()`` method in the producer node. \
    At each iteration it checks for a termination signal, and if so it \
    sends a termination message to its child task and breaks the infinite loop.

    A producer has no inputs, so it has no per-message failure path: an exception
    from ``next()`` is not something that can be naked or dead-lettered. It ends
    the producer, and ``NodeTask.run`` publishes ABORT on the way out so the rest
    of the graph does not wait forever for an end-of-stream.
    '''
    def __init__(self, producer : ProducerNode, messenger : Messenger, has_children : bool,
                ctx : Optional[RuntimeContext] = None,
                breaker : Optional["ConsecutiveFailureBreaker"] = None,
                deadline : Optional["ProgressDeadline"] = None,
                on_error : str = DEFAULT_DISPOSITION) -> None:
        self._producer = producer
        super(ProducerTask, self).__init__(producer, messenger, has_children, ctx,
                                        breaker, deadline, on_error)

    def _run(self) -> None:
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
    def __init__(self, processor : ProcessorNode, messenger : Messenger, has_children : bool,
                parent_names : List[str], ctx : Optional[RuntimeContext] = None,
                breaker : Optional["ConsecutiveFailureBreaker"] = None,
                deadline : Optional["ProgressDeadline"] = None,
                on_error : str = DEFAULT_DISPOSITION) -> None:
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
        super(ProcessorTask, self).__init__(processor, messenger, has_children, ctx,
                                        breaker, deadline, on_error)

    @property
    def device_type(self) -> str:
        return self._processor.device_type

    def change_device(self, device_type : str) -> None:
        self._processor.change_device(device_type)

    def _run(self) -> None:
        previous_end_t = time.time()
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs_d = self._messenger.receive_message()
                    # Order matters: process(*inputs) is positional, so entries must
                    # follow the node's own declared parent order, not dict iteration
                    # order (the messenger may assemble the join in arrival order).
                    entries = [inputs_d[name] for name in self._parent_names]
                    raise_if_aborted(entries, self._messenger, self._has_children)
                    if any(e['is_stop_signal'] for e in entries):
                        if self._has_children:
                            self._messenger.publish_stop_signal()
                        break

                    # Process (and publish) then ack. If process/publish raises, the
                    # inputs are failed — what that costs is decided by the node's
                    # DeliveryPolicy and by how the error classified — and the worker
                    # keeps running: a poison message never crashes the pod.
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
                        self._record_success()
                    except Exception as e:
                        error = as_runtime_error(e, default = self._on_error,
                                                node = self._processor.name)
                        logger.exception(
                            f'{self._processor} failed to process a message '
                            f'[{error.code}/{error.disposition}]: {e}')
                        self._messenger.fail_inputs(error)
                        self._record_failure(error)
                    if self._deadline is not None:
                        self._deadline.check()
            except KeyboardInterrupt:
                continue

class ConsumerTask(NodeTask):
    '''
    It runs forever, blocking until it receives a message from every parent node \
    through the messenger. It consumes the message and does not publish anything \
    back down the pipe — consumers are the leaves of the graph.
    '''
    def __init__(self, consumer : ConsumerNode, messenger : Messenger, has_children : bool,
                parent_names : List[str], ctx : Optional[RuntimeContext] = None,
                idempotency_store : Optional["IdempotencyStore"] = None,
                breaker : Optional["ConsecutiveFailureBreaker"] = None,
                deadline : Optional["ProgressDeadline"] = None,
                on_error : str = DEFAULT_DISPOSITION) -> None:
        self._consumer = consumer
        self._parent_names = list(parent_names)
        # Sink-effect dedup (opt-in via ConsumerNode(idempotent=True) + a store).
        self._idem_store = idempotency_store if consumer.idempotent else None
        super(ConsumerTask, self).__init__(consumer, messenger, has_children, ctx,
                                        breaker, deadline, on_error)

    def _run(self) -> None:
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs_d = self._messenger.receive_message()
                    entries = [inputs_d[name] for name in self._parent_names]
                    raise_if_aborted(entries, self._messenger, self._has_children)
                    if any(e['is_stop_signal'] for e in entries):
                        break

                    try:
                        # Idempotent sink: if we've already applied this exact input's
                        # effects (a redelivery/restart), skip re-consuming.
                        store = self._idem_store
                        key = self._messenger.last_input_key() if store is not None else None
                        if store is not None and key is not None and store.seen(key):
                            self._messenger.ack_inputs()
                            self._record_success()
                            continue

                        if not self._consumer.metadata:
                            inputs = [e['message'] for e in entries]
                            self._call(self._consumer.consume, *inputs)
                        else:
                            metadatas = [e['metadata'] for e in entries]
                            self._call(self._consumer.consume, *metadatas)

                        if store is not None and key is not None:
                            store.mark(key)
                        self._messenger.ack_inputs()
                        self._record_success()
                    except Exception as e:
                        error = as_runtime_error(e, default = self._on_error,
                                                node = self._consumer.name)
                        logger.exception(
                            f'{self._consumer} failed to consume a message '
                            f'[{error.code}/{error.disposition}]: {e}')
                        self._messenger.fail_inputs(error)
                        self._record_failure(error)
                    if self._deadline is not None:
                        self._deadline.check()
            except KeyboardInterrupt:
                continue

def raise_if_aborted(entries : List[dict], messenger : Messenger,
                    has_children : bool) -> None:
    '''
    Ends this node when any parent reports that it terminated abnormally.

    A clean end-of-stream and an abnormal one are different facts, and only one of
    them used to exist on the wire: a producer that died mid-run left its children
    blocking forever on an EOS that was never coming. ABORT is that missing fact,
    and it walks the graph exactly the way EOS does — this node propagates it to
    its own children before stopping.

    - Raises:
        - UpstreamAborted: naming the parent that aborted and carrying its error.
    '''
    for entry in entries:
        if not entry.get('is_abort'):
            continue
        origin = entry.get('abort_origin') or 'an upstream node'
        detail = entry.get('abort_error') or {}
        if has_children:
            try:
                messenger.publish_abort(detail)
            except Exception:
                logger.debug('could not propagate ABORT downstream', exc_info = True)
        raise UpstreamAborted(
            f'{origin} terminated abnormally '
            f'({detail.get("code", "unknown")}: {detail.get("message", "no detail")}), '
            f'so this node stops rather than waiting for an end-of-stream that is '
            f'not coming.',
            remedy = f'Fix the failure reported by {origin}; this node is a casualty, not the cause.',
            origin = origin,
        )
