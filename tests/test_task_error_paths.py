'''
The task loop's error behaviour, driven against a recording messenger.

There was no unit coverage of this file at all before: every assertion about what
the run loop does with a failure needed a live broker, which is why the
interesting properties (ack strictly after publish, a bad message never crashing
the pod, a dead producer telling its children) were only ever verified by
watching a real flow.

Pure/unit: no NATS, no subprocesses.
'''
from __future__ import absolute_import, division, print_function

import pytest
from support_errors import ERROR_KINDS
from support_messenger import RecordingMessenger

from videoflow.consumers import CommandlineConsumer
from videoflow.core.errors import (
    POISON,
    TRANSIENT,
    WORKER_FATAL,
    DeviceError,
    SchemaError,
    UpstreamAborted,
    WorkerUnhealthy,
)
from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode
from videoflow.core.supervision import ConsecutiveFailureBreaker
from videoflow.core.task import ConsumerTask, ProcessorTask, ProducerTask, invoke_node


class _Doubler(ProcessorNode):
    def process(self, item):               # type: ignore[override]
        return item * 2


class _Raiser(ProcessorNode):
    def __init__(self, kind = 'untyped', **kwargs):
        self._kind = kind
        super().__init__(**kwargs)

    def process(self, item):               # type: ignore[override]
        raise ERROR_KINDS[self._kind](item)


class _Counter(ProducerNode):
    def __init__(self, limit = 3, raise_at = None, **kwargs):
        self._limit = limit
        self._raise_at = raise_at
        self._n = 0
        super().__init__(**kwargs)

    def next(self):                        # type: ignore[override]
        self._n += 1
        if self._raise_at is not None and self._n == self._raise_at:
            raise DeviceError('the camera fell over')
        if self._n > self._limit:
            raise StopIteration()
        return self._n


class _Collector(ConsumerNode):
    def __init__(self, **kwargs):
        self.seen = []
        super().__init__(**kwargs)

    def consume(self, item):               # type: ignore[override]
        self.seen.append(item)


# -- invoke_node -------------------------------------------------------------

def test_invoke_node_classifies_and_enriches():
    def boom(item):
        raise ConnectionError('db down')

    with pytest.raises(Exception) as exc:
        invoke_node(boom, 1, node = 'sink', replica = 2, trace_id = 't7', seq = 9)
    assert exc.value.disposition == TRANSIENT
    assert exc.value.context == {'node': 'sink', 'replica': 2, 'trace_id': 't7', 'seq': 9}


def test_invoke_node_passes_ctx_only_to_methods_that_declare_it():
    seen = {}

    def with_ctx(item, ctx = None):
        seen['ctx'] = ctx
        return item

    def with_context(item, context = None):
        seen['context'] = context
        return item

    def without(item):
        return item

    sentinel = object()
    invoke_node(with_ctx, 1, node = 'n', ctx = sentinel)
    invoke_node(with_context, 1, node = 'n', ctx = sentinel)
    invoke_node(without, 1, node = 'n', ctx = sentinel)      # must not raise
    assert seen == {'ctx': sentinel, 'context': sentinel}


def test_invoke_node_awaits_async_methods():
    async def slow(item):
        return item + 1

    ran = []
    def run_async(awaitable):
        ran.append(True)
        import asyncio
        return asyncio.new_event_loop().run_until_complete(awaitable)

    assert invoke_node(slow, 1, node = 'n', run_async = run_async) == 2
    assert ran == [True]


def test_invoke_node_lets_control_flow_through_untouched():
    '''
    KeyboardInterrupt and SystemExit are not message failures, and StopIteration
    is a producer's normal end of stream. Wrapping any of them would turn correct
    control flow into a spurious dead letter.
    '''
    for control in (KeyboardInterrupt, SystemExit, StopIteration):
        def raiser(item, exc = control):
            raise exc()
        with pytest.raises(control):
            invoke_node(raiser, 1, node = 'n')


def test_invoke_node_honours_the_per_node_default():
    def boom(item):
        raise ValueError('bad row')

    with pytest.raises(Exception) as exc:
        invoke_node(boom, 1, node = 'n', on_error = POISON)
    assert exc.value.disposition == POISON


# -- processor -----------------------------------------------------------------

def test_success_publishes_then_acks_in_that_order():
    '''
    Ack-after-process (DELIV-1) is the guarantee that makes a crash safe. The
    order is the assertion; a passing count would not catch an inversion.
    '''
    messenger = RecordingMessenger([RecordingMessenger.data(21)])
    ProcessorTask(_Doubler(name = 'd'), messenger, True, ['p']).run()
    assert messenger.call_names() == ['receive', 'publish', 'ack', 'receive', 'stop']
    assert messenger.published[0][0] == 42


@pytest.mark.parametrize('kind, disposition', [
    ('poison', POISON), ('transient', TRANSIENT), ('untyped', TRANSIENT),
])
def test_a_failing_message_is_failed_not_crashed(kind, disposition):
    '''A bad message must never take down the pod — the oldest rule here.'''
    messenger = RecordingMessenger([RecordingMessenger.data(1)])
    ProcessorTask(_Raiser(kind, name = 'r'), messenger, True, ['p']).run()
    assert len(messenger.failures) == 1
    assert messenger.failures[0].disposition == disposition
    assert messenger.acks == 0
    # ...and the loop went on to terminate normally rather than dying on it.
    assert messenger.call_names()[-1] == 'stop'


def test_a_worker_fatal_error_ends_the_worker_after_handing_the_message_back():
    '''
    The other half of the same rule. ``worker_fatal`` is the node asserting that
    nothing it is given will succeed, so the message goes back to the broker for a
    healthy replica and this worker stops — rather than NAKing the rest of the
    stream one message at a time on the way to the same conclusion.
    '''
    messenger = RecordingMessenger([RecordingMessenger.data(i) for i in range(5)])
    task = ProcessorTask(_Raiser('worker_fatal', name = 'r'), messenger, True, ['p'])
    with pytest.raises(DeviceError):
        task.run()
    assert len(messenger.failures) == 1          # exactly one, then out
    assert messenger.failures[0].disposition == WORKER_FATAL
    assert messenger.acks == 0                   # the message was handed back
    # It does NOT tell its children the stream is over: a sick worker is
    # replaceable, and announcing this death would kill them while the
    # replacement was still starting. The supervisor speaks if it gives up.
    assert messenger.aborts == []


def test_the_error_handed_to_the_messenger_names_the_node():
    messenger = RecordingMessenger([RecordingMessenger.data(1)])
    ProcessorTask(_Raiser('poison', name = 'detector'), messenger, True, ['p']).run()
    assert messenger.failures[0].context['node'] == 'detector'


def test_a_processor_with_no_children_does_not_publish():
    messenger = RecordingMessenger([RecordingMessenger.data(3)])
    ProcessorTask(_Doubler(name = 'd'), messenger, False, ['p']).run()
    assert messenger.published == []
    assert messenger.acks == 1


def test_the_breaker_stops_a_worker_whose_failures_never_say_so():
    '''
    The unlabelled version of the same illness: a library error nothing
    classifies, arriving on every message. Nothing declares the worker sick, so
    only the density of the failures reveals it.
    '''
    messenger = RecordingMessenger([RecordingMessenger.data(i) for i in range(10)])
    task = ProcessorTask(_Raiser('untyped', name = 'r'), messenger, True, ['p'],
                        breaker = ConsecutiveFailureBreaker(threshold = 3, node_name = 'r'))
    with pytest.raises(WorkerUnhealthy):
        task.run()
    # Exactly the threshold, then out — not the whole stream shredded one at a time.
    assert len(messenger.failures) == 3
    # Same restraint as an explicit worker-fatal death: replaceable, so silent.
    assert messenger.aborts == []


def test_the_breaker_tolerates_a_stream_with_some_bad_messages():
    messenger = RecordingMessenger(
        [RecordingMessenger.data(v) for v in [1, 'bad', 2, 'bad', 3, 'bad', 4]])

    class _Picky(ProcessorNode):
        def process(self, item):           # type: ignore[override]
            if item == 'bad':
                raise SchemaError('nope')
            return item

    task = ProcessorTask(_Picky(name = 'p'), messenger, True, ['p'],
                        breaker = ConsecutiveFailureBreaker(threshold = 3))
    task.run()                             # must not raise
    assert len(messenger.failures) == 3
    assert messenger.acks == 4


# -- producer ------------------------------------------------------------------

def test_a_producer_ends_cleanly_on_stop_iteration():
    messenger = RecordingMessenger()
    ProducerTask(_Counter(limit = 3, name = 'c'), messenger, True).run()
    assert [m for m, _meta in messenger.published] == [1, 2, 3]
    assert messenger.stop_signals == 1
    assert messenger.aborts == []          # a clean finish is not an abort


def test_a_dying_producer_never_claims_a_clean_finish():
    '''
    The hang this whole mechanism exists for: a producer that raises never reaches
    publish_stop_signal(), so before ABORT its children waited forever.

    What it must *not* do is publish the clean end-of-stream anyway — that would
    tell the graph the run completed successfully. Whether the abort comes from
    the producer or from the supervisor depends on whether the death is
    recoverable; either way, a clean finish is never claimed.
    '''
    messenger = RecordingMessenger()
    task = ProducerTask(_Counter(limit = 10, raise_at = 3, name = 'c'), messenger, True)
    with pytest.raises(DeviceError):
        task.run()
    assert [m for m, _meta in messenger.published] == [1, 2]
    assert messenger.stop_signals == 0


def test_a_death_no_restart_can_fix_is_announced_in_band():
    '''
    The other side of the restraint. A poison-classified death will repeat
    identically in any replacement, so there is nothing to wait for and the
    worker says so immediately — carrying the cause, which the supervisor's
    control-abort could not.
    '''
    class _BadData(ProcessorNode):
        def open(self):
            raise SchemaError('the model file is not a model file')

        def process(self, item):           # type: ignore[override]
            return item

    messenger = RecordingMessenger()
    with pytest.raises(SchemaError):
        ProcessorTask(_BadData(name = 'b'), messenger, True, ['p']).run()
    assert len(messenger.aborts) == 1
    assert messenger.aborts[0]['code'] == 'VF_POISON_SCHEMA'


def test_a_childless_producer_publishes_neither_marker():
    messenger = RecordingMessenger()
    task = ProducerTask(_Counter(limit = 5, raise_at = 2, name = 'c'), messenger, False)
    with pytest.raises(DeviceError):
        task.run()
    assert messenger.aborts == [] and messenger.stop_signals == 0


# -- abort propagation ---------------------------------------------------------

def test_an_aborted_parent_stops_this_node_and_is_relayed():
    messenger = RecordingMessenger([RecordingMessenger.abort('p')], parents = ['p'])
    task = ProcessorTask(_Doubler(name = 'd'), messenger, True, ['p'])
    with pytest.raises(UpstreamAborted) as exc:
        task.run()
    assert 'terminated abnormally' in str(exc.value)
    assert messenger.aborts == [{'code': 'VF_DEVICE', 'message': 'card fell over'}]
    assert messenger.stop_signals == 0     # an abort is not a clean end of stream


def test_a_clean_eos_is_still_a_clean_eos():
    messenger = RecordingMessenger([RecordingMessenger.eos('p')], parents = ['p'])
    ProcessorTask(_Doubler(name = 'd'), messenger, True, ['p']).run()
    assert messenger.stop_signals == 1 and messenger.aborts == []


def test_a_consumer_reports_the_abort_without_relaying_it():
    messenger = RecordingMessenger([RecordingMessenger.abort('p')], parents = ['p'])
    task = ConsumerTask(_Collector(name = 'c'), messenger, False, ['p'])
    with pytest.raises(UpstreamAborted):
        task.run()
    assert messenger.aborts == []          # a leaf has nobody to tell


# -- consumer ------------------------------------------------------------------

def test_a_consumer_acks_what_it_consumed():
    messenger = RecordingMessenger([RecordingMessenger.data(v) for v in (1, 2)])
    sink = _Collector(name = 'c')
    ConsumerTask(sink, messenger, False, ['p']).run()
    assert sink.seen == [1, 2]
    assert messenger.acks == 2


def test_a_failing_consumer_fails_the_input_and_keeps_running():
    class _Sink(ConsumerNode):
        def consume(self, item):           # type: ignore[override]
            raise SchemaError(f'cannot write {item}')

    messenger = RecordingMessenger([RecordingMessenger.data(v) for v in (1, 2)])
    ConsumerTask(_Sink(name = 's'), messenger, False, ['p']).run()
    assert len(messenger.failures) == 2 and messenger.acks == 0


def test_close_runs_even_when_the_loop_raises():
    class _Loud(ProcessorNode):
        closed = False

        def process(self, item):           # type: ignore[override]
            return item

        def close(self):
            type(self).closed = True

    messenger = RecordingMessenger([RecordingMessenger.abort('p')], parents = ['p'])
    with pytest.raises(UpstreamAborted):
        ProcessorTask(_Loud(name = 'l'), messenger, True, ['p']).run()
    assert _Loud.closed is True


def test_a_failing_open_propagates_without_running_the_loop():
    '''
    open() runs before the run loop, so there is no input to fail and nothing to
    retry. A device that could not be acquired may well be acquirable by the
    replacement, so this death is left for the supervisor to announce.
    '''
    class _BadOpen(ProcessorNode):
        def open(self):
            raise DeviceError('no device')

        def process(self, item):           # type: ignore[override]
            return item

    messenger = RecordingMessenger([RecordingMessenger.data(1)])
    with pytest.raises(DeviceError):
        ProcessorTask(_BadOpen(name = 'b'), messenger, True, ['p']).run()
    assert messenger.aborts == []
    assert messenger.call_names() == []    # the loop never started


def test_consumer_node_accepts_the_error_handling_knobs():
    sink = CommandlineConsumer(name = 'out', delivery = 'at-least-once',
                            on_error = 'poison')
    assert sink.delivery_policy() == {'delivery': 'at-least-once', 'on_error': 'poison'}
    # A node that overrides nothing ships nothing, so unchanged flows render
    # exactly the manifests and env they always did.
    assert CommandlineConsumer(name = 'plain').delivery_policy() is None


if __name__ == '__main__':
    pytest.main([__file__])
