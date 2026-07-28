'''
The error taxonomy: the classes, their stable codes, and how an arbitrary
exception is classified into a disposition.

The taxonomy only earns its keep if something branches on it, so what is asserted
here is exactly what other code reads: the ``disposition`` that drives the retry
ladder, the ``code`` that labels metrics and dead letters, and the ``exit_code``
that lets CI tell a bad flow from a bad cluster.
'''
from __future__ import absolute_import, division, print_function

import json

import pytest

from videoflow.core import errors as e


def _all_error_classes():
    '''Every VideoflowError subclass reachable from the module.'''
    return [obj for obj in vars(e).values()
            if isinstance(obj, type) and issubclass(obj, e.VideoflowError)]


def test_every_class_has_a_unique_code():
    # Codes are the join key for metrics and DLQ queries; two classes sharing one
    # would silently merge unrelated failures in a dashboard.
    codes = [cls.code for cls in _all_error_classes()]
    assert len(codes) == len(set(codes)), sorted(codes)


def test_exit_codes_separate_the_three_fault_classes():
    assert e.VideoflowUserError('x').exit_code == e.EXIT_USER
    assert e.GraphError('x').exit_code == e.EXIT_USER
    assert e.BrokerUnavailable('x').exit_code == e.EXIT_ENVIRONMENT
    assert e.ClusterError('x').exit_code == e.EXIT_ENVIRONMENT
    assert e.FlowFailed('x').exit_code == e.EXIT_FLOW_FAILED
    assert e.FlowStalled('x').exit_code == e.EXIT_FLOW_STALLED
    # A stall and a failure are different outcomes and get different codes: one
    # means the flow ran and lost nodes, the other that it could never finish.
    assert e.FlowFailed('x').exit_code != e.FlowStalled('x').exit_code


def test_dispositions_are_carried_by_the_runtime_classes():
    assert e.PoisonMessage('x').disposition == e.POISON
    assert e.DecodeError('x').disposition == e.POISON
    assert e.SchemaError('x').disposition == e.POISON
    assert e.TransientFailure('x').disposition == e.TRANSIENT
    assert e.UpstreamUnavailable('x').disposition == e.TRANSIENT
    assert e.WorkerFatal('x').disposition == e.WORKER_FATAL
    assert e.DeviceError('x').disposition == e.WORKER_FATAL
    assert e.WorkerUnhealthy('x').disposition == e.WORKER_FATAL
    assert e.ProgressStalled('x').disposition == e.WORKER_FATAL


def test_remedy_and_context_stay_out_of_the_message():
    '''
    Structured fields must stay structured: interpolating them into the message
    is what made the old error strings unaggregatable in the first place.
    '''
    err = e.DeviceError('CUDA out of memory', remedy = 'Lower the batch size.',
                        node = 'detector', replica = 2)
    assert err.message == 'CUDA out of memory'
    assert err.remedy == 'Lower the batch size.'
    assert err.context == {'node': 'detector', 'replica': 2}
    # str() still shows the fix, because plenty of renderers only ever call str().
    assert 'Lower the batch size.' in str(err)


def test_to_dict_is_json_safe_even_with_unserializable_context():
    err = e.TransientFailure('boom', node = 'n', payload = object())
    d = err.to_dict()
    json.dumps(d)                                    # must not raise
    assert d['code'] == 'VF_TRANSIENT'
    assert d['disposition'] == e.TRANSIENT
    assert d['context']['node'] == 'n'
    assert d['context']['payload'].startswith('<object')


def test_error_to_dict_normalizes_every_input_shape():
    typed = e.DeviceError('gone', remedy = 'reboot it')
    assert e.error_to_dict(typed)['code'] == 'VF_DEVICE'
    # A bare exception still gets a code, derived from its type.
    assert e.error_to_dict(ValueError('nope'))['code'] == 'VF_VALUEERROR'
    # An already-normalized dict passes through untouched, which is how an ABORT
    # is relayed hop to hop without each node rewriting the origin's account.
    relayed = {'code': 'VF_DEVICE', 'message': 'from further upstream'}
    assert e.error_to_dict(relayed) is relayed


class _CudaOOM(Exception):
    '''Stands in for torch.cuda.OutOfMemoryError — a type we cannot subclass.'''


def test_registry_classifies_types_we_do_not_own(monkeypatch):
    monkeypatch.setattr(e, '_CLASSIFIERS', list(e._CLASSIFIERS))
    assert e.classify(_CudaOOM()) == e.DEFAULT_DISPOSITION       # unknown ⇒ default
    e.register_error_classifier(_CudaOOM, e.WORKER_FATAL)
    assert e.classify(_CudaOOM()) == e.WORKER_FATAL


def test_later_registrations_win(monkeypatch):
    monkeypatch.setattr(e, '_CLASSIFIERS', list(e._CLASSIFIERS))
    e.register_error_classifier(_CudaOOM, e.TRANSIENT)
    e.register_error_classifier(_CudaOOM, e.POISON)
    assert e.classify(_CudaOOM()) == e.POISON


def test_registering_an_unknown_disposition_names_the_known_ones():
    with pytest.raises(ValueError) as exc:
        e.register_error_classifier(_CudaOOM, 'catastrophic')
    for known in e.DISPOSITIONS:
        assert known in str(exc.value)


def test_registering_a_non_exception_is_rejected():
    with pytest.raises(ValueError, match = 'exception class'):
        e.register_error_classifier(int, e.POISON)      # type: ignore[arg-type]


def test_builtin_classifiers_cover_the_unambiguous_stdlib_types():
    assert e.classify(ConnectionError('reset')) == e.TRANSIENT
    assert e.classify(TimeoutError()) == e.TRANSIENT
    assert e.classify(MemoryError()) == e.WORKER_FATAL
    assert e.classify(json.JSONDecodeError('bad', 'doc', 0)) == e.POISON
    # Anything arguable is left alone rather than guessed at.
    assert e.classify(ValueError('who knows')) == e.DEFAULT_DISPOSITION


def test_the_default_is_the_pre_taxonomy_behaviour():
    '''
    Adopting the taxonomy must change nothing until someone opts in: an
    unclassified error keeps being retried and then dead-lettered.
    '''
    assert e.DEFAULT_DISPOSITION == e.TRANSIENT


def test_as_runtime_error_wraps_and_enriches():
    wrapped = e.as_runtime_error(ConnectionError('db down'), node = 'sink', seq = 4)
    assert isinstance(wrapped, e.TransientFailure)
    assert wrapped.disposition == e.TRANSIENT
    assert wrapped.context == {'node': 'sink', 'seq': 4}
    assert isinstance(wrapped.__cause__, ConnectionError)


def test_as_runtime_error_enriches_ours_in_place_instead_of_re_wrapping():
    '''
    Re-wrapping would bury the original code, which is the one thing metrics and
    the DLQ key on.
    '''
    original = e.SchemaError('bad frame')
    wrapped = e.as_runtime_error(original, node = 'detector')
    assert wrapped is original
    assert wrapped.code == 'VF_POISON_SCHEMA'
    assert wrapped.context['node'] == 'detector'


def test_as_runtime_error_honours_the_per_node_default():
    wrapped = e.as_runtime_error(ValueError('bad row'), default = e.POISON)
    assert wrapped.disposition == e.POISON
    assert isinstance(wrapped, e.PoisonMessage)


if __name__ == '__main__':
    pytest.main([__file__])
