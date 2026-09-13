'''
"Unknown is not zero": the reads a decision depends on report their own
failure instead of coercing it into an empty result — the occupancy and
inventory readers in ``deploy.cluster``, the exclusive preflight that reports
on them, the progress deadline's broker probe, and the messenger's EOS drain.
Each test pairs the unreadable case with the genuinely-empty case, because the
whole point is that the two lead to different decisions.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess

import pytest

from videoflow.backends.outcomes import Unknown, is_known, known, unknown
from videoflow.core.engine import Messenger
from videoflow.core.errors import BrokerUnavailable, ProgressStalled
from videoflow.core.supervision import ProgressDeadline
from videoflow.deploy import cluster, gpu
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.runtime.health import InstrumentedMessenger


def _runner(responses, failing = ()):
    '''subprocess.run stand-in: canned stdout by command substring; commands
    matching a ``failing`` needle exit 1 like a forbidden API call.'''
    def run(cmd, **kwargs):
        joined = ' '.join(cmd)
        for needle in failing:
            if needle in joined:
                return subprocess.CompletedProcess(
                    cmd, 1, '', 'Error from server (Forbidden): pods is forbidden: '
                                'User "vf" cannot list resource "pods" at the cluster scope')
        for needle, out in responses.items():
            if needle in joined:
                return subprocess.CompletedProcess(cmd, 0, out, '')
        return subprocess.CompletedProcess(cmd, 0, '', '')
    return run


_POOL = json.dumps({'items': [{
    'metadata': {'name': 'gpu-box',
                 'labels': {'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
                            'nvidia.com/gpu.count': '2', 'nvidia.com/gpu.memory': '81920'}},
    'status': {'allocatable': {'nvidia.com/gpu': '2'}}}]})


# -- cluster reads ---------------------------------------------------------

def test_occupancy_read_reports_its_own_failure(monkeypatch):
    monkeypatch.setattr(subprocess, 'run', _runner({}, failing = ('pods -A',)))
    observed = cluster.gpu_units_in_use_observed()
    assert isinstance(observed, Unknown)
    assert observed.reason == 'failed' and 'Forbidden' in observed.detail
    assert cluster.gpu_units_in_use() == {}            # the display-only wrapper, and only it

    # Exit 0 with no JSON is not a listing either — kubectl never prints that.
    monkeypatch.setattr(subprocess, 'run', _runner({}))
    assert cluster.gpu_units_in_use_observed().reason == 'malformed'

    def missing(cmd, **kwargs):
        raise FileNotFoundError(cmd[0])
    monkeypatch.setattr(subprocess, 'run', missing)
    assert cluster.gpu_units_in_use_observed().reason == 'missing'

    # A real, empty listing is the one reading that means "nothing runs".
    monkeypatch.setattr(subprocess, 'run', _runner({'pods -A': '{"items": []}'}))
    observed = cluster.gpu_units_in_use_observed()
    assert is_known(observed) and observed.value == {}


def test_availability_says_when_free_is_only_an_upper_bound(monkeypatch):
    monkeypatch.setattr(subprocess, 'run',
                        _runner({'gpu-pool=true -o json': _POOL}, failing = ('pods -A',)))
    availability = cluster.gpu_availability()
    assert (availability.allocatable, availability.free) == (2, 2)
    assert availability.occupancy_known is False
    monkeypatch.setattr(subprocess, 'run',
                        _runner({'gpu-pool=true -o json': _POOL, 'pods -A': '{"items": []}'}))
    assert cluster.gpu_availability().occupancy_known is True


def test_inventory_read_distinguishes_unreadable_from_empty(monkeypatch):
    monkeypatch.setattr(subprocess, 'run', _runner({}, failing = ('gpu-pool=true -o json',)))
    assert isinstance(cluster.gpu_inventory_observed(), Unknown)
    assert cluster.gpu_inventory() == []                # display-only wrapper

    # Nodes readable, pods not: the records exist but carry occupancy_known=False.
    monkeypatch.setattr(subprocess, 'run',
                        _runner({'gpu-pool=true -o json': _POOL}, failing = ('pods -A',)))
    observed = cluster.gpu_inventory_observed()
    assert is_known(observed)
    assert [(n.name, n.occupancy_known) for n in observed.value] == [('gpu-box', False)]

    monkeypatch.setattr(subprocess, 'run',
                        _runner({'gpu-pool=true -o json': _POOL, 'pods -A': '{"items": []}'}))
    assert [n.occupancy_known for n in cluster.gpu_inventory_observed().value] == [True]


def test_exclusive_preflight_reports_unknown_occupancy(monkeypatch):
    '''The capacity math still runs — against an assumed-idle pool — but the
    report opens with UNOBSERVABLE_GPU_STATE so nobody reads "2 free" as a fact.'''
    responses = {'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                 'gpu-pool=true -o json': _POOL}
    monkeypatch.setattr(subprocess, 'run', _runner(responses, failing = ('pods -A',)))
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 2})
    assert len(problems) == 1
    assert problems[0].startswith(gpu.UNOBSERVABLE_GPU_STATE)
    assert 'Forbidden' in problems[0] and 'not zero' in problems[0]
    assert not problems[0].startswith(gpu.IMPOSSIBLE_GPU_REQUEST)    # exclusive: promotable, not fatal
    # Demand beyond even the optimistic bound is still reported on top.
    monkeypatch.setattr(subprocess, 'run', _runner(responses, failing = ('pods -A',)))
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 3})
    assert [p.startswith(gpu.UNOBSERVABLE_GPU_STATE) for p in problems] == [True, False]
    assert 'demands 3 x nvidia.com/gpu' in problems[1]
    # With the listing readable the note disappears.
    responses['pods -A'] = '{"items": []}'
    monkeypatch.setattr(subprocess, 'run', _runner(responses))
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 2}) == []


def test_exclusive_preflight_rejects_spanning_an_mps_pool(monkeypatch):
    mps = json.dumps({'items': [{
        'metadata': {'name': 'gpu-box',
                     'labels': {'nvidia.com/gpu.sharing-strategy': 'mps',
                                'nvidia.com/gpu.replicas': '4',
                                'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB-SHARED'}},
        'status': {'allocatable': {'nvidia.com/gpu': '4'}}}]})
    monkeypatch.setattr(subprocess, 'run',
                        _runner({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                                 'gpu-pool=true -o json': mps, 'nodes -o json': mps,
                                 'pods -A': '{"items": []}'}))
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 2},
                                     max_per_pod = {'nvidia.com/gpu': 2})
    assert len(problems) == 1
    assert problems[0].startswith(gpu.IMPOSSIBLE_GPU_REQUEST) and 'MPS' in problems[0]


def test_gpu_resource_predicate():
    assert gpu._is_gpu_resource('nvidia.com/gpu')
    assert gpu._is_gpu_resource('nvidia.com/gpu.shared')
    assert gpu._is_gpu_resource('nvidia.com/mig-1g.10gb')
    assert gpu._is_gpu_resource('amd.com/gpu')
    assert not gpu._is_gpu_resource('hugepages-2Mi')
    assert not gpu._is_gpu_resource('intel.com/sriov_vf')
    assert not gpu._is_gpu_resource('cpu')


# -- progress deadline -----------------------------------------------------

class _Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t


def test_deadline_unknown_neither_resets_nor_trips():
    clock = _Clock()
    state = {'observed': known(3)}
    deadline = ProgressDeadline(10, lambda: state['observed'], 'n', clock.now)
    clock.t = 5
    deadline.check()                                   # silent for 5 < 10: not consulted
    state['observed'] = unknown('timeout', 'consumer_info timed out')
    clock.t = 11
    deadline.check()                                   # Unknown: the node is not blamed...
    state['observed'] = known(3)
    clock.t = 12
    with pytest.raises(ProgressStalled):               # ...but the silence window never reset
        deadline.check()


def test_deadline_sustained_unknown_is_the_brokers_fault():
    clock = _Clock()
    deadline = ProgressDeadline(10, lambda: unknown('auth', 'permissions denied'), 'n', clock.now)
    clock.t = 10
    deadline.check()                                   # first Unknown: grace starts
    clock.t = 29
    deadline.check()                                   # 19 s < 2 x timeout
    clock.t = 30
    with pytest.raises(BrokerUnavailable, match = 'not the same as empty') as excinfo:
        deadline.check()
    assert excinfo.value.context['unknown_seconds'] == 20.0
    assert not isinstance(excinfo.value, ProgressStalled)


def test_deadline_known_after_unknown_resumes_normal_accounting():
    clock = _Clock()
    state = {'observed': unknown('timeout', 'x')}
    deadline = ProgressDeadline(10, lambda: state['observed'], 'n', clock.now,
                                unknown_grace_seconds = 100)
    clock.t = 10
    deadline.check()
    state['observed'] = known(0)                       # idle, and observably so
    clock.t = 15
    deadline.check()                                   # resets the window
    clock.t = 24
    deadline.check()                                   # 9 s of silence: fine
    state['observed'] = unknown('timeout', 'x')
    clock.t = 200
    deadline.check()                                   # grace restarted at 200, not at 10


def test_deadline_still_accepts_a_plain_count():
    clock = _Clock()
    deadline = ProgressDeadline(10, lambda: 2, 'n', clock.now)
    clock.t = 10
    with pytest.raises(ProgressStalled):
        deadline.check()


# -- messenger -------------------------------------------------------------

class _NoPending:
    def has_pending_from(self, parent):
        return False


class _NothingPrefetched:
    '''The backend surface the drain reads besides the observation: nothing parked locally.'''
    def prefetched(self, subscription):
        return 0


def _bare_messenger():
    m = NATSMessenger.__new__(NATSMessenger)
    m._stopped_parents = set()
    m._eos_seen = {'p'}
    m._parent_names = ['p']
    m._backend = _NothingPrefetched()
    m._data_subs = {'p': 'sub-p', 'q': 'sub-q'}
    m._quiescent_since = {'p': 0.0}
    m._eos_quiescence_s = 0.0
    m._assembler = _NoPending()
    return m


def test_drain_never_completes_on_an_unobserved_durable():
    m = _bare_messenger()
    acked = []
    m._ack_eos = acked.append
    m._consumer_pending = lambda parent: unknown('timeout', 'consumer_info timed out')
    assert m._is_parent_stopped('p') is False
    assert 'p' not in m._quiescent_since                # the quiescence clock restarts
    assert isinstance(m.pending_observation(), Unknown)
    assert m.pending_count() == 0                       # legacy count: local queue only
    assert acked == []
    m._consumer_pending = lambda parent: known((0, 0))
    assert m._is_parent_stopped('p') is False           # first quiescent reading starts the clock
    assert m._is_parent_stopped('p') is True            # second (quiescence 0 s) completes
    assert acked == ['p']
    assert m.pending_observation().value == 0


def test_pending_observation_is_unknown_if_any_parent_is():
    m = _bare_messenger()
    m._parent_names = ['p', 'q']
    m._consumer_pending = lambda parent: known((2, 1)) if parent == 'p' else unknown('api', 'x')
    assert isinstance(m.pending_observation(), Unknown)
    m._consumer_pending = lambda parent: known((2, 1))
    assert m.pending_observation().value == 6
    assert m.pending_count() == 6


def test_instrumented_messenger_delegates_observations():
    class _Inner(Messenger):
        quiesced = False

        def pending_count(self):
            return 4

        def quiesce(self):
            self.quiesced = True

    inner = _Inner()
    wrapped = InstrumentedMessenger(inner, object())
    assert wrapped.pending_observation().value == 4
    wrapped.quiesce()
    assert inner.quiesced
