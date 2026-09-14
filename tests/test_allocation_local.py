'''
The local ``AcceleratorAllocationBackend`` (videoflow/deploy/allocation_local.py),
its wiring into the local engine and the worker-side grant check
(videoflow/runtime/gpucheck.py) — plan Phase 4, ALLOC-014/015/016, RUN-039/043/044.

Pure/unit: the ``nvidia-smi`` reads are injected, so a host with one, two, zero
or unobservable GPUs is a lambda away.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess

import pytest

from videoflow.backends.allocation import (
    CLAIM_FAILED,
    CLAIM_READY,
    RELEASE_STALE,
    SHARING_COOPERATIVE,
    SHARING_EXCLUSIVE,
    DeliveredGrant,
    DeviceIdentity,
    FeasiblePlan,
    Infeasible,
    WorkloadRequest,
)
from videoflow.backends.outcomes import Unknown, known, unknown
from videoflow.core.compiler import NodeSpec
from videoflow.core.errors import ResourceUnavailable
from videoflow.deploy.allocation_local import GRANT_ENV, LocalAllocationBackend, grant_from_env
from videoflow.engines.local import allocate_local_gpus, assign_local_gpus, local_workload_requests
from videoflow.runtime import gpucheck
from videoflow.utils import system

GIB = 1 << 30


def _host(n, memory_gib = 96):
    return [DeviceIdentity(None, i, f'GPU-{i:04x}', None, 'RTX PRO 6000', memory_gib * GIB) for i in range(n)]


def _backend(policy, devices, used = None, headroom = GIB):
    host = (lambda: unknown('failed', 'nvidia-smi exploded')) if devices is None else (lambda: known(list(devices)))
    used_reader = (lambda: unknown('timeout', 'slow')) if used == 'unknown' else (lambda: known(dict(used or {})))
    return LocalAllocationBackend(policy, headroom_bytes = headroom, host_reader = host, used_reader = used_reader,
                                  inherit_mask = False)


def _request(workload, count = 1, peak_gib = None):
    return WorkloadRequest('f', 'r', workload, count,
                           SHARING_COOPERATIVE if peak_gib is not None else SHARING_EXCLUSIVE,
                           declared_peak_memory_bytes = int(peak_gib * GIB) if peak_gib is not None else None)


def _spec(name, n = 1, gpus = 1, mem = None, device = 'gpu'):
    return NodeSpec(name, 'x.Y', {}, [], 'processor', True, n, device, True, gpu_count = gpus, gpu_memory_gib = mem)


# -- the backend ---------------------------------------------------------------

def test_unobservable_host_is_unknown_never_zero_gpus():
    assert isinstance(_backend('strict', None).inventory({}), Unknown)
    assert isinstance(_backend('shared', None).inventory({}), Unknown)


def test_inherited_uuid_mask_narrows_the_pool():
    backend = LocalAllocationBackend('strict', host_reader = lambda: known(_host(4)), used_reader = lambda: known({}),
                                     mask = 'GPU-0003,GPU-0001')
    snapshot = backend.inventory({}).value
    assert [d.uuid for d in snapshot.devices] == ['GPU-0003', 'GPU-0001']       # mask order, as CUDA sees it
    assert LocalAllocationBackend('strict', host_reader = lambda: known(_host(4)), used_reader = lambda: known({}),
                                  mask = '').inventory({}).value.devices == ()   # the hide-all idiom


def test_strict_refuses_short_grants_before_anything_launches():
    backend = _backend('strict', _host(1))
    outcome = backend.plan([_request('a', 2)], backend.inventory({}).value)
    assert isinstance(outcome, Infeasible) and 'need 2 distinct device(s); 1 visible' in outcome.reasons[0]
    outcome = backend.plan([_request('a'), _request('b')], backend.inventory({}).value)
    assert isinstance(outcome, Infeasible)
    fits = backend.plan([_request('a')], backend.inventory({}).value)
    assert isinstance(fits, FeasiblePlan) and [d.uuid for d in fits.assignments['a']] == ['GPU-0000']


def test_shared_wraps_around_and_labels_the_grant_nonexclusive():
    backend = _backend('shared', _host(1))
    snapshot = backend.inventory({}).value
    plan = backend.plan([_request('a', 2), _request('b'), _request('c')], snapshot)
    assert isinstance(plan, FeasiblePlan)
    assert [d.uuid for d in plan.assignments['a']] == ['GPU-0000']                # 2 asked, 1 distinct
    assert any('a: asked for 2 devices, 1 distinct' in n for n in plan.notes)
    assert any('demand 4 exceeds the 1 visible' in n for n in plan.notes)
    claim = backend.reserve(plan, 'f:r', plan.snapshot_generation)
    grant = backend.grant(claim.claim_id, 'a')
    assert (grant.requested, len(grant.devices), grant.exclusive, grant.policy) == (2, 1, False, 'shared')
    env = backend.bindings(claim.claim_id, 'b').env
    assert env['CUDA_VISIBLE_DEVICES'] == 'GPU-0000' and env['VF_GPU_COUNT'] == '1'
    assert DeliveredGrant.from_dict(json.loads(env[GRANT_ENV])).exclusive is False


def test_strict_admits_sharers_within_declared_peaks_plus_headroom():
    # 96 GiB card, 6 GiB in use, 1 GiB headroom: 89 GiB budget → two 40 GiB peaks fit, three do not.
    backend = _backend('strict', _host(1), used = {'GPU-0000': 6 * GIB})
    snapshot = backend.inventory({}).value
    assert snapshot.occupancy == {'GPU-0000': 6 * GIB}
    two = backend.plan([_request('s', peak_gib = 40), _request('t', peak_gib = 40)], snapshot)
    assert isinstance(two, FeasiblePlan) and two.notes == (f'headroom {GIB} B per shared device',)
    three = backend.plan([_request(w, peak_gib = 40) for w in 'stu'], snapshot)
    assert isinstance(three, Infeasible) and 'u: declared peak' in three.reasons[0] and 'headroom' in three.reasons[0]
    undeclared = backend.plan([_request('s', peak_gib = 40), WorkloadRequest('f', 'r', 'v', 1, SHARING_COOPERATIVE)],
                              snapshot)
    assert isinstance(undeclared, Infeasible) and 'needs a declared peak memory' in undeclared.reasons[0]
    # An exclusive grant and the sharers never share a card.
    backend = _backend('strict', _host(2))
    plan = backend.plan([_request('x'), _request('s', peak_gib = 10)], backend.inventory({}).value)
    assert isinstance(plan, FeasiblePlan)
    assert plan.assignments['x'][0].uuid != plan.assignments['s'][0].uuid
    claim = backend.reserve(plan, 'f:r', plan.snapshot_generation)
    assert backend.grant(claim.claim_id, 'x').exclusive is True
    assert backend.grant(claim.claim_id, 's').exclusive is True      # alone on its card under strict


def test_strict_needs_the_memory_read_for_sharers():
    backend = _backend('strict', _host(1), used = 'unknown')
    snapshot = backend.inventory({}).value
    assert snapshot.completeness == 'partial'
    outcome = backend.plan([_request('s', peak_gib = 1)], snapshot)
    assert isinstance(outcome, Infeasible) and 'could not be read' in outcome.reasons[0]


def test_reserve_and_release_are_fenced_on_the_host_generation():
    devices = _host(2)
    backend = _backend('strict', devices)
    plan = backend.plan([_request('a')], backend.inventory({}).value)
    assert isinstance(plan, FeasiblePlan)
    devices.pop()                                                    # a card vanished since the plan
    claim = backend.reserve(plan, 'f:r', plan.snapshot_generation)
    assert claim.status == CLAIM_FAILED and 'changed' in claim.evidence['reason']
    devices.append(_host(2)[1])                                      # back as it was
    claim = backend.reserve(plan, 'f:r', plan.snapshot_generation)
    assert claim.grant is not None
    assert backend.observe(claim.claim_id).value.status == CLAIM_READY
    devices.clear()
    assert backend.observe(claim.claim_id).value.status == CLAIM_FAILED
    assert backend.release(claim.claim_id, 'f:r', 'stale').status == RELEASE_STALE
    assert backend.release(claim.claim_id, 'f:r', claim.desired_generation, keep_workloads = True).remaining == ('a',)
    assert backend.release(claim.claim_id, 'f:r', claim.desired_generation).remaining == ()


# -- the local engine ----------------------------------------------------------

def test_shared_plan_reproduces_todays_ordinal_walk():
    specs = [_spec('a', n = 2, gpus = 2), _spec('b', n = 3), _spec('native', device = 'gpu'), _spec('cpu', device = 'cpu')]
    specs[2] = NodeSpec('native', None, {}, [], 'processor', True, 1, 'gpu', True, component_ref = 'oci://x',
                        descriptor = {'spec': {'runtime': {}}})                  # docker-run: never granted
    for n in (1, 2, 3, 5):
        legacy = assign_local_gpus(specs, list(range(n)))
        backend = _backend('shared', _host(n))
        plan = backend.plan(local_workload_requests(specs, 'f', 'r'), backend.inventory({}).value)
        assert isinstance(plan, FeasiblePlan)
        assert {tuple(k.split('/')): [d.ordinal for d in v] for k, v in plan.assignments.items()} == {
            (name, str(i)): ordinals for (name, i), ordinals in legacy.items()}


def test_allocate_local_gpus_strict_raises_before_launch_and_shared_reports():
    with pytest.raises(ResourceUnavailable) as excinfo:
        allocate_local_gpus([_spec('a', gpus = 2)], 'f', 'r', _backend('strict', _host(1)))
    assert '--gpu-policy strict' in str(excinfo.value) and 'need 2 distinct' in str(excinfo.value)
    with pytest.raises(ResourceUnavailable) as excinfo:
        allocate_local_gpus([_spec('a')], 'f', 'r', _backend('strict', None))
    assert 'unobserved host' in str(excinfo.value)
    env = allocate_local_gpus([_spec('a', gpus = 2), _spec('b')], 'f', 'r', _backend('shared', _host(1)))
    assert env[('a', 0)]['CUDA_VISIBLE_DEVICES'] == 'GPU-0000' and env[('a', 0)]['VF_GPU_COUNT'] == '1'
    grant = DeliveredGrant.from_dict(json.loads(env[('b', 0)][GRANT_ENV]))
    assert (grant.requested, grant.exclusive, grant.host) == (1, False, 'observed')
    # Unobservable host under shared: no mask, and the grant says the host was not observed.
    env = allocate_local_gpus([_spec('a')], 'f', 'r', _backend('shared', None))
    assert set(env[('a', 0)]) == {GRANT_ENV}
    assert DeliveredGrant.from_dict(json.loads(env[('a', 0)][GRANT_ENV])).host == 'unobserved'
    # No GPU at all under shared: today's CPU fallback, reported as zero delivered.
    env = allocate_local_gpus([_spec('a')], 'f', 'r', _backend('shared', []))
    assert env[('a', 0)]['VF_GPU_COUNT'] == '0'
    assert allocate_local_gpus([_spec('cpu', device = 'cpu')], 'f', 'r', _backend('strict', None)) == {}


# -- the worker-side check -----------------------------------------------------

def _enumerate(monkeypatch, devices):
    monkeypatch.setattr(system, 'host_devices_observed',
                        (lambda: unknown('missing', 'no nvidia-smi')) if devices is None else (lambda: known(devices)))
    monkeypatch.setattr(gpucheck, 'host_devices_observed', system.host_devices_observed)


def test_verify_grant_prefers_the_launchers_grant_then_the_enumerated_namespace(monkeypatch):
    _enumerate(monkeypatch, _host(4))
    grant = DeliveredGrant('a/0', tuple(_host(4)[2:]), True, 2, 'strict')
    report = gpucheck.verify_grant(2, environ = {GRANT_ENV: json.dumps(grant.to_dict())})
    assert report.source == 'grant' and report.exclusive and report.problems == ()
    report = gpucheck.verify_grant(2, environ = {'CUDA_VISIBLE_DEVICES': 'GPU-0003,GPU-0001'})
    assert report.source == 'enumeration' and [d.uuid for d in report.delivered] == ['GPU-0003', 'GPU-0001']
    assert report.to_dict()['delivered'] == ['GPU-0003', 'GPU-0001'] and report.execution == 'gpu'


def test_verify_grant_reports_or_refuses_a_short_grant_by_the_nodes_fallback(monkeypatch):
    _enumerate(monkeypatch, _host(1))
    report = gpucheck.verify_grant(2, 'cpu', environ = {})
    assert report.problems == ('requested 2 device(s), delivered 1 (GPU-0000)',) and report.execution == 'gpu'
    with pytest.raises(ResourceUnavailable) as excinfo:
        gpucheck.verify_grant(2, 'none', environ = {}, node_name = 'det')
    assert 'Node det requires 2 GPU(s)' in str(excinfo.value)
    empty = gpucheck.verify_grant(1, 'cpu', environ = {'CUDA_VISIBLE_DEVICES': ''})
    assert empty.execution == 'cpu' and empty.delivered == ()
    with pytest.raises(ResourceUnavailable):
        gpucheck.verify_grant(1, 'none', environ = {'CUDA_VISIBLE_DEVICES': ''})


def test_verify_grant_never_reads_a_failed_enumeration_as_zero_gpus(monkeypatch):
    _enumerate(monkeypatch, None)
    report = gpucheck.verify_grant(1, 'cpu', environ = {})
    assert report.source == 'unobserved' and report.execution == 'unknown'
    with pytest.raises(ResourceUnavailable) as excinfo:
        gpucheck.verify_grant(1, 'none', environ = {})
    assert 'cannot be verified' in str(excinfo.value)
    # A launcher grant made on an unobserved host is no evidence either.
    unobserved = DeliveredGrant('a/0', (), False, 1, 'shared', host = 'unobserved')
    assert gpucheck.verify_grant(1, 'cpu', environ = {GRANT_ENV: json.dumps(unobserved.to_dict())}).source == 'unobserved'


def test_peer_access_is_verified_from_the_p2p_matrix_not_assumed(monkeypatch):
    _enumerate(monkeypatch, _host(4))
    matrix = {'ok': ('\tGPU0\tGPU1\tGPU2\tGPU3\n GPU0\tX\tOK\tNS\tNS\n GPU1\tOK\tX\tNS\tNS\n'
                     ' GPU2\tNS\tNS\tX\tNS\n GPU3\tNS\tNS\tNS\tX\n\nLegend:\n  X    = Self\n  OK   = Status Ok\n')}

    def fake_check_output(cmd, **kwargs):
        assert cmd[:4] == ['nvidia-smi', 'topo', '-p2p', 'r']
        return matrix['ok'].encode()
    monkeypatch.setattr(gpucheck.subprocess, 'check_output', fake_check_output)
    assert gpucheck.parse_p2p_matrix(matrix['ok'])[(0, 1)] == 'OK'
    paired = gpucheck.verify_grant(2, 'none', True, environ = {'CUDA_VISIBLE_DEVICES': 'GPU-0000,GPU-0001'})
    assert paired.peer_access is not None and paired.peer_access.value is True
    with pytest.raises(ResourceUnavailable) as excinfo:
        gpucheck.verify_grant(2, 'none', True, environ = {'CUDA_VISIBLE_DEVICES': 'GPU-0002,GPU-0003'})
    assert 'has none' in str(excinfo.value)
    # Count-only nodes remain usable on the weaker topology.
    assert gpucheck.verify_grant(2, 'none', False, environ = {'CUDA_VISIBLE_DEVICES': 'GPU-0002,GPU-0003'}).problems == ()
    # Unverifiable is a refusal, not a pass.
    monkeypatch.setattr(gpucheck.subprocess, 'check_output',
                        lambda cmd, **kw: (_ for _ in ()).throw(subprocess.TimeoutExpired(cmd, 5)))
    with pytest.raises(ResourceUnavailable) as excinfo:
        gpucheck.verify_grant(2, 'none', True, environ = {'CUDA_VISIBLE_DEVICES': 'GPU-0000,GPU-0001'})
    assert 'could not be verified' in str(excinfo.value)


def test_grant_from_env_round_trips():
    grant = DeliveredGrant('a/0', tuple(_host(2)), True, 2, 'strict')
    assert grant_from_env({GRANT_ENV: json.dumps(grant.to_dict())}) == grant
    assert grant_from_env({}) is None
