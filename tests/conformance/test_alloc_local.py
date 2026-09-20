'''
Conformance cases: ALLOC-014, ALLOC-015, ALLOC-016.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects_alloc.py``).

The process level runs the real launcher path (``engines.local.allocate_local_gpus``
over ``LocalAllocationBackend``) against a fake ``nvidia-smi`` on PATH
(``tools/fake_nvidia_smi.py``), and enumerates the grant *from a child process*
through ``videoflow.utils.system`` — the environment alone proves nothing. The gpu
level repeats it on this host's idle test devices (``VF_TEST_GPU_UUIDS``) with an
independent CUDA-runtime probe (``_gpu.py``) as the second opinion.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_alloc
import pytest
from _gpu import measured_memory, run_probe, start_holder, wait_for_processes

from videoflow.backends.allocation import SHARING_COOPERATIVE, DeliveredGrant, WorkloadRequest
from videoflow.backends.outcomes import Unknown, known
from videoflow.core.compiler import NodeSpec
from videoflow.core.errors import ResourceUnavailable
from videoflow.deploy.allocation_local import GRANT_ENV, LocalAllocationBackend
from videoflow.engines.local import allocate_local_gpus
from videoflow.utils.system import apply_mask, mask_entries

GIB = 1 << 30
#: What a worker sees of its grant, from a fresh interpreter: one JSON line per
#: environment handed on stdin (a list of dicts), each applied over the parent's.
ENUMERATE = ('import json, os, sys\n'
             'from videoflow.utils.system import visible_devices, granted_gpus\n'
             'from videoflow.deploy.allocation_local import grant_from_env\n'
             'for env in json.load(sys.stdin):\n'
             '    os.environ.pop("CUDA_VISIBLE_DEVICES", None)\n'
             '    os.environ.update(env)\n'
             '    g = grant_from_env()\n'
             '    print(json.dumps({"mask": os.environ.get("CUDA_VISIBLE_DEVICES"), '
             '"visible": [d.mig_uuid or d.uuid for d in visible_devices()], "indices": granted_gpus(), '
             '"grant": g.to_dict() if g else None}))\n')


def _fake_host(devices : int, memory_mib : int = 98304) -> List[Dict[str, Any]]:
    return [{'index': i, 'uuid': f'GPU-{i:08x}-fake-4000-8000-000000000000', 'name': 'Fake GPU',
             'memory_total_mib': memory_mib, 'memory_used_mib': 0} for i in range(devices)]


def _spec(name : str, nb_tasks : int = 1, gpu_count : int = 1, gpu_memory_gib : Optional[float] = None) -> NodeSpec:
    return NodeSpec(name, 'videoflow.processors.basic.IdentityProcessor', {}, [], 'processor', True, nb_tasks, 'gpu',
                    True, gpu_count = gpu_count, gpu_memory_gib = gpu_memory_gib)


def enumerate_in_children(envs : List[Dict[str, str]]) -> List[Dict[str, Any]]:
    '''
    What workers launched with each of ``envs`` see — a fresh interpreter, never
    this process's view. One interpreter serves every environment in turn: the
    grant is read from the environment on each pass, and the interpreter start
    (the videoflow import, mostly) is what the four launches of ALLOC-014 paid for.
    '''
    full = dict(os.environ)
    full.pop('CUDA_VISIBLE_DEVICES', None)
    out = subprocess.run([sys.executable, '-c', ENUMERATE], input = json.dumps(envs), capture_output = True,
                         text = True, env = full, timeout = 60, check = True)
    lines = out.stdout.strip().splitlines()[-len(envs):]
    return [json.loads(line) for line in lines]


def enumerate_in_child(env : Dict[str, str]) -> Dict[str, Any]:
    return enumerate_in_children([env])[0]


def _grant(env : Dict[str, str]) -> DeliveredGrant:
    return DeliveredGrant.from_dict(json.loads(env[GRANT_ENV]))


# -- ALLOC-014 ---------------------------------------------------------------------

def _oracle_alloc_014(configure : Callable[..., None], evidence : Dict[str, Any]) -> None:
    '''Strict refuses short grants before launch; shared reports what each child really got; a failed
    discovery is never a zero-GPU verdict.'''
    launched : List[str] = []
    # One GPU, a worker asking for two: strict launches nothing.
    configure(_fake_host(1))
    with pytest.raises(ResourceUnavailable) as excinfo:
        allocate_local_gpus([_spec('pair', gpu_count = 2)], 'f', 'r', LocalAllocationBackend('strict'))
    assert 'need 2 distinct device(s); 1 visible' in str(excinfo.value)
    evidence['strict_two_on_one'] = str(excinfo.value)
    # More one-GPU workers than devices: strict refuses too.
    with pytest.raises(ResourceUnavailable):
        allocate_local_gpus([_spec('w', nb_tasks = 3)], 'f', 'r', LocalAllocationBackend('strict'))
    assert not launched
    # Shared (opted in): the same fixtures launch, and every child enumerates exactly its grant.
    env = allocate_local_gpus([_spec('pair', gpu_count = 2), _spec('w', nb_tasks = 3)], 'f', 'r',
                              LocalAllocationBackend('shared'))
    evidence['shared_env'] = {f'{k[0]}/{k[1]}': v for k, v in env.items()}
    children = enumerate_in_children(list(env.values()))
    for (key, worker_env), seen in zip(env.items(), children):
        grant = _grant(worker_env)
        evidence.setdefault('children', {})[f'{key[0]}/{key[1]}'] = seen
        assert seen['visible'] == [d.uuid for d in grant.devices], (key, seen, grant)
        assert seen['indices'] == list(range(len(grant.devices)))
        assert grant.exclusive is False and grant.policy == 'shared'
        assert worker_env['VF_GPU_COUNT'] == str(len(grant.devices))
    pair = _grant(env[('pair', 0)])
    assert pair.requested == 2 and len(pair.devices) == 1      # requested is not presented as delivered
    # An empty visible pool: strict refuses; shared reports zero delivered, not the request.
    configure(_fake_host(0))
    with pytest.raises(ResourceUnavailable):
        allocate_local_gpus([_spec('w')], 'f', 'r', LocalAllocationBackend('strict'))
    env = allocate_local_gpus([_spec('w')], 'f', 'r', LocalAllocationBackend('shared'))
    assert env[('w', 0)]['VF_GPU_COUNT'] == '0' and _grant(env[('w', 0)]).devices == ()
    assert _grant(env[('w', 0)]).host == 'observed'
    # A discovery failure is an unobserved host: strict refuses, shared says so in the grant.
    configure(_fake_host(4), fail = True)
    with pytest.raises(ResourceUnavailable) as excinfo:
        allocate_local_gpus([_spec('w')], 'f', 'r', LocalAllocationBackend('strict'))
    assert 'unobserved host' in str(excinfo.value)
    env = allocate_local_gpus([_spec('w')], 'f', 'r', LocalAllocationBackend('shared'))
    grant = _grant(env[('w', 0)])
    assert grant.host == 'unobserved' and grant.devices == ()
    assert 'VF_GPU_COUNT' not in env[('w', 0)]                  # no count is claimed either way
    evidence['discovery_failure_grant'] = grant.to_dict()


@pytest.mark.case('ALLOC-014')
@pytest.mark.level('process')
def test_alloc_014_make_strict_local_grants_truthful_under_insufficient_gpu(fake_smi, evidence_dir) -> None:
    '''
    ALLOC-014 (P1, allocation, process): Make strict local grants truthful under insufficient
    GPU count.

    Acceptance: Strict mode launches zero under-provisioned GPU workers; degraded mode, when
    selected, reports exactly the grant observed by each child.
    '''
    evidence : Dict[str, Any] = {}
    try:
        _oracle_alloc_014(fake_smi, evidence)
    finally:
        (evidence_dir / 'launcher_records.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-014')
@pytest.mark.level('gpu')
@pytest.mark.variant('real-devices')
def test_alloc_014_strict_grants_on_real_devices_match_the_cuda_runtime(gpu, evidence_dir, monkeypatch) -> None:
    '''On this host's test GPUs: strict grants each worker one device the CUDA runtime confirms; a third worker is refused.'''
    uuids = gpu['uuids']
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', ','.join(uuids))
    backend = LocalAllocationBackend('strict')
    env = allocate_local_gpus([_spec('w', nb_tasks = len(uuids))], 'f', 'r', backend)
    reports = {}
    for key, worker_env in env.items():
        report = run_probe(worker_env['CUDA_VISIBLE_DEVICES'])
        reports[f'{key[0]}/{key[1]}'] = report
        assert report.get('count') == 1 and report['devices'][0]['uuid'] == _grant(worker_env).devices[0].uuid
        assert _grant(worker_env).exclusive is True
    with pytest.raises(ResourceUnavailable):
        allocate_local_gpus([_spec('w', nb_tasks = len(uuids) + 1)], 'f', 'r', LocalAllocationBackend('strict'))
    (evidence_dir / 'probe_reports.json').write_text(json.dumps(reports, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-014')
def test_alloc_014_detects_a_launcher_that_shares_silently(fake_smi, monkeypatch) -> None:
    defects_alloc.wraparound_strict(monkeypatch)
    assert defects.detects(_oracle_alloc_014, fake_smi, {})
    monkeypatch.undo()
    fake_smi(_fake_host(1))
    defects_alloc.discovery_failure_is_zero_gpus(monkeypatch)
    assert defects.detects(_oracle_alloc_014, fake_smi, {})


# -- ALLOC-015 ---------------------------------------------------------------------

#: The runtime's mask rules as observed on this host's driver (R595) and recorded
#: in ``videoflow.utils.system.apply_mask``: (mask, exposed ordinals) over four cards.
MASK_RULES = [
    ('2,9,3', [2]),                        # enumeration stops at the first invalid ordinal
    ('3,GPU-nope,2', [3]),                 # ... and at the first unknown UUID
    ('3,2', [3, 2]),                       # mask order is CUDA order
    ('3,2,3', []),                         # a duplicate invalidates the whole mask
    ('', []),                              # the hide-all idiom
]


def _oracle_alloc_015_model(evidence : Dict[str, Any]) -> None:
    from videoflow.backends.allocation import DeviceIdentity
    host = [DeviceIdentity(None, i, f'GPU-{i:04x}-aaaa', None, 'Card', GIB) for i in range(4)]
    host.append(DeviceIdentity(None, 0, 'GPU-0000-aaaa', 'MIG-0000-1', 'Card', GIB // 4, '1g.24gb'))
    rows = []
    for mask, expected in MASK_RULES:
        got = [d.ordinal for d in apply_mask(host, mask_entries(mask))]
        rows.append({'mask': mask, 'expected': expected, 'adapter': got})
        assert got == expected, rows[-1]
    # UUID grants and MIG grants are grants, in mask order; a unique prefix suffices.
    assert [d.uuid for d in apply_mask(host, mask_entries('GPU-0003,GPU-0001-aaaa'))] == ['GPU-0003-aaaa', 'GPU-0001-aaaa']
    assert [d.mig_uuid for d in apply_mask(host, mask_entries('MIG-0000-1'))] == ['MIG-0000-1']
    assert apply_mask(host, None) == host[:4]                              # unset: every card, never a MIG instance
    evidence['mask_rules'] = rows


def _oracle_alloc_015_gpu(uuids : List[str], evidence : Dict[str, Any]) -> None:
    '''Every supported mask form, resolved by the adapter and by an independent CUDA probe, agree.'''
    from videoflow.utils.system import host_devices_observed
    observed = host_devices_observed()
    assert not isinstance(observed, Unknown), observed
    host = observed.value
    by_uuid = {d.uuid: d for d in host if d.uuid}
    ordinals = [str(by_uuid[u].ordinal) for u in uuids]
    masks = [','.join(uuids), ','.join(reversed(uuids)), ','.join(ordinals), ','.join(reversed(ordinals)),
             uuids[0][:12], '', 'GPU-00000000-not-a-device', f'{uuids[0]},GPU-00000000-not-a-device',
             f'{uuids[0]},{ordinals[-1]}']
    rows = []
    for mask in masks:
        adapter = [d.uuid for d in apply_mask(host, mask_entries(mask))]
        probe = run_probe(mask)
        runtime = [d['uuid'] for d in probe.get('devices', [])]
        rows.append({'mask': mask, 'adapter': adapter, 'runtime': runtime, 'runtime_count': probe.get('count'),
                     'error': probe.get('enumeration_error')})
        assert 'error' not in probe, probe
        assert adapter == runtime, rows[-1]
        assert [d['index'] for d in probe.get('devices', [])] == list(range(len(runtime)))
    # The child-side enumeration through the videoflow API says the same as the runtime.
    child = enumerate_in_child({'CUDA_VISIBLE_DEVICES': ','.join(reversed(uuids))})
    assert child['visible'] == list(reversed(uuids)) and child['indices'] == list(range(len(uuids)))
    evidence.update({'driver': probe.get('driver'), 'runtime': probe.get('runtime'), 'masks': rows, 'child': child})


@pytest.mark.case('ALLOC-015')
@pytest.mark.level('gpu')
def test_alloc_015_enumerate_uuid_and_mig_grants_through_the_actual_cuda(gpu, evidence_dir, monkeypatch) -> None:
    '''
    ALLOC-015 (P1, allocation, gpu): Enumerate UUID and MIG grants through the actual CUDA-
    visible namespace.

    Acceptance: For every supported mask the reported grant exactly matches usable CUDA devices;
    unsupported masks produce an explicit capability result without claiming successful
    isolation.
    '''
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    evidence : Dict[str, Any] = {}
    try:
        _oracle_alloc_015_gpu(gpu['uuids'], evidence)
    finally:
        (evidence_dir / 'mask_enumeration.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.case('ALLOC-015')
@pytest.mark.level('model')
@pytest.mark.variant('mask-rules')
def test_alloc_015_adapter_follows_the_runtimes_mask_rules(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_015_model(evidence)
    (evidence_dir / 'mask_rules.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-015')
def test_alloc_015_detects_integer_only_masks(monkeypatch) -> None:
    defects_alloc.integer_only_masks(monkeypatch)
    from videoflow.utils import system
    assert defects.detects(_oracle_alloc_015_model, {}) or system.apply_mask is not apply_mask


# -- ALLOC-016 ---------------------------------------------------------------------

def _oracle_alloc_016_model(evidence : Dict[str, Any]) -> None:
    '''Strict admission reserves the aggregate declared peaks plus headroom; shared never claims enforcement.'''
    from videoflow.backends.allocation import DeviceIdentity
    card = DeviceIdentity(None, 0, 'GPU-0000', None, 'Card', 96 * GIB)

    def backend(policy : str, used : int = 6 * GIB) -> LocalAllocationBackend:
        return LocalAllocationBackend(policy, headroom_bytes = GIB, host_reader = lambda: known([card]),
                                      used_reader = lambda: known({'GPU-0000': used}), inherit_mask = False)

    def request(name : str, peak_gib : Optional[float]) -> WorkloadRequest:
        return WorkloadRequest('f', 'r', name, 1, SHARING_COOPERATIVE,
                               declared_peak_memory_bytes = int(peak_gib * GIB) if peak_gib is not None else None)
    strict = backend('strict')
    snapshot = strict.inventory({}).value
    fitting = strict.plan([request('a', 40), request('b', 40)], snapshot)      # 80 + 6 used + 1 headroom <= 96
    over = strict.plan([request('a', 40), request('b', 40), request('c', 40)], snapshot)
    missing = strict.plan([request('a', 40), request('b', None)], snapshot)
    assert not hasattr(fitting, 'reasons'), fitting
    assert fitting.notes == (f'headroom {GIB} B per shared device',)             # the reservation is stated
    assert hasattr(over, 'reasons') and 'c: declared peak' in over.reasons[0] and 'headroom' in over.reasons[0]
    assert hasattr(missing, 'reasons') and 'needs a declared peak memory' in missing.reasons[0]
    caps = strict.capabilities({})
    assert caps.memory_enforcement == 'accounting'                             # never 'hardware'
    shared = backend('shared')
    assert shared.capabilities({}).memory_enforcement == 'none'
    admitted = shared.plan([request('a', 40), request('b', 40), request('c', None)], shared.inventory({}).value)
    assert not hasattr(admitted, 'reasons') and any('unaccounted' in n for n in admitted.notes)
    evidence.update({'strict_over_budget': list(over.reasons), 'strict_missing_declaration': list(missing.reasons),
                     'shared_notes': list(admitted.notes), 'enforcement': {'strict': caps.memory_enforcement,
                                                                            'shared': 'none'}})


def _oracle_alloc_016_gpu(uuids : List[str], devices : List[Dict[str, Any]], evidence : Dict[str, Any]) -> None:
    '''Real workers with controlled peaks: the fitting set is admitted and measured within budget; the
    over-budget set is refused before any worker starts.'''
    from videoflow.backends.allocation import DeviceIdentity
    device = devices[0]
    total = int(device['memory_bytes'] or 0)
    assert total > 0
    identity = DeviceIdentity(None, device['ordinal'], device['uuid'], None, device['product'], total)
    hold = 2 * GIB                                             # what each worker really allocates
    declared = hold + 512 * (1 << 20)                          # declared peak: the hold plus the context
    used_now = measured_memory([device['uuid']])
    budget = total - sum(used_now.values())
    fitting = max(1, min(4, budget // (declared + GIB) - 1))
    backend = LocalAllocationBackend('strict', headroom_bytes = GIB, mask = device['uuid'])
    snapshot = backend.inventory({}).value
    requests = [WorkloadRequest('f', 'r', f'w{i}', 1, SHARING_COOPERATIVE, declared_peak_memory_bytes = declared)
                for i in range(fitting)]
    plan = backend.plan(requests, snapshot)
    assert not hasattr(plan, 'reasons'), plan
    too_many = int(budget // declared) + 2
    over = backend.plan([WorkloadRequest('f', 'r', f'w{i}', 1, SHARING_COOPERATIVE, declared_peak_memory_bytes = declared)
                         for i in range(too_many)], snapshot)
    assert hasattr(over, 'reasons'), over
    claim = backend.reserve(plan, 'f:r', plan.snapshot_generation)
    holders = []
    try:
        for req in requests:
            env = backend.bindings(claim.claim_id, req.workload_id).env
            holders.append(start_holder(env['CUDA_VISIBLE_DEVICES'], hold, 20.0))
        seen = wait_for_processes([device['uuid']], len(holders), timeout = 60)
        measured = measured_memory([device['uuid']])
        pids = {h.pid for h in holders}
        mine = {pid: b for pid, b in measured.items() if pid in pids}
        evidence.update({'fitting_workers': fitting, 'declared_peak_bytes': declared, 'held_bytes': hold,
                         'measured_by_pid': mine, 'processes_seen': seen, 'over_budget_reasons': list(over.reasons)})
        assert len(mine) == len(holders), (mine, seen)
        for pid, measured_bytes in mine.items():
            assert hold <= measured_bytes <= declared + GIB, (pid, measured_bytes)   # within the admitted budget
        assert sum(mine.values()) <= fitting * (declared + GIB)
    finally:
        for h in holders:
            h.terminate()
        for h in holders:
            try:
                h.wait(timeout = 30)
            except subprocess.TimeoutExpired:
                h.kill()
        time.sleep(1.0)
    assert identity.uuid == device['uuid']


@pytest.mark.case('ALLOC-016')
@pytest.mark.level('gpu')
def test_alloc_016_admit_shared_local_workers_using_peak_memory_and_declared(gpu, evidence_dir, monkeypatch) -> None:
    '''
    ALLOC-016 (P1, allocation, gpu): Admit shared local workers using peak memory and declared
    guarantees.

    Acceptance: The over-budget set is rejected in strict mode before concurrent launch; the
    fitting set runs through measured peak phases within the admitted budget.
    '''
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    evidence : Dict[str, Any] = {}
    try:
        _oracle_alloc_016_gpu(gpu['uuids'], gpu['devices'], evidence)
    finally:
        (evidence_dir / 'admission_ledger.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-016')
@pytest.mark.level('model')
@pytest.mark.variant('backend')
def test_alloc_016_strict_admission_is_a_budget_and_shared_promises_nothing(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_016_model(evidence)
    (evidence_dir / 'admission_model.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-016')
def test_alloc_016_detects_budget_blind_sharing(monkeypatch) -> None:
    defects_alloc.budget_blind_sharing(monkeypatch)
    assert defects.detects(_oracle_alloc_016_model, {})
