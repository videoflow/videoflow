'''
Conformance cases: ALLOC-032.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it.

A profile is *qualified* only against the workload's own thresholds (``bench``
gate): a memory-only fit is a measurement, never an adequacy claim. The
workload is the CUDA probe's transfer-bound loop (``_gpu_probe.py workload``)
run inside each candidate MIG instance on one idle host card; the instance
inventory (``nvidia-smi mig -lgi``) and the probe's own device name are the
independent witnesses that the profile under test is what ran.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import subprocess
from typing import Any, Dict, List

import pytest
from _bench import Verdict, environment_manifest
from _status import not_run


def _mig_uuids(index : int) -> List[str]:
    '''The MIG device UUIDs ``nvidia-smi -L`` lists under GPU ``index``.'''
    out = subprocess.check_output(['nvidia-smi', '-L'], text = True, timeout = 20)
    uuids : List[str] = []
    current = None
    for line in out.splitlines():
        if line.startswith('GPU '):
            current = int(line.split()[1].rstrip(':'))
        elif current == index and 'MIG' in line and 'UUID: ' in line:
            uuids.append(line.split('UUID: ')[1].rstrip(')').strip())
    return uuids


def _qualify(profile : str, workload : Dict[str, Any], thresholds : Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    '''
    The comparison: a profile is qualified only when *every* supplied threshold
    holds over the trial — delivered rate, latency and peak memory — never
    because its memory fits the budget.
    '''
    return {
        'delivered_rate_hz': {'value': workload['delivered_rate_hz'],
                              'holds': workload['delivered_rate_hz'] >= float(thresholds['min_delivered_rate_hz']),
                              'threshold': thresholds['min_delivered_rate_hz']},
        'p95_latency_ms': {'value': workload['latency_ms']['p95'],
                           'holds': workload['latency_ms']['p95'] <= float(thresholds['max_p95_latency_ms']),
                           'threshold': thresholds['max_p95_latency_ms']},
        'peak_memory_bytes': {'value': workload['peak_memory_bytes'],
                              'holds': workload['peak_memory_bytes'] <= int(thresholds['max_peak_memory_bytes']),
                              'threshold': thresholds['max_peak_memory_bytes']},
    }


def _qualified(checks : Dict[str, Dict[str, Any]]) -> bool:
    return all(c['holds'] for c in checks.values())


@pytest.mark.negative_control(of = 'ALLOC-032')
def test_alloc_032_detects_a_memory_only_qualification(monkeypatch) -> None:
    '''A qualification that deems the smallest memory fit adequate hides a profile that misses the rate it was asked for.'''
    import defects
    import defects_alloc
    thresholds = {'min_delivered_rate_hz': 25, 'max_p95_latency_ms': 20, 'max_peak_memory_bytes': 4_000_000_000}
    starved = {'delivered_rate_hz': 9.0, 'latency_ms': {'p95': 95.0}, 'peak_memory_bytes': 2_100_000_000}
    assert not _qualified(_qualify('1g.24gb', starved, thresholds))
    defects_alloc.memory_only_qualification(monkeypatch)
    assert defects.detects(lambda: _qualified(_qualify('1g.24gb', starved, thresholds)) and pytest.fail('memory fit deemed adequate'))


@pytest.mark.case('ALLOC-032')
@pytest.mark.level('benchmark')
def test_alloc_032_qualify_memory_fitting_mig_profiles_with_measured_workload(bench, gpu, evidence_dir) -> None:
    '''
    ALLOC-032 (P2, allocation, benchmark): Qualify memory-fitting MIG profiles with measured
    workload budgets.

    Acceptance: Qualification requires meeting every supplied threshold over the defined trial;
    absent workload targets produce measurements only, not a scalability guarantee.

    On the idle host card named by ``VF_TEST_MIG_GPU_UUID`` (never GPUs 0/1 of
    this host; MIG mode toggled and restored): every candidate profile the
    thresholds name is instantiated, the workload runs inside it, and the
    profile is qualified only when its delivered rate, latency and peak memory
    meet every supplied threshold. Compute, media-engine and transfer limits are
    what the per-profile comparison exposes; the smallest memory fit is never
    deemed adequate on its own.
    '''
    import _mig
    from _gpu import busy_now, run_probe
    uuid = os.environ.get('VF_TEST_MIG_GPU_UUID', '').strip()
    if not uuid:
        not_run('VF_TEST_MIG_GPU_UUID unset (name one of the VF_TEST_GPU_UUIDS devices a MIG toggle may touch)')
    device = next((d for d in gpu['devices'] if d['uuid'] == uuid), None)
    if device is None:
        not_run(f'VF_TEST_MIG_GPU_UUID={uuid} is not one of the gated devices {gpu["uuids"]}')
    reason = _mig.sudo_available()
    if reason is not None:
        not_run(reason)
    index = int(device['ordinal'])
    holders = _mig.device_holders(index)
    if holders:
        not_run(f'/dev/nvidia{index} is held open by {holders}; MIG instances cannot be created on it')
    workload = bench['workload']
    candidates = [str(p) for p in bench['candidate_profiles']]
    timeline = _mig.Timeline()
    ids = _mig.profiles_query(index, timeline)
    missing = [p for p in candidates if p not in ids]
    if missing:
        not_run(f'the card offers no profile(s) {missing}; it offers {sorted(ids)}')
    evidence : Dict[str, Any] = {'environment': environment_manifest({'thresholds': bench}), 'device': device, 'profiles': {}}
    verdict = Verdict()
    mode_before = _mig.mig_mode(index)
    try:
        _mig.set_mig_mode(index, True, timeline)
        for profile in candidates:
            rc, out = _mig.create_instances(index, [ids[profile]['id']], timeline)
            assert rc == 0, f'could not create {profile}: {out.strip()[-300:]}'
            instances = _mig.instances_query(index, timeline)
            mig_uuids = _mig_uuids(index)
            assert len(mig_uuids) == 1 and [i['name'] for i in instances] == [profile], (instances, mig_uuids)
            report = run_probe(mig_uuids[0], mode = 'workload', hold_bytes = int(workload['input_bytes']),
                               hold_seconds = float(workload['rate_hz']), timeout = float(workload['duration_seconds']) + 120,
                               extra_args = [str(float(workload['duration_seconds'])), str(int(workload.get('resident_bytes', 0)))])
            _mig.destroy_instances(index, timeline)
            record = {'instance': instances[0], 'mig_uuid': mig_uuids[0], 'probe': report}
            evidence['profiles'][profile] = record
            assert 'error' not in report, report
            assert any('MIG' in d['name'] or d['total_memory'] < device['memory_bytes'] for d in report['devices']), report['devices']
            checks = _qualify(profile, report['workload'], bench)
            record['qualified'] = _qualified(checks)
            record['checks'] = checks
            for name, check in checks.items():
                verdict.check(f'{profile}: {name}', check['value'], check['holds'], check['threshold'])
    finally:
        _mig.destroy_instances(index, timeline)
        restored = _mig.set_mig_mode(index, False, timeline)
        evidence['restore'] = {'mode_before': mode_before, 'mode_after': restored}
        evidence['timeline'] = timeline.entries
        evidence['verdict'] = verdict.checks
        (evidence_dir / 'profile_qualification.json').write_text(json.dumps(evidence, indent = 2, default = str))
        leaked = busy_now([uuid])
        assert not leaked, leaked
    qualified = sorted(p for p, r in evidence['profiles'].items() if r.get('qualified'))
    evidence['qualified_profiles'] = qualified
    assert not verdict.failed, verdict.failed
