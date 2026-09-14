'''
Conformance cases: RUN-035, RUN-036, RUN-037, RUN-038, RUN-039, RUN-040, RUN-041, RUN-042, RUN-043, RUN-044.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.

RUN-039/043/044 are the component-side GPU contract of plan Phase 4: what a worker
does with its grant before the node opens (``videoflow.runtime.gpucheck``, called by
``videoflow.runtime.worker`` right after the node is rebuilt). The process level runs
it against a fake ``nvidia-smi`` on PATH (``tools/fake_nvidia_smi.py``); the gpu level
against this host's idle test devices with the CUDA-runtime probe of ``_gpu.py`` as
the independent witness. The fused/batching cases (035/036) and the contrib
component cases (037/038/040/041/042) stay pending.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import pathlib
import sys
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_alloc
import pytest
from _gpu import FAKE_SMI, run_probe

from videoflow.backends.allocation import DeliveredGrant, DeviceIdentity
from videoflow.core.constants import GPU
from videoflow.core.errors import ResourceUnavailable
from videoflow.core.node import ProcessorNode
from videoflow.deploy.allocation_local import GRANT_ENV, LocalAllocationBackend
from videoflow.engines.local import _runs_via_docker, allocate_local_gpus
from videoflow.runtime import gpucheck
from videoflow.runtime.gpucheck import verify_grant, verify_node_grant

GIB = 1 << 30


def _fake_host(devices : int) -> List[Dict[str, Any]]:
    return [{'index': i, 'uuid': f'GPU-{i:08x}-fake-4000-8000-000000000000', 'name': 'Fake GPU',
             'memory_total_mib': 98304, 'memory_used_mib': 0} for i in range(devices)]


@pytest.fixture
def fake_smi(tmp_path : pathlib.Path, monkeypatch : pytest.MonkeyPatch) -> Callable[..., None]:
    '''A fake ``nvidia-smi`` first on PATH; ``configure(devices, p2p='0-1', fail=False)`` describes the host.'''
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    shim = bindir / 'nvidia-smi'
    shim.write_text(f'#!/bin/sh\nexec {sys.executable} {FAKE_SMI} "$@"\n')
    shim.chmod(0o755)
    monkeypatch.setenv('PATH', f'{bindir}{os.pathsep}{os.environ.get("PATH", "")}')
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)

    def configure(devices : List[Dict[str, Any]], p2p : str = '', fail : bool = False) -> None:
        monkeypatch.setenv('VF_FAKE_SMI_JSON', json.dumps(devices))
        monkeypatch.setenv('VF_FAKE_SMI_P2P', p2p)
        if fail:
            monkeypatch.setenv('VF_FAKE_SMI_FAIL', '1')
        else:
            monkeypatch.delenv('VF_FAKE_SMI_FAIL', raising = False)
    return configure


class TwoDeviceNode(ProcessorNode):
    '''A component with a hard two-device requirement and no CPU path.'''
    gpu_fallback = 'none'

    def __init__(self, **kwargs : Any) -> None:
        super().__init__(device_type = GPU, gpu_count = 2, **kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        return item


class PeerNode(TwoDeviceNode):
    '''The same, whose execution path needs peer access between its devices.'''
    requires_peer_access = True


class OptionalGpuNode(ProcessorNode):
    '''A component that declares its CPU fallback explicitly (the default contract).'''
    def __init__(self, **kwargs : Any) -> None:
        super().__init__(device_type = GPU, gpu_count = 1, **kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        return item


def _report(requested : int, fallback : str, environ : Dict[str, str], peer : bool = False,
            grant : Optional[DeliveredGrant] = None) -> Dict[str, Any]:
    return verify_grant(requested, fallback, peer, environ, grant = grant).to_dict()


@pytest.mark.case('RUN-035')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_run_035_fused_gpu_execution_groups_preserve_semantics_and_remove() -> None:
    '''
    RUN-035 (P2, component, gpu): Fused GPU execution groups preserve semantics and remove
    internal transfers.

    Acceptance: A supported fused path matches the reference semantics and has zero broker body
    transfers on declared internal edges; unsupported fusion must fail compilation before
    deployment.
    '''


@pytest.mark.case('RUN-036')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_run_036_cross_camera_dynamic_batching_respects_latency_identity() -> None:
    '''
    RUN-036 (P2, component, gpu): Cross-camera dynamic batching respects latency, identity and
    fairness contracts.

    Acceptance: All admitted items are accounted for, maximum batching delay honors D plus
    declared tolerance, and the selected fairness policy holds without an invented throughput
    target.
    '''


@pytest.mark.case('RUN-037')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_run_037_tracktor_model_and_tensors_use_compatible_granted_cuda() -> None:
    '''
    RUN-037 (P1, component, gpu): Tracktor model and tensors use compatible granted CUDA
    devices.

    Acceptance: The full affected inference path executes successfully on the intended granted
    devices for both ordinary and remapped grants.
    '''


@pytest.mark.case('RUN-038')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_run_038_teamclassifier_siglip_executes_on_its_declared_backend() -> None:
    '''
    RUN-038 (P1, component, gpu): TeamClassifier SigLIP executes on its declared backend.

    Acceptance: GPU mode demonstrably executes the affected model on CUDA; the unmet-requirement
    fixture fails before readiness rather than reserving an idle GPU while using CPU.
    '''


# -- RUN-039 ---------------------------------------------------------------------

def _oracle_run_039_process(configure : Callable[..., None], evidence : Dict[str, Any]) -> None:
    '''Every grant form the worker accepts enumerates to the intended identities; invalid and
    empty grants never read as a satisfied multi-device request.'''
    configure(_fake_host(4))
    uuids = [d['uuid'] for d in _fake_host(4)]
    forms = {'ordinals': '3,1', 'uuids': f'{uuids[3]},{uuids[1]}', 'prefixes': 'GPU-00000003,GPU-00000001',
             'unset': None}
    rows = []
    for name, mask in forms.items():
        environ = {} if mask is None else {'CUDA_VISIBLE_DEVICES': mask}
        report = _report(2, 'none', environ)
        rows.append({'form': name, 'mask': mask, 'report': report})
        assert report['source'] == 'enumeration' and report['problems'] == [], report
        expected = [uuids[3], uuids[1]] if mask is not None else uuids
        assert report['delivered'] == expected, (name, report)
    # A launcher-recorded grant is the first word: it names the devices, the mask is its shadow.
    grant = DeliveredGrant('w/0', (DeviceIdentity(None, 3, uuids[3], None, 'Fake GPU', 96 * GIB),
                                   DeviceIdentity(None, 1, uuids[1], None, 'Fake GPU', 96 * GIB)), True, 2, 'strict')
    report = _report(2, 'none', {GRANT_ENV: json.dumps(grant.to_dict()), 'CUDA_VISIBLE_DEVICES': f'{uuids[3]},{uuids[1]}'})
    assert report['source'] == 'grant' and report['delivered'] == [uuids[3], uuids[1]] and report['exclusive'] is True
    rows.append({'form': 'launcher-grant', 'report': report})
    # Empty grant and an invalid token: explicit failures for a hard requirement, never "2 devices".
    for mask in ('', 'GPU-not-a-device', f'{uuids[3]},GPU-not-a-device'):
        with pytest.raises(ResourceUnavailable) as excinfo:
            verify_grant(2, 'none', False, {'CUDA_VISIBLE_DEVICES': mask}, node_name = 'pair')
        rows.append({'form': 'negative', 'mask': mask, 'error': str(excinfo.value)})
        assert 'requires 2 GPU(s)' in str(excinfo.value)
        assert _report(2, 'cpu', {'CUDA_VISIBLE_DEVICES': mask})['delivered'] != [uuids[3], uuids[1]]
    # Discovery failure: unobserved, not zero — and fatal only for a hard requirement.
    configure(_fake_host(4), fail = True)
    report = _report(1, 'cpu', {})
    assert report['source'] == 'unobserved' and report['execution'] == 'unknown'
    with pytest.raises(ResourceUnavailable):
        verify_grant(1, 'none', False, {})
    evidence['forms'] = rows


def _oracle_run_039_gpu(uuids : List[str], evidence : Dict[str, Any]) -> None:
    '''The worker's report and the CUDA runtime agree for ordinal, UUID and empty grants on real devices.'''
    from videoflow.utils.system import host_devices_observed
    host = host_devices_observed().value
    by_uuid = {d.uuid: d for d in host if d.uuid}
    ordinals = [str(by_uuid[u].ordinal) for u in uuids]
    rows = []
    for mask in (','.join(uuids), ','.join(reversed(ordinals)), uuids[0], '', 'GPU-not-a-device'):
        report = _report(len(uuids), 'cpu', {'CUDA_VISIBLE_DEVICES': mask})
        probe = run_probe(mask)
        rows.append({'mask': mask, 'worker': report['delivered'], 'runtime': [d['uuid'] for d in probe.get('devices', [])],
                     'runtime_indices': [d['index'] for d in probe.get('devices', [])]})
        assert report['delivered'] == rows[-1]['runtime'], rows[-1]
        assert rows[-1]['runtime_indices'] == list(range(len(report['delivered'])))
    with pytest.raises(ResourceUnavailable):
        verify_grant(2, 'none', False, {'CUDA_VISIBLE_DEVICES': ''})
    evidence['masks'] = rows


@pytest.mark.case('RUN-039')
@pytest.mark.level('gpu')
def test_run_039_runtime_grant_enumeration_accepts_ordinal_and_uuid_device(gpu, evidence_dir, monkeypatch) -> None:
    '''
    RUN-039 (P1, component, gpu): Runtime grant enumeration accepts ordinal and UUID device
    masks.

    Acceptance: For every supported grant form the reported logical device set equals CUDA
    visibility; invalid or empty grants cannot be misreported as a valid requested multi-device
    allocation.
    '''
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    evidence : Dict[str, Any] = {}
    try:
        _oracle_run_039_gpu(gpu['uuids'], evidence)
    finally:
        (evidence_dir / 'grant_enumeration.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.case('RUN-039')
@pytest.mark.level('process')
@pytest.mark.variant('fake-driver')
def test_run_039_worker_reads_every_grant_form_through_the_driver(fake_smi, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    try:
        _oracle_run_039_process(fake_smi, evidence)
    finally:
        (evidence_dir / 'grant_forms.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'RUN-039')
def test_run_039_detects_integer_only_grants(fake_smi, monkeypatch) -> None:
    defects_alloc.integer_only_masks(monkeypatch)
    assert defects.detects(_oracle_run_039_process, fake_smi, {})


@pytest.mark.case('RUN-040')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_run_040_shared_tensorflow_workers_honor_declared_peak_memory() -> None:
    '''
    RUN-040 (P1, component, gpu): Shared TensorFlow workers honor declared peak-memory
    admission.

    Acceptance: Under strict aggregate admission, the over-budget demand set is rejected before
    concurrent launch; the fitting fixture starts and processes within its declared memory
    envelope. Both cooperative-sharing fixtures explicitly report lack of hard isolation;
    advisory mode never certifies a strict admission guarantee.
    '''


@pytest.mark.case('RUN-041')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_run_041_pose_gpu_environment_selects_one_onnx_runtime_distribution() -> None:
    '''
    RUN-041 (P1, component, gpu): Pose GPU environment selects one ONNX Runtime distribution and
    verifies CUDA execution.

    Acceptance: Each environment has one intended ONNX Runtime distribution; GPU inference shows
    the declared provider behavior and the negative fixture is rejected before readiness.
    '''


@pytest.mark.case('RUN-042')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_run_042_vlm_automatic_device_mapping_reports_actual_use_and() -> None:
    '''
    RUN-042 (P1, component, gpu): VLM automatic device mapping reports actual use and enforces
    offload policy.

    Acceptance: Observed placement obeys the declared offload policy and reports actual resource
    use; no throughput claim is derived solely from gpu_count.
    '''


# -- RUN-043 ---------------------------------------------------------------------

def _oracle_run_043(devices : List[str], with_peer : Optional[str], without_peer : str,
                    evidence : Dict[str, Any]) -> None:
    '''Hard topology requirements are verified before readiness; a count-only component stays usable
    on the weaker topology; a two-device grant without the property cannot satisfy the requirement.'''
    rows = []
    # The count-only control opens on both topologies.
    for mask in filter(None, (with_peer, without_peer)):
        report = verify_node_grant(TwoDeviceNode(name = 'count-only'), {'CUDA_VISIBLE_DEVICES': mask})
        assert report is not None and report.problems == () and report.peer_access is None
        rows.append({'node': 'count-only', 'mask': mask, 'execution': report.execution})
    # The peer-requiring component: refused on the pair without peer access, before any processing.
    with pytest.raises(ResourceUnavailable) as excinfo:
        verify_node_grant(PeerNode(name = 'peer'), {'CUDA_VISIBLE_DEVICES': without_peer})
    assert 'peer access' in str(excinfo.value) and 'has none' in str(excinfo.value)
    rows.append({'node': 'peer', 'mask': without_peer, 'refused': str(excinfo.value)})
    if with_peer is not None:
        report = verify_node_grant(PeerNode(name = 'peer'), {'CUDA_VISIBLE_DEVICES': with_peer})
        assert report is not None and report.peer_access is not None and report.peer_access.value is True
        rows.append({'node': 'peer', 'mask': with_peer, 'peer_access': True})
    # A single device never satisfies a two-device requirement, peer or not.
    with pytest.raises(ResourceUnavailable):
        verify_node_grant(PeerNode(name = 'peer'), {'CUDA_VISIBLE_DEVICES': devices[0]})
    evidence['rows'] = rows


@pytest.mark.case('RUN-043')
@pytest.mark.level('gpu')
def test_run_043_multi_device_workload_validates_topology_rather_than_only(gpu, evidence_dir, monkeypatch) -> None:
    '''
    RUN-043 (P1, component, gpu): Multi-device workload validates topology rather than only a
    device count.

    Acceptance: Compatible fixtures execute the required operation and incompatible fixtures
    fail before processing; no universal NVLink requirement is imposed on components that do not
    need it.

    On this host the test pair has no peer access (``nvidia-smi topo -p2p r`` reports
    NS between every card), which is the incompatible fixture; the CUDA runtime's
    ``cudaDeviceCanAccessPeer`` is the independent witness. A pair with peer access
    (the compatible fixture) exists only where the hardware has it — the process-level
    variant covers that branch with a fake topology.
    '''
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    uuids = gpu['uuids']
    if len(uuids) < 2:
        pytest.skip('not_run: RUN-043 needs two devices in VF_TEST_GPU_UUIDS')
    probe = run_probe(','.join(uuids[:2]))
    assert probe.get('count') == 2, probe
    runtime_peer = probe['peer'].get('0-1') and probe['peer'].get('1-0')
    observed = gpucheck.peer_access_observed(tuple(DeviceIdentity(None, d['ordinal'], d['uuid'], None, d['product'], None)
                                                   for d in gpu['devices'][:2]))
    evidence : Dict[str, Any] = {'runtime_peer': runtime_peer, 'driver_peer': getattr(observed, 'value', 'unknown')}
    assert getattr(observed, 'value', None) == bool(runtime_peer), evidence   # the two witnesses agree
    pair = ','.join(uuids[:2])
    try:
        if runtime_peer:
            _oracle_run_043(uuids, pair, pair, evidence)      # no incompatible pair on this host
            pytest.skip('unsupported: every test pair has peer access here; the incompatible branch ran at process level')
        _oracle_run_043(uuids, None, pair, evidence)
    finally:
        (evidence_dir / 'topology.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('RUN-043')
@pytest.mark.level('process')
@pytest.mark.variant('fake-topology')
def test_run_043_both_topologies_with_a_fake_driver(fake_smi, evidence_dir) -> None:
    fake_smi(_fake_host(4), p2p = '0-1')
    uuids = [d['uuid'] for d in _fake_host(4)]
    evidence : Dict[str, Any] = {}
    _oracle_run_043(uuids, f'{uuids[0]},{uuids[1]}', f'{uuids[2]},{uuids[3]}', evidence)
    (evidence_dir / 'topology.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'RUN-043')
def test_run_043_detects_a_count_only_readiness_check(fake_smi, monkeypatch) -> None:
    fake_smi(_fake_host(4), p2p = '0-1')
    uuids = [d['uuid'] for d in _fake_host(4)]
    defects_alloc.count_only_readiness(monkeypatch)
    assert defects.detects(_oracle_run_043, uuids, f'{uuids[0]},{uuids[1]}', f'{uuids[2]},{uuids[3]}', {})


# -- RUN-044 ---------------------------------------------------------------------

def _oracle_run_044(configure : Callable[..., None], evidence : Dict[str, Any]) -> None:
    '''Hard requirements fail on short/empty delivery; optional fallback is explicit CPU execution; a
    docker-run native gets no grant; wrap-around sharing is labelled non-exclusive.'''
    from videoflow.core.compiler import NodeSpec
    rows : List[Dict[str, Any]] = []

    def gpu_spec(name : str, gpu_count : int = 1, native : bool = False) -> NodeSpec:
        if native:
            return NodeSpec(name, None, {}, [], 'processor', True, 1, 'gpu', True, component_ref = 'oci://vendor/x',
                            descriptor = {'spec': {'runtime': {}}}, gpu_count = gpu_count)
        return NodeSpec(name, 'x.Y', {}, [], 'processor', True, 1, 'gpu', True, gpu_count = gpu_count)
    for visible in (1, 0):
        configure(_fake_host(visible))
        # The launcher: strict refuses the hard two-device node before any launch.
        with pytest.raises(ResourceUnavailable):
            allocate_local_gpus([gpu_spec('pair', 2)], 'f', 'r', LocalAllocationBackend('strict'))
        # The worker: whatever was delivered, the hard requirement is checked at open.
        env = allocate_local_gpus([gpu_spec('pair', 2)], 'f', 'r', LocalAllocationBackend('shared'))
        with pytest.raises(ResourceUnavailable) as excinfo:
            verify_node_grant(TwoDeviceNode(name = 'pair'), env[('pair', 0)])
        rows.append({'visible': visible, 'pair': str(excinfo.value)})
        assert f'delivered {visible}' in str(excinfo.value)
        # The optional node: explicit CPU execution when nothing is delivered, reported, never fatal.
        env = allocate_local_gpus([gpu_spec('opt')], 'f', 'r', LocalAllocationBackend('shared'))
        report = verify_node_grant(OptionalGpuNode(name = 'opt'), env[('opt', 0)])
        assert report is not None
        rows.append({'visible': visible, 'opt': report.to_dict()})
        assert report.execution == ('gpu' if visible else 'cpu')
        assert report.exclusive is False
    # A docker-run native component receives no grant at all — no mask, no count, no promise.
    configure(_fake_host(2))
    native = gpu_spec('native', native = True)
    assert _runs_via_docker(native)
    env = allocate_local_gpus([native, gpu_spec('py')], 'f', 'r', LocalAllocationBackend('strict'))
    assert ('native', 0) not in env and ('py', 0) in env
    # Oversubscribed cooperative sharing, explicitly requested: labelled, never exclusive.
    configure(_fake_host(1))
    env = allocate_local_gpus([gpu_spec('a'), gpu_spec('b')], 'f', 'r', LocalAllocationBackend('shared'))
    grants = [DeliveredGrant.from_dict(json.loads(env[k][GRANT_ENV])) for k in sorted(env)]
    assert all(g.exclusive is False and g.policy == 'shared' for g in grants)
    assert grants[0].devices == grants[1].devices
    for k in sorted(env):
        report = verify_node_grant(OptionalGpuNode(name = k[0]), env[k])
        assert report is not None and report.exclusive is False
    evidence['rows'] = rows
    evidence['shared_grants'] = [g.to_dict() for g in grants]


@pytest.mark.case('RUN-044')
@pytest.mark.level('process')
def test_run_044_local_undersupply_and_fallback_preserve_the_component(fake_smi, evidence_dir) -> None:
    '''
    RUN-044 (P1, integration, process): Local undersupply and fallback preserve the component
    resource contract.

    Acceptance: No worker is declared ready under an unmet hard requirement; optional fallback
    is explicit; local shared grants are reported as nonexclusive.
    '''
    evidence : Dict[str, Any] = {}
    try:
        _oracle_run_044(fake_smi, evidence)
    finally:
        (evidence_dir / 'delivered_vs_requested.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-044')
def test_run_044_detects_requested_counts_presented_as_delivered(fake_smi, monkeypatch) -> None:
    defects_alloc.wraparound_strict(monkeypatch)
    assert defects.detects(_oracle_run_044, fake_smi, {})
