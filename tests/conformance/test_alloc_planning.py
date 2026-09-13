'''
Conformance cases: ALLOC-001, ALLOC-009, ALLOC-010, ALLOC-023, ALLOC-034.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects.py``). A ``pending`` marker means
the case is a skeleton reporting NOT_RUN until its implementation phase lands.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
import itertools
import json
import random
import subprocess
from typing import Any, Callable, Dict, List

import defects
import pytest
from support_kubectl import fake_run, nodes_json

from videoflow.backends.allocation import (
    ELASTICITY_ELASTIC,
    FEATURE_CONSUMABLE_CAPACITY,
    FEATURE_DYNAMIC_MIG,
    FEATURE_MPS,
    SHARING_EXCLUSIVE,
    SHARING_ISOLATED_MIG,
    FeasiblePlan,
    Infeasible,
    WorkloadRequest,
    allocation_rejections,
)
from videoflow.backends.memory.allocation import (
    AUTHORITY_DEVICE_PLUGIN,
    AUTHORITY_DRA,
    MemoryAllocationBackend,
    NodeFixture,
)
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.mig_geometry import check_layout
from videoflow.core.compiler import NodeSpec
from videoflow.core.errors import CapabilityError, OwnershipConflict
from videoflow.deploy import cluster, gpu
from videoflow.deploy.mig import LayoutError, NodeInventory, solve_layout

A100 = 'NVIDIA-A100-SXM4-80GB'
H100 = 'NVIDIA-H100-80GB-HBM3'


def _gpu_spec(name : str, gpu_count : int = 1, gpu_memory_gib : float | None = None, nb_tasks : int = 1) -> NodeSpec:
    return NodeSpec(name, 'videoflow.processors.basic.IdentityProcessor', {}, [],
                    'processor', True, nb_tasks, 'gpu', True,
                    gpu_count = gpu_count, gpu_memory_gib = gpu_memory_gib)


def _no_cluster(monkeypatch : pytest.MonkeyPatch) -> List[List[str]]:
    '''Planning must be pure: any kubectl call is a mutation attempt the audit records — and a failure.'''
    calls : List[List[str]] = []

    def run(cmd : List[str], **kwargs : Any) -> Any:
        calls.append(list(cmd))
        raise AssertionError(f'planning reached the cluster: {cmd}')
    monkeypatch.setattr(subprocess, 'run', run)
    return calls


# -- ALLOC-001 ---------------------------------------------------------------------

def _oracle_alloc_001(solver : Callable[..., Any], evidence : Dict[str, Any]) -> None:
    # The independent fixture (vendor-documented geometry, kept apart from the
    # solver's own tables) rejects the regression by memory, not by slice count:
    # 4 x 1g.20gb + 3 x 1g.10gb is 7 compute slices on a 7-slice card but 110 GB
    # of 80.
    direct = check_layout(H100, {'1g.20gb': 4, '1g.10gb': 3})
    assert any('memory' in p for p in direct), direct
    assert not any('usable' in p.lower() for p in direct)          # geometry, not a byte-count quibble
    evidence['direct_fixture'] = {'layout': {'1g.20gb': 4, '1g.10gb': 3}, 'problems': direct}

    demand = [_gpu_spec('twenty', gpu_memory_gib = 20, nb_tasks = 4),
              _gpu_spec('ten', gpu_memory_gib = 10, nb_tasks = 3)]
    one_card = [NodeInventory('h100', H100, 1, 80)]
    try:
        layout = solver(one_card, demand)
    except LayoutError as e:
        evidence['one_card'] = {'result': 'rejected', 'message': str(e)}
        assert 'MIG' in str(e)
    else:
        # A returned plan is judged by the independent oracle, never by the solver's fit().
        plans = [c for c in layout.cards if c.is_mig]
        problems = {f'{c.node}[{c.card_index}]': check_layout(H100, c.profiles) for c in plans}
        evidence['one_card'] = {'result': 'planned', 'cards': {k: v for k, v in problems.items()}}
        assert not any(problems.values()), f'the solver placed an impossible layout: {problems}'
        raise AssertionError('one 80 GB card was declared able to hold 110 GB of slices')

    # The historical numeric variant: the same demand phrased as 20-unit / 10-unit requests.
    numeric = [_gpu_spec('twenty', gpu_memory_gib = 20.0, nb_tasks = 4),
               _gpu_spec('ten', gpu_memory_gib = 10.0, nb_tasks = 3)]
    with pytest.raises(LayoutError):
        solver(one_card, numeric)

    # Positive control: with a second card the plan exists and every card passes the oracle.
    layout = solver([NodeInventory('h100', H100, 2, 80)], demand)
    cards = {f'{c.node}[{c.card_index}]': c.profiles for c in layout.cards if c.is_mig}
    assert cards, 'no MIG geometry planned on two cards'
    for card, profiles in cards.items():
        assert check_layout(H100, profiles) == [], (card, profiles)
    evidence['two_cards'] = cards


@pytest.mark.case('ALLOC-001')
@pytest.mark.level('model')
def test_alloc_001_reject_layouts_exceeding_physical_mig_memory(monkeypatch, evidence_dir) -> None:
    '''
    ALLOC-001 (P0, allocation, model): Reject layouts exceeding physical MIG memory.

    Acceptance: The one-H100 regression is rejected; every accepted plan satisfies both
    independently counted memory and compute slice limits.
    '''
    calls = _no_cluster(monkeypatch)
    evidence : Dict[str, Any] = {}
    _oracle_alloc_001(solve_layout, evidence)
    evidence['mutation_audit'] = calls
    assert calls == []
    (evidence_dir / 'planner_evaluation.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-001')
def test_alloc_001_detects_a_memory_blind_solver(monkeypatch) -> None:
    defects.memory_blind_card(monkeypatch)
    assert defects.detects(_oracle_alloc_001, solve_layout, {})


# -- ALLOC-009 ---------------------------------------------------------------------

#: (labels, allocatable, expected classification). Sources: NVIDIA k8s-device-plugin
#: README "Shared Access to GPUs" (sharing-strategy / replicas / -SHARED suffix) and
#: GPU Feature Discovery labels; the last row is this session's k3s cluster verbatim.
_POOLS = {
    'mps-explicit': ({'nvidia.com/gpu.sharing-strategy': 'mps', 'nvidia.com/gpu.product': A100},
                     {'nvidia.com/gpu': '4'}, 'mps'),
    'mps-with-auxiliary-labels': ({'nvidia.com/gpu.sharing-strategy': 'mps', 'nvidia.com/gpu.replicas': '4',
                                   'nvidia.com/gpu.product': A100 + '-SHARED'}, {'nvidia.com/gpu': '4'}, 'mps'),
    'time-slicing': ({'nvidia.com/gpu.sharing-strategy': 'time-slicing', 'nvidia.com/gpu.replicas': '4',
                      'nvidia.com/gpu.product': A100 + '-SHARED'}, {'nvidia.com/gpu': '4'}, 'time-sliced'),
    'mig-single-strategy': ({'nvidia.com/mig.capable': 'true', 'nvidia.com/mig.strategy': 'single',
                             'nvidia.com/mig.config': 'all-1g.10gb', 'nvidia.com/mig.config.state': 'success',
                             'nvidia.com/gpu.product': A100}, {'nvidia.com/gpu': '7'}, 'mig'),
    'whole': ({'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '4'}, {'nvidia.com/gpu': '4'}, 'physical'),
    'unlabeled': ({}, {'nvidia.com/gpu': '4'}, 'unknown'),
    'mig-capable-but-disabled': ({'nvidia.com/mig.capable': 'true', 'nvidia.com/mig.strategy': 'single',
                                  'nvidia.com/mig.config': 'all-disabled', 'nvidia.com/mig.config.state': 'success',
                                  'nvidia.com/gpu.count': '4', 'nvidia.com/gpu.replicas': '1',
                                  'nvidia.com/gpu.sharing-strategy': 'none',
                                  'nvidia.com/gpu.product': 'NVIDIA-RTX-PRO-6000-Blackwell-Server-Edition'},
                                 {'nvidia.com/gpu': '4'}, 'physical'),
}
_MIXED = {
    'whole+unlabeled': (('whole', 'unlabeled'), 'unknown'),
    'whole+mps-explicit': (('whole', 'mps-explicit'), 'mps'),
    'whole+whole': (('whole', 'whole'), 'physical'),
}


def _pool_listing(members : tuple) -> str:
    return nodes_json(*((f'gpu-{i}', _POOLS[m][0], _POOLS[m][1]) for i, m in enumerate(members)))


def _oracle_alloc_009(monkeypatch : pytest.MonkeyPatch, decisions : Dict[str, Any]) -> None:
    fixtures = {name: ((name,), expected) for name, (_l, _a, expected) in _POOLS.items()}
    fixtures.update(_MIXED)
    for name, (members, expected) in fixtures.items():
        listing = _pool_listing(members)
        monkeypatch.setattr(subprocess, 'run', fake_run({'gpu-pool=true -o json': listing}))
        kind = cluster.classify_gpu_resource()
        assert kind == expected, f'{name}: classified {kind!r}, expected {expected!r}'
        # Two distinct physical GPUs in one pod against this pool:
        monkeypatch.setattr(subprocess, 'run', fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-0',
                                                         'gpu-pool=true -o json': listing}))
        problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 2},
                                         max_per_pod = {'nvidia.com/gpu': 2})
        decisions[name] = {'classification': kind, 'problems': problems}
        if expected == 'physical':
            assert problems == [], f'{name}: a whole-device pool must admit two GPUs: {problems}'
        elif expected == 'unknown':
            assert any('cannot classify' in p for p in problems), problems
            assert not any(p.startswith(gpu.IMPOSSIBLE_GPU_REQUEST) for p in problems)
        else:
            fatal = [p for p in problems if p.startswith(gpu.IMPOSSIBLE_GPU_REQUEST)]
            assert len(fatal) == 1, f'{name}: expected one impossible-request rejection: {problems}'
            assert {'mps': 'MPS', 'time-sliced': 'time-sliced', 'mig': 'MIG'}[expected] in fatal[0]


@pytest.mark.case('ALLOC-009')
@pytest.mark.level('model')
def test_alloc_009_recognize_mps_and_mixed_or_incomplete_sharing_evidence(monkeypatch, evidence_dir) -> None:
    '''
    ALLOC-009 (P0, allocation, model): Recognize MPS and mixed or incomplete sharing evidence.

    Acceptance: No fixture with known sharing or unresolved evidence is certified as a
    two-physical-GPU grant; whole-device positive controls pass.
    '''
    decisions : Dict[str, Any] = {}
    _oracle_alloc_009(monkeypatch, decisions)
    (evidence_dir / 'contract_decisions.json').write_text(json.dumps(decisions, indent = 2))


@pytest.mark.case('ALLOC-009')
@pytest.mark.level('model')
@pytest.mark.variant('memory-backend')
def test_alloc_009_memory_backend_never_upgrades_shares_to_devices() -> None:
    '''The reference allocator: shares and unresolved evidence are excluded from exclusive plans.'''
    nodes = [NodeFixture(name, labels.get('nvidia.com/gpu.product', ''), 4, 80.0, labels)
             for name, (labels, _alloc, _expected) in _POOLS.items()]
    backend = MemoryAllocationBackend(nodes, FakeClock())
    snapshot = backend.inventory({}).value
    assert snapshot.sharing == {name: expected for name, (_l, _a, expected) in _POOLS.items()}
    request = WorkloadRequest('flowA', 'run1', 'w', 2, SHARING_EXCLUSIVE)
    plan = backend.plan([request], snapshot)
    assert isinstance(plan, FeasiblePlan)
    assert {d.node for d in plan.assignments['w']} <= {'whole', 'mig-capable-but-disabled'}
    shares_only = MemoryAllocationBackend([n for n in nodes if _POOLS[n.name][2] != 'physical'], FakeClock())
    assert isinstance(shares_only.plan([request], shares_only.inventory({}).value), Infeasible)
    assert shares_only.mutations() == []


@pytest.mark.negative_control(of = 'ALLOC-009')
def test_alloc_009_detects_an_mps_blind_classifier(monkeypatch) -> None:
    defects.mps_blind_classifier(monkeypatch)
    assert defects.detects(_oracle_alloc_009, monkeypatch, {})


# -- ALLOC-010 ---------------------------------------------------------------------

@pytest.mark.case('ALLOC-010')
@pytest.mark.level('model')
def test_alloc_010_detect_per_host_fragmentation_before_claiming_a_whole_gpu(seed, monkeypatch, evidence_dir) -> None:
    '''
    ALLOC-010 (P1, allocation, model): Detect per-host fragmentation before claiming a
    whole-GPU plan is feasible.

    Acceptance: All bounded generated cases agree with the independent oracle and the known 3+3
    regression cannot be marked fully feasible.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_alloc_010(seed, monkeypatch, evidence)
    (evidence_dir / 'packing_oracle.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-010')
def test_alloc_010_detects_aggregate_only_feasibility(monkeypatch, seed) -> None:
    defects.aggregate_only_packing(monkeypatch)
    assert defects.detects(_oracle_alloc_010, seed, monkeypatch, {})


@pytest.mark.case('ALLOC-023')
@pytest.mark.level('model')
def test_alloc_023_reject_unsupported_allocation_features_before_any_cluster(evidence_dir) -> None:
    '''
    ALLOC-023 (P0, allocation, model): Reject unsupported allocation features before any
    cluster mutation.

    Acceptance: Every unsupported matrix entry is rejected with no side effects; supported
    controls reach the normal allocation path.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_alloc_023(_deploy, evidence)
    (evidence_dir / 'validation_matrix.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-023')
@pytest.mark.level('model')
@pytest.mark.variant('mix-strategy-audit')
def test_alloc_023_mix_strategy_rejects_before_touching_kubectl(monkeypatch) -> None:
    '''The real mix strategy: an impossible request fails in planning with zero kubectl writes.'''
    from support_kubectl import FakeKubectl
    fake = FakeKubectl({'gpu-pool=true -o json': nodes_json(
        ('gpu-a', {'nvidia.com/gpu.product': 'NVIDIA-L4', 'nvidia.com/gpu.count': '2',
                   'nvidia.com/gpu.memory': '23034'}, {'nvidia.com/gpu': '2'})),
                        'pods -A': '{"items": []}'})
    monkeypatch.setattr(subprocess, 'run', fake)
    with pytest.raises(LayoutError, match = 'MIG-capable'):
        gpu.MixGpu().resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)], flow_id = 'flow1')
    assert fake.mutations() == []


@pytest.mark.negative_control(of = 'ALLOC-023')
def test_alloc_023_detects_an_adapter_that_writes_before_validating() -> None:
    assert defects.detects(_oracle_alloc_023, defects.eager_deploy, {})


# -- ALLOC-034 ---------------------------------------------------------------------

@pytest.mark.case('ALLOC-034')
@pytest.mark.level('model')
def test_alloc_034_resolve_gpu_count_and_memory_defaults_with_explicit(evidence_dir) -> None:
    '''
    ALLOC-034 (P1, allocation, model): Resolve GPU count and memory defaults with explicit
    provenance.

    Acceptance: Every table row has one documented deterministic result with accurate
    provenance; defaults never accidentally defeat an explicit requirement and incompatible
    explicit requests launch no worker.
    '''
    table : List[Dict[str, Any]] = []
    _oracle_alloc_034(table)
    (evidence_dir / 'provenance_table.json').write_text(json.dumps(table, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-034')
def test_alloc_034_detects_a_last_wins_merge(monkeypatch) -> None:
    defects.last_wins_resolver(monkeypatch)
    assert defects.detects(_oracle_alloc_034, [])


# -- ALLOC-023 oracle ------------------------------------------------------------------------

def _deploy(backend : MemoryAllocationBackend, requests : List[WorkloadRequest], capabilities : Any) -> Any:
    '''The validate-before-write boundary: rejections happen before any plan or claim.'''
    reasons = allocation_rejections(requests, capabilities)
    if reasons:
        return reasons
    snapshot = backend.inventory({}).value
    plan = backend.plan(requests, snapshot)
    if isinstance(plan, Infeasible):
        return list(plan.reasons)
    return backend.reserve(plan, f'{requests[0].flow_id}:op', plan.snapshot_generation)


def _oracle_alloc_023(deploy : Callable[..., Any], evidence : Dict[str, Any]) -> None:
    def node(name : str) -> NodeFixture:
        return NodeFixture(name, A100, 4, 80.0, {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '4',
                                                 'nvidia.com/mig.capable': 'true',
                                                 'nvidia.com/mig.strategy': 'single'})

    def request(workload : str, sharing : str = SHARING_EXCLUSIVE, count : int = 1, **kw : Any) -> WorkloadRequest:
        return WorkloadRequest('flowA', 'run1', workload, count, sharing, **kw)

    device_plugin = MemoryAllocationBackend([node('n1')], FakeClock(), authority = AUTHORITY_DEVICE_PLUGIN)
    dp_caps = device_plugin.capabilities({'version_matrix': {'version': 'k8s-device-plugin v0.16', 'features': []}})
    dra = MemoryAllocationBackend([node('n2')], FakeClock(), authority = AUTHORITY_DRA)
    dra_caps = dra.capabilities({'version_matrix': {'version': 'dra-driver-nvidia v0.5.0 / k8s 1.34',
                                                    'features': [FEATURE_DYNAMIC_MIG, FEATURE_MPS,
                                                                 FEATURE_CONSUMABLE_CAPACITY]}})
    single_device = MemoryAllocationBackend([node('n3')], FakeClock())
    sd_caps = dataclasses.replace(single_device.capabilities({}), multi_device = False, elastic = False)

    matrix = [
        ('dynamic MIG on a device-plugin authority', device_plugin, dp_caps,
         [request('mig', SHARING_ISOLATED_MIG, features = frozenset({FEATURE_DYNAMIC_MIG}), minimum_usable_memory_bytes = 20 << 30)],
         ('isolated MIG', FEATURE_DYNAMIC_MIG, 'device-plugin')),
        ('MPS plus DynamicMIG', dra, dra_caps,
         [request('both', features = frozenset({FEATURE_MPS, FEATURE_DYNAMIC_MIG}))],
         ('cannot be combined', FEATURE_MPS, FEATURE_DYNAMIC_MIG)),
        ('MPS plus consumable shares', dra, dra_caps,
         [request('both', features = frozenset({FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY}))],
         ('cannot be combined', FEATURE_CONSUMABLE_CAPACITY)),
        ('unsupported multi-device policy', single_device, sd_caps,
         [request('span', count = 2)], ('2 devices', 'grants one')),
        ('elastic request on a fixed authority', single_device, sd_caps,
         [request('grow', elasticity = ELASTICITY_ELASTIC)], ('elasticity',)),
    ]
    for title, backend, caps, requests, expected_words in matrix:
        outcome = deploy(backend, requests, caps)
        assert isinstance(outcome, list) and outcome, f'{title}: admitted ({outcome})'
        text = '\n'.join(outcome)
        for word in expected_words:
            assert word in text, f'{title}: rejection does not name {word!r}: {text}'
        assert backend.mutations() == [], f'{title}: mutations before rejection: {backend.mutations()}'
        evidence[title] = {'rejected': outcome, 'mutations': backend.mutations()}

    # A stale capability/inventory snapshot is not applied: the plan carries the
    # generation it was made on, and the reserve must revalidate against it.
    snapshot = device_plugin.inventory({}).value
    plan = device_plugin.plan([request('w')], snapshot)
    assert isinstance(plan, FeasiblePlan)
    device_plugin.node('n1').resource_version += 1                # a foreign change lands
    try:
        device_plugin.reserve(plan, 'flowA:op', plan.snapshot_generation)
    except OwnershipConflict as e:
        evidence['stale_snapshot'] = str(e)
    else:
        raise AssertionError('a plan made on a stale snapshot was applied')
    assert device_plugin.mutations() == []

    # Positive control: a supported request reaches the normal allocation path.
    claim = deploy(device_plugin, [request('ok')], dp_caps)
    assert not isinstance(claim, list), claim
    assert claim.status == 'allocated' and device_plugin.node('n1').owner == 'flowA'
    evidence['positive_control'] = {'claim': claim.claim_id, 'mutations': device_plugin.mutations()}


# -- ALLOC-010 / ALLOC-034 oracles -----------------------------------------------------------

def _exhaustive_packing(claims : List[int], free : Dict[str, int]) -> Dict[int, str] | None:
    '''Independent oracle: every assignment of pods to nodes, for bounded instances.'''
    nodes = sorted(free)
    for assignment in itertools.product(nodes, repeat = len(claims)):
        load : Dict[str, int] = {n: 0 for n in nodes}
        for claim, node in zip(claims, assignment):
            load[node] += claim
        if all(load[n] <= free[n] for n in nodes):
            return dict(enumerate(assignment))
    return None


def _realizable(claims : List[int], free : Dict[str, int], placement : Dict[int, str]) -> bool:
    load : Dict[str, int] = {}
    for index, node in placement.items():
        load[node] = load.get(node, 0) + claims[index]
    return set(placement) == set(range(len(claims))) and all(units <= free.get(node, 0) for node, units in load.items())


def _oracle_alloc_010(seed : int, monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    # The regression: 3 + 3 free, three pods of 2 — totals and maxima pass, packing cannot.
    regression = gpu.pack_pod_claims([2, 2, 2], {'h1': 3, 'h2': 3})
    assert regression.feasible is False and regression.proven is True, regression
    assert _exhaustive_packing([2, 2, 2], {'h1': 3, 'h2': 3}) is None
    for claims, free in (([2, 2, 2], {'h1': 2, 'h2': 2, 'h3': 2}), ([2, 2], {'h1': 3, 'h2': 3})):
        control = gpu.pack_pod_claims(claims, free)
        assert control.feasible and _realizable(claims, free, control.placement), (claims, free, control)
    evidence['regression'] = str(regression)
    # Bounded generated instances agree with the exhaustive oracle, including
    # pairs with identical totals and maxima but different packability.
    rng = random.Random(seed)
    agreements = 0
    disagreements = []
    for _ in range(150):
        nodes = rng.randint(1, 4)
        free = {f'n{i}': rng.randint(0, 4) for i in range(nodes)}
        claims = [rng.randint(1, 3) for _ in range(rng.randint(1, 6))]
        oracle = _exhaustive_packing(claims, free)
        packing = gpu.pack_pod_claims(claims, free)
        if packing.feasible != (oracle is not None) or (packing.feasible and not _realizable(claims, free, packing.placement)):
            disagreements.append({'claims': claims, 'free': free, 'oracle': oracle, 'packing': str(packing)})
        else:
            agreements += 1
    assert not disagreements, disagreements[:5]
    evidence['generated'] = {'agreements': agreements}
    same_totals = [([2, 2, 2], {'a': 3, 'b': 3}), ([2, 2, 2], {'a': 2, 'b': 2, 'c': 2}),
                   ([3, 1, 1, 1], {'a': 3, 'b': 3}), ([2, 2, 1, 1], {'a': 3, 'b': 3})]
    evidence['same_totals'] = [{'claims': c, 'free': f, 'feasible': gpu.pack_pod_claims(c, f).feasible,
                                'oracle': _exhaustive_packing(c, f) is not None} for c, f in same_totals]
    assert all(row['feasible'] == row['oracle'] for row in evidence['same_totals'])
    # Through the real preflight: the pool [3, 3] against three 2-GPU pods is reported, with a proof.
    pool = nodes_json(('h1', {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '3'}, {'nvidia.com/gpu': '3'}),
                      ('h2', {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '3'}, {'nvidia.com/gpu': '3'}))
    monkeypatch.setattr(subprocess, 'run', fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/h1',
                                                     'gpu-pool=true -o json': pool}))
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 6},
                                     max_per_pod = {'nvidia.com/gpu': 2}, pod_claims = {'nvidia.com/gpu': [2, 2, 2]})
    assert len(problems) == 1 and 'cannot all be placed' in problems[0] and 'exhaustive' in problems[0], problems
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 4},
                                 max_per_pod = {'nvidia.com/gpu': 2}, pod_claims = {'nvidia.com/gpu': [2, 2]}) == []
    evidence['preflight'] = problems
    # A backend that delegates placement to the scheduler still reports infeasible, not admitted.
    backend = MemoryAllocationBackend([NodeFixture(n, A100, 3, 80.0, {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '3'})
                                       for n in ('h1', 'h2')], FakeClock())
    plan = backend.plan([WorkloadRequest('flowA', 'r', f'w{i}', 2, SHARING_EXCLUSIVE) for i in range(3)],
                        backend.inventory({}).value)
    assert isinstance(plan, Infeasible) and 'packing' in plan.reasons[0] and backend.mutations() == []


def _oracle_alloc_034(table : List[Dict[str, Any]]) -> None:
    from videoflow.core import provenance
    from videoflow.core.compiler import compile_flow, gpu_provenance
    from videoflow.core.provenance import (
        FIELD_DEVICE_TYPE,
        FIELD_GPU_COUNT,
        FIELD_GPU_MEMORY_GIB,
        SOURCE_DEFAULT,
        SOURCE_DESCRIPTOR,
        SOURCE_NODE,
        builtin_defaults,
        descriptor_declarations,
        node_declarations,
    )

    def resolve(title : str, declarations : list, expect : Dict[str, Any] | None = None,
                expect_conflict_naming : tuple = ()) -> None:
        try:
            result = provenance.resolve_gpu_requirements(declarations + builtin_defaults(), subject = title)
        except (CapabilityError, ValueError) as e:
            assert expect is None, f'{title}: unexpected rejection {e}'
            message = str(e)
            for source in expect_conflict_naming:
                assert source in message, f'{title}: rejection does not name {source!r}: {message}'
            table.append({'row': title, 'result': 'rejected', 'message': message[:200]})
            return
        assert expect is not None, f'{title}: admitted {result} but a rejection was expected'
        for field_name, (value, source) in expect.items():
            assert result.values.get(field_name) == value, (title, field_name, result)
            assert result.provenance.get(field_name) == source, (title, field_name, result)
        again = provenance.resolve_gpu_requirements(declarations + builtin_defaults(), subject = title)
        assert again == result, f'{title}: not deterministic'
        table.append({'row': title, 'result': 'resolved', 'values': result.values, 'provenance': result.provenance,
                      'dropped': result.dropped})

    resolve('no GPU declaration', node_declarations('cpu'),
            {FIELD_DEVICE_TYPE: ('cpu', SOURCE_NODE), FIELD_GPU_COUNT: (1, SOURCE_DEFAULT),
             FIELD_GPU_MEMORY_GIB: (None, SOURCE_DEFAULT)})
    # The default count=1 is a default, not a whole-device demand that cancels a slice request.
    resolve('default count with explicit memory', node_declarations('gpu', gpu_memory_gib = 10),
            {FIELD_GPU_COUNT: (1, SOURCE_DEFAULT), FIELD_GPU_MEMORY_GIB: (10, SOURCE_NODE)})
    resolve('explicit count 1', node_declarations('gpu', gpu_count = 1),
            {FIELD_GPU_COUNT: (1, SOURCE_NODE), FIELD_GPU_MEMORY_GIB: (None, SOURCE_DEFAULT)})
    resolve('explicit count 2', node_declarations('gpu', gpu_count = 2),
            {FIELD_GPU_COUNT: (2, SOURCE_NODE), FIELD_GPU_MEMORY_GIB: (None, SOURCE_DEFAULT)})
    resolve('descriptor memory default', node_declarations('gpu') + descriptor_declarations(['cpu', 'gpu'], gpu_memory_gib = 10),
            {FIELD_GPU_MEMORY_GIB: (10, SOURCE_DESCRIPTOR), FIELD_GPU_COUNT: (1, SOURCE_DEFAULT)})
    resolve('descriptor count default', node_declarations('gpu') + descriptor_declarations(['gpu'], gpu_count = 2),
            {FIELD_GPU_COUNT: (2, SOURCE_DESCRIPTOR), FIELD_DEVICE_TYPE: ('gpu', SOURCE_NODE)})
    # An explicit count does not silently combine with an inherited memory default (or the reverse):
    # the outcome is deterministic and any rejection names the descriptor.
    for title, declarations in (('explicit count 2 with descriptor memory default',
                                 node_declarations('gpu', gpu_count = 2) + descriptor_declarations(['gpu'], gpu_memory_gib = 10)),
                                ('explicit memory with descriptor count default',
                                 node_declarations('gpu', gpu_memory_gib = 10) + descriptor_declarations(['gpu'], gpu_count = 2))):
        try:
            first = provenance.resolve_gpu_requirements(declarations + builtin_defaults(), subject = title)
        except (CapabilityError, ValueError) as e:
            assert SOURCE_DESCRIPTOR in str(e) and SOURCE_NODE in str(e), str(e)
            table.append({'row': title, 'result': 'rejected', 'message': str(e)[:200]})
            continue
        assert not (first.values[FIELD_GPU_COUNT] > 1 and first.values[FIELD_GPU_MEMORY_GIB] is not None), \
            f'{title}: a multi-device count was silently combined with a memory slice: {first}'
        assert first == provenance.resolve_gpu_requirements(declarations + builtin_defaults(), subject = title)
        table.append({'row': title, 'result': 'resolved', 'values': first.values, 'provenance': first.provenance})
    # Two incompatible explicit requirements fail before launch, naming both sources.
    resolve('node cpu against a GPU-only component', node_declarations('cpu') + descriptor_declarations(['gpu']),
            expect_conflict_naming = (SOURCE_NODE, SOURCE_DESCRIPTOR))
    # A node contradicting itself (cpu with its own memory demand) is the constructor's
    # rejection, before any resolver: the explicit GPU request is never silently dropped.
    from videoflow.processors import IdentityProcessor as _Identity
    try:
        _Identity(name = 'self-contradiction', device_type = 'cpu', gpu_memory_gib = 10)
    except ValueError as e:
        table.append({'row': 'node cpu with its own explicit memory', 'result': 'rejected', 'message': str(e)[:200]})
    else:
        raise AssertionError('a CPU node with an explicit GPU memory demand was constructed')
    # A CPU run sets aside an inherited GPU memory default — and says so.
    cpu_variant = provenance.resolve_gpu_requirements(
        node_declarations('cpu') + descriptor_declarations(['cpu', 'gpu'], gpu_memory_gib = 10) + builtin_defaults(), 'cpu run')
    assert cpu_variant.values[FIELD_GPU_MEMORY_GIB] is None and cpu_variant.dropped.get(FIELD_GPU_MEMORY_GIB) == SOURCE_DESCRIPTOR
    table.append({'row': 'cpu run of a dual-device component', 'result': 'resolved', 'values': cpu_variant.values,
                  'provenance': cpu_variant.provenance, 'dropped': cpu_variant.dropped})

    # Round trip: node -> spec -> serialized -> rebuilt node keeps the requirement and its provenance.
    from videoflow.consumers import CommandlineConsumer
    from videoflow.core import Flow
    from videoflow.core.compiler import NodeSpec as _Spec
    from videoflow.core.constants import BATCH
    from videoflow.processors import IdentityProcessor
    from videoflow.producers import IntProducer
    p = IntProducer(0, 3, name = 'src')
    worker = IdentityProcessor(name = 'slice', device_type = 'gpu', gpu_memory_gib = 10)(p)
    flow = Flow([CommandlineConsumer(name = 'sink')(worker)], flow_type = BATCH, flow_id = 'prov')
    assert worker.gpu_provenance[FIELD_GPU_MEMORY_GIB] == SOURCE_NODE and worker.gpu_provenance[FIELD_GPU_COUNT] == SOURCE_DEFAULT
    document = gpu_provenance(flow)
    assert document['slice'] == worker.gpu_provenance
    spec = next(s for s in compile_flow(flow) if s.name == 'slice')
    rebuilt = _Spec.from_dict(json.loads(json.dumps(spec.to_dict())))
    assert (rebuilt.gpu_count, rebuilt.gpu_memory_gib, rebuilt.device_type) == (1, 10, 'gpu')
    node_class = rebuilt.node_class.rsplit('.', 1)
    module = __import__(node_class[0], fromlist = [node_class[1]])
    again = getattr(module, node_class[1])(**rebuilt.params)
    # The resolved requirement survives the worker-side rebuild verbatim. Provenance
    # is a compile-time document (D8: params carry values, not sources), so the
    # rebuilt node legitimately reports its rebuilt arguments as explicit.
    assert again.get_params() == worker.get_params()
    table.append({'row': 'round trip', 'result': 'resolved', 'params': again.get_params(),
                  'compile_time_provenance': document['slice']})
    # Changing the backend changes the resource name's source, never the user's declaration.
    for default, resolved, source in ((None, None, 'default'), ('amd.com/gpu', None, 'cli-default'),
                                      (None, 'nvidia.com/mig-1g.10gb', 'strategy')):
        candidate = dataclasses.replace(spec, gpu_resource_name = resolved)
        name, where = gpu.gpu_resource_provenance(candidate, default)
        assert where == source and (name == resolved if resolved else True)
        assert (candidate.gpu_count, candidate.gpu_memory_gib) == (1, 10)
        table.append({'row': f'backend resource ({source})', 'result': name, 'provenance': where})
    # Delivered count is distinct from requested: an oversubscribed host grants one of two.
    from videoflow.engines.local import _worker_env, assign_local_gpus
    two = _Spec('work', 'videoflow.processors.basic.IdentityProcessor', {}, ['src'], 'processor', True, 1, 'gpu', True,
                gpu_count = 2)
    grant = assign_local_gpus([two], [0])
    env = _worker_env(two, 'nats://x:4222', 'demo', BATCH, 'run1', None, 0, 3, gpu_devices = grant[('work', 0)])
    assert two.gpu_count == 2 and env['VF_GPU_COUNT'] == '1'
    table.append({'row': 'delivered vs requested', 'requested': 2, 'delivered': env['VF_GPU_COUNT']})
