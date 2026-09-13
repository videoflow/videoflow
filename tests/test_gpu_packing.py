'''
Per-host packing in the exclusive GPU preflight (ALLOC-010).

The preflight used to compare total demand with total free capacity and the
largest single-pod claim with the largest node. Both are necessary and neither
is sufficient: per-node free ``[3, 3]`` against pods ``[2, 2, 2]`` passes both
(6 <= 6, 2 <= 3) and places only two pods. ``pack_pod_claims`` decides the real
question — can every pod's whole claim land on one host at the same time — with
first-fit-decreasing and, for small instances, an exhaustive search so a
negative answer is a proof.

The checker is tested against an independent brute-force oracle over seeded
random small instances (``random.Random(seed)``; no hypothesis — the corpus is
bounded and explicit): it must never say "fits" when the oracle says it cannot,
never say "cannot" when it can, and every placement it returns must be
realizable host by host. The known 3+3 regression is then driven through
``cluster.gpu_preflight`` with a fake kubectl.
'''
from __future__ import absolute_import, division, print_function

import itertools
import json
import random
import subprocess
import time

import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import GPU, REALTIME
from videoflow.deploy import cluster, gpu
from videoflow.deploy.manifests import gpu_demand, gpu_max_per_pod, gpu_pod_claims
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

# -- the oracle --------------------------------------------------------------------

def _oracle(claims, free):
    '''Brute force: some assignment of every claim to a node keeps every node within its free units.'''
    nodes = sorted(free)
    if not claims:
        return True
    for assignment in itertools.product(nodes, repeat = len(claims)):
        load = dict.fromkeys(nodes, 0)
        for claim, node in zip(claims, assignment):
            load[node] += claim
        if all(load[node] <= free[node] for node in nodes):
            return True
    return False


def _realizable(claims, free, placement):
    assert sorted(placement) == list(range(len(claims))), 'every pod is placed exactly once'
    load = dict.fromkeys(free, 0)
    for index, node in placement.items():
        load[node] += claims[index]
    return all(load[node] <= free[node] for node in free)


@pytest.mark.parametrize('seed', [0, 1, 2, 3, 4])
def test_packer_agrees_with_the_brute_force_oracle(seed):
    rng = random.Random(seed)
    disagreements = []
    infeasible = feasible = 0
    for _ in range(120):
        free = {f'n{i}': rng.randint(0, 4) for i in range(rng.randint(1, 4))}
        claims = [rng.randint(1, 3) for _ in range(rng.randint(0, 6))]
        packing = gpu.pack_pod_claims(claims, free)
        expected = _oracle(claims, free)
        assert packing.proven, (claims, free)              # small instances always get a proof
        if packing.feasible != expected:
            disagreements.append((claims, free, packing))
        if packing.feasible:
            feasible += 1
            assert _realizable(claims, free, packing.placement), (claims, free, packing)
        else:
            infeasible += 1
            assert packing.unplaced and all(0 <= i < len(claims) for i in packing.unplaced)
    assert disagreements == []
    assert feasible and infeasible, 'the corpus must exercise both answers'


def test_identical_totals_and_maxima_with_different_packability():
    '''Same total (6) and same largest claim (2): only the per-host check tells them apart.'''
    fragmented = gpu.pack_pod_claims([2, 2, 2], {'a': 3, 'b': 3})
    assert (fragmented.feasible, fragmented.proven) == (False, True)
    assert [2 for _ in fragmented.unplaced] == [2]
    for free in ({'a': 2, 'b': 2, 'c': 2}, {'a': 4, 'b': 2}, {'a': 6}):
        packing = gpu.pack_pod_claims([2, 2, 2], free)
        assert packing.feasible and packing.proven and _realizable([2, 2, 2], free, packing.placement)
    # Two pods on the original hosts, and the case's positive control.
    assert gpu.pack_pod_claims([2, 2], {'a': 3, 'b': 3}).feasible
    assert gpu.pack_pod_claims([], {'a': 3}).feasible


def test_first_fit_failure_is_settled_by_the_exact_search():
    '''Free [4, 3] against [3, 2, 2]: first-fit puts the 3 on the 4-node and strands
    a 2, but 2+2 on the 4-node and 3 on the 3-node fits — the exhaustive search
    finds it, so the answer is "fits", proven, with a realizable placement.'''
    claims, free = [3, 2, 2], {'big': 4, 'small': 3}
    placement, unplaced = gpu._first_fit_decreasing(claims, free)
    assert unplaced == [2]
    packing = gpu.pack_pod_claims(claims, free)
    assert packing.feasible and packing.proven
    assert _realizable(claims, free, packing.placement)
    assert packing.placement[0] == 'small'


def test_large_instance_without_a_placement_is_reported_unproven():
    '''Beyond the exact-search bounds a first-fit failure is an opinion, not a
    proof: the same fixable [4, 3]/[3, 2, 2] core padded with one-unit pods on
    one-unit nodes is feasible, and the checker must not claim it cannot fit.'''
    claims = [3, 2, 2] + [1] * 11
    free = {'big': 4, 'small': 3, **{f'one{i}': 1 for i in range(11)}}
    assert len(claims) > gpu.PACKING_EXACT_MAX_PODS and len(free) > gpu.PACKING_EXACT_MAX_NODES
    packing = gpu.pack_pod_claims(claims, free)
    assert not packing.feasible and not packing.proven
    assert packing.unplaced
    # Within bounds, the same core is settled exactly.
    assert gpu.pack_pod_claims([3, 2, 2, 1], {'big': 4, 'small': 3, 'one': 1}).feasible


def test_aggregate_shortfalls_are_proven_without_a_search():
    assert gpu.pack_pod_claims([2] * 13, {f'n{i}': 1 for i in range(13)}) == gpu.PodPacking(
        False, True, {}, tuple(range(13)))
    assert gpu.pack_pod_claims([1] * 13, {f'n{i}': 1 for i in range(12)}).proven


def test_exact_search_is_fast_at_its_bounds():
    '''An adversarial instance at the bounds: distinct node sizes, equal claims
    that cannot all fit. Symmetry pruning and the failed-state memo keep it instant.'''
    free = {f'n{i}': size for i, size in enumerate([8, 7, 6, 5, 4, 3, 2, 1])}
    # 36 units free, but size-3 claims fit 2+2+2+1+1+1+0+0 = 9 of them: the 10th
    # (and the 12th, at the pod bound) is infeasible by fragmentation, not by total.
    started = time.monotonic()
    packing = gpu.pack_pod_claims([3] * 12, free)
    assert (packing.feasible, packing.proven) == (False, True)
    assert (gpu.pack_pod_claims([3] * 10, free).feasible, gpu.pack_pod_claims([3] * 10, free).proven) == (False, True)
    nine = gpu.pack_pod_claims([3] * 9, free)
    assert nine.feasible and _realizable([3] * 9, free, nine.placement)
    assert time.monotonic() - started < 2.0

# -- gpu_pod_claims ------------------------------------------------------------------

def test_gpu_pod_claims_lists_one_claim_per_replica():
    producer = IntProducer(name = 'p')
    spanner = IdentityProcessor(name = 'span', device_type = GPU, nb_tasks = 3, gpu_count = 2)(producer)
    single = IdentityProcessor(name = 'single', device_type = GPU)(spanner)
    cpu = IdentityProcessor(name = 'cpu', nb_tasks = 4)(single)
    printer = CommandlineConsumer(name = 'c')(cpu)
    specs = compile_flow(Flow([printer], flow_type = REALTIME, flow_id = 'g'))
    claims = gpu_pod_claims(specs)
    assert claims == {'nvidia.com/gpu': [2, 2, 2, 1]}
    # Consistent with its siblings, and grouped by the same resolved resource name.
    assert sum(claims['nvidia.com/gpu']) == gpu_demand(specs)['nvidia.com/gpu']
    assert max(claims['nvidia.com/gpu']) == gpu_max_per_pod(specs)['nvidia.com/gpu']
    assert gpu_pod_claims(specs, default_resource = 'amd.com/gpu') == {'amd.com/gpu': [2, 2, 2, 1]}
    assert gpu_pod_claims([s for s in specs if s.device_type != 'gpu']) == {}

# -- through cluster.gpu_preflight with a fake kubectl -------------------------------

_PHYSICAL = {'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB', 'nvidia.com/gpu.count': '3'}


class _Proc:
    def __init__(self, stdout = '', returncode = 0):
        self.stdout = stdout
        self.returncode = returncode


def _fake_run(responses):
    '''subprocess.run stand-in: canned stdout by command substring. An unmatched
    pod listing answers with an empty list, so "nothing runs" reads as an honest
    empty occupancy rather than an unreadable one (see tests/test_cluster.py).'''
    def run(cmd, **kwargs):
        joined = ' '.join(cmd)
        for key, out in responses.items():
            if key in joined:
                return _Proc(stdout = out)
        if 'pods -A' in joined:
            return _Proc(stdout = '{"items": []}')
        return _Proc()
    return run


def _pool_json(*nodes):
    '''A pool listing: nodes as (name, allocatable nvidia.com/gpu) pairs with plain physical GFD labels.'''
    return json.dumps({'items': [
        {'metadata': {'name': name, 'labels': dict(_PHYSICAL, **{'nvidia.com/gpu.count': str(count)})},
         'status': {'allocatable': {'nvidia.com/gpu': str(count)}}}
        for name, count in nodes
    ]})


def _pods_json(*pods):
    '''A pod listing: pods as (node, units of nvidia.com/gpu held) pairs.'''
    return json.dumps({'items': [
        {'spec': {'nodeName': node, 'containers': [{'resources': {'limits': {'nvidia.com/gpu': str(units)}}}]},
         'status': {'phase': 'Running'}}
        for node, units in pods
    ]})


def _preflight(monkeypatch, pool, claims, pods = None):
    responses = {'version': '{}', 'gpu-pool=true -o name': 'node/gpu-a', 'gpu-pool=true -o json': pool}
    if pods is not None:
        responses['pods -A'] = pods
    monkeypatch.setattr(subprocess, 'run', _fake_run(responses))
    resource = 'nvidia.com/gpu'
    return cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {resource: sum(claims)},
                                 max_per_pod = {resource: max(claims)}, pod_claims = {resource: list(claims)})


def test_preflight_reports_the_fragmented_pool(monkeypatch):
    '''The ALLOC-010 regression: two hosts with three free GPUs each, three
    replicas needing two GPUs on one host. Total and largest-node checks pass;
    the flow is not reported schedulable.'''
    problems = _preflight(monkeypatch, _pool_json(('gpu-a', 3), ('gpu-b', 3)), [2, 2, 2])
    assert len(problems) == 1
    problem = problems[0]
    assert '3 GPU pod(s) (3 pod(s) x 2 x nvidia.com/gpu) cannot all be placed' in problem
    assert 'gpu-a=3, gpu-b=3' in problem and '6 free for 6 demanded' in problem
    assert 'exhaustive search proves' in problem
    assert '1 pod(s) needing [2] unplaced' in problem
    assert 'Fix: free 2 unit(s) on one host, add a node with >= 2 free nvidia.com/gpu' in problem
    assert not problem.startswith(gpu.IMPOSSIBLE_GPU_REQUEST)     # a capacity warning, strict-mode promotable


def test_preflight_accepts_packable_pools(monkeypatch):
    '''Positive controls: three hosts with two free each, and two replicas on the original hosts.'''
    assert _preflight(monkeypatch, _pool_json(('gpu-a', 2), ('gpu-b', 2), ('gpu-c', 2)), [2, 2, 2]) == []
    assert _preflight(monkeypatch, _pool_json(('gpu-a', 3), ('gpu-b', 3)), [2, 2]) == []
    assert _preflight(monkeypatch, _pool_json(('gpu-a', 4), ('gpu-b', 2)), [2, 2, 2]) == []


def test_preflight_packs_against_free_units_not_allocatable(monkeypatch):
    '''Running pods hold one card on each of two 4-GPU hosts: allocatable [4, 4]
    would pack three 2-GPU pods, free [3, 3] does not.'''
    pool = _pool_json(('gpu-a', 4), ('gpu-b', 4))
    assert _preflight(monkeypatch, pool, [2, 2, 2]) == []
    problems = _preflight(monkeypatch, pool, [2, 2, 2], pods = _pods_json(('gpu-a', 1), ('gpu-b', 1)))
    assert len(problems) == 1 and 'gpu-a=3, gpu-b=3' in problems[0]


def test_preflight_does_not_repeat_shortfalls_the_aggregate_checks_report(monkeypatch):
    pool = _pool_json(('gpu-a', 3), ('gpu-b', 3))
    problems = _preflight(monkeypatch, pool, [2, 2, 2, 2])             # 8 > 6: the capacity check's case
    assert len(problems) == 1 and 'demands 8 x nvidia.com/gpu' in problems[0]
    problems = _preflight(monkeypatch, pool, [4, 1])                   # 4 > 3: the largest-node check's case
    assert len(problems) == 1 and 'largest cluster node has only 3' in problems[0]


def test_pod_claims_is_an_optional_keyword_everywhere(monkeypatch):
    '''Existing callers pass nothing and get exactly the old checks; the base
    strategy and mix accept the keyword; a **kwargs third-party strategy is untouched.'''
    monkeypatch.setattr(subprocess, 'run', _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-a',
                                                      'gpu-pool=true -o json': _pool_json(('gpu-a', 3), ('gpu-b', 3))}))
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 6},
                                 max_per_pod = {'nvidia.com/gpu': 2}) == []
    assert gpu.GpuStrategy().preflight_problems(pod_claims = {'nvidia.com/gpu': [2, 2, 2]}) == []
    mix = gpu.MixGpu().preflight_problems(pod_claims = {'nvidia.com/gpu': [2, 2, 2]})
    assert len(mix) == 1 and 'resolve_specs' in mix[0]

    class _Picky(gpu.GpuStrategy):
        name = 'picky-packing'

        def preflight_problems(self, kubectl = 'kubectl', demand = None, gpu_runtime_class = None, **kwargs):
            return [f'picky saw pod_claims={kwargs.get("pod_claims")}']

    monkeypatch.setattr(gpu, '_GPU_STRATEGIES', dict(gpu._GPU_STRATEGIES))
    gpu.register_gpu_mode(_Picky())
    assert cluster.gpu_preflight(gpu_mode = 'picky-packing', pod_claims = {'nvidia.com/gpu': [1]}) == [
        "picky saw pod_claims={'nvidia.com/gpu': [1]}"]


if __name__ == '__main__':
    pytest.main([__file__])
