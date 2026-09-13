'''
An independent MIG geometry oracle.

``videoflow/deploy/mig.py`` plans layouts with a deliberately simplified card
model (compute slices only, no placement positions). This table is the
*independent* check the conformance cases evaluate a plan against: memory slices
are counted separately from compute slices, and every profile carries the start
positions the hardware allows, so a layout that fits by slice count but cannot be
placed is rejected here — and a layout the planner rejects for the wrong reason
is caught as well. Values are transcribed from the NVIDIA MIG user guide
("Supported MIG profiles" tables); the family key is a substring of the GFD
``nvidia.com/gpu.product`` label. Keep this file boring: no imports from
``deploy/``, nothing shared with the planner it audits.
'''
from __future__ import absolute_import, division, print_function

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen = True)
class Profile:
    name : str
    compute_slices : int
    memory_slices : int
    memory_gib : float
    max_instances : int
    starts : tuple[int, ...]          # legal start positions, in memory-slice units

@dataclass(frozen = True)
class Family:
    key : str
    compute_slices : int
    memory_slices : int
    memory_gib : float
    profiles : Mapping[str, Profile]

# A100 80GB: 7 compute slices, 8 memory slices (1g.10gb spans one memory slice).
_A100_80 = Family('A100-SXM4-80GB', 7, 8, 80.0, {
    '1g.10gb': Profile('1g.10gb', 1, 1, 10.0, 7, (0, 1, 2, 3, 4, 5, 6)),
    '1g.20gb': Profile('1g.20gb', 1, 2, 20.0, 4, (0, 2, 4, 6)),
    '2g.20gb': Profile('2g.20gb', 2, 2, 20.0, 3, (0, 2, 4)),
    '3g.40gb': Profile('3g.40gb', 3, 4, 40.0, 2, (0, 4)),
    '4g.40gb': Profile('4g.40gb', 4, 4, 40.0, 1, (0,)),
    '7g.80gb': Profile('7g.80gb', 7, 8, 80.0, 1, (0,)),
})
# H100 80GB (SXM/PCIe): same slice geometry as A100 80GB.
_H100_80 = Family('H100-80GB', 7, 8, 80.0, dict(_A100_80.profiles))
# RTX PRO 6000 Blackwell Server Edition (96 GiB): four equal partitions, no NVLink.
# Profiles observed with `nvidia-smi mig -lgip` on the target cluster (Sep 2026).
_RTX_PRO_6000 = Family('RTX-PRO-6000-Blackwell', 4, 4, 96.0, {
    '1g.24gb': Profile('1g.24gb', 1, 1, 24.0, 4, (0, 1, 2, 3)),
    '2g.48gb': Profile('2g.48gb', 2, 2, 48.0, 2, (0, 2)),
    '4g.96gb': Profile('4g.96gb', 4, 4, 96.0, 1, (0,)),
})

FAMILIES : tuple[Family, ...] = (_A100_80, _H100_80, _RTX_PRO_6000)

def family_for(product : str) -> Family | None:
    for family in FAMILIES:
        if family.key in product:
            return family
    return None

def check_layout(product : str, layout : Mapping[str, int]) -> list[str]:
    '''
    Problems with placing ``layout`` (profile name -> instance count) on one card
    of ``product``: memory over capacity, compute slices over capacity, per-profile
    instance caps, and — the check the planner lacks — whether the instances can
    actually be placed at legal, non-overlapping positions.
    '''
    family = family_for(product)
    if family is None:
        return [f'no independent geometry table for product {product!r}']
    problems : list[str] = []
    unknown = sorted(name for name in layout if name not in family.profiles)
    if unknown:
        return [f'unknown profile(s) {unknown} for {family.key}']
    memory = sum(family.profiles[n].memory_gib * c for n, c in layout.items())
    if memory > family.memory_gib:
        problems.append(f'requested {memory:g} GiB of MIG memory on a {family.memory_gib:g} GiB card')
    compute = sum(family.profiles[n].compute_slices * c for n, c in layout.items())
    if compute > family.compute_slices:
        problems.append(f'requested {compute} compute slices on a card with {family.compute_slices}')
    mem_slices = sum(family.profiles[n].memory_slices * c for n, c in layout.items())
    if mem_slices > family.memory_slices:
        problems.append(f'requested {mem_slices} memory slices on a card with {family.memory_slices}')
    for name, count in layout.items():
        if count > family.profiles[name].max_instances:
            problems.append(f'{count} x {name} exceeds the {family.profiles[name].max_instances} instances the card allows')
    if not problems and not _placeable(family, layout):
        problems.append('no legal non-overlapping placement exists for this combination')
    return problems

def _placeable(family : Family, layout : Mapping[str, int]) -> bool:
    '''Backtracking placement over legal start positions; instances are few, so exhaustive is cheap.'''
    instances = [family.profiles[n] for n, c in layout.items() for _ in range(c)]
    instances.sort(key = lambda p: -p.memory_slices)
    occupied = [False] * family.memory_slices

    def place(i : int) -> bool:
        if i == len(instances):
            return True
        profile = instances[i]
        for start in profile.starts:
            span = range(start, start + profile.memory_slices)
            if span.stop > family.memory_slices or any(occupied[s] for s in span):
                continue
            for s in span:
                occupied[s] = True
            if place(i + 1):
                return True
            for s in span:
                occupied[s] = False
        return False

    return place(0)

def smallest_profile(product : str, memory_gib : float) -> Profile | None:
    family = family_for(product)
    if family is None:
        return None
    fitting = [p for p in family.profiles.values() if p.memory_gib >= memory_gib]
    return min(fitting, key = lambda p: (p.memory_gib, p.compute_slices)) if fitting else None
