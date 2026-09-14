'''
Conformance cases: ALLOC-002.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it.

The oracle is the vendor's placement table — ``fixtures/mig_placements.json``, a
pinned copy of NVIDIA's "GPU instance profile placements", which on hardware is
compared with the card's own ``nvidia-smi mig -lgipp`` answer — and never the
solver's tables: the model variant checks that ``videoflow.deploy.mig`` carries
the same data and accepts exactly the placeable multisets, the gpu-level primary
instantiates every accepted layout on an isolated, idle card and reads the
instances back.
'''
from __future__ import absolute_import, division, print_function

import itertools
import json
import os
import pathlib
import random
from typing import Any, Dict, List, Optional, Sequence

import defects
import defects_alloc
import pytest
from _status import not_run

from videoflow.deploy import mig

FIXTURE = pathlib.Path(__file__).parent / 'fixtures' / 'mig_placements.json'


def _vendor_table() -> Dict[str, Any]:
    return json.loads(FIXTURE.read_text())


def _oracle_place(names : Sequence[str], family : Dict[str, Any]) -> Optional[List[int]]:
    '''Exhaustive placement over the vendor table: legal starts per profile, or None.'''
    grid = int(family['grid'])
    occupied = [False] * grid
    starts : List[int] = [-1] * len(names)
    order = sorted(range(len(names)), key = lambda i: -int(family['profiles'][names[i]]['width']))

    def rec(k : int) -> bool:
        if k == len(order):
            return True
        profile = family['profiles'][names[order[k]]]
        width = int(profile['width'])
        for start in profile['starts']:
            if start + width <= grid and not any(occupied[start:start + width]):
                for pos in range(start, start + width):
                    occupied[pos] = True
                starts[order[k]] = start
                if rec(k + 1):
                    return True
                for pos in range(start, start + width):
                    occupied[pos] = False
        return False

    return starts if rec(0) else None


def _fits_budgets(names : Sequence[str], family : Dict[str, Any]) -> bool:
    '''The two budgets that are not placement: compute slices and card memory.'''
    profiles = family['profiles']
    return (sum(int(profiles[n]['compute_slices']) for n in names) <= int(family['compute_slices'])
            and sum(float(profiles[n]['memory_gib']) for n in names) <= float(family['memory_gib']))


def _multisets(family : Dict[str, Any]) -> List[List[str]]:
    '''Every non-empty multiset of the family's profiles within the per-card maxima.'''
    names = sorted(family['profiles'])
    ranges = [range(int(family['profiles'][n]['max']) + 1) for n in names]
    out = []
    for counts in itertools.product(*ranges):
        ms = [n for n, c in zip(names, counts) for _ in range(c)]
        if ms:
            out.append(ms)
    return out


def _solver_accepts(names : Sequence[str], table : mig.MigTable, memory_gib : float, order : Sequence[str]) -> bool:
    '''Whether the solver's card model takes the multiset when fed in ``order``.'''
    by_name = {p.name: p for p in table.profiles}
    card = mig._Card('gpu', 0, table, memory_gib = memory_gib)
    for name in order:
        profile = by_name[name]
        if not card.fits(profile):
            return False
        card.add(profile)
    return True


def _oracle_alloc_002_model(seed : int, evidence : Dict[str, Any]) -> None:
    '''
    For every family the vendor table knows: the solver's table carries the same
    placement data, and over every multiset within the per-card maxima the solver
    accepts exactly those the vendor grid can place (given the compute and memory
    budgets), in whatever order the request arrives.
    '''
    vendor = _vendor_table()['families']
    rng = random.Random(seed)
    summary : Dict[str, Any] = {}
    for family_name, family in sorted(vendor.items()):
        table = next((t for t in mig._MIG_TABLES.values() if t.family == family_name), None)
        assert table is not None, f'the solver knows no family {family_name}'
        assert table.grid == int(family['grid']), (family_name, table.grid)
        for name, profile in family['profiles'].items():
            mine = next((p for p in table.profiles if p.name == name), None)
            assert mine is not None, f'{family_name}: solver lacks profile {name}'
            assert (mine.width, tuple(mine.placements), mine.max_per_gpu, mine.slices) == \
                (int(profile['width']), tuple(profile['starts']), int(profile['max']), int(profile['compute_slices'])), \
                f'{family_name}/{name}: solver placement data differs from the vendor table'
        accepted, rejected, impossible = 0, 0, 0
        disagreements = []
        for ms in _multisets(family):
            expected = _fits_budgets(ms, family) and _oracle_place(ms, family) is not None
            orders = [list(ms)]
            for _ in range(2):
                shuffled = list(ms)
                rng.shuffle(shuffled)
                orders.append(shuffled)
            verdicts = {_solver_accepts(ms, table, float(family['memory_gib']), order) for order in orders}
            if len(verdicts) != 1:
                disagreements.append({'multiset': ms, 'reason': 'order-dependent verdict'})
                continue
            got = verdicts.pop()
            if got != expected:
                disagreements.append({'multiset': ms, 'solver': got, 'oracle': expected})
            if expected:
                accepted += 1
            elif _fits_budgets(ms, family):
                impossible += 1         # fits the totals, unplaceable — the case's subject
            else:
                rejected += 1
        summary[family_name] = {'placeable': accepted, 'unplaceable_by_grid_only': impossible,
                                'over_budget': rejected, 'disagreements': disagreements}
        assert not disagreements, f'{family_name}: {disagreements[:5]}'
        assert accepted > 0, f'{family_name}: the solver rejects every layout — a shortcut, not a placement check'
    evidence['families'] = summary


@pytest.mark.case('ALLOC-002')
@pytest.mark.level('model')
@pytest.mark.variant('vendor-table')
def test_alloc_002_solver_agrees_with_the_vendor_placement_table(seed, evidence_dir) -> None:
    '''Every family's multisets, three request orders each, against the pinned vendor grid.'''
    evidence : Dict[str, Any] = {}
    _oracle_alloc_002_model(seed, evidence)
    (evidence_dir / 'placement_oracle.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-002')
def test_alloc_002_detects_a_totals_only_card(monkeypatch) -> None:
    defects_alloc.totals_only_card(monkeypatch)
    assert defects.detects(_oracle_alloc_002_model, 0, {})


def _family_for(product : str) -> tuple[str, Dict[str, Any]]:
    vendor = _vendor_table()['families']
    for name, family in vendor.items():
        if all(s in product for s in family['product_substrings']):
            return name, family
    raise LookupError(product)


@pytest.mark.case('ALLOC-002')
@pytest.mark.level('gpu')
def test_alloc_002_validate_mig_placement_constraints_independently_of(gpu, evidence_dir) -> None:
    '''
    ALLOC-002 (P0, allocation, gpu): Validate MIG placement constraints independently of slice
    totals.

    Acceptance: Every accepted sampled layout can be instantiated exactly; all independently
    known impossible layouts are rejected without changing the card.

    On one idle host card named by ``VF_TEST_MIG_GPU_UUID`` (one of the ``gpu``
    gate's devices, never GPUs 0/1 of this host): the card's own placement query
    is compared with the pinned vendor table and the solver's; MIG mode is
    switched on (restored at the end, whatever happens); every layout the solver
    accepts for the family is created through ``nvidia-smi mig -cgi`` and read
    back through ``-lgi``; every multiset the vendor grid cannot place is refused
    by the solver, and the mutation timeline shows no create for it.
    '''
    import _mig
    from _gpu import busy_now
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
        # Idle for compute is not idle for MIG: a driver client with the device
        # open (here, a container started with every card visible) makes every
        # instance creation fail "In use by another client". Never ours to stop.
        not_run(f'/dev/nvidia{index} is held open by {holders}; MIG instances cannot be created on it')
    family_name, family = _family_for(str(device['product']).replace(' ', '-'))
    table = next(t for t in mig._MIG_TABLES.values() if t.family == family_name)
    timeline = _mig.Timeline()
    evidence : Dict[str, Any] = {'device': device, 'family': family_name}
    # 1. The card's own placement answer versus the pinned vendor table and the solver's table.
    live = _mig.placements_query(index, timeline)
    ids = _mig.profiles_query(index, timeline)
    evidence['placement_query'] = {name: {'id': ids[name]['id'], **live[ids[name]['id']]} for name in family['profiles'] if name in ids}
    for name, profile in family['profiles'].items():
        assert name in ids, f'the card offers no profile {name}'
        got = live[ids[name]['id']]
        assert (got['width'], got['starts']) == (int(profile['width']), tuple(profile['starts'])), (name, got)
        mine = next(p for p in table.profiles if p.name == name)
        assert (mine.width, tuple(mine.placements)) == (got['width'], got['starts']), (name, mine)
    # 2. The sampled layouts: every multiset within the maxima, sorted by the solver's verdict.
    accepted, impossible = [], []
    for ms in _multisets(family):
        placeable = _fits_budgets(ms, family) and _oracle_place(ms, family) is not None
        solver = _solver_accepts(ms, table, float(family['memory_gib']), ms)
        assert solver == placeable, {'multiset': ms, 'solver': solver, 'vendor_grid': placeable}
        (accepted if solver else impossible).append(ms)
    evidence['sampled'] = {'accepted': accepted, 'impossible': impossible}
    assert accepted, 'nothing to instantiate'
    # 3. Instantiate every accepted layout on the isolated card; read back what exists.
    before = _mig.instances_query(index, timeline)
    assert before == [], f'the card already holds instances: {before}'
    mode_before = _mig.mig_mode(index)
    rounds = []
    try:
        _mig.set_mig_mode(index, True, timeline)
        for ms in accepted:
            profile_ids = [ids[name]['id'] for name in ms]
            rc, out = _mig.create_instances(index, profile_ids, timeline)
            instances = _mig.instances_query(index, timeline)
            record = {'layout': ms, 'create_exit': rc, 'instances': instances}
            rounds.append(record)
            assert rc == 0, f'the driver refused an accepted layout {ms}: {out.strip()[-300:]}'
            assert sorted(i['name'] for i in instances) == sorted(ms), record
            spans = sorted((i['start'], i['start'] + i['width']) for i in instances)
            assert all(a[1] <= b[0] for a, b in zip(spans, spans[1:])), record
            for inst in instances:
                assert inst['start'] in family['profiles'][inst['name']]['starts'], record
                assert inst['width'] == int(family['profiles'][inst['name']]['width']), record
            _mig.destroy_instances(index, timeline)
            assert _mig.instances_query(index, timeline) == [], 'instances survived the destroy'
        # 4. The impossible layouts: the solver refused them before any command ran.
        creates_before = len([e for e in timeline.mutations() if '-cgi' in e['command']])
        for ms in impossible:
            assert not _solver_accepts(ms, table, float(family['memory_gib']), ms), ms
        assert len([e for e in timeline.mutations() if '-cgi' in e['command']]) == creates_before
        assert creates_before == len(accepted)
    finally:
        _mig.destroy_instances(index, timeline)
        restored = _mig.set_mig_mode(index, False, timeline)
        evidence['restore'] = {'mode_before': mode_before, 'mode_after': restored,
                               'instances_after': _mig.instances_query(index, timeline)}
        evidence['rounds'] = rounds
        evidence['timeline'] = timeline.entries
        (evidence_dir / 'placements.json').write_text(json.dumps(evidence, indent = 2, default = str))
        leaked = busy_now([uuid])
        assert not leaked, leaked
    assert evidence['restore']['mode_after'] == 'Disabled' and evidence['restore']['instances_after'] == []
