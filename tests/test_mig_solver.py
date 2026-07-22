'''
The mix-mode MIG layout solver (videoflow.deploy.mig, RFC 0004).

Pure/unit: the solver never touches a cluster — every test hands it a fabricated
inventory and compiled-flow specs and checks the card plan, the resolved
extended resources, and the generated mig-parted config. Determinism is load-
bearing (prepare() idempotency and a truthful `explain` both ride on it), so it
is asserted explicitly.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.core.compiler import NodeSpec
from videoflow.deploy.mig import (
    LayoutError,
    MigProfile,
    MigTable,
    NodeInventory,
    layout_to_mig_parted_config,
    mig_table_for_product,
    register_mig_table,
    solve_layout,
)


def _gpu_spec(name, gpu_count = 1, gpu_memory_gib = None, nb_tasks = 1):
    return NodeSpec(name, 'videoflow.processors.basic.IdentityProcessor', {}, [],
                    'processor', True, nb_tasks, 'gpu', True,
                    gpu_count = gpu_count, gpu_memory_gib = gpu_memory_gib)


def _a100_80(name = 'gpu-a', cards = 2):
    return NodeInventory(name, 'NVIDIA-A100-SXM4-80GB', cards, 80)


def _consumer_gpu(name = 'gpu-dev', cards = 1):
    return NodeInventory(name, 'NVIDIA-GeForce-RTX-3090', cards, 24)


# -- profile tables --------------------------------------------------------

def test_mig_table_matches_gfd_product_labels():
    assert mig_table_for_product('NVIDIA-A100-SXM4-80GB').family == 'A100-80GB'
    assert mig_table_for_product('NVIDIA-A100-PCIE-40GB').family == 'A100-40GB'
    assert mig_table_for_product('NVIDIA-A30').family == 'A30'
    assert mig_table_for_product('NVIDIA-H100-80GB-HBM3').family == 'H100-80GB'
    # Consumer cards have no MIG support at all.
    assert mig_table_for_product('NVIDIA-GeForce-RTX-3090') is None


def test_smallest_profile_is_chosen_by_memory_then_slices():
    table = mig_table_for_product('NVIDIA-A100-SXM4-80GB')
    assert table.smallest_profile_for(8).name == '1g.10gb'
    assert table.smallest_profile_for(10).name == '1g.10gb'
    assert table.smallest_profile_for(11).name == '2g.20gb'
    # 40 GiB fits both 3g.40gb and 4g.40gb: fewer slices wins.
    assert table.smallest_profile_for(40).name == '3g.40gb'
    assert table.smallest_profile_for(81) is None


def test_register_mig_table_extends_the_solver(monkeypatch):
    from videoflow.deploy import mig
    monkeypatch.setattr(mig, '_MIG_TABLES', dict(mig._MIG_TABLES))
    register_mig_table(MigTable('B200', ['B200'], 7, [MigProfile('1g.23gb', 23, 1, 7)]))
    assert mig_table_for_product('NVIDIA-B200-SXM-180GB').family == 'B200'


# -- solving ---------------------------------------------------------------

def test_sharers_pack_onto_one_card_and_spanners_take_whole_cards():
    specs = [_gpu_spec('span', gpu_count = 2),
             _gpu_spec('share', gpu_memory_gib = 10, nb_tasks = 3)]
    layout = solve_layout([_a100_80(cards = 3)], specs)
    whole = [c for c in layout.cards if not c.is_mig]
    mig_cards = [c for c in layout.cards if c.is_mig]
    assert len(whole) == 2 and len(mig_cards) == 1
    assert mig_cards[0].profiles == {'1g.10gb': 3}
    assert layout.spec_resources == {'share': 'nvidia.com/mig-1g.10gb'}
    assert layout.slice_demand == {'nvidia.com/mig-1g.10gb': 3}


def test_undeclared_demand_gets_a_whole_card():
    # A plain device_type=GPU node means the same thing in both modes: one whole
    # physical device (the degenerate spanner).
    layout = solve_layout([_a100_80(cards = 2)], [_gpu_spec('plain')])
    assert layout.spec_resources == {}
    assert sum(1 for c in layout.cards if not c.is_mig) == 2


def test_mixed_profile_sizes_share_one_card():
    # Two 20 GiB + three 10 GiB demands fit one A100-80: 2x2g.20gb + 3x1g.10gb = 7 slices.
    specs = [_gpu_spec('big', gpu_memory_gib = 20, nb_tasks = 2),
             _gpu_spec('small', gpu_memory_gib = 10, nb_tasks = 3)]
    layout = solve_layout([_a100_80(cards = 1)], specs)
    [card] = [c for c in layout.cards if c.is_mig]
    assert card.profiles == {'1g.10gb': 3, '2g.20gb': 2}
    assert layout.spec_resources == {'big': 'nvidia.com/mig-2g.20gb',
                                     'small': 'nvidia.com/mig-1g.10gb'}


def test_spanners_prefer_non_mig_capable_nodes():
    # The consumer node cannot be partitioned, so the spanner should burn it and
    # leave the A100 free for the sharer.
    specs = [_gpu_spec('span', gpu_count = 1),
             _gpu_spec('share', gpu_memory_gib = 10)]
    layout = solve_layout([_a100_80(cards = 1), _consumer_gpu()], specs)
    consumer_card = next(c for c in layout.cards if c.node == 'gpu-dev')
    a100_card = next(c for c in layout.cards if c.node == 'gpu-a')
    assert not consumer_card.is_mig
    assert a100_card.profiles == {'1g.10gb': 1}


def test_solver_is_deterministic():
    specs = [_gpu_spec('span', gpu_count = 2),
             _gpu_spec('big', gpu_memory_gib = 40, nb_tasks = 2),
             _gpu_spec('small', gpu_memory_gib = 5, nb_tasks = 4)]
    inventory = [_a100_80('gpu-b', 2), _a100_80('gpu-a', 3)]
    first = solve_layout(inventory, specs)
    second = solve_layout(list(reversed(inventory)), list(reversed(specs)))
    assert first == second


def test_replicas_of_one_sharer_resolve_to_one_resource_name():
    # 20 GiB is 2g.20gb on A100-80 but 3g.20gb on A100-40: all replicas of one
    # spec must land on ONE family, because a spec requests one resource name.
    specs = [_gpu_spec('share', gpu_memory_gib = 20, nb_tasks = 2)]
    inventory = [NodeInventory('gpu-40', 'NVIDIA-A100-PCIE-40GB', 1, 40),
                 _a100_80('gpu-80', 1)]
    layout = solve_layout(inventory, specs)
    assert len(set(layout.spec_resources.values())) == 1
    assert layout.slice_demand[layout.spec_resources['share']] == 2


def test_infeasible_spanner_names_the_gap():
    with pytest.raises(LayoutError, match = 'whole GPU'):
        solve_layout([_a100_80(cards = 1)], [_gpu_spec('span', gpu_count = 2)])


def test_infeasible_sharer_names_the_gap():
    # 90 GiB exceeds every profile on every registered family.
    with pytest.raises(LayoutError, match = 'MIG slice'):
        solve_layout([_a100_80()], [_gpu_spec('share', gpu_memory_gib = 90)])
    # A consumer-only pool has no MIG capability at all.
    with pytest.raises(LayoutError, match = 'MIG-capable'):
        solve_layout([_consumer_gpu()], [_gpu_spec('share', gpu_memory_gib = 10)])


def test_empty_inventory_is_a_layout_error():
    with pytest.raises(LayoutError, match = 'GPU Feature Discovery'):
        solve_layout([], [_gpu_spec('share', gpu_memory_gib = 10)])


def test_failed_family_attempt_leaves_no_residue():
    # 24 GiB fits A30's 4g.24gb but two replicas exceed one A30 card (4 slices
    # each, 4 total) — the solver must roll back the A30 attempt cleanly and
    # place both on the A100 family instead.
    specs = [_gpu_spec('share', gpu_memory_gib = 24, nb_tasks = 2)]
    inventory = [NodeInventory('a30-box', 'NVIDIA-A30', 1, 24), _a100_80('gpu-a', 1)]
    layout = solve_layout(inventory, specs)
    a30_card = next(c for c in layout.cards if c.node == 'a30-box')
    assert not a30_card.is_mig                       # rollback left it untouched
    assert layout.spec_resources['share'] == 'nvidia.com/mig-3g.40gb'


# -- mig-parted config emission --------------------------------------------

def test_mig_parted_config_covers_every_card_of_a_mig_node():
    specs = [_gpu_spec('share', gpu_memory_gib = 10, nb_tasks = 2)]
    layout = solve_layout([_a100_80('gpu-a', 2)], specs)
    config = layout_to_mig_parted_config(layout)
    assert config == (
        'version: v1\n'
        'mig-configs:\n'
        '  videoflow-gpu-a:\n'
        '    - devices: [0]\n'
        '      mig-enabled: true\n'
        '      mig-devices:\n'
        '        1g.10gb: 2\n'
        '    - devices: [1]\n'
        '      mig-enabled: false\n'
    )


def test_mig_parted_config_is_empty_without_mig_cards():
    layout = solve_layout([_a100_80()], [_gpu_spec('span', gpu_count = 2)])
    assert layout.mig_nodes() == []
    assert layout_to_mig_parted_config(layout) == 'version: v1\nmig-configs:\n'


if __name__ == '__main__':
    pytest.main([__file__])
