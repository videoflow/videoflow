'''
MIG geometry knowledge and the mix-mode layout solver (RFC 0004).

The ``mix`` GPU strategy turns declared demands — spanners needing whole physical
devices (``gpu_count``) and sharers needing an isolated fraction of one device
(``gpu_memory_gib``) — into a concrete per-card plan: which cards stay whole,
which get MIG-partitioned, and into which profiles. Everything in this module is
pure and deterministic: it computes against an inventory handed to it and never
talks to a cluster, which is what makes the solver exhaustively unit-testable.
The strategy (``deploy/gpu.py``) owns fetching the inventory and applying the
result; ``deploy/cluster.py`` owns reading the inventory off GPU Feature
Discovery labels.

The card model is deliberately simpler than real MIG placement rules: a card
holds any multiset of profiles whose slice total fits its capacity, subject to
each profile's per-card maximum. NVIDIA's placement constraints are slightly
stricter for some mixed geometries; ``nvidia-mig-parted`` remains the authority
and will reject a combination it cannot place, with the generated config in hand
for the operator to adjust. The table errs on the side of profiles that are
valid everywhere.
'''
from __future__ import absolute_import, division, print_function

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from ..core.compiler import NodeSpec

logger = logging.getLogger(__package__)


class LayoutError(ValueError):
    '''No feasible MIG layout exists for the flow's demands on this inventory.
    The message names the demand that failed and the fix.'''


@dataclass
class MigProfile:
    '''One MIG profile a card family supports: ``1g.10gb`` needs 1 of the card's
    compute slices and yields a 10 GiB instance, at most ``max_per_gpu`` per card.'''
    name : str
    memory_gib : float
    slices : int
    max_per_gpu : int

    @property
    def resource(self) -> str:
        '''The Kubernetes extended resource the device plugin advertises for it.'''
        return f'nvidia.com/mig-{self.name}'


@dataclass
class MigTable:
    '''The MIG geometry of one GPU family: how to recognize it from the GFD
    product label, how many compute slices a card has, and the profiles it offers.'''
    family : str                      # registry key, e.g. 'A100-80GB'
    match_substrings : List[str]      # all must appear in nvidia.com/gpu.product
    total_slices : int
    profiles : List[MigProfile]

    def matches(self, product : str) -> bool:
        return all(s in product for s in self.match_substrings)

    def smallest_profile_for(self, memory_gib : float) -> Optional[MigProfile]:
        fitting = [p for p in self.profiles if p.memory_gib >= memory_gib]
        return min(fitting, key = lambda p: (p.memory_gib, p.slices)) if fitting else None


_MIG_TABLES : Dict[str, MigTable] = {}


def register_mig_table(table : MigTable) -> None:
    '''Registers a GPU family's MIG geometry. Same extension shape as the GPU
    strategy and cluster-flavor registries: a new card family is one call, not an
    edit to the solver.'''
    if not table.family:
        raise ValueError('MigTable must set a non-empty family')
    _MIG_TABLES[table.family] = table


def mig_table_for_product(product : str) -> Optional[MigTable]:
    '''The registered MIG table matching a GFD ``nvidia.com/gpu.product`` label,
    or None for a card with no MIG support (consumer GPUs, unknown families).'''
    for family in sorted(_MIG_TABLES):
        if _MIG_TABLES[family].matches(product):
            return _MIG_TABLES[family]
    return None


# Profiles that are valid in any geometry, per NVIDIA's supported-profile tables.
register_mig_table(MigTable('A30', ['A30'], 4, [
    MigProfile('1g.6gb', 6, 1, 4),
    MigProfile('2g.12gb', 12, 2, 2),
    MigProfile('4g.24gb', 24, 4, 1),
]))
register_mig_table(MigTable('A100-40GB', ['A100', '40GB'], 7, [
    MigProfile('1g.5gb', 5, 1, 7),
    MigProfile('2g.10gb', 10, 2, 3),
    MigProfile('3g.20gb', 20, 3, 2),
    MigProfile('4g.20gb', 20, 4, 1),
    MigProfile('7g.40gb', 40, 7, 1),
]))
register_mig_table(MigTable('A100-80GB', ['A100', '80GB'], 7, [
    MigProfile('1g.10gb', 10, 1, 7),
    MigProfile('2g.20gb', 20, 2, 3),
    MigProfile('3g.40gb', 40, 3, 2),
    MigProfile('4g.40gb', 40, 4, 1),
    MigProfile('7g.80gb', 80, 7, 1),
]))
register_mig_table(MigTable('H100-80GB', ['H100', '80GB'], 7, [
    MigProfile('1g.10gb', 10, 1, 7),
    MigProfile('1g.20gb', 20, 1, 4),
    MigProfile('2g.20gb', 20, 2, 3),
    MigProfile('3g.40gb', 40, 3, 2),
    MigProfile('4g.40gb', 40, 4, 1),
    MigProfile('7g.80gb', 80, 7, 1),
]))
register_mig_table(MigTable('H100-94GB', ['H100', '94GB'], 7, [
    MigProfile('1g.12gb', 12, 1, 7),
    MigProfile('1g.24gb', 24, 1, 4),
    MigProfile('2g.24gb', 24, 2, 3),
    MigProfile('3g.47gb', 47, 3, 2),
    MigProfile('4g.47gb', 47, 4, 1),
    MigProfile('7g.94gb', 94, 7, 1),
]))


@dataclass
class NodeInventory:
    '''One GPU node's physical inventory, as read off GFD labels
    (``cluster.gpu_inventory``).'''
    name : str
    product : str
    card_count : int
    memory_gib_per_card : float


@dataclass
class CardPlan:
    '''What one physical card becomes under the layout: whole (``profiles`` is
    None — spanner or undeclared-demand territory) or MIG'd into ``profiles``
    (profile name -> instance count).'''
    node : str
    card_index : int
    profiles : Optional[Dict[str, int]] = None

    @property
    def is_mig(self) -> bool:
        return self.profiles is not None


@dataclass
class GpuLayout:
    '''
    The solver's answer: the per-card plan, each sharer spec's resolved extended
    resource, and the total slice demand per resource (what the device plugin
    must advertise once the geometry is applied — preflight compares against it).
    '''
    cards : List[CardPlan] = field(default_factory = list)
    spec_resources : Dict[str, str] = field(default_factory = dict)
    slice_demand : Dict[str, int] = field(default_factory = dict)

    def mig_nodes(self) -> List[str]:
        '''Nodes with at least one MIG'd card, sorted — the nodes whose geometry
        ``prepare()`` must apply.'''
        return sorted({c.node for c in self.cards if c.is_mig})


class _Card:
    '''Mutable per-card packing state used only inside the solver.'''
    def __init__(self, node : str, index : int, table : Optional[MigTable]) -> None:
        self.node = node
        self.index = index
        self.table = table
        self.whole_owner : Optional[str] = None      # spanner spec name, once taken
        self.mig_counts : Dict[str, int] = {}        # profile name -> count
        self.slices_used = 0

    @property
    def free(self) -> bool:
        return self.whole_owner is None and not self.mig_counts

    def fits(self, profile : MigProfile) -> bool:
        if self.whole_owner is not None or self.table is None:
            return False
        if self.slices_used + profile.slices > self.table.total_slices:
            return False
        return self.mig_counts.get(profile.name, 0) < profile.max_per_gpu

    def add(self, profile : MigProfile) -> None:
        self.mig_counts[profile.name] = self.mig_counts.get(profile.name, 0) + 1
        self.slices_used += profile.slices


def solve_layout(inventory : List[NodeInventory], specs : List[NodeSpec]) -> GpuLayout:
    '''
    Computes a feasible card layout for the flow's GPU demands, or raises
    ``LayoutError`` naming the demand that cannot be placed and the fix.

    Deterministic (sorted walks throughout) so re-running against the same
    inventory reproduces the same geometry — which is what makes ``prepare()``
    idempotent and ``explain`` truthful about what deploy will do. Placement
    order:

    1. **Spanners first** — every replica of a GPU spec with no declared
       ``gpu_memory_gib`` takes ``gpu_count`` whole cards on one node
       (``gpu_count`` defaults to 1: the undeclared-demand node is the degenerate
       spanner). Rigid demand, so it goes first, largest claims first.
    2. **Sharers packed second** — every replica of a spec with
       ``gpu_memory_gib`` takes one MIG instance of the smallest fitting profile,
       first-fit onto already-open MIG cards, opening new cards as needed. All of
       one spec's replicas use one profile on one card family, so the spec
       resolves to a single extended-resource name.
    '''
    cards : List[_Card] = []
    for node in sorted(inventory, key = lambda n: n.name):
        table = mig_table_for_product(node.product)
        for index in range(node.card_count):
            cards.append(_Card(node.name, index, table))
    if not cards:
        raise LayoutError('the GPU pool has no cards to lay out — no node advertises '
                          'GPU Feature Discovery labels (nvidia.com/gpu.count/.product). '
                          'mix mode needs GFD to see the inventory; install it alongside '
                          'the device plugin, or use --gpu-mode exclusive.')

    gpu_specs = [s for s in specs if s.device_type == 'gpu']
    spanners = sorted((s for s in gpu_specs if s.gpu_memory_gib is None),
                      key = lambda s: (-s.gpu_count, s.name))
    sharers = sorted((s for s in gpu_specs if s.gpu_memory_gib is not None),
                     key = lambda s: (-float(s.gpu_memory_gib or 0), s.name))

    layout = GpuLayout()

    # Spanners prefer nodes whose cards cannot be MIG'd anyway (consumer/unknown
    # families) so whole-card claims don't starve the sharers of partitionable cards.
    def _node_order(node_name : str) -> tuple:
        mig_capable = any(c.node == node_name and c.table is not None for c in cards)
        return (mig_capable, node_name)

    for spec in spanners:
        for _replica in range(spec.nb_tasks):
            placed = False
            for node_name in sorted({c.node for c in cards}, key = _node_order):
                free = [c for c in cards if c.node == node_name and c.free]
                if len(free) >= spec.gpu_count:
                    for card in free[:spec.gpu_count]:
                        card.whole_owner = spec.name
                    placed = True
                    break
            if not placed:
                largest = max((sum(1 for c in cards if c.node == n and c.free)
                               for n in {c.node for c in cards}), default = 0)
                raise LayoutError(
                    f'node {spec.name!r} needs {spec.gpu_count} whole GPU(s) per replica on one '
                    f'host, but after placing larger demands the best node has only {largest} '
                    f'free card(s). Fix: add GPU capacity, reduce gpu_count/nb_tasks, or give '
                    f'smaller nodes a gpu_memory_gib so they share a card instead.')

    for spec in sharers:
        memory = float(spec.gpu_memory_gib or 0)
        placed_cards = _place_sharer(cards, spec.name, memory, spec.nb_tasks)
        if placed_cards is None:
            families = sorted({c.table.family for c in cards if c.table is not None})
            raise LayoutError(
                f'node {spec.name!r} needs {spec.nb_tasks} x {memory} GiB MIG slice(s) but no '
                f'single card family can hold them (MIG-capable families in the pool: '
                f'{families or "none"}). Fix: add MIG-capable capacity, lower gpu_memory_gib '
                f'or nb_tasks, or drop gpu_memory_gib to grant whole devices.')
        profile = placed_cards[0]
        layout.spec_resources[spec.name] = profile.resource
        layout.slice_demand[profile.resource] = (
            layout.slice_demand.get(profile.resource, 0) + spec.nb_tasks)

    for card in sorted(cards, key = lambda c: (c.node, c.index)):
        if card.mig_counts:
            layout.cards.append(CardPlan(card.node, card.index, dict(sorted(card.mig_counts.items()))))
        else:
            layout.cards.append(CardPlan(card.node, card.index, None))
    return layout


def _place_sharer(cards : List[_Card], spec_name : str, memory : float,
                  replicas : int) -> Optional[List[MigProfile]]:
    '''
    Places all ``replicas`` slices of one sharer onto one card family, or returns
    None. All-or-nothing: a partial placement is rolled back, so a failed family
    leaves no residue. Returns the profile used (one entry per replica; all the
    same profile, so callers read ``[0]``).
    '''
    families = sorted({c.table.family for c in cards if c.table is not None})
    for family in families:
        family_cards = [c for c in cards if c.table is not None and c.table.family == family]
        profile = family_cards[0].table.smallest_profile_for(memory)   # type: ignore[union-attr]
        if profile is None:
            continue
        added : List[_Card] = []
        for _replica in range(replicas):
            # First-fit: open MIG cards first (pack tight), then a free card.
            target = next((c for c in family_cards if c.mig_counts and c.fits(profile)), None)
            if target is None:
                target = next((c for c in family_cards if c.free and c.fits(profile)), None)
            if target is None:
                for card in added:      # roll back this family attempt entirely
                    card.mig_counts[profile.name] -= 1
                    if card.mig_counts[profile.name] == 0:
                        del card.mig_counts[profile.name]
                    card.slices_used -= profile.slices
                added = []
                break
            target.add(profile)
            added.append(target)
        else:
            return [profile] * replicas
    return None


def layout_to_mig_parted_config(layout : GpuLayout, config_prefix : str = 'videoflow') -> str:
    '''
    The ``nvidia-mig-parted`` config implementing ``layout``, as YAML text — one
    named config per MIG'd node (``<prefix>-<node>``), covering every card on
    that node so untouched cards are explicitly ``mig-enabled: false``. This is
    both what ``prepare()`` applies through the GPU Operator's MIG manager and
    what preflight prints for manual application when no MIG manager is present.

    Emitted by string assembly rather than a YAML library on purpose: the shape
    is fixed and tiny, and this module must import cleanly without the optional
    ``yaml`` extra.
    '''
    lines = ['version: v1', 'mig-configs:']
    for node in layout.mig_nodes():
        lines.append(f'  {config_prefix}-{node}:')
        for card in [c for c in layout.cards if c.node == node]:
            lines.append(f'    - devices: [{card.card_index}]')
            if card.profiles:
                lines.append('      mig-enabled: true')
                lines.append('      mig-devices:')
                for profile_name, count in card.profiles.items():
                    lines.append(f'        {profile_name}: {count}')
            else:
                lines.append('      mig-enabled: false')
    return '\n'.join(lines) + '\n'
