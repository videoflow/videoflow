'''
Provenance-aware resolution of a node's GPU requirement (RFC 0003 / RFC 0004).

A node's GPU requirement — ``device_type``, ``gpu_count``, ``gpu_memory_gib`` and,
at deploy time, the extended-resource name — can be declared in several places:
an explicit argument on the node or on ``component(...)``, a remote component's
``component.yaml`` (``spec.resources.gpu``, and its ``device`` list), the deploy
CLI's ``--gpu-resource-name`` default, and a GPU strategy's own resolution (the
``mix`` solver stamps a sharer's MIG profile). Before this module the merge was
spread over ``core/remote.py``, ``core/node.py`` and ``deploy/gpu.py`` with
``x if y is None else z`` chains: the precedence was implicit, nobody could tell
afterwards *where* a resolved value had come from, and two declarations that
genuinely contradicted each other were silently overridden or reported without
naming the loser.

The resolver here is one pure function over tagged declarations. Every candidate
value arrives as a ``Declaration`` naming its ``field``, its ``source`` and whether
it is **hard** (a requirement) or **soft** (a default another source may
override). The rules, which reproduce exactly what the framework did before for
every input it accepted:

1. A hard declaration always beats a soft one. Among declarations of equal
   hardness the source order ``SOURCE_PRECEDENCE`` decides: node, strategy,
   descriptor, cli-default, default. So an explicit ``gpu_count=`` beats the
   descriptor's ``spec.resources.gpu.count``, which beats the built-in ``1``;
   a strategy's resolved resource name beats ``--gpu-resource-name``, which
   beats ``nvidia.com/gpu``.
2. Two hard declarations of one field from *different* sources that disagree
   are a contradiction (``GpuRequirementConflict``, a ``CapabilityError``
   whose remedy names both sources). The descriptor's ``device`` list counts as
   a hard ``device_type`` declaration when it names a single device: a
   component that only runs on GPU contradicts a node asking for CPU.
3. A GPU-only requirement (``gpu_count > 1`` or a ``gpu_memory_gib``) on a node
   whose ``device_type`` resolves to CPU is a contradiction when the two come
   from different sources — with one documented exception: a *soft* memory
   demand describes the component's GPU flavor, so a CPU run drops it
   (recorded in ``GpuResolution.dropped``) rather than failing (RFC 0004). A
   descriptor's *count* default is deliberately not dropped: the author must say
   ``gpu_count=1`` to run a multi-GPU component on CPU (RFC 0003).
4. ``gpu_count > 1`` and a memory demand are mutually exclusive (a model cannot
   span MIG slices); from different sources that is a contradiction naming both.

Contradictions *within one source* — a node passing ``device_type='cpu'`` and
``gpu_count=2`` in the same call — are not this module's business: the node
constructor already rejects them with the messages the API documents.

The output is the resolved values **plus** ``{field: source}`` provenance. That
record is what ``ProcessorNode.gpu_provenance`` and
``videoflow.core.compiler.gpu_provenance`` expose; it deliberately lives beside
``NodeSpec`` rather than on it, so that a spec's serialized form — every specs
ConfigMap byte — is unchanged.
'''
from __future__ import absolute_import, division, print_function

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .constants import CPU, GPU
from .errors import CapabilityError

#: An explicit argument on the node (``ProcessorNode(gpu_count = 2)``,
#: ``component(..., device_type = 'gpu')``).
SOURCE_NODE = 'node'
#: A GPU strategy's own resolution at deploy time (``mix`` stamping a MIG profile).
SOURCE_STRATEGY = 'strategy'
#: A remote component's ``component.yaml``: ``spec.resources.gpu`` and ``spec.device``.
SOURCE_DESCRIPTOR = 'descriptor'
#: A deploy-level default from the CLI (``--gpu-resource-name``).
SOURCE_CLI_DEFAULT = 'cli-default'
#: What the framework assumes when nobody says anything.
SOURCE_DEFAULT = 'default'

#: Precedence among declarations of equal hardness, highest first. A hard
#: declaration beats a soft one regardless of this order.
SOURCE_PRECEDENCE = (SOURCE_NODE, SOURCE_STRATEGY, SOURCE_DESCRIPTOR, SOURCE_CLI_DEFAULT, SOURCE_DEFAULT)

FIELD_DEVICE_TYPE = 'device_type'
FIELD_GPU_COUNT = 'gpu_count'
FIELD_GPU_MEMORY_GIB = 'gpu_memory_gib'
FIELD_GPU_RESOURCE_NAME = 'gpu_resource_name'
FIELDS = (FIELD_DEVICE_TYPE, FIELD_GPU_COUNT, FIELD_GPU_MEMORY_GIB, FIELD_GPU_RESOURCE_NAME)

#: The fields that only mean something on a GPU node.
_GPU_ONLY_FIELDS = (FIELD_GPU_COUNT, FIELD_GPU_MEMORY_GIB)

class GpuRequirementConflict(CapabilityError, ValueError):
    '''
    Two declarations of one node's GPU requirement contradict each other and
    neither is a default the other may override. The message names both values
    and both sources; ``remedy`` says which of the two to change.

    A ``CapabilityError`` (code ``VF_CAPABILITY``, exit code 2): the graph asks a
    component for something its own declaration rules out. It is also a
    ``ValueError`` because ``component()`` and the node constructors promise graph
    authors a ``ValueError`` for every build-time misconfiguration, and the CLI
    converts that class at its boundaries — the taxonomy class is what the CLI
    renders, the builtin base is what existing callers catch.
    '''

@dataclass(frozen = True)
class Declaration:
    '''
    One candidate value for one field of a node's GPU requirement.

    - Arguments:
        - field: one of ``FIELDS``.
        - value: the declared value (``None`` is a legitimate declaration for \
            ``gpu_memory_gib``: "no memory demand").
        - source: one of ``SOURCE_PRECEDENCE``.
        - hard: True for a requirement, False for a default another source may \
            override.
    '''
    field : str
    value : Any
    source : str
    hard : bool = True

@dataclass(frozen = True)
class GpuResolution:
    '''
    The resolver's answer: the winning value per field and where it came from.

    - Attributes:
        - values: field -> resolved value.
        - provenance: field -> the ``source`` of the winning declaration.
        - dropped: field -> the source of a soft GPU-only default that was set \
            aside because the node runs on CPU (rule 3 above). Evidence for the \
            provenance table; empty when nothing was dropped.
    '''
    values : Dict[str, Any] = field(default_factory = dict)
    provenance : Dict[str, str] = field(default_factory = dict)
    dropped : Dict[str, str] = field(default_factory = dict)

def builtin_defaults() -> List[Declaration]:
    '''The soft declarations a node makes by saying nothing: CPU, one device, no memory demand.'''
    return [
        Declaration(FIELD_DEVICE_TYPE, CPU, SOURCE_DEFAULT, hard = False),
        Declaration(FIELD_GPU_COUNT, 1, SOURCE_DEFAULT, hard = False),
        Declaration(FIELD_GPU_MEMORY_GIB, None, SOURCE_DEFAULT, hard = False),
    ]

def node_declarations(device_type : str, gpu_count : Optional[int] = None,
                      gpu_memory_gib : int | float | None = None) -> List[Declaration]:
    '''
    The hard declarations an explicit node / ``component()`` call makes.
    ``device_type`` is always declared (the node API has no "unspecified"
    device); ``gpu_count`` and ``gpu_memory_gib`` only when the caller passed
    them — ``None`` means "I did not say", not "zero".
    '''
    declarations = [Declaration(FIELD_DEVICE_TYPE, device_type, SOURCE_NODE)]
    if gpu_count is not None:
        declarations.append(Declaration(FIELD_GPU_COUNT, gpu_count, SOURCE_NODE))
    if gpu_memory_gib is not None:
        declarations.append(Declaration(FIELD_GPU_MEMORY_GIB, gpu_memory_gib, SOURCE_NODE))
    return declarations

def descriptor_declarations(device : Sequence[str], gpu_count : Optional[int] = None,
                            gpu_memory_gib : int | float | None = None) -> List[Declaration]:
    '''
    What a component descriptor declares. ``spec.resources.gpu.count`` /
    ``memoryGiB`` are soft — RFC 0003/0004 make them defaults, not floors. A
    single-entry ``spec.device`` list is a hard ``device_type``: a component that
    only ships a GPU flavor *requires* a GPU (a two-entry list declares nothing;
    the node's choice merely has to be one of them, which ``component()`` checks).
    '''
    declarations : List[Declaration] = []
    if len(device) == 1:
        declarations.append(Declaration(FIELD_DEVICE_TYPE, device[0], SOURCE_DESCRIPTOR))
    if gpu_count is not None:
        declarations.append(Declaration(FIELD_GPU_COUNT, gpu_count, SOURCE_DESCRIPTOR, hard = False))
    if gpu_memory_gib is not None:
        declarations.append(Declaration(FIELD_GPU_MEMORY_GIB, gpu_memory_gib, SOURCE_DESCRIPTOR, hard = False))
    return declarations

def _rank(declaration : Declaration) -> int:
    return SOURCE_PRECEDENCE.index(declaration.source)

def _requires_gpu(field_name : str, value : Any) -> bool:
    '''Whether a resolved value of a GPU-only field is an actual GPU demand.'''
    if field_name == FIELD_GPU_COUNT:
        return isinstance(value, int) and value > 1
    if field_name == FIELD_GPU_MEMORY_GIB:
        return value is not None
    return False

def _where(source : str, field_name : str) -> str:
    '''How a human changes the declaration ``source`` made for ``field_name``.'''
    if source == SOURCE_NODE:
        return f'the {field_name}= argument on the node'
    if source == SOURCE_DESCRIPTOR:
        if field_name == FIELD_DEVICE_TYPE:
            return 'spec.device in the component descriptor'
        key = 'memoryGiB' if field_name == FIELD_GPU_MEMORY_GIB else 'count'
        return f'spec.resources.gpu.{key} in the component descriptor'
    if source == SOURCE_STRATEGY:
        return f'the GPU strategy\'s resolved {field_name}'
    if source == SOURCE_CLI_DEFAULT:
        return '--gpu-resource-name on the deploy command line'
    return f'the built-in default for {field_name}'

def _winner(candidates : List[Declaration]) -> Declaration:
    '''The highest-precedence candidate; ``min`` keeps declaration order among equals.'''
    return min(candidates, key = _rank)

def _disagreement(subject : str, field_name : str, first : Declaration, other : Declaration) -> str:
    '''The message for two hard declarations of one field that differ.'''
    if field_name == FIELD_DEVICE_TYPE and SOURCE_DESCRIPTOR in (first.source, other.source):
        asked, supports = (first, other) if other.source == SOURCE_DESCRIPTOR else (other, first)
        return (f'{subject}: device_type={asked.value!r} ({asked.source}) is not supported — '
                f'spec.device in the component descriptor declares {supports.value!r} as the only '
                f'device the component runs on.')
    return (f'{subject}: {field_name} is declared twice with different values — '
            f'{first.source} says {first.value!r} and {other.source} says {other.value!r}, '
            f'and neither is a default the other may override.')

def _select(field_name : str, candidates : List[Declaration], subject : str) -> Declaration:
    hard = sorted((c for c in candidates if c.hard), key = _rank)
    if not hard:
        return _winner(candidates)
    first = hard[0]
    for other in hard[1:]:
        if other.source != first.source and other.value != first.value:
            raise GpuRequirementConflict(
                _disagreement(subject, field_name, first, other),
                remedy = f'Make the two agree: change {_where(first.source, field_name)} or '
                         f'{_where(other.source, field_name)}.',
                field = field_name, sources = [first.source, other.source])
    return first

def resolve_gpu_requirements(declarations : Iterable[Declaration],
                             subject : str = 'node') -> GpuResolution:
    '''
    Resolve a node's GPU requirement from every declaration made about it.

    - Arguments:
        - declarations: the candidates, in any order. Every field the caller \
            cares about should carry a soft ``SOURCE_DEFAULT`` declaration (see \
            ``builtin_defaults``) so its provenance is always recorded.
        - subject: how to name the node in an error (``"component 'acme/x'"``).

    - Returns:
        - a ``GpuResolution``: values, ``{field: source}`` provenance, and the \
            soft GPU-only defaults dropped because the node runs on CPU.

    - Raises:
        - ``GpuRequirementConflict`` (a ``CapabilityError`` and a ``ValueError``): \
            two sources make incompatible hard declarations, or a source's hard \
            declaration is incompatible with another source's GPU-only requirement \
            (see the module docstring for the exact rules). The remedy names both \
            sources.
        - ``ValueError``: a declaration names an unknown field or source.
    '''
    by_field : Dict[str, List[Declaration]] = {}
    for declaration in declarations:
        if declaration.field not in FIELDS:
            raise ValueError(f'unknown GPU requirement field {declaration.field!r}; known: {FIELDS}')
        if declaration.source not in SOURCE_PRECEDENCE:
            raise ValueError(f'unknown declaration source {declaration.source!r}; known: {SOURCE_PRECEDENCE}')
        by_field.setdefault(declaration.field, []).append(declaration)

    winners : Dict[str, Declaration] = {name: _select(name, candidates, subject)
                                        for name, candidates in by_field.items()}
    dropped : Dict[str, str] = {}

    # Rule 3: GPU-only requirements against a CPU device, across sources.
    device = winners.get(FIELD_DEVICE_TYPE)
    if device is not None and device.value != GPU:
        for name in _GPU_ONLY_FIELDS:
            winner = winners.get(name)
            if winner is None or not _requires_gpu(name, winner.value) or winner.source == device.source:
                continue
            if not winner.hard and name == FIELD_GPU_MEMORY_GIB:
                # RFC 0004: a memory default describes the gpu flavor of a
                # dual-device component; a CPU run sets it aside. A hard one from
                # the same source as the device is the node contradicting itself,
                # which the constructor reports; any other combination is below.
                dropped[name] = winner.source
                applicable = [c for c in by_field[name]
                              if c.hard or not _requires_gpu(name, c.value)]
                winners[name] = _select(name, applicable, subject)
                continue
            raise GpuRequirementConflict(
                f"{subject}: {name}={winner.value!r} ({winner.source}) requires device_type='gpu', "
                f'but device_type={device.value!r} comes from {device.source}.',
                remedy = f"Pass device_type='gpu', or override {_where(winner.source, name)} — "
                         f'e.g. {name}={1 if name == FIELD_GPU_COUNT else None!r} on the node.',
                field = name, sources = [device.source, winner.source])

    # Rule 4: whole devices and a memory slice are mutually exclusive, across sources.
    count = winners.get(FIELD_GPU_COUNT)
    memory = winners.get(FIELD_GPU_MEMORY_GIB)
    if (count is not None and memory is not None and _requires_gpu(FIELD_GPU_COUNT, count.value)
            and _requires_gpu(FIELD_GPU_MEMORY_GIB, memory.value) and count.source != memory.source):
        raise GpuRequirementConflict(
            f'{subject}: gpu_count={count.value!r} ({count.source}) and gpu_memory_gib={memory.value!r} '
            f'({memory.source}) are mutually exclusive — a model cannot span MIG slices, so a node '
            f'declares either a fraction of one device or whole devices, never both (RFC 0004).',
            remedy = f'Change {_where(count.source, FIELD_GPU_COUNT)} (gpu_count=1 keeps the memory '
                     f'demand) or {_where(memory.source, FIELD_GPU_MEMORY_GIB)}.',
            field = FIELD_GPU_MEMORY_GIB, sources = [count.source, memory.source])

    # Field order is fixed (``FIELDS``) so the record serializes the same way
    # whatever order the declarations arrived in.
    ordered = [name for name in FIELDS if name in winners]
    return GpuResolution(
        values = {name: winners[name].value for name in ordered},
        provenance = {name: winners[name].source for name in ordered},
        dropped = dropped)
