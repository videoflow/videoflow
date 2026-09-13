'''
Collision-resistant logical identities over today's physical names.

Every broker subject, stream, durable and Kubernetes resource is derived from
user-chosen strings — flow ids, run ids, node names — through two lossy encodings:
``topology.sanitize`` (any run of characters outside ``[A-Za-z0-9_-]`` becomes a
single ``_``) and ``manifests.k8s_name`` (lower-case, non-DNS characters to ``-``,
truncated to 63). Lossy means collisions: ``a.b`` and ``a_b`` share a stream;
``Node`` and ``node`` share a Deployment; two long names can truncate onto each
other. And hyphen-joined tuples collide across positions: stream ``vf-a-b-c-n``
is the same string for ``(flow='a-b', run='c')`` and ``(flow='a', run='b-c')``.

The design package asks for identities that are reversible or tied to exact
owner metadata. This module does the second without renaming anything (a rename
is an RFC): it enumerates every physical name a compiled flow will use, rejects
the flow at compile time when two distinct logical identities map to one physical
name, and produces the owner labels that provisioning writes into stream and
consumer metadata (RFC 0006) so teardown can match exactly instead of by prefix.

The naming functions themselves stay where they are (``messaging/topology.py``,
``deploy/manifests.py``): this module only calls them, so there is still one
source of truth for every name.
'''
from __future__ import absolute_import, division, print_function

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Sequence

from ..core.compiler import NodeSpec

LABEL_FLOW = 'videoflow.io/flow-id'
LABEL_RUN = 'videoflow.io/run-id'
LABEL_NODE = 'videoflow.io/node'
LABEL_KIND = 'videoflow.io/kind'
LABEL_GENERATION = 'videoflow.io/generation'

@dataclass(frozen = True)
class LogicalIdentity:
    '''What a physical name *means*: its kind and the exact logical parts it encodes.'''
    kind : str
    parts : tuple[str, ...]

    def render(self) -> str:
        return f'{self.kind}({", ".join(repr(p) for p in self.parts)})'

@dataclass(frozen = True)
class Collision:
    physical : str
    identities : tuple[LogicalIdentity, ...]

    def render(self) -> str:
        return f'{self.physical!r} <- ' + ' and '.join(i.render() for i in self.identities)

def owner_labels(flow_id : str, run_id : str, node : str | None = None, kind : str | None = None,
                 generation : str | None = None) -> dict[str, str]:
    '''
    Exact-ownership metadata for a broker resource. Written as JetStream stream /
    consumer ``metadata`` (RFC 0006) and compared verbatim by teardown, which is
    what makes ``r`` and ``r-x`` distinguishable when their names share a prefix.
    '''
    labels = {LABEL_FLOW: flow_id, LABEL_RUN: run_id}
    if node is not None:
        labels[LABEL_NODE] = node
    if kind is not None:
        labels[LABEL_KIND] = kind
    if generation is not None:
        labels[LABEL_GENERATION] = generation
    return labels

def flow_labels(flow_id : str, kind : str) -> dict[str, str]:
    '''
    Ownership metadata for a **flow**-scoped resource — the dead-letter stream,
    which outlives every run. It carries no run label on purpose: ``owns`` is
    False for it under every run id, so no run's teardown can match it.
    '''
    return {LABEL_FLOW: flow_id, LABEL_KIND: kind}

def has_owner_labels(metadata : Mapping[str, str] | None) -> bool:
    '''Whether a resource carries videoflow ownership metadata at all — labelled (RFC 0006) versus legacy.'''
    if not metadata:
        return False
    return LABEL_FLOW in metadata or LABEL_RUN in metadata

def owns(metadata : Mapping[str, str] | None, flow_id : str, run_id : str,
         generation : str | None = None) -> bool:
    '''
    Whether ``metadata`` names exactly this run (both labels present and equal)
    and, when ``generation`` is given, this provisioning generation too.
    '''
    if not metadata:
        return False
    if metadata.get(LABEL_FLOW) != flow_id or metadata.get(LABEL_RUN) != run_id:
        return False
    return generation is None or metadata.get(LABEL_GENERATION) == generation

def derived_names(specs : Sequence[NodeSpec], flow_id : str, run_id : str,
                  instance_ids : Mapping[str, Sequence[str]] | None = None) -> dict[str, list[LogicalIdentity]]:
    '''
    Every physical name a compiled flow will create or bind, mapped to the logical
    identities that produce it. The exact set — not a prefix — that a teardown may
    delete, and the corpus the collision check runs over.

    - Arguments:
        - instance_ids: per-node EOS instance ids when known (the per-process \
            uuid-suffixed durables are minted at worker start, so provisioning \
            cannot enumerate them; pass what a test or a running flow knows).
    '''
    # Function-level: topology imports the optional `nats` extra and manifests the
    # optional `yaml` extra at module scope; identities must be computable wherever
    # the graph can be compiled (an operator machine without either).
    from ..deploy.manifests import k8s_name
    from ..messaging import topology as t

    names : dict[str, list[LogicalIdentity]] = {}

    def add(physical : str, kind : str, *parts : str) -> None:
        names.setdefault(physical, []).append(LogicalIdentity(kind, parts))

    add(t.control_subject_for(flow_id, run_id), 'control_subject', flow_id, run_id)
    add(t.dlq_stream_name(flow_id), 'dlq_stream', flow_id)
    add(t.stream_label_selector(flow_id, run_id), 'stream_prefix', flow_id, run_id)
    for k8s_kind in ('broker', 'specs', 'netpol', 'provision'):
        add(k8s_name('vf', flow_id, k8s_kind), 'k8s_' + k8s_kind, flow_id)
    by_name = {spec.name: spec for spec in specs}
    for spec in specs:
        n = spec.name
        add(t.subject_for(flow_id, run_id, n), 'subject', flow_id, run_id, n)
        add(t.stream_name_for(flow_id, run_id, n), 'stream', flow_id, run_id, n)
        add(t.eos_subject_for(flow_id, run_id, n), 'eos_subject', flow_id, run_id, n)
        add(t.eos_anchor_durable_name_for(n), 'eos_anchor', n)
        add(t.dlq_subject_for(flow_id, run_id, n), 'dlq_subject', flow_id, run_id, n)
        add(k8s_name('vf', flow_id, n), 'k8s_workload', flow_id, n)
        for suffix in ('env', 'hl', 'pdb', 'scaler'):
            add(k8s_name('vf', flow_id, n, suffix), 'k8s_' + suffix, flow_id, n)
        for parent in spec.parents:
            if parent not in by_name:
                continue
            if spec.partition_by and spec.nb_tasks > 1:
                for replica in range(spec.nb_tasks):
                    add(t.partitioned_durable_name_for(n, parent, replica), 'partitioned_durable',
                        n, parent, str(replica))
            else:
                add(t.durable_name_for(n, parent), 'durable', n, parent)
            for instance in (instance_ids or {}).get(n, ()):
                add(t.eos_durable_name_for(n, parent, instance), 'eos_durable', n, parent, instance)
    return names

def collisions(specs : Sequence[NodeSpec], flow_id : str, run_id : str) -> list[Collision]:
    '''
    Physical names that two *distinct* logical identities of the same kind map
    to. Different kinds may legitimately share a string (a subject and a stream
    never occupy the same namespace), so only same-kind clashes count.
    '''
    found : list[Collision] = []
    for physical, identities in derived_names(specs, flow_id, run_id).items():
        by_kind : dict[str, list[LogicalIdentity]] = {}
        for identity in identities:
            by_kind.setdefault(identity.kind, []).append(identity)
        for same_kind in by_kind.values():
            distinct = tuple(sorted(set(same_kind), key = lambda i: i.parts))
            if len(distinct) > 1:
                found.append(Collision(physical, distinct))
    return found

def collisions_across_runs(specs : Sequence[NodeSpec], flow_id : str, run_ids : Iterable[str]) -> list[Collision]:
    '''Collisions between two runs of one flow (``r`` vs ``r-x``), for teardown safety checks.'''
    merged : dict[str, list[LogicalIdentity]] = {}
    for run_id in run_ids:
        for physical, identities in derived_names(specs, flow_id, run_id).items():
            merged.setdefault(physical, []).extend(identities)
    found : list[Collision] = []
    for physical, identities in merged.items():
        distinct = tuple(sorted(set(identities), key = lambda i: (i.kind, i.parts)))
        kinds = {i.kind for i in distinct}
        if len(distinct) > 1 and len(kinds) == 1:
            found.append(Collision(physical, distinct))
    return found

def render_collisions(found : Sequence[Collision], encoder : Callable[[str], str] | None = None) -> str:
    return '\n'.join(f'  - {c.render()}' for c in found)

def node_name_collisions(names : Sequence[str]) -> list[Collision]:
    '''
    Node names that encode to one physical name under either encoder — the check
    the graph validator runs before a flow is ever compiled. Flow and run ids are
    constant across a flow's nodes, so within one flow only the node part can
    collide.
    '''
    # Function-level: topology imports the optional `nats` extra and manifests the
    # optional `yaml` extra at module scope; graph validation must not need either.
    from ..deploy.manifests import k8s_name
    from ..messaging.topology import sanitize

    found : list[Collision] = []
    for kind, encode in (('broker', sanitize), ('kubernetes', lambda n: k8s_name('vf', 'f', n))):
        groups : dict[str, list[str]] = {}
        for name in names:
            groups.setdefault(encode(name), []).append(name)
        for physical, members in groups.items():
            distinct = sorted(set(members))
            if len(distinct) > 1:
                found.append(Collision(physical, tuple(LogicalIdentity(kind, (m,)) for m in distinct)))
    return found
