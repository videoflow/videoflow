'''
Conformance cases: MSG-001, MSG-019, MSG-020.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions taking the component under
test as an argument, so the paired negative control can run the same oracle against
the reviewed defect (``defects.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
import itertools
import json
import random
from typing import Any, Callable, Dict, List

import defects
import pytest

from videoflow.backends import faults, identity
from videoflow.backends.capabilities import (
    DURABLE_CONTROL,
    LIVE_LATEST,
    MESSAGING_PROFILES,
    RELIABLE_WORK,
    REPLAY_ARCHIVE,
    RETENTION_INTEREST,
    FlowRequirements,
    PayloadCapabilities,
    ProfileRequest,
    RuntimeCapabilities,
    plan_composition,
)
from videoflow.backends.identity import owner_labels
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import MemoryMessagingBackend, make_channel, make_envelope
from videoflow.backends.messaging import ChannelId
from videoflow.backends.outcomes import Accepted, known
from videoflow.core.compiler import NodeSpec
from videoflow.core.errors import IncompatibleProfile
from videoflow.deploy.manifests import k8s_name
from videoflow.messaging import topology

# -- MSG-001 ----------------------------------------------------------------------

#: Which advertised capability each profile cannot do without (the planner's rules).
_MANDATORY = {
    RELIABLE_WORK: ('retained_backlog', 'recoverable_delivery'),
    DURABLE_CONTROL: ('durable_control',),
    REPLAY_ARCHIVE: ('archive',),
}

_DURABLE_RUNTIME = RuntimeCapabilities('redis', durable = known(True), shared_across_processes = True,
                                       restart_safe_joins = True, elastic_state = False)


def _full_backend(clock : FakeClock, **kwargs : Any) -> MemoryMessagingBackend:
    return MemoryMessagingBackend(clock, durable_control = True, archive = True, **kwargs)


def _deploy(backend : MemoryMessagingBackend, requirements : FlowRequirements,
            planner : Callable[..., Any], **compose : Any) -> Any:
    '''
    The public path a deploy takes: admit the composition, then provision every
    channel, then start the producer. The order is the point — a rejection must
    come first, so a planner that admits by downgrading would leave channels and
    a sentinel behind, which the oracle checks for.
    '''
    caps = compose.pop('capabilities', None) or backend.capabilities()
    plan = planner(requirements, caps, **compose)
    for channel, profile in plan.channel_profiles.items():
        backend.ensure_channel(make_channel('f', 'r', channel, profile, plan.channel_retention[channel]), 'op')
        out = backend.publish(make_envelope(ChannelId('f', 'r', channel), f'sentinel-{channel}'), 0)
        assert isinstance(out, Accepted)
    return plan


def _oracle_msg_001(planner : Callable[..., Any], report : List[Dict[str, Any]]) -> None:
    clock = FakeClock()
    # Positive control: the complete composition admits every profile.
    for profile in MESSAGING_PROFILES:
        backend = _full_backend(clock)
        plan = _deploy(backend, FlowRequirements(profiles = (ProfileRequest('p', profile),)),
                       planner, runtime = _DURABLE_RUNTIME)
        assert plan.channel_profiles == {'p': profile}
        assert [c.node for c in backend.channel_ids()] == ['p']
        report.append({'profile': profile, 'removed': None, 'outcome': 'admitted'})

    def expect_rejection(backend : MemoryMessagingBackend, profile : str, removed : str,
                         **compose : Any) -> None:
        req = FlowRequirements(profiles = (ProfileRequest('p', profile),))
        try:
            _deploy(backend, req, planner, **compose)
        except IncompatibleProfile as e:
            assert e.code == 'VF_INCOMPATIBLE_PROFILE'
            assert 'p' in e.context.get('channels', ()), e.context
            assert profile in str(e) and e.remedy
            report.append({'profile': profile, 'removed': removed, 'outcome': 'rejected',
                           'message': str(e)[:300]})
        else:
            raise AssertionError(f'{profile} without {removed} was admitted instead of rejected')
        # Zero side effects: nothing provisioned, nothing published.
        assert backend.channel_ids() == [], f'channels provisioned before the rejection: {backend.channel_ids()}'

    # Each mandatory capability removed in turn.
    for profile, needed in _MANDATORY.items():
        for capability in needed:
            backend = _full_backend(clock)
            weakened = dataclasses.replace(backend.capabilities(), **{capability: False})
            expect_rejection(backend, profile, capability, capabilities = weakened, runtime = _DURABLE_RUNTIME)
    # Durable control also needs a durable runtime store, and payload references a durable store.
    expect_rejection(_full_backend(clock), DURABLE_CONTROL, 'durable runtime store')
    evictable = PayloadCapabilities('memory', durable = known(False), evictable = known(True),
                                    atomic_multikey = known(True), max_object_bytes = None,
                                    reader_identities = True)
    expect_rejection(_full_backend(clock), RELIABLE_WORK, 'durable payload references',
                     payload = evictable, payload_refs_in_use = True)

    # Core-only cannot downgrade: composing a durable runtime does not make the
    # transport retain or recover anything, and the rejection says which half is missing.
    for profile in (RELIABLE_WORK, DURABLE_CONTROL, REPLAY_ARCHIVE):
        core = MemoryMessagingBackend(clock, core_only = True)
        expect_rejection(core, profile, 'core-only transport', runtime = _DURABLE_RUNTIME)
    core = MemoryMessagingBackend(clock, core_only = True)
    plan = _deploy(core, FlowRequirements(profiles = (ProfileRequest('p', LIVE_LATEST),)), planner)
    assert plan.channel_profiles == {'p': LIVE_LATEST}

    # live_latest with explicit per-key freshness is admitted only when implemented.
    per_key = FlowRequirements(profiles = (ProfileRequest('p', LIVE_LATEST, {'latest_per_key': True}),))
    backend = _full_backend(clock)
    try:
        _deploy(backend, per_key, planner)
    except IncompatibleProfile as e:
        assert 'per-key' in str(e)
        assert backend.channel_ids() == []
    else:
        raise AssertionError('latest_per_key was admitted on a backend that does not implement it')
    keyed = MemoryMessagingBackend(clock, latest_per_key = True)
    assert _deploy(keyed, per_key, planner).channel_profiles == {'p': LIVE_LATEST}


@pytest.mark.case('MSG-001')
@pytest.mark.level('model')
def test_msg_001_capability_negotiation_rejects_unsupported_reliability(evidence_dir) -> None:
    '''
    MSG-001 (P0, messaging, model): Capability negotiation rejects unsupported reliability
    before publication.

    Acceptance: Every unsupported profile is an expected rejection test that passes by
    rejecting; never mark it skipped or silently run weaker assertions. Zero publish or
    allocation side effects before rejection.
    '''
    report : List[Dict[str, Any]] = []
    _oracle_msg_001(plan_composition, report)
    (evidence_dir / 'capability_manifest.json').write_text(json.dumps(report, indent = 2))
    assert sum(r['outcome'] == 'rejected' for r in report) >= 9


@pytest.mark.negative_control(of = 'MSG-001')
def test_msg_001_detects_a_downgrading_planner() -> None:
    '''A planner that serves a rejected profile at the nearest weaker one must fail the oracle.'''
    assert defects.detects(_oracle_msg_001, defects.downgrading_planner(), [])


# -- MSG-019 ----------------------------------------------------------------------

#: Names chosen to collide after normalisation: ``a.b``/``a_b`` (sanitize maps both
#: to ``a_b``), case folds and truncations in Kubernetes names, hyphen-joined tuples
#: whose flow/run split is ambiguous, Unicode and punctuation.
_ATOMS = ['a', 'A', 'a.b', 'a_b', 'a-b', 'a b', 'ä', 'a/b', 'a:b', 'b-c', 'c', 'node', 'Node',
          'nödé', 'x' * 70 + 'a', 'x' * 70 + 'b', 'r', 'r-x', 'r_x', 'p', 'q']


def _spec(name : str, parents : tuple = ()) -> NodeSpec:
    return NodeSpec(name = name, node_class = None, params = {}, parents = list(parents),
                    kind = 'processor', has_children = True, nb_tasks = 1, device_type = 'cpu',
                    is_finite = False)


def _nats_match(pattern : str, subject : str) -> bool:
    '''NATS subject matching: tokens split on ``.``, ``*`` one token, ``>`` one or more.'''
    p, s = pattern.split('.'), subject.split('.')
    for i, token in enumerate(p):
        if token == '>':
            return len(s) > i
        if i >= len(s) or (token != '*' and token != s[i]):
            return False
    return len(p) == len(s)


def _oracle_msg_019(seed : int, corpus : Dict[str, Any]) -> None:
    rng = random.Random(seed)
    tuples : set = set()
    while len(tuples) < 120:
        tuples.add((rng.choice(_ATOMS), rng.choice(_ATOMS), rng.choice(_ATOMS)))
    ordered = sorted(tuples)
    physical : Dict[str, List[tuple]] = {}
    for flow, run, node in ordered:
        names = identity.derived_names([_spec('src'), _spec(node, ('src',))], flow, run)
        for phys, ids in names.items():
            for ident in ids:
                physical.setdefault(phys, []).append(((flow, run, node), ident.kind, tuple(ident.parts)))
    corpus['tuples'] = [list(t) for t in ordered]
    corpus['physical'] = {phys: [{'tuple': list(t), 'kind': k, 'parts': list(parts)} for t, k, parts in entries]
                          for phys, entries in physical.items()}

    # (a) Two node names of one graph that encode to one physical resource are a
    # compile-time rejection, not a shared stream — checked with an independent
    # equality on the encoded names, then against the compiler's own verdict.
    rejections = []
    for a, b in itertools.combinations(sorted({t[2] for t in ordered}), 2):
        same_stream = topology.stream_name_for('f', 'r', a) == topology.stream_name_for('f', 'r', b)
        same_k8s = k8s_name('f', a) == k8s_name('f', b)
        if same_stream or same_k8s:
            found = identity.node_name_collisions([a, b])
            assert found, f'{a!r} and {b!r} encode to one physical name but the compiler accepts both'
            rejections.append({'names': [a, b], 'physical': [c.physical for c in found]})
    assert rejections, 'the corpus produced no colliding node pair — the corpus is broken, not the codec'
    corpus['rejected_pairs'] = rejections

    # (b) Distinct tuples that share a physical name across flows/runs are tied to
    # exact owner metadata: the recorded parts are the originals, never the
    # normalised form, so ownership is decidable without reversing the encoding.
    tied = 0
    for phys, entries in physical.items():
        distinct = {e[0] for e in entries}
        if len(distinct) < 2:
            continue
        for (t1, kind1, parts1), (t2, kind2, parts2) in itertools.combinations(entries, 2):
            if t1 == t2 or kind1 != kind2:
                continue
            if parts1 == parts2:
                # The same logical identity reached from two tuples — a flow-level
                # resource (the DLQ stream, the control subject) shared by runs of
                # one flow. Not a collision: the parts say exactly whose it is.
                continue
            l1 = owner_labels(*parts1[:3], kind = kind1) if len(parts1) >= 2 else {'parts': parts1}
            l2 = owner_labels(*parts2[:3], kind = kind2) if len(parts2) >= 2 else {'parts': parts2}
            assert l1 != l2, f'{phys}: owner labels of {t1} and {t2} coincide: {l1}'
            tied += 1
    corpus['owner_metadata_ties'] = tied

    # (c) Subscription filters never cross flow/run boundaries: a run-scoped DLQ
    # filter or data subject of one tuple matches no other tuple's subjects
    # unless their normalised (flow, run) coincide — and then (b) applies.
    checked = 0
    for (f1, r1, n1), (f2, r2, n2) in itertools.islice(itertools.combinations(ordered, 2), 4000):
        same_scope = (topology.sanitize(f1), topology.sanitize(r1)) == (topology.sanitize(f2), topology.sanitize(r2))
        if same_scope:
            continue
        assert not _nats_match(topology.dlq_subject_filter(f1, r1), topology.dlq_subject_for(f2, r2, n2))
        assert not _nats_match(topology.subject_for(f1, r1, n1), topology.subject_for(f2, r2, n2))
        assert not _nats_match(topology.dlq_subject_filter(f1, r1, n1), topology.dlq_subject_for(f2, r2, n2))
        if topology.sanitize(f1) != topology.sanitize(f2):
            assert not _nats_match(topology.dlq_subject_filter(f1), topology.dlq_subject_for(f2, r2, n2))
        checked += 1
    corpus['boundary_pairs_checked'] = checked


@pytest.mark.case('MSG-019')
@pytest.mark.level('model')
def test_msg_019_broker_identities_are_collision_resistant_across(seed, evidence_dir) -> None:
    '''
    MSG-019 (P0, messaging, model): Broker identities are collision resistant across normalized
    names.

    Acceptance: Property-generated distinct namespace tuples never silently share a physical
    identity; paired sentinels are visible only to their intended subscriptions.
    '''
    corpus : Dict[str, Any] = {'seed': seed}
    _oracle_msg_019(seed, corpus)
    (evidence_dir / 'namespace_corpus.json').write_text(json.dumps(corpus, indent = 2, ensure_ascii = False))


@pytest.mark.negative_control(of = 'MSG-019')
def test_msg_019_detects_a_compiler_that_accepts_colliding_names(monkeypatch, seed) -> None:
    defects.silent_identity_codec(monkeypatch)
    assert defects.detects(_oracle_msg_019, seed, {})


# -- MSG-020 ----------------------------------------------------------------------

def _oracle_msg_020(backend : MemoryMessagingBackend, record : Dict[str, Any]) -> None:
    r, rx = ChannelId('f', 'r', 'n'), ChannelId('f', 'r-x', 'n')
    lookalike = ChannelId('f-r', 'n', 'x')            # a different flow whose stream shares the text
    dlq = ChannelId('f', '', 'dlq')                    # flow-level, outlives every run
    for cid in (r, rx, lookalike):
        backend.ensure_channel(make_channel(cid.flow_id, cid.run_id, cid.node, RELIABLE_WORK, RETENTION_INTEREST), 'op')
        assert isinstance(backend.publish(make_envelope(cid, f'sentinel-{cid.run_id}-{cid.node}'), 0), Accepted)
    backend.ensure_channel(make_channel('f', '', 'dlq', RELIABLE_WORK, RETENTION_INTEREST,
                                        owner_labels = owner_labels('f', '', 'dlq', 'dlq_stream')), 'op')
    assert isinstance(backend.publish(make_envelope(dlq, 'dead-letter'), 0), Accepted)
    prefix = topology.stream_name_for('f', 'r', '')          # 'vf-f-r-'
    assert topology.stream_name_for('f', 'r-x', 'n').startswith(prefix)     # the deliberate textual overlap
    assert topology.stream_name_for('f-r', 'n', 'x').startswith(prefix)
    before = {str(c): [e.publication_id for e in backend.stored(c)] for c in backend.channel_ids()}
    record['before'] = before

    schedule = faults.FaultSchedule({'delete.before': faults.Nth(1, faults.RaiseError(lambda: ConnectionError('injected delete failure')))})
    with schedule:
        first = backend.close([r], 'g')
        assert first.complete is False and first.remaining == ('n',) and 'injected' in first.reason
        assert r in backend.channel_ids(), 'a failed delete must not be reported as done'
        second = backend.close([r], 'g')
    record['faults'] = schedule.fired()
    record['cleanup'] = [dataclasses.asdict(first), dataclasses.asdict(second)]
    assert second.complete and second.removed == ('n',)
    remaining = backend.channel_ids()
    assert r not in remaining
    for cid, sentinel in ((rx, 'sentinel-r-x-n'), (lookalike, 'sentinel-n-x'), (dlq, 'dead-letter')):
        assert cid in remaining, f'{cid} was deleted by a teardown of run r'
        assert [e.publication_id for e in backend.stored(cid)] == [sentinel]
    record['after'] = {str(c): [e.publication_id for e in backend.stored(c)] for c in remaining}


class _FlakyJetStream:
    '''A JetStream context whose first ``delete_stream`` fails — the injected delete failure.'''
    def __init__(self, inner : Any) -> None:
        self._inner = inner
        self.failures = 0

    def __getattr__(self, name : str) -> Any:
        return getattr(self._inner, name)

    async def delete_stream(self, name : str) -> Any:
        if self.failures == 0:
            self.failures += 1
            raise ConnectionError(f'injected delete failure for {name}')
        return await self._inner.delete_stream(name)


@pytest.mark.case('MSG-020')
@pytest.mark.level('broker')
@pytest.mark.timeout(120)
def test_msg_020_run_teardown_uses_exact_ownership_and_preserves_other_runs(nats_url, evidence_dir) -> None:
    '''
    MSG-020 (P0, messaging, broker): Run teardown uses exact ownership and preserves other runs
    and DLQ.

    Acceptance: After successful cleanup only r resources are absent; r-x sentinel and DLQ
    evidence remain readable. A fault produces an incomplete-cleanup result until reconciled.
    '''
    import asyncio
    import uuid

    import nats

    from videoflow.core.constants import REALTIME
    from videoflow.messaging.topology import delete_run_streams, provision_flow_sync

    flow = f'conf-{uuid.uuid4().hex[:6]}'
    specs = [_spec('src'), _spec('n', ('src',)), _spec('sink', ('n',))]
    for run in ('r', 'r-x'):
        provision_flow_sync(nats_url, specs, flow, run, REALTIME)
    mine = {topology.stream_name_for(flow, 'r', name) for name in ('src', 'n', 'sink')}
    theirs = {topology.stream_name_for(flow, 'r-x', name) for name in ('src', 'n', 'sink')}
    dlq = topology.dlq_stream_name(flow)
    record : Dict[str, Any] = {'flow': flow}

    async def _streams(js : Any) -> set:
        names = set()
        page = await js.streams_info_iterator(offset = 0) if hasattr(js, 'streams_info_iterator') else await js.streams_info()
        for info in page:
            names.add(info.config.name)
        return names

    async def _go() -> None:
        nc = await nats.connect(nats_url)
        try:
            js = nc.jetstream()
            await js.publish(topology.subject_for(flow, 'r', 'n'), b'sentinel-r')
            await js.publish(topology.subject_for(flow, 'r-x', 'n'), b'sentinel-r-x')
            await js.publish(topology.dlq_subject_for(flow, 'r', 'n'), b'dead-letter')
            before = await _streams(js)
            assert mine <= before and theirs <= before and dlq in before, before
            record['before'] = sorted(n for n in before if flow in n)
            flaky = _FlakyJetStream(js)
            nc.jetstream = lambda **kw: flaky                       # type: ignore[method-assign]
            first = await delete_run_streams(nc, flow, 'r')
            record['first'] = dataclasses.asdict(first)
            assert first.complete is False and first.remaining, first
            assert flaky.failures == 1
            second = await delete_run_streams(nc, flow, 'r')
            record['second'] = dataclasses.asdict(second)
            assert second.complete, second
            assert set(first.removed) | set(second.removed) == mine, (first, second)
            after = await _streams(js)
            record['after'] = sorted(n for n in after if flow in n)
            assert not (mine & after), mine & after
            assert theirs <= after and dlq in after
            survivor = await js.get_last_msg(topology.stream_name_for(flow, 'r-x', 'n'), topology.subject_for(flow, 'r-x', 'n'))
            assert survivor.data == b'sentinel-r-x'
            evidence = await js.get_last_msg(dlq, topology.dlq_subject_for(flow, 'r', 'n'))
            assert evidence.data == b'dead-letter'
        finally:
            nc.jetstream = lambda **kw: js                         # type: ignore[method-assign]
            for name in sorted(theirs) + [dlq]:
                try:
                    await js.delete_stream(name)
                except Exception:                                  # noqa: BLE001 — test hygiene only
                    pass
            await nc.drain()

    asyncio.run(_go())
    (evidence_dir / 'teardown_trace.json').write_text(json.dumps(record, indent = 2, default = str))


@pytest.mark.case('MSG-020')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_020_memory_backend_tears_down_by_exact_ownership(evidence_dir, record_faults) -> None:
    '''The reference backend: exact owner labels, an injected delete failure, and a retry.'''
    record : Dict[str, Any] = {}
    schedule_holder = faults.FaultSchedule({'delete.before': faults.DropResponse()})   # shape check only
    assert schedule_holder.unfired() == ['delete.before']
    _oracle_msg_020(MemoryMessagingBackend(FakeClock()), record)
    (evidence_dir / 'ownership_inventory.json').write_text(json.dumps(record, indent = 2, default = str))
    assert record['faults']['delete.before'] == 2


@pytest.mark.negative_control(of = 'MSG-020')
def test_msg_020_detects_a_prefix_teardown(monkeypatch) -> None:
    defects.prefix_teardown(monkeypatch)
    assert defects.detects(_oracle_msg_020, MemoryMessagingBackend(FakeClock()), {})
