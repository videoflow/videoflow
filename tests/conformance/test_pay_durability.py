'''
Conformance case: PAY-011 — the kubernetes-level payload-store restart case,
against the durable Redis (append-only file on a claim) that
``scripts/k3s-test-up.sh --durable`` prepares and the dev Redis (emptyDir, no
persistence) in the plain test namespace. The only mutation is deleting the
durable Redis pod, which its Deployment recreates on the same claim.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import json
import time
from typing import Any, Callable, Dict, List

import defects
import defects_k8s
import pytest
from _k8s import delete_pods, kubectl_json, pod_names, port_forward, wait_ready

from videoflow.backends.capabilities import (
    LEDGER_WINDOW,
    RELIABLE_WORK,
    FlowRequirements,
    MessagingCapabilities,
    PayloadCapabilities,
    ProfileRequest,
    plan_composition,
)
from videoflow.backends.outcomes import Known, known
from videoflow.backends.payload import PayloadBytes, RetentionContract
from videoflow.core.errors import IncompatibleProfile


def _replicated_broker() -> MessagingCapabilities:
    '''A broker that *does* survive a pod loss — so nothing but the payload store decides the verdict.'''
    return MessagingCapabilities(
        adapter = 'jetstream', version = '2.10', retained_backlog = True, recoverable_delivery = True,
        latest_per_key = False, dedup_window_seconds = 120, publication_ledger = LEDGER_WINDOW,
        replication_factor = known(3), persistent_storage = known(True), max_payload_bytes = known(1 << 20),
        credit_resizable = True, control_shares_data_slot = True)


def _oracle_pay_011_admission(planner : Callable[..., Any], payload : PayloadCapabilities) -> str:
    '''
    Whether the composition may claim pod-loss durability with payload references
    in use: ``certified`` only when the *store* is durable — the broker's own
    persistence never substitutes.
    '''
    req = FlowRequirements(profiles = (ProfileRequest('p', RELIABLE_WORK),), tolerated_failures = 1)
    try:
        planner(req, _replicated_broker(), payload = payload, payload_refs_in_use = True)
    except IncompatibleProfile:
        return 'rejected'
    return 'certified'


def _oracle_pay_011(planner : Callable[..., Any], durable_caps : PayloadCapabilities,
                    ephemeral_caps : PayloadCapabilities, recovered : List[Dict[str, Any]],
                    lost : List[Dict[str, Any]] | None = None) -> None:
    '''
    The durable fixture certifies and every protected reference reads back
    byte-identical from a fresh process after the restart; the ephemeral fixture
    is an explicit rejection — it is not run as a "passed" recovery — and, when
    it was restarted too, lost every reference (a recovery from a hidden cache
    would make its declaration a lie).
    '''
    assert isinstance(durable_caps.durable, Known) and durable_caps.durable.value, durable_caps
    assert isinstance(durable_caps.persistent_storage, Known) and durable_caps.persistent_storage.value, durable_caps
    assert _oracle_pay_011_admission(planner, durable_caps) == 'certified'
    assert recovered, 'no reference was checked after the restart'
    for entry in recovered:
        assert entry['outcome'] == 'PayloadBytes', entry
        assert entry['digest_after'] == entry['digest_before'], entry
    # Durably written is not durably kept: the dev Redis writes its append-only
    # file to an emptyDir, so it is durable (a container restart replays it)
    # without persistent storage (the pod takes it along).
    assert isinstance(ephemeral_caps.persistent_storage, Known) and not ephemeral_caps.persistent_storage.value, \
        ephemeral_caps
    assert _oracle_pay_011_admission(planner, ephemeral_caps) == 'rejected'
    for entry in lost or []:
        assert entry['outcome'] == 'Missing', entry


def _store(addr : str) -> Any:
    from videoflow.wire.redis_payload_store import RedisPayloadStore
    return RedisPayloadStore(f'redis://{addr}/0')


def _declared_caps(namespace : str, live : PayloadCapabilities) -> PayloadCapabilities:
    '''
    What ``deploy`` judges a reused Redis by: the profile its creator recorded on
    the Service (``videoflow.io/profile``), which says what backs its data
    directory — the one thing the wire cannot. The live read-back must agree on
    everything the wire *can* see, or the fixture is not what it claims.
    '''
    from videoflow.deploy.admission import redis_payload_capabilities
    from videoflow.deploy.broker_profiles import RedisProfile
    from videoflow.deploy.infra import LABEL_PROFILE
    labels = (kubectl_json('get', 'svc', 'redis', '-n', namespace).get('metadata') or {}).get('labels') or {}
    recorded = labels.get(LABEL_PROFILE)
    profiles = {'dev': RedisProfile.dev(), 'durable': RedisProfile.durable(),
                'cache': RedisProfile(persistence = 'none', eviction = 'volatile-lru')}
    if recorded not in profiles:
        pytest.fail(f'INVALID_TEST: the redis Service in {namespace} records no known profile ({recorded!r}); '
                    f'run scripts/k3s-test-up.sh, which stamps it')
    declared = redis_payload_capabilities(profiles[recorded])
    for field in ('durable', 'evictable'):
        got, want = getattr(live, field), getattr(declared, field)
        if not (isinstance(got, Known) and isinstance(want, Known) and got.value == want.value):
            pytest.fail(f'INVALID_TEST: the redis in {namespace} records profile {recorded!r} but reads back '
                        f'{field}={got!r} (declared {want!r})')
    return declared


def _restart_and_reread(namespace : str, payloads : Dict[str, bytes], evidence : Dict[str, Any], label : str) -> tuple:
    '''Write from one process, delete the pod, read from a fresh one: ``(caps_as_declared, outcomes)``.'''
    refs : Dict[str, Any] = {}
    evidence[f'{label}_pods_before'] = pod_names(namespace, 'app=redis')
    with port_forward(namespace, 'deploy/redis', 6379) as addr:
        store = _store(addr)
        live = store.capabilities()
        for content_id, data in payloads.items():
            refs[content_id] = store.put(data, content_id, RetentionContract(3600, 3600, True, ('reader',)))
        del store
    evidence[f'{label}_live_caps'] = repr(live)
    declared = _declared_caps(namespace, live)
    evidence[f'{label}_declared_caps'] = repr(declared)
    evidence[f'{label}_refs'] = {cid: ref.key for cid, ref in refs.items()}
    t0 = time.monotonic()
    delete_pods(namespace, pod_names(namespace, 'app=redis'))
    wait_ready(namespace, 'app=redis', 1, timeout = 300)
    evidence[f'{label}_pods_after'] = pod_names(namespace, 'app=redis')
    evidence[f'{label}_restart_seconds'] = round(time.monotonic() - t0, 1)
    outcomes : List[Dict[str, Any]] = []
    with port_forward(namespace, 'deploy/redis', 6379) as addr:
        fresh = _store(addr)
        for content_id, ref in refs.items():
            outcome = fresh.read(ref)
            outcomes.append({
                'content_id': content_id, 'outcome': type(outcome).__name__,
                'digest_before': hashlib.sha256(payloads[content_id]).hexdigest(),
                'digest_after': hashlib.sha256(outcome.data).hexdigest() if isinstance(outcome, PayloadBytes) else None,
                'verified': getattr(outcome, 'digest_verified', None),
            })
        for ref in refs.values():                                   # leave nothing behind
            fresh.release_obligation(ref, 'reader', 'conformance')
    evidence[f'{label}_after_restart'] = outcomes
    return declared, outcomes


@pytest.mark.case('PAY-011')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(900)
def test_pay_011_payload_store_restart_obeys_the_declared_persistence(k3s, k3s_ha, evidence_dir) -> None:
    '''
    PAY-011 (P0, payload, kubernetes): Payload-store restart obeys the declared persistence
    failure model.

    Acceptance: Fresh process reads byte-identical frames for every protected reference after
    each claimed supported fault; no recovery from a hidden worker cache is accepted.

    Two fixtures, each judged the way ``deploy`` judges a reused store — by the
    profile recorded on its Service, cross-checked with the live read-back: the
    durable profile (an append-only file on a claim) is deleted and recreated,
    and every reference reads back byte-identical from a fresh process; the dev
    profile (the same file on an emptyDir) is durable across a container restart
    but declares no persistent storage, so surviving a pod loss with it is an
    explicit rejection — and its pod deletion indeed loses every reference,
    which is what its declaration promised and what a hidden cache would belie.
    '''
    payloads = {f'frame-{i}': hashlib.sha256(f'seed-{i}'.encode()).digest() * (1024 * (i + 1)) for i in range(5)}
    evidence : Dict[str, Any] = {}
    try:
        durable_caps, recovered = _restart_and_reread(k3s_ha['namespace'], payloads, evidence, 'durable')
        ephemeral_caps, lost = _restart_and_reread(k3s['VF_K8S_NAMESPACE'], payloads, evidence, 'ephemeral')
    finally:
        (evidence_dir / 'pay011.json').write_text(json.dumps(evidence, indent = 2, default = str))
    _oracle_pay_011(plan_composition, durable_caps, ephemeral_caps, recovered, lost)


@pytest.mark.negative_control(of = 'PAY-011')
def test_pay_011_negative_control_broker_durability_is_not_payload_durability() -> None:
    '''A planner that lets the broker's persistence stand in for the store's admits the emptyDir cache; the oracle must fail.'''
    durable = PayloadCapabilities('redis', durable = known(True), evictable = known(False),
                                  atomic_multikey = known(True), max_object_bytes = None, reader_identities = True,
                                  persistent_storage = known(True))
    ephemeral = PayloadCapabilities('redis', durable = known(True), evictable = known(False),
                                    atomic_multikey = known(True), max_object_bytes = None, reader_identities = True,
                                    persistent_storage = known(False))
    recovered = [{'content_id': 'frame-0', 'outcome': 'PayloadBytes', 'digest_before': 'd', 'digest_after': 'd'}]
    lost = [{'content_id': 'frame-0', 'outcome': 'Missing', 'digest_before': 'd', 'digest_after': None}]
    _oracle_pay_011(plan_composition, durable, ephemeral, recovered, lost)
    assert defects.detects(_oracle_pay_011, defects_k8s.broker_durability_planner(), durable, ephemeral, recovered, lost)
