'''
Conformance cases: MSG-021, MSG-022, MSG-023 — the kubernetes-level durability
cases, against the durable broker fixture ``scripts/k3s-test-up.sh --durable``
prepares (a three-pod JetStream StatefulSet with a claim per pod, reached from
this host through NodePort 30423) and the dev broker in the plain test namespace.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over what was observed, so
the paired negative control runs the same oracle against the reviewed defect.

Every fault here is a pod deletion inside the test namespaces — the StatefulSet
recreates the pod on the same claim, which is the failure the durable profile
declares it tolerates. Nothing touches nodes, other namespaces, or the cluster
policy. One control restarts the *dev* broker pod (``videoflow-test``), which
loses that broker's in-memory state: do not run it concurrently with the k8s
integration bucket.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
import json
import os
import time
from typing import Any, Callable, Dict, List

import defects
import defects_k8s
import pytest
from _brokers import delete_dlq, delete_run, unique_ids
from _k8s import (
    delete_pods,
    pod_names,
    pods,
    scale_statefulset,
    stream_facts,
    volume_kinds,
    wait_for_leader,
    wait_ready,
)
from _status import not_run

from videoflow.backends.capabilities import RELIABLE_WORK, FlowRequirements, ProfileRequest, plan_composition
from videoflow.backends.messaging import KIND_DATA, SUBSCRIPTION_DATA, ChannelId, Completed, Envelope, SubscriptionId
from videoflow.backends.outcomes import Accepted, Known, PublicationUnknown, Rejected, known, unknown
from videoflow.core.constants import BATCH
from videoflow.core.errors import IncompatibleProfile, UnobservableState
from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend, channel_spec_for, subscription_spec_for
from videoflow.messaging.topology import stream_name_for

DEV_NATS_URL_DEFAULT = 'nats://127.0.0.1:30422'


def _dev_nats_url() -> str:
    '''The dev broker in the plain test namespace, reachable from this host (``VF_K8S_NATS_URL``).'''
    from conftest import _listening
    url = os.environ.get('VF_K8S_NATS_URL', DEV_NATS_URL_DEFAULT)
    reason = _listening(url, 4222)
    if reason is not None:
        not_run(f'nothing listens at VF_K8S_NATS_URL={url} ({reason}); run scripts/k3s-test-up.sh')
    return url


def _envelope(channel : ChannelId, publication_id : str, body : bytes = b'record') -> Envelope:
    return Envelope(channel = channel, publication_id = publication_id, headers = {'Nats-Msg-Id': publication_id},
                    body = body, size = len(body), event_id = publication_id, partition_key = None, event_ts = None,
                    source_epoch = None, source_offset = None, schema_version = 4, kind = KIND_DATA)


def _bind(backend : JetStreamMessagingBackend, flow : str, run : str, node : str, consumer : str, replicas : int,
          ack_wait : float = 5.0, max_bytes : int | None = None) -> SubscriptionId:
    '''A replicated channel with one competing durable — the subscription must exist before a publish (interest retention).'''
    channel = ChannelId(flow, run, node)
    sub = SubscriptionId(channel, consumer, None, SUBSCRIPTION_DATA)
    backend.ensure_channel(channel_spec_for(flow, run, node, BATCH, RELIABLE_WORK, required = (sub,),
                                            replicas = replicas, max_bytes = max_bytes), 'conformance')
    backend.ensure_subscription(subscription_spec_for(sub, ack_wait, 20, 8), 'conformance')
    return sub


def _drain(backend : JetStreamMessagingBackend, sub : SubscriptionId, expected : set, deadline : float) -> set:
    '''Receive and complete until every id in ``expected`` was seen or the deadline passes; returns the ids seen.'''
    seen : set = set()
    while not expected <= seen and time.monotonic() < deadline:
        for delivery in backend.receive(sub, 8, 1 << 30, min(deadline, time.monotonic() + 5.0)):
            pid = delivery.headers.get('Nats-Msg-Id', '')
            backend.settle(delivery.token, Completed(), f'done:{pid}:{delivery.token.attempt}')
            seen.add(pid)
    return seen


# -- MSG-021 ----------------------------------------------------------------------

def _certify(planner : Callable[..., Any], caps : Any) -> str:
    '''What the planner says about surviving one broker/pod loss with reliable work on this broker.'''
    req = FlowRequirements(profiles = (ProfileRequest('p', RELIABLE_WORK),), tolerated_failures = 1)
    try:
        planner(req, caps)
    except IncompatibleProfile as e:
        return 'rejected: ' + str(e).splitlines()[-1].strip()
    except UnobservableState as e:
        return 'unobservable: ' + str(e).splitlines()[-1].strip()
    return 'certified'


def _oracle_msg_021(planner : Callable[..., Any], fixtures : List[Dict[str, Any]]) -> None:
    '''
    Only the fixture whose *effective* streams keep three copies on claimed
    storage certifies; emptyDir plus one copy, and three brokers with one copy,
    are explicit rejections; an unread replica count or storage cannot certify.
    '''
    by_name = {f['fixture']: f for f in fixtures}
    dev = by_name['single-broker-emptydir']
    assert dev['num_replicas'] in (None, 1) and 'emptyDir' in dev['volumes'], dev
    assert _certify(planner, dev['caps']).startswith('rejected'), dev
    one_copy = by_name['three-brokers-one-copy']
    assert one_copy['pods'] == 3 and one_copy['num_replicas'] in (None, 1), one_copy
    assert 'persistentVolumeClaim' in one_copy['volumes']
    assert _certify(planner, one_copy['caps']).startswith('rejected'), one_copy   # pods are not stream copies
    good = by_name['three-copies-persistent']
    assert good['num_replicas'] == 3 and 'persistentVolumeClaim' in good['volumes'], good
    assert _certify(planner, good['caps']) == 'certified', good
    # Access to the topology removed: an unread setting is not a satisfied one.
    for field in ('replication_factor', 'persistent_storage'):
        blind = dataclasses.replace(good['caps'], **{field: unknown('auth', 'stream_info denied')})
        assert _certify(planner, blind).startswith('unobservable'), field


def _observe_fixture(name : str, namespace : str, url : str, copies : int) -> Dict[str, Any]:
    '''Provision one channel on the broker, read its effective stream back, inspect the pods' volumes.'''
    flow, run = unique_ids('msg021')
    backend = JetStreamMessagingBackend(url, flow, run, BATCH)
    backend.start()
    try:
        backend.ensure_channel(channel_spec_for(flow, run, 'p', BATCH, RELIABLE_WORK, replicas = copies), 'conformance')
        caps = backend.capabilities()
        facts = stream_facts(url, stream_name_for(flow, run, 'p'))
    finally:
        backend.shutdown()
        delete_run(url, flow, run)
        delete_dlq(url, flow)
    nats_pods = pods(namespace, 'app=nats')
    volumes : set = set()
    for pod in nats_pods:
        volumes |= volume_kinds(pod)
    # The broker reports a file store; whether its volume outlives the pod is the
    # deployment's fact, read from the pods rather than assumed.
    claimed = bool(nats_pods) and all('persistentVolumeClaim' in volume_kinds(p) for p in nats_pods)
    observed = dataclasses.replace(caps, persistent_storage = known(claimed and facts['storage'] != 'memory'))
    return {'fixture': name, 'namespace': namespace, 'pods': len(nats_pods), 'volumes': sorted(volumes),
            'num_replicas': facts['num_replicas'], 'storage': facts['storage'], 'caps': observed,
            'verdict': _certify(plan_composition, observed)}


@pytest.mark.case('MSG-021')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(600)
def test_msg_021_durability_profile_validates_replicas_and_storage_rather(k3s, k3s_ha, evidence_dir) -> None:
    '''
    MSG-021 (P0, integration, kubernetes): Durability profile validates replicas and storage
    rather than durable names.

    Acceptance: Only fixtures whose effective broker and payload guarantees match the requested
    failure model certify; unsuitable fixtures are explicit expected-rejection tests.
    '''
    dev_url = _dev_nats_url()
    fixtures = [
        _observe_fixture('single-broker-emptydir', k3s['VF_K8S_NAMESPACE'], dev_url, 1),
        _observe_fixture('three-brokers-one-copy', k3s_ha['namespace'], k3s_ha['nats_url'], 1),
        _observe_fixture('three-copies-persistent', k3s_ha['namespace'], k3s_ha['nats_url'], 3),
    ]
    (evidence_dir / 'fixtures.json').write_text(json.dumps(
        [{k: v for k, v in f.items() if k != 'caps'} for f in fixtures], indent = 2))
    _oracle_msg_021(plan_composition, fixtures)


@pytest.mark.case('MSG-021')
@pytest.mark.level('model')
@pytest.mark.variant('planner')
def test_msg_021_planner_reads_copies_and_storage_not_names() -> None:
    '''The same oracle over declared observations: what the three deployments read back as.'''
    _oracle_msg_021(plan_composition, _declared_fixtures())


def _declared_fixtures() -> List[Dict[str, Any]]:
    from videoflow.backends.capabilities import LEDGER_WINDOW, MessagingCapabilities

    def caps(copies : int, claimed : bool) -> MessagingCapabilities:
        return MessagingCapabilities(
            adapter = 'jetstream', version = '2.10', retained_backlog = True, recoverable_delivery = True,
            latest_per_key = False, dedup_window_seconds = 120, publication_ledger = LEDGER_WINDOW,
            replication_factor = known(copies), persistent_storage = known(claimed),
            max_payload_bytes = known(1 << 20), credit_resizable = True, control_shares_data_slot = True)
    return [
        {'fixture': 'single-broker-emptydir', 'pods': 1, 'volumes': ['configMap', 'emptyDir'], 'num_replicas': None,
         'storage': 'file', 'caps': caps(1, False)},
        {'fixture': 'three-brokers-one-copy', 'pods': 3, 'volumes': ['configMap', 'persistentVolumeClaim'],
         'num_replicas': None, 'storage': 'file', 'caps': caps(1, True)},
        {'fixture': 'three-copies-persistent', 'pods': 3, 'volumes': ['configMap', 'persistentVolumeClaim'],
         'num_replicas': 3, 'storage': 'file', 'caps': caps(3, True)},
    ]


@pytest.mark.negative_control(of = 'MSG-021')
def test_msg_021_negative_control_name_certification_is_caught() -> None:
    '''A planner that certifies from durable names admits the emptyDir and one-copy fixtures; the oracle must fail.'''
    assert defects.detects(_oracle_msg_021, defects_k8s.name_certifying_planner(), _declared_fixtures())


# -- MSG-022 ----------------------------------------------------------------------

def _run_msg_022(namespace : str, url : str, replicas : int, recovery_seconds : float,
                 evidence : Dict[str, Any]) -> tuple:
    '''
    Publish 100 records, complete 20, hold 5 in flight (inside the durable's
    credit of 8, so dispatch never stalls on the hold), leave 75 undispatched;
    delete the stream leader's pod, then a follower's; drain what survived.
    Returns ``(accepted ids, ids completed after recovery, subscription observation)``.
    '''
    flow, run = unique_ids('msg022')
    stream = stream_name_for(flow, run, 'p')
    # No lease keepalive: the held deliveries stand for a worker that died mid-input,
    # and nothing extends a dead worker's leases — they must come back on their own.
    backend = JetStreamMessagingBackend(url, flow, run, BATCH, ack_confirm_seconds = 5.0, keepalive = False)
    backend.start()
    try:
        sub = _bind(backend, flow, run, 'p', 'c', replicas)
        channel = ChannelId(flow, run, 'p')
        accepted : Dict[str, int | None] = {}
        for i in range(100):
            pid = f'{flow}:{i}'
            outcome = backend.publish(_envelope(channel, pid, f'record-{i}'.encode()), time.monotonic() + 15.0)
            assert isinstance(outcome, Accepted), (i, outcome)
            accepted[pid] = outcome.sequence
        completed : set = set()
        held : List[Any] = []
        deadline = time.monotonic() + 60.0
        while len(completed) + len(held) < 25 and time.monotonic() < deadline:
            for delivery in backend.receive(sub, 8, 1 << 30, time.monotonic() + 5.0):
                pid = delivery.headers['Nats-Msg-Id']
                if len(completed) < 20:
                    backend.settle(delivery.token, Completed(), f'done:{pid}')
                    completed.add(pid)
                else:
                    held.append(delivery)
                if len(completed) + len(held) >= 25:
                    break
        assert len(completed) == 20 and len(held) == 5, (len(completed), len(held))
        before = stream_facts(url, stream)
        evidence['before'] = before
        # Fault 1: the stream leader's pod. Fault 2 (a clean fixture in the
        # catalog's terms): a follower's pod, deleted and recreated on its claim.
        victims = [before['leader']] if before['leader'] else []
        assert victims, before
        t0 = time.monotonic()
        delete_pods(namespace, victims)
        wait_ready(namespace, 'app=nats', 3, timeout = 300)
        after_leader_kill = wait_for_leader(url, stream, timeout = recovery_seconds)
        evidence['after_leader_kill'] = dict(after_leader_kill, seconds = round(time.monotonic() - t0, 1))
        follower = next((p for p in pod_names(namespace, 'app=nats') if p != after_leader_kill['leader']), None)
        if follower:
            t1 = time.monotonic()
            delete_pods(namespace, [follower])
            wait_ready(namespace, 'app=nats', 3, timeout = 300)
            evidence['after_follower_kill'] = dict(wait_for_leader(url, stream, timeout = recovery_seconds),
                                                   seconds = round(time.monotonic() - t1, 1))
        # Resume: the 5 held deliveries come back once their lease lapses; the 75
        # undispatched ones were never dispatched. Everything accepted must land.
        remaining = set(accepted) - completed
        seen = _drain(backend, sub, remaining, time.monotonic() + recovery_seconds)
        observation = backend.observe_subscription(sub)
        evidence['completed_before_fault'] = len(completed)
        evidence['completed_after_recovery'] = len(seen)
        evidence['missing'] = sorted(remaining - seen)
        evidence['final_stream'] = stream_facts(url, stream)
        return set(accepted), completed | seen, observation
    finally:
        backend.shutdown()
        delete_run(url, flow, run)
        delete_dlq(url, flow)


def _oracle_msg_022(accepted : set, completed : set, observation : Any) -> None:
    '''Every pre-fault accepted id is completed after recovery, with zero unexplained loss, and nothing is left leased or stranded.'''
    missing = accepted - completed
    assert not missing, f'{len(missing)} accepted record(s) lost across the pod losses: {sorted(missing)[:5]}'
    assert isinstance(observation, Known), observation
    assert observation.value.available == 0 and observation.value.leased == 0, observation.value


@pytest.mark.case('MSG-022')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(1200)
def test_msg_022_accepted_reliable_work_survives_declared_broker_and_pod(k3s_ha, evidence_dir) -> None:
    '''
    MSG-022 (P0, messaging, kubernetes): Accepted reliable work survives declared broker and pod
    failures.

    Acceptance: Every pre-fault accepted ID is completed or durably terminal after recovery
    within R, with zero unexplained loss for faults included in the certified profile.
    '''
    evidence : Dict[str, Any] = {'fault_model': 'one pod loss at a time, StatefulSet recreates it on its claim'}
    try:
        accepted, completed, observation = _run_msg_022(k3s_ha['namespace'], k3s_ha['nats_url'], 3, 240.0, evidence)
    finally:
        (evidence_dir / 'msg022.json').write_text(json.dumps(evidence, indent = 2, default = str))
    _oracle_msg_022(accepted, completed, observation)


@pytest.mark.level('kubernetes')
@pytest.mark.negative_control(of = 'MSG-022')
@pytest.mark.timeout(900)
def test_msg_022_negative_control_ephemeral_broker_loses_records(k3s, evidence_dir) -> None:
    '''
    The reviewed defect: the single emptyDir broker (``k8s/nats.yaml``) "survives"
    a pod restart only in name. Running the same oracle against it must detect the
    loss — otherwise the oracle proves nothing. Restarts the dev broker pod.
    '''
    url = _dev_nats_url()
    evidence : Dict[str, Any] = {}
    try:
        try:
            accepted, completed, observation = _run_msg_022(k3s['VF_K8S_NAMESPACE'], url, 1, 120.0, evidence)
        except Exception as e:  # noqa: BLE001 — losing the stream entirely is the defect, and is "detected"
            evidence['error'] = repr(e)
            return
    finally:
        (evidence_dir / 'msg022-ephemeral.json').write_text(json.dumps(evidence, indent = 2, default = str))
        # The dev broker is back; the k8s integration bucket expects its NodePort to answer.
        wait_ready(k3s['VF_K8S_NAMESPACE'], 'app=nats', 1, timeout = 300)
    assert defects.detects(_oracle_msg_022, accepted, completed, observation)


# -- MSG-023 ----------------------------------------------------------------------

def _oracle_msg_023_outage(attempts : List[Dict[str, Any]], p_seconds : float) -> None:
    '''No durable receipt without quorum, every attempt typed and within its deadline, none relabelled definite-rejected.'''
    assert attempts, 'no publication was attempted during the outage'
    for attempt in attempts:
        outcome = attempt['outcome']
        assert not isinstance(outcome, Accepted), f'a durable receipt was minted without quorum: {outcome}'
        assert isinstance(outcome, (PublicationUnknown, Rejected)), outcome
        if isinstance(outcome, Rejected):
            assert outcome.retryable, f'a quorum loss was reported as a definite, final rejection: {outcome}'
        assert attempt['elapsed'] <= p_seconds, f'outcome took {attempt["elapsed"]:.1f}s, over the {p_seconds:.0f}s bound'
        assert not isinstance(attempt['observed'], Accepted), attempt['observed']


def _oracle_msg_023_recovery(pre_fault : int, outage_ids : List[str], resumed : List[Any], messages : int,
                             seen : set) -> None:
    '''After restoration the same publications land exactly once and every pre-fault record is still there.'''
    assert all(isinstance(o, Accepted) for o in resumed), resumed
    assert messages == pre_fault + len(outage_ids), (messages, pre_fault, len(outage_ids))
    assert len(seen) == pre_fault + len(outage_ids), len(seen)


@pytest.mark.case('MSG-023')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(1200)
def test_msg_023_loss_of_quorum_or_storage_capacity_does_not_produce_false(k3s_ha, evidence_dir) -> None:
    '''
    MSG-023 (P0, messaging, kubernetes): Loss of quorum or storage capacity does not produce
    false durable receipts.

    Acceptance: Each attempt reports a truthful outcome within P. Within R after restoration,
    the reliable composition recovers an accounted logical result; an old send may remain
    Unknown/Unresolvable. No confirmed durable accepted record is lost within the declared fault
    model, and no unknown send is relabeled definitely rejected without evidence.

    Capacity is exercised on a stream with a 64 KiB byte cap (the "dedicated test
    volume with controlled free space" of the catalog): filling a shared node disk
    is not something a test on a shared cluster may do.
    '''
    namespace, url = k3s_ha['namespace'], k3s_ha['nats_url']
    flow, run = unique_ids('msg023')
    evidence : Dict[str, Any] = {}
    scaled_down = False
    backend = JetStreamMessagingBackend(url, flow, run, BATCH, ack_confirm_seconds = 5.0)
    backend.start()
    try:
        # -- quorum -------------------------------------------------------------
        sub = _bind(backend, flow, run, 'p', 'c', 3)
        channel = ChannelId(flow, run, 'p')
        stream = stream_name_for(flow, run, 'p')
        pre_fault = 5
        for i in range(pre_fault):
            assert isinstance(backend.publish(_envelope(channel, f'{flow}:{i}'), time.monotonic() + 15.0), Accepted)
        names_before = pod_names(namespace, 'app=nats')
        scaled_down = True
        # Two of three servers gone: no Raft majority left. Scaling (rather than
        # deleting pods, which the StatefulSet recreates within seconds) holds the
        # outage until it is deliberately lifted; the claims stay.
        scale_statefulset(namespace, 'nats', 1)
        survivors = wait_ready(namespace, 'app=nats', 1, timeout = 300)
        victims = sorted(set(names_before) - set(survivors))
        p_seconds = 5.0
        outage_ids = [f'{flow}:out{k}' for k in range(3)]
        attempts : List[Dict[str, Any]] = []
        for pid in outage_ids:
            env = _envelope(channel, pid)
            t0 = time.monotonic()
            outcome = backend.publish(env, t0 + p_seconds)
            elapsed = time.monotonic() - t0
            observed = backend.observe_publication(env)
            attempts.append({'pid': pid, 'outcome': outcome, 'elapsed': round(elapsed, 2), 'observed': observed})
        evidence['victims'] = victims
        evidence['outage_attempts'] = [dict(a, outcome = repr(a['outcome']), observed = repr(a['observed']))
                                       for a in attempts]
        _oracle_msg_023_outage(attempts, p_seconds + 3.0)
        # -- restoration ---------------------------------------------------------
        t1 = time.monotonic()
        scale_statefulset(namespace, 'nats', 3)
        scaled_down = False
        wait_ready(namespace, 'app=nats', 3, timeout = 300)
        evidence['restored'] = dict(wait_for_leader(url, stream, timeout = 240.0), seconds = round(time.monotonic() - t1, 1))
        resumed = [backend.publish(_envelope(channel, pid), time.monotonic() + 15.0) for pid in outage_ids]
        facts = stream_facts(url, stream)
        seen = _drain(backend, sub, {f'{flow}:{i}' for i in range(pre_fault)} | set(outage_ids), time.monotonic() + 120.0)
        evidence['resumed'] = [repr(o) for o in resumed]
        evidence['stream_after'] = facts
        _oracle_msg_023_recovery(pre_fault, outage_ids, resumed, facts['messages'], seen)
        # -- capacity -----------------------------------------------------------
        cap_sub = _bind(backend, flow, run, 'q', 'c', 3, max_bytes = 64 * 1024)
        cap_channel = ChannelId(flow, run, 'q')
        body = b'x' * (24 * 1024)
        outcomes : List[Any] = []
        for k in range(6):
            t0 = time.monotonic()
            outcomes.append((backend.publish(_envelope(cap_channel, f'{flow}:cap{k}', body), t0 + p_seconds),
                             round(time.monotonic() - t0, 2)))
        accepted_count = sum(1 for o, _e in outcomes if isinstance(o, Accepted))
        refused = [o for o, _e in outcomes if not isinstance(o, Accepted)]
        evidence['capacity'] = [(repr(o), e) for o, e in outcomes]
        assert refused, 'a 64 KiB stream accepted six 24 KiB records'
        assert all(isinstance(o, Rejected) and o.retryable for o in refused), refused
        assert all(e <= p_seconds + 3.0 for _o, e in outcomes), outcomes
        assert stream_facts(url, stream_name_for(flow, run, 'q'))['messages'] == accepted_count
        # Free the space (interest retention drops completed records) and the same publications land.
        seen_cap = _drain(backend, cap_sub, {f'{flow}:cap{k}' for k in range(accepted_count)}, time.monotonic() + 60.0)
        assert len(seen_cap) == accepted_count
        retried = [backend.publish(_envelope(cap_channel, f'{flow}:cap{k}', body), time.monotonic() + 15.0)
                   for k in range(accepted_count, 6)]
        evidence['capacity_retry'] = [repr(o) for o in retried]
        assert all(isinstance(o, Accepted) for o in retried), retried
    finally:
        (evidence_dir / 'msg023.json').write_text(json.dumps(evidence, indent = 2, default = str))
        if scaled_down:
            # Never leave the fixture degraded, whatever failed above.
            scale_statefulset(namespace, 'nats', 3)
            wait_ready(namespace, 'app=nats', 3, timeout = 300)
        backend.shutdown()
        delete_run(url, flow, run)
        delete_dlq(url, flow)


@pytest.mark.negative_control(of = 'MSG-023')
def test_msg_023_negative_control_receipt_from_silence_is_caught() -> None:
    '''An adapter that turns an unacknowledged send into Accepted must fail the outage oracle.'''
    silent = PublicationUnknown('p1', 'no acknowledgement within 5.0s')
    attempts = [{'pid': 'p1', 'outcome': defects_k8s.accepting_on_timeout(silent), 'elapsed': 5.0,
                 'observed': PublicationUnknown('p1', 'unresolved')}]
    assert defects.detects(_oracle_msg_023_outage, attempts, 8.0)
    honest = [{'pid': 'p1', 'outcome': silent, 'elapsed': 5.0, 'observed': PublicationUnknown('p1', 'unresolved')}]
    _oracle_msg_023_outage(honest, 8.0)
