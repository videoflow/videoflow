'''
Conformance cases: MSG-026.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it.

A benchmark, not a scale claim: the graph series, the SLOs and the declared
supported size all come from the operator's thresholds (``bench`` gate); what is
measured is written as evidence with the environment it was measured on.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import subprocess
import time
from typing import Any, Dict, List

import defects
import pytest
from _bench import Verdict, broker_resources, environment_manifest, jsz, summary
from _brokers import delete_dlq, delete_run, run_async, unique_ids
from _msgdrivers import spec

from videoflow.backends.capabilities import MAX_STREAMS_ENV
from videoflow.core.constants import BATCH
from videoflow.core.errors import IncompatibleProfile
from videoflow.deploy import admission
from videoflow.deploy.admission import graph_size, jetstream_capabilities_observed
from videoflow.messaging.topology import provision_flow_sync, subject_for


def _graph(nodes : int, fanout : int, replicas : int) -> List[Any]:
    '''A producer feeding a ``fanout``-ary tree of processors, ``nodes`` nodes in all, one sink leaf per branch.'''
    specs = [spec('src', [], 'producer', True)]
    parents = ['src']
    index = 1
    while len(specs) < nodes:
        next_level = []
        for parent in parents:
            for _ in range(fanout):
                if len(specs) >= nodes:
                    break
                name = f'n{index}'
                index += 1
                specs.append(spec(name, [parent], 'processor', True, nb_tasks = replicas))
                next_level.append(name)
        parents = next_level or parents
    return specs


def _publish_roundtrips(nats_url : str, subjects : List[str], count : int = 20) -> List[float]:
    '''Fixed compact messages to each subject; the PubAck round trip is the steady-state figure.'''
    import nats  # optional dep (distributed extras)

    async def _go() -> List[float]:
        nc = await nats.connect(nats_url)
        js = nc.jetstream()
        took : List[float] = []
        try:
            for i in range(count):
                subject = subjects[i % len(subjects)]
                t0 = time.perf_counter()
                await js.publish(subject, b'{"seq": %d}' % i, headers = {'Nats-Msg-Id': f'bench-{subject}-{i}'})
                took.append(time.perf_counter() - t0)
        finally:
            await nc.drain()
        return took
    return run_async(_go, timeout = 120.0)


def _reconnect_seconds(nats_url : str, container : str) -> Dict[str, Any]:
    '''Restart the broker container and time a client's reconnect and the stream inventory's return.'''
    import nats  # optional dep

    async def _go() -> Dict[str, Any]:
        events : Dict[str, float] = {}

        async def disconnected() -> None:
            events.setdefault('disconnected', time.perf_counter())

        async def reconnected() -> None:
            events.setdefault('reconnected', time.perf_counter())
        nc = await nats.connect(nats_url, disconnected_cb = disconnected, reconnected_cb = reconnected,
                                max_reconnect_attempts = -1, reconnect_time_wait = 0.2)
        try:
            before = jsz() or {}
            t0 = time.perf_counter()
            subprocess.run(['docker', 'restart', container], check = True, capture_output = True, timeout = 120)
            deadline = time.monotonic() + 60
            while 'reconnected' not in events and time.monotonic() < deadline:
                import asyncio
                await asyncio.sleep(0.1)
            recovered = None
            while time.monotonic() < deadline:
                after = jsz()
                if after and after.get('streams') == before.get('streams') and after.get('consumers') == before.get('consumers'):
                    recovered = time.perf_counter() - t0
                    break
                import asyncio
                await asyncio.sleep(0.2)
            return {'restart_at': t0, 'reconnect_seconds': (events.get('reconnected', float('nan')) - t0),
                    'inventory_recovered_seconds': recovered, 'streams_before': before.get('streams'),
                    'consumers_before': before.get('consumers'), 'inventory_after': jsz()}
        finally:
            await nc.close()
    return run_async(_go, timeout = 180.0)


def _oracle_msg_026(nats_url : str, thresholds : Dict[str, Any], evidence : Dict[str, Any],
                    monkeypatch : pytest.MonkeyPatch) -> None:
    verdict = Verdict()
    baseline = jsz()
    assert baseline is not None, 'the broker monitoring endpoint (/jsz) is needed for resource evidence'
    evidence['baseline'] = baseline
    evidence['resource_series'] = [broker_resources()]
    # 1. The adapter declares its supported size: the account limits read back,
    #    capped by the operator's declared maximum.
    declared = int(thresholds.get('declared_max_streams', 0)) or None
    if declared:
        monkeypatch.setenv(MAX_STREAMS_ENV, str(declared))
    caps = jetstream_capabilities_observed(nats_url)
    evidence['declared_limit'] = {'max_streams': getattr(caps.max_streams, 'value', None),
                                  'max_consumers': getattr(caps.max_consumers, 'value', None)}
    # 2. The graph series: provision, count, publish, tear down.
    series = []
    for point in thresholds['graph_series']:
        specs = _graph(int(point['nodes']), int(point['fanout']), int(point['replicas']))
        flow_id, _ = unique_ids('bench')
        runs = [unique_ids('r')[1] for _ in range(int(point.get('runs', 1)))]
        expected_streams, expected_consumers = graph_size(specs, flow_id, runs[0])
        before = jsz() or {}
        provision_took = []
        for run_id in runs:
            t0 = time.perf_counter()
            provision_flow_sync(nats_url, specs, flow_id, run_id, BATCH, timeout = 60.0)
            provision_took.append(time.perf_counter() - t0)
        after = jsz() or {}
        # Provisioning creates the streams, the EOS anchors and the data durables;
        # the per-process EOS durables are the workers' (not counted here).
        anchors_and_durables = expected_consumers - sum(s.nb_tasks for s in specs for p in s.parents)
        growth = {'streams': (after.get('streams', 0) - before.get('streams', 0)),
                  'consumers': (after.get('consumers', 0) - before.get('consumers', 0))}
        # The DLQ stream is one per flow, shared by the runs.
        expected_growth = {'streams': (expected_streams - 1) * len(runs) + 1,
                           'consumers': anchors_and_durables * len(runs)}
        roundtrips = _publish_roundtrips(nats_url, [subject_for(flow_id, runs[0], s.name) for s in specs])
        evidence['resource_series'].append(broker_resources())
        for run_id in runs:
            delete_run(nats_url, flow_id, run_id)
        delete_dlq(nats_url, flow_id)
        record = {'point': point, 'flow_id': flow_id, 'runs': runs, 'expected_per_run': {'streams': expected_streams, 'consumers': expected_consumers},
                  'growth': growth, 'expected_growth': expected_growth, 'provision_seconds': provision_took,
                  'publish_roundtrip': summary(roundtrips)}
        series.append(record)
        verdict.check(f'growth matches topology ({point})', growth, growth == expected_growth, expected_growth)
        verdict.check(f'provision seconds ({point})', max(provision_took),
                      max(provision_took) <= float(thresholds['provision_seconds_max']), thresholds['provision_seconds_max'])
        verdict.check(f'publish round trip p95 ({point})', record['publish_roundtrip']['p95'],
                      record['publish_roundtrip']['p95'] <= float(thresholds['publish_roundtrip_seconds_max']),
                      thresholds['publish_roundtrip_seconds_max'])
    evidence['series'] = series
    # 3. Repeated create/teardown cycles return the inventory to its baseline.
    cycles = []
    specs = _graph(8, 2, 1)
    tolerance = int(thresholds.get('teardown_baseline_tolerance', 0))
    for cycle in range(int(thresholds.get('teardown_cycles', 3))):
        flow_id, run_id = unique_ids('cycle')
        before = jsz() or {}
        provision_flow_sync(nats_url, specs, flow_id, run_id, BATCH, timeout = 60.0)
        delete_run(nats_url, flow_id, run_id)
        # The flow-level DLQ stream is deliberately not a run's to delete (STREAM-8);
        # a flow-wide teardown removes it, which is what the harness does here.
        delete_dlq(nats_url, flow_id)
        after = jsz() or {}
        leftover = {'streams': after.get('streams', 0) - before.get('streams', 0),
                    'consumers': after.get('consumers', 0) - before.get('consumers', 0)}
        cycles.append({'cycle': cycle, 'before': before.get('streams'), 'after': after.get('streams'), 'leftover': leftover})
        verdict.check(f'teardown returns to baseline (cycle {cycle})', leftover,
                      abs(leftover['streams']) <= tolerance and abs(leftover['consumers']) <= tolerance, tolerance)
    evidence['teardown_cycles'] = cycles
    evidence['resource_series'].append(broker_resources())
    # 4. A restart, when the thresholds name the broker container: reconnect and inventory recovery.
    container = thresholds.get('restart_broker_container')
    if container:
        flow_id, run_id = unique_ids('restart')
        provision_flow_sync(nats_url, _graph(8, 2, 1), flow_id, run_id, BATCH, timeout = 60.0)
        try:
            recovery = _reconnect_seconds(nats_url, str(container))
        finally:
            delete_run(nats_url, flow_id, run_id)
        evidence['restart'] = recovery
        verdict.check('reconnect seconds', recovery['reconnect_seconds'],
                      recovery['reconnect_seconds'] <= float(thresholds['reconnect_seconds_max']), thresholds['reconnect_seconds_max'])
        verdict.check('inventory recovered', recovery['inventory_recovered_seconds'],
                      recovery['inventory_recovered_seconds'] is not None, 'streams and consumers back after restart')
    # 5. Over the declared limit: an explicit rejection before anything is provisioned.
    if declared:
        limit = getattr(caps.max_streams, 'value', None)
        assert limit is not None and limit <= declared, caps.max_streams
        oversized = _graph(limit + 2, 2, 1)
        flow_id, run_id = unique_ids('over')
        before = jsz() or {}
        with pytest.raises(IncompatibleProfile) as e:
            admission.verify_graph_size(oversized, flow_id, run_id, caps)
            provision_flow_sync(nats_url, oversized, flow_id, run_id, BATCH, timeout = 60.0)
        after = jsz() or {}
        evidence['over_limit'] = {'graph_streams': graph_size(oversized, flow_id, run_id)[0], 'limit': limit,
                                  'rejection': str(e.value.message)[:400], 'streams_before': before.get('streams'),
                                  'streams_after': after.get('streams')}
        assert after.get('streams') == before.get('streams'), 'the over-limit graph was partially provisioned'
        verdict.check('over-limit graph rejected before provisioning', evidence['over_limit']['rejection'][:60], True, limit)
    evidence['verdict'] = verdict.checks
    assert not verdict.failed, verdict.failed


@pytest.mark.case('MSG-026')
@pytest.mark.level('benchmark')
def test_msg_026_stream_and_subscription_growth_is_measured_against_the(bench, nats_url, evidence_dir, monkeypatch) -> None:
    '''
    MSG-026 (P2, messaging, benchmark): Stream and subscription growth is measured against the
    supported graph limit.

    Acceptance: Publish measured limits with hardware and versions; pass only when the declared
    SLO and bounded post-teardown resource baseline hold. No universal camera-count claim from
    this test.
    '''
    evidence : Dict[str, Any] = {'environment': environment_manifest({'thresholds': bench}),
                                 'monitor': os.environ.get('VF_TEST_NATS_MONITOR_URL', 'http://localhost:8222')}
    try:
        _oracle_msg_026(nats_url, bench, evidence, monkeypatch)
    finally:
        (evidence_dir / 'graph_growth.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'MSG-026')
def test_msg_026_detects_a_limit_the_planner_never_checks(monkeypatch) -> None:
    '''Without the graph-size check an over-limit graph is provisioned as far as the account allows.'''
    from videoflow.backends.capabilities import MessagingCapabilities
    from videoflow.backends.outcomes import known
    caps = MessagingCapabilities('jetstream', '2.10', True, True, False, 120, 'window', known(1), known(True),
                                 known(1 << 20), True, True, max_streams = known(4))

    def oracle() -> None:
        with pytest.raises(IncompatibleProfile):
            admission.verify_graph_size(_graph(8, 2, 1), 'f', 'r', caps)
    oracle()                                        # the check refuses nine streams against a limit of four
    defects.no_graph_limit(monkeypatch)
    assert defects.detects(oracle)
