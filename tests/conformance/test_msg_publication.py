'''
Conformance cases: MSG-013, MSG-014, MSG-015.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a driver (``_msgdrivers``),
so the broker-level primary, the ``memory`` variant and the negative control decide
with the same assertions.

The three cases are about what a publisher may *claim*. A send whose receipt never
came is ``PublicationUnknown`` — neither accepted nor rejected — and is reconciled
through the backend's dedup ledger inside its window and through the runtime's own
intent beyond it (MSG-013, MSG-014); a live publisher whose broker is gone reports
every attempt as unknown or dropped, never as accepted (MSG-015). On the broker the
faults are real: toxiproxy stalls, cuts and delays the connection the adapters use,
while the driver's own bookkeeping goes to the broker directly.
'''
from __future__ import absolute_import, division, print_function

import json
import time
from typing import Any, Callable, Dict, List

import defects
import pytest
from _brokers import unique_ids
from _msgdrivers import JetStreamDriver, MemoryDriver
from _toxiproxy import Toxiproxy

from videoflow.backends import faults
from videoflow.backends.capabilities import (
    LEDGER_PERSISTENT,
    LEDGER_WINDOW,
    LIVE_LATEST,
    RELIABLE_WORK,
    RETENTION_INTEREST,
    RETENTION_LIMITS,
)
from videoflow.backends.messaging import Completed
from videoflow.backends.outcomes import Accepted, Known, PublicationUnknown, PublicationUnresolvable, Rejected
from videoflow.core.constants import REALTIME
from videoflow.messaging import nats_messenger

NATS_TIMEOUT = 180


def _write(evidence_dir : Any, name : str, record : Dict[str, Any]) -> None:
    (evidence_dir / name).write_text(json.dumps(record, indent = 2, default = str))


def _collect(driver : Any, bound : Any, wanted : int, timeout : float) -> List[Any]:
    got : List[Any] = []

    def step() -> bool:
        for delivery in driver.receive(bound, timeout = 0.5):
            got.append(delivery)
            driver.settle(bound, delivery.token, Completed())
        return len(got) >= wanted
    driver.until(step, timeout)
    return got


# -- MSG-013 -------------------------------------------------------------------------

def _oracle_msg_013(driver : Any, record : Dict[str, Any]) -> None:
    channel = driver.channel('out', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 1)
    bound = driver.bind(channel, 'c', receiver = 'c', ack_wait = 10, max_deliver = 4, credit = 8, prefetch = 1)
    trace : List[Dict[str, Any]] = []
    # A full channel: the refusal is definite and typed, and clears when capacity frees.
    assert isinstance(driver.publish(channel, 'op-1', b'1'), Accepted)
    assert driver.until(lambda: driver.retained(channel) == 1, 10)
    refused = driver.publish(channel, 'op-2', b'2', deadline = time.monotonic() + 2.0)
    assert isinstance(refused, Rejected) and refused.retryable, refused
    trace.append({'op': 'op-2', 'outcome': str(refused)})
    assert _collect(driver, bound, 1, 15), 'op-1 was never delivered'
    assert driver.until(lambda: driver.retained(channel) == 0, 10)
    retried = driver.publish(channel, 'op-2', b'2')
    assert isinstance(retried, Accepted) and retried.duplicate is False, retried
    trace.append({'op': 'op-2', 'outcome': str(retried), 'note': 'retry after the definite refusal'})
    assert _collect(driver, bound, 1, 15)
    # A stalled broker: the caller's deadline passes before any acceptance is known.
    with driver.stalled_acceptance(channel):
        started = time.monotonic()
        unknown = driver.publish(channel, 'op-3', b'3', deadline = time.monotonic() + 1.0)
        waited = time.monotonic() - started
        assert isinstance(unknown, PublicationUnknown), unknown
        during = driver.observe_publication(channel, 'op-3', b'3')
        assert not isinstance(during, (Accepted, Rejected)), during
        cancelled = driver.cancel_publication(channel, 'op-3')
        trace.append({'op': 'op-3', 'outcome': str(unknown), 'caller_waited': waited, 'while_stalled': str(during),
                      'cancelled': cancelled})
    # After recovery: a definite cancellation means the original never lands and the
    # retry is the one copy; an ambiguous one is reconciled through the ledger.
    if cancelled:
        assert driver.until(lambda: driver.retained(channel) == 0, 10), 'a cancelled send landed'
        retried = driver.publish(channel, 'op-3', b'3')
        assert isinstance(retried, Accepted) and retried.duplicate is False, retried
        trace.append({'op': 'op-3', 'outcome': str(retried), 'note': 'idempotent retry after the definite cancellation'})
    else:
        resolved : List[Any] = []

        def reconciled() -> bool:
            resolved.append(driver.observe_publication(channel, 'op-3', b'3'))
            return isinstance(resolved[-1], Accepted)
        assert driver.until(reconciled, 30), resolved[-1]
        # Either the stalled original landed late (a duplicate acceptance) or the
        # broker dropped the stalled connection and it never did (a fresh one):
        # the ledger cannot tell in advance, and the outcome is one copy either way.
        trace.append({'op': 'op-3', 'outcome': str(resolved[-1]), 'original_landed': resolved[-1].duplicate,
                      'note': 'reconciled through the dedup ledger'})
    got = _collect(driver, bound, 1, 15)
    ids = [d.token.message_id for d in got]
    assert ids.count('op-3') == 1, ids
    assert driver.until(lambda: driver.retained(channel) == 0, 10)
    record['operations'] = trace


@pytest.mark.case('MSG-013')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_013_publish_timeout_has_a_single_cancellation_and(nats_url, nats_proxied_url, toxiproxy_url,
                                                               evidence_dir) -> None:
    '''
    MSG-013 (P0, messaging, broker): Publish timeout has a single cancellation and
    reconciliation lifecycle.

    Acceptance: For each operation, reported acceptance/cancellation is truthful; no late
    publish follows a definitely-cancelled result. After broker recovery the reliable
    composition recovers an accounted logical output within R using durable intent/idempotent
    retry, while the original send may correctly remain Unknown/Unresolvable if native
    acceptance history is unavailable.
    '''
    toxiproxy = Toxiproxy(toxiproxy_url)
    flow, run = unique_ids('msg013')
    driver = JetStreamDriver(nats_url, flow, run, client_url = nats_proxied_url, toxiproxy = toxiproxy)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_013(driver, record)
    finally:
        toxiproxy.reset()
        driver.close()
        _write(evidence_dir, 'operation_trace.json', record)


@pytest.mark.case('MSG-013')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_013_memory_unknown_then_cancelled_or_reconciled(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_013(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'operation_trace.json', record)


@pytest.mark.negative_control(of = 'MSG-013')
def test_msg_013_detects_a_publisher_that_guesses(monkeypatch) -> None:
    defects.guessing_publisher(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_013, driver, {})
    finally:
        driver.close()


# -- MSG-014 -------------------------------------------------------------------------

WINDOW = 2


def _oracle_msg_014(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    channel = driver.channel('out', RELIABLE_WORK, RETENTION_INTEREST, dedup_window = WINDOW)
    effective_window = driver.effective_dedup_window(channel)
    assert effective_window == WINDOW, effective_window
    capabilities = driver.capabilities()
    record['declared'] = {'ledger': capabilities.publication_ledger, 'adapter_window': capabilities.dedup_window_seconds,
                          'channel_window': effective_window}
    bound = driver.bind(channel, 'c', receiver = 'c', ack_wait = 10, max_deliver = 4, credit = 8, prefetch = 1)
    # The broker stores A; its acknowledgment is lost before the publisher sees it.
    schedule = faults.FaultSchedule({'publish.receipt.before': faults.Nth(1, faults.DropResponse())})
    with schedule:
        lost = driver.publish(channel, 'A', b'A')
    if record_faults is not None:
        record_faults(schedule)
    assert isinstance(lost, PublicationUnknown), lost                  # never a definite rejection
    sent_at = time.monotonic()
    # Within the window the retry under the same operation id is coalesced.
    retry = driver.publish(channel, 'A', b'A')
    assert isinstance(retry, Accepted) and retry.duplicate is True, retry
    resolved = driver.observe_publication(channel, 'A', b'A')
    assert isinstance(resolved, Accepted) and resolved.duplicate is True, resolved
    record['within_window'] = {'lost': str(lost), 'retry': str(retry), 'resolved': str(resolved)}
    # Beyond it the ledger cannot say, and a retry is a new physical copy.
    driver.wait(WINDOW + 0.5)
    beyond = driver.observe_publication(channel, 'A', b'A')
    assert isinstance(beyond, PublicationUnresolvable), beyond
    late = driver.publish(channel, 'A', b'A')
    assert isinstance(late, Accepted), late
    record['beyond_window'] = {'observed': str(beyond), 'retry': str(late), 'elapsed': time.monotonic() - sent_at}
    if capabilities.publication_ledger == LEDGER_PERSISTENT:
        assert late.duplicate is True, 'a persistent-ledger claim that only relies on the window'
    else:
        assert capabilities.publication_ledger == LEDGER_WINDOW, capabilities.publication_ledger
    copies = _collect(driver, bound, 2 if not late.duplicate else 1, 20)
    ids = [d.token.message_id for d in copies]
    record['raw_deliveries'] = ids
    assert ids and set(ids) == {'A'}, ids
    assert len(ids) == (1 if late.duplicate else 2), ids           # duplicates only where the contract permits them
    assert driver.until(lambda: driver.retained(channel) == 0, 10)


@pytest.mark.case('MSG-014')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_014_lost_publication_acknowledgment_preserves_truthful(nats_url, nats_proxied_url, toxiproxy_url,
                                                                    evidence_dir, record_faults) -> None:
    '''
    MSG-014 (P0, messaging, broker): Lost publication acknowledgment preserves truthful
    deduplication guarantees.

    Acceptance: No accepted ID is lost. Observed duplicate behavior matches the declared window
    and capability contract; a backend claiming unlimited idempotent publish fails if it only
    relies on W.

    The lost receipt is first the adapter-boundary model (the barrier discards the
    PubAck), then a real one: a latency toxic holds the server's acknowledgment past
    the caller's deadline.
    '''
    toxiproxy = Toxiproxy(toxiproxy_url)
    flow, run = unique_ids('msg014')
    driver = JetStreamDriver(nats_url, flow, run, client_url = nats_proxied_url, toxiproxy = toxiproxy)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_014(driver, record, record_faults)
        channel = driver.channel('slow', RELIABLE_WORK, RETENTION_INTEREST)
        bound = driver.bind(channel, 'c', receiver = 'slow-c', ack_wait = 10, max_deliver = 4, credit = 8, prefetch = 1)
        name = toxiproxy.add_toxic('nats', 'latency', {'latency': 3000, 'jitter': 0}, name = 'vf-late-ack', stream = 'downstream')
        try:
            delayed = driver.publish(channel, 'B', b'B', deadline = time.monotonic() + 1.0)
        finally:
            toxiproxy.remove_toxic('nats', name)
        assert isinstance(delayed, PublicationUnknown), delayed
        resolved : List[Any] = []

        def reconciled() -> bool:
            resolved.append(driver.observe_publication(channel, 'B', b'B'))
            return isinstance(resolved[-1], Accepted)
        assert driver.until(reconciled, 30), resolved[-1]
        assert resolved[-1].duplicate is True, resolved[-1]
        copies = _collect(driver, bound, 1, 20)
        assert [d.token.message_id for d in copies] == ['B'], copies
        record['real_lost_ack'] = {'delayed': str(delayed), 'resolved': str(resolved[-1])}
    finally:
        toxiproxy.reset()
        driver.close()
        _write(evidence_dir, 'dedup_trace.json', record)


@pytest.mark.case('MSG-014')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_014_memory_lost_receipt_is_unknown_and_coalesced_inside_the_window(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r', dedup_window_seconds = WINDOW)
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_014(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'dedup_trace.json', record)


@pytest.mark.negative_control(of = 'MSG-014')
def test_msg_014_detects_an_unlimited_idempotence_claim(monkeypatch) -> None:
    defects.unlimited_idempotence_claim(monkeypatch)
    driver = MemoryDriver('f', 'r', dedup_window_seconds = WINDOW)
    try:
        assert defects.detects(_oracle_msg_014, driver, {})
    finally:
        driver.close()


# -- MSG-015 -------------------------------------------------------------------------

FRAMES = [f'frame-{i:02d}' for i in range(20)]
LIVE_RECOVERY_BOUND_SECONDS = 30.0


def _oracle_msg_015(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    channel = driver.channel('cam', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 1)
    bound = driver.bind(channel, 'display', receiver = 'display', ack_wait = 5, max_deliver = 1, credit = 8, prefetch = 1)
    publisher = driver.messenger('cam', [], REALTIME)
    receipts : Dict[str, str] = {}
    with driver.disconnected():
        for pid in FRAMES:
            outcome = driver.publish(channel, pid, pid.encode(), deadline = time.monotonic() + 0.3)
            receipts[pid] = type(outcome).__name__
            assert not isinstance(outcome, Accepted), f'{pid}: accepted while the broker was unreachable'
            assert isinstance(outcome, (Rejected, PublicationUnknown)), outcome
        publisher.publish_message({'frame': 'during-outage'})
    stats_during = dict(publisher.publication_stats)
    assert stats_during.get('accepted', 0) == 0, stats_during
    assert stats_during.get('unknown', 0) + stats_during.get('dropped', 0) >= 1, stats_during
    record['during_outage'] = {'receipts': receipts, 'messenger_stats': stats_during}
    # Reconnection: fresh delivery resumes within the live bound.
    started = time.monotonic()
    assert driver.until(driver.connected, LIVE_RECOVERY_BOUND_SECONDS), 'the publisher never reconnected'
    sentinel = driver.publish(channel, 'sentinel', b'S')
    assert isinstance(sentinel, Accepted), sentinel
    latency = time.monotonic() - started
    assert latency <= LIVE_RECOVERY_BOUND_SECONDS, latency
    publisher.publish_message({'frame': 'after-reconnect'})
    assert publisher.publication_stats.get('accepted', 0) == 1, publisher.publication_stats
    # An ambiguous acknowledgment loss is reported as unknown — distinguishable from a drop.
    schedule = faults.FaultSchedule({'publish.receipt.before': faults.Nth(1, faults.DropResponse())})
    with schedule:
        ambiguous = driver.publish(channel, 'ack-lost', b'L')
    if record_faults is not None:
        record_faults(schedule)
    assert isinstance(ambiguous, PublicationUnknown), ambiguous
    # Freshness: whatever the client buffered during the outage did not replace the newest frame.
    assert driver.until(lambda: driver.retained(channel) == 1, 10)
    newest = driver.last_retained(channel)
    assert newest == 'ack-lost', newest
    observed = driver.observe(bound)
    assert isinstance(observed, Known), observed
    record['after_reconnect'] = {'sentinel_latency': latency, 'ambiguous': str(ambiguous), 'newest_retained': newest,
                                 'dropped_reported': observed.value.dropped}
    # A definite refusal is visible with its reason and counted as a drop, never as acceptance.
    driver.drop_channel(channel)
    refused = driver.publish(channel, 'orphan', b'O', deadline = time.monotonic() + 2.0)
    assert isinstance(refused, Rejected) and refused.reason, refused
    publisher.publish_message({'frame': 'after-drop'})
    drops = publisher.take_drops()
    assert drops.get('publish_discarded') == 1, drops
    assert publisher.publication_stats.get('accepted', 0) == 1 and publisher.publication_stats.get('dropped', 0) == 1, \
        publisher.publication_stats
    record['definite_drop'] = {'refused': str(refused), 'messenger_drops': drops, 'messenger_stats': dict(publisher.publication_stats)}
    publisher.close()


@pytest.mark.case('MSG-015')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_015_live_publication_failures_are_observable_and_do_not(nats_url, nats_proxied_url, toxiproxy_url,
                                                                     evidence_dir, record_faults, monkeypatch) -> None:
    '''
    MSG-015 (P1, messaging, broker): Live publication failures are observable and do not
    masquerade as acceptance.

    Acceptance: All 20 attempts reconcile to observed receipt states; no false confirmed-
    accepted receipt. Sentinel is delivered within the configured live recovery bound after
    reconnection.
    '''
    monkeypatch.setattr(nats_messenger, '_PUBLISH_TIMEOUT', 2)          # the live publisher's patience, for the test's clock
    toxiproxy = Toxiproxy(toxiproxy_url)
    flow, run = unique_ids('msg015')
    driver = JetStreamDriver(nats_url, flow, run, REALTIME, client_url = nats_proxied_url, toxiproxy = toxiproxy)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_015(driver, record, record_faults)
    finally:
        toxiproxy.reset()
        driver.close()
        _write(evidence_dir, 'live_receipts.json', record)


@pytest.mark.case('MSG-015')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_015_memory_live_outcomes_are_unknown_or_dropped_never_accepted(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_015(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'live_receipts.json', record)


@pytest.mark.negative_control(of = 'MSG-015')
def test_msg_015_detects_a_swallowed_live_failure(monkeypatch) -> None:
    defects.swallowed_live_failure(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_015, driver, {})
    finally:
        driver.close()
