'''
Negative controls for the RUN join and completion cases (RUN-001, RUN-002,
RUN-005, RUN-006, RUN-007, RUN-008, RUN-009, RUN-010, RUN-016): one reproduction
per reviewed defect, applied as a monkeypatch so the paired oracle in
``test_run_joins.py`` / ``test_run_completion.py`` can be shown to *fail*
against it. A case is only evidence if its oracle would have caught the bug it
was written for; the results writer marks a case INVALID_TEST when a defect
goes undetected.

Like ``defects.py``, these live in the test tree on purpose: nothing under
``videoflow/`` may import them, and none is a behaviour anyone should be able
to switch on. Each reproduction keeps the fault barriers of the code it replaces,
so a schedule written for the fixed code still fires against the defect.
'''
from __future__ import absolute_import, division, print_function

from typing import Any

import defects
import pytest

from videoflow.backends import faults
from videoflow.backends.outcomes import Unknown, known
from videoflow.core.policies import JoinPolicy
from videoflow.messaging import grouping
from videoflow.messaging.grouping import TraceGroupAssembler
from videoflow.messaging.nats_messenger import NATSMessenger, _AckHandle
from videoflow.wire.serialization import decode_envelope

# -- RUN-016: identity from the rounded timestamp alone ---------------------------------------

def timestamp_only_group_identity(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``JOIN-20``'s ``tw-{µs}``: two groups at one rounded microsecond minted one id.'''
    monkeypatch.setattr(grouping, 'group_identity',
                        lambda members, window_id, rounded_micros = None: f'tw-{rounded_micros}')


# -- RUN-001: a redelivered half TERMed the handle it replaced ----------------------------------

def term_on_supersede(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed assembler terminated the stale handle of a redelivered half, and
    the transport of the day applied that TERM to the message itself (no attempt
    fence) — so the second attempt of A removed A from the broker.
    '''
    defects.unfenced_settle(monkeypatch)
    real_add = TraceGroupAssembler.add

    def add(self : TraceGroupAssembler, parent_name : str, entry : Any, handle : Any) -> None:
        stale = self._handles.get(entry.trace_id, {}).get(parent_name)
        if stale is not None and isinstance(stale, _AckHandle):
            stale._resolved = False
            stale.term('ledger:defect/superseded')
            stale._resolved = True
        real_add(self, parent_name, entry, handle)
    monkeypatch.setattr(TraceGroupAssembler, 'add', add)


# -- RUN-002: no durable group decision --------------------------------------------------------

def no_group_ledger(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the ledger the join kept no record of a committed group: a replacement waits for a reclaimed member forever.'''
    monkeypatch.setattr(NATSMessenger, '_persist_group', lambda self, ready: None)


# -- RUN-005: a credit fixed at bind, no working set --------------------------------------------

def fixed_join_credit(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed bind used ``queue_maxsize + 2`` whatever the join held; nothing computed a working set.'''
    monkeypatch.setattr(JoinPolicy, 'working_set', lambda self, nb_parents: 1)


# -- RUN-006: an indefinite wait that reports nothing -----------------------------------------------

def unobservable_wait(monkeypatch : pytest.MonkeyPatch) -> None:
    '''BATCH's default waited forever and no seam reported it: no pending count, no age of the oldest group.'''
    monkeypatch.setattr(TraceGroupAssembler, 'pending_count', lambda self: 0)
    monkeypatch.setattr(TraceGroupAssembler, 'oldest_wait_seconds', lambda self, now = None: 0.0)


# -- RUN-007: completion on the first terminator plus a quiet interval -------------------------------

def quiescence_completion(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The historical drain (``EOS-3``): one replica's EOS plus 500 ms of quiet declared the parent complete.'''
    monkeypatch.setattr(NATSMessenger, '_barrier_applies', lambda self, parent: False)


# -- RUN-008: terminators collapsed onto the first one seen ------------------------------------------

def _collapsing_on_terminator(self : NATSMessenger, parent_name : str, delivery : Any) -> None:
    '''The reviewed ``_eos_pull_loop`` body: first marker held, every later one acked and ignored; nothing recorded.'''
    aborted = False
    try:
        decoded = decode_envelope(delivery.envelope_bytes, resolve_blobs = False)
        aborted = bool(decoded.get('is_abort'))
    except Exception:  # noqa: BLE001
        decoded = {}
    kind = 'abort' if aborted else 'eos'
    replica = int(decoded.get('replica_id', 0))
    faults.barrier('eos.record.before', parent = parent_name, replica = replica, kind = kind)
    if aborted:
        self._aborted_parents[parent_name] = decoded.get('error') or {}
    handle = _AckHandle(delivery.token, self, raw = delivery.envelope_bytes)
    if parent_name in self._eos_seen:
        handle.ack()
        faults.barrier('eos.record.after', parent = parent_name, replica = replica, kind = kind)
        return
    self._eos_seen.add(parent_name)
    self._eos_handles.setdefault(parent_name, []).append(handle)
    faults.barrier('eos.record.after', parent = parent_name, replica = replica, kind = kind)


def collapsing_terminators(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Pre-``EOS-7``: the parent ended on its first marker and completed on quiet, whatever the other replica still owed.'''
    monkeypatch.setattr(NATSMessenger, '_on_terminator', _collapsing_on_terminator)
    monkeypatch.setattr(NATSMessenger, '_barrier_applies', lambda self, parent: False)


# -- RUN-009: a failed state query read as an empty durable -----------------------------------------

def zero_on_failure_pending(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``_consumer_pending`` returned ``(0, 0)`` for every failed ``consumer_info``: the drain completed on nothing.'''
    real = NATSMessenger._consumer_pending

    def pending(self : NATSMessenger, parent : str) -> Any:
        observed = real(self, parent)
        return known((0, 0)) if isinstance(observed, Unknown) else observed
    monkeypatch.setattr(NATSMessenger, '_consumer_pending', pending)


# -- RUN-010: the extra terminator acked away, the abort kept only in memory ------------------------------

def acked_extra_terminators(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed receiver noted an ABORT that followed an EOS in memory and acked
    the marker at once; a restart found only the EOS on the broker and finished
    cleanly. Nothing durable, no ``_eos_seen`` restore.
    '''
    monkeypatch.setattr(NATSMessenger, '_on_terminator', _collapsing_on_terminator)
    monkeypatch.setattr(NATSMessenger, '_barrier_applies', lambda self, parent: False)
