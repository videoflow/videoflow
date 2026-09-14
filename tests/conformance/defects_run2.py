'''
Negative controls for the RUN cases of the runtime ledger's commit path
(RUN-003/004/013/014/015/017/020/021/022/023/034): one reproduction per
reviewed defect, applied as a monkeypatch so the paired oracle can be shown to
*fail* against it. The rule is the one ``defects.py`` states: a conformance
case is evidence only if its oracle would have caught the bug it was written
for. These live in the test tree; nothing under ``videoflow/`` imports them.
'''
from __future__ import absolute_import, division, print_function

from typing import Any, Optional

import pytest

from videoflow.backends.runtime import FlowRuntime  # noqa: F401 — the RUN-023 control patches it
from videoflow.core.context import RuntimeContext
from videoflow.core.policies import ReorderBuffer
from videoflow.messaging import nats_messenger
from videoflow.messaging.nats_messenger import NATSMessenger

# -- RUN-003 / RUN-022: a checkpoint written the moment the node asks for it -----------------------

def eager_checkpoint(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed shape: ``ctx.checkpoint`` wrote the state at once, on its own
    key, before the output was committed (``task.py:356-378`` published after
    ``process`` returned with nothing tying the two together). A crash between
    the state and the output leaves the group covered and its output lost.
    '''
    real = RuntimeContext.checkpoint

    def checkpoint(self : RuntimeContext, state : bytes) -> None:
        emits, self.emits_output = self.emits_output, False
        try:
            real(self, state)
        finally:
            self.emits_output = emits
    monkeypatch.setattr(RuntimeContext, 'checkpoint', checkpoint)


# -- RUN-004: a recovery that recomputes instead of replaying the committed result -------------------

def recomputing_recovery(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    Before the outbox: no committed body, so a restarted worker re-ran the
    component on the redelivered input and published whatever it produced this
    time under the same id (``nats_messenger.py:732-736``).
    '''
    monkeypatch.setattr(NATSMessenger, '_remember_committed', lambda self, publication_id, buf: None)


# -- RUN-013: a publisher that mints a fresh identity for every attempt ------------------------------

def fresh_identity_per_attempt(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed publication loop (``nats_messenger.py:694-728``) kept retrying
    inside the messenger; a caller that gave up and retried minted a new
    ``Nats-Msg-Id`` per attempt, so an orphaned send and its retry both landed.
    '''
    import itertools
    counter = itertools.count(1)
    real = nats_messenger.derive_message_id

    def derive(flow_id : str, run_id : str, producer_name : str, trace_id : str, seq : int, msg_type : str) -> str:
        if producer_name == 'proc' and msg_type == 'data':
            return real(flow_id, run_id, producer_name, f'{trace_id}#attempt{next(counter)}', seq, msg_type)
        return real(flow_id, run_id, producer_name, trace_id, seq, msg_type)
    monkeypatch.setattr(nats_messenger, 'derive_message_id', derive)


# -- RUN-014: trace ids from a bare local counter ---------------------------------------------------

def epochless_source(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``trace_id = f'{node}:{n}'`` (``nats_messenger.py:272``): a replacement's first frames wear the dead process's ids.'''
    monkeypatch.setattr(nats_messenger, 'source_epoch_trace_id', lambda node, epoch, sequence: f'{node}:{sequence}')


# -- RUN-015: the checkpoint advanced on the send, not on the acceptance ------------------------------

def checkpoint_on_send(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    A source that records an offset as done the moment it was handed to the
    transport (``nats_messenger.py:601-612``): a send whose receipt never came is
    reported accepted, nothing is reconciled, and the replay position advances
    past an offset that may never have landed.
    '''
    from videoflow.backends.outcomes import Accepted, PublicationUnknown
    real = NATSMessenger._publish_envelope

    def publish_envelope(self : NATSMessenger, envelope : Any, is_abort : bool) -> Any:
        outcome = self._backend.publish(envelope, __import__('time').monotonic() + 5.0)
        if isinstance(outcome, PublicationUnknown):
            self._count('accepted')
            return Accepted(envelope.publication_id, None, False, 'memory'), None, False
        return real(self, envelope, is_abort)
    monkeypatch.setattr(NATSMessenger, '_publish_envelope', publish_envelope)


def versionless_analysis(monkeypatch : pytest.MonkeyPatch) -> None:
    '''A new analysis of the same media minting the old ``{node}:{offset}`` ids.'''
    monkeypatch.setattr(nats_messenger, 'replayable_trace_id',
                        lambda node, offset, analysis_version = None: f'{node}:{offset}')


# -- RUN-017: a marker written before the effect, certifying it ------------------------------------

def marker_before_effect(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed sink loop (``task.py:422-441``, ``idempotency.py:22-33``): the
    marker was the guarantee. Written *before* ``consume`` returns, it makes a
    crashed effect look applied and a replay skip it — or, for the plain sink,
    it is trusted where only the external key could have been.
    '''
    from videoflow.core import task as task_module
    real = task_module.ConsumerTask._run

    def run(self : Any) -> None:
        store = self._idem_store
        if store is not None:
            real_mark = store.mark
            real_seen = store.seen
            messenger = self._messenger

            def seen(key : str) -> bool:
                # Mark on the way in: the effect is "certified" before it happened.
                if real_seen(key):
                    return True
                real_mark(key)
                return False
            store.seen = seen  # type: ignore[method-assign]
            store.mark = lambda key: None  # type: ignore[method-assign]
            del messenger
        real(self)
    monkeypatch.setattr(task_module.ConsumerTask, '_run', run)


# -- RUN-020: an unusable key hashed as its str() ----------------------------------------------------

def hash_str_of_anything(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``str(metadata.get(key))`` hashed for every record (``nats_messenger.py:381-391``): ``None`` is a partition.'''
    import hashlib

    def verdict(self : NATSMessenger, trace_id : Optional[str], metadata : Optional[dict]) -> tuple:
        if not self._partition_by:
            return self._replica_id, False
        key = trace_id if self._partition_by == 'trace_id' else (metadata or {}).get(self._partition_by)
        digest = hashlib.sha256(str(key).encode('utf-8')).hexdigest()
        return int(digest[:8], 16) % self._nb_tasks, False
    monkeypatch.setattr(NATSMessenger, '_partition_verdict', verdict)


# -- RUN-021: arrival order is the order ----------------------------------------------------------------

def arrival_order_buffer(monkeypatch : pytest.MonkeyPatch) -> None:
    '''A buffer that applies every record on arrival, whatever the declared policy (the pre-RFC path).'''
    def offer(self : ReorderBuffer, seq : int, record : Any) -> list:
        self._applied.add(seq)
        return [(seq, record, False)]
    monkeypatch.setattr(ReorderBuffer, 'offer', offer)


# -- RUN-023: an unfenced commit ------------------------------------------------------------------------

def unfenced_commit(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Ownership decided by the durable name alone (``nats_messenger.py:381-391``): a stale owner's commit is taken.'''
    monkeypatch.setattr(FlowRuntime, 'check_authority', lambda self, token: None)


# -- RUN-034: the body fetched before the ownership decision ------------------------------------------

def hydrate_before_ownership(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed pull loop decoded the whole envelope — payload included — before
    asking who owns it (``nats_messenger.py:424-450``): every replica fetched
    every frame to discard 7/8. Reproduced on both admission paths the messenger
    has today (the backend's admission filter and the receive-side fallback).
    '''
    from videoflow.wire.serialization import peek_envelope

    def fetch_first(self : NATSMessenger, delivery : Any) -> None:
        try:
            ref = peek_envelope(delivery.envelope_bytes).get('blob_ref')
        except Exception:  # noqa: BLE001 — undecodable bytes are not this defect's concern
            return
        if ref is not None and self._blob_store is not None:
            self._blob_store.get(ref)                       # the whole body, before the decision
    real_loop = NATSMessenger._admit_on_loop
    real_data = NATSMessenger._admit_data

    def admit_on_loop(self : NATSMessenger, delivery : Any) -> bool:
        fetch_first(self, delivery)
        return real_loop(self, delivery)

    def admit_data(self : NATSMessenger, parent_name : str, delivery : Any) -> Any:
        fetch_first(self, delivery)
        return real_data(self, parent_name, delivery)
    monkeypatch.setattr(NATSMessenger, '_admit_on_loop', admit_on_loop)
    monkeypatch.setattr(NATSMessenger, '_admit_data', admit_data)
