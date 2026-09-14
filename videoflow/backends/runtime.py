'''
Runtime contracts: the durable state that makes processing correct across
restarts, independent of the transport underneath.

``RuntimeStore`` is a small versioned key-value store with compare-and-swap;
three implementations exist (memory for tests, a file directory for the local
engine, Redis for clusters) and the planner reads their capabilities to decide
whether a restart-safe profile can be admitted at all. The ``FlowRuntime`` built
on it (Phase 3 of the backend plan) owns the group ledger, ownership epochs,
completion barriers, the publication outbox and source epochs.

This module also carries the one identity rule that must be pure and stable
across languages: ``group_identity`` — the id of a time-aligned input group is
derived from its *members*, not from a rounded timestamp, so two distinct groups
whose event times round to the same microsecond never collide, and a replay of
the same members yields the same id.
'''
from __future__ import absolute_import, division, print_function

import abc
import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from ..core.errors import WORKER_FATAL, ConfigError, OwnershipConflict, StaleAuthority
from .capabilities import RuntimeCapabilities
from .outcomes import Accepted, Known, Observation, PublicationOutcome, Rejected


@dataclass(frozen = True)
class OwnershipToken:
    '''A fencing token: commits carry it, and a commit under a superseded epoch is refused.'''
    partition_id : str
    epoch : int
    fencing_token : str

@dataclass(frozen = True)
class RecoveryRecord:
    group_id : str
    members : Mapping[str, str]
    version : str

@dataclass(frozen = True)
class CommitReceipt:
    group_id : str
    state_version : str
    output_intents : tuple[str, ...]
    effect_intents : tuple[str, ...]

@dataclass(frozen = True)
class CompletionReceipt:
    partition_id : str
    epoch : int
    final_sequence : int
    version : str

class RuntimeStore(abc.ABC):
    '''
    Versioned KV. ``cas`` succeeds only when the key's current version equals
    ``expected_version`` (``None`` = must not exist). Versions are opaque strings
    the store mints; callers never fabricate one.
    '''
    @abc.abstractmethod
    def get(self, key : str) -> tuple[bytes | None, str | None]:
        ...

    @abc.abstractmethod
    def cas(self, key : str, expected_version : str | None, value : bytes) -> bool:
        ...

    @abc.abstractmethod
    def append(self, log : str, record : bytes) -> int:
        ...

    @abc.abstractmethod
    def scan(self, prefix : str) -> list[tuple[str, bytes, str]]:
        ...

    @abc.abstractmethod
    def delete(self, key : str, expected_version : str | None) -> bool:
        ...

    @abc.abstractmethod
    def log_entries(self, log : str) -> list[bytes]:
        '''Every record appended to ``log``, oldest first (empty for an unknown log).'''
        ...

    @abc.abstractmethod
    def capabilities(self) -> RuntimeCapabilities:
        ...

def group_identity(members : Mapping[str, tuple[str, str, int]], window_id : str | None,
                   rounded_micros : int | None = None) -> str:
    '''
    Canonical identity of an input group.

    - Arguments:
        - members: parent name -> (producer, trace_id, seq) of the member it contributed.
        - window_id: the join window or namespace the group formed in (``None`` for trace joins).
        - rounded_micros: the legacy ``tw-<µs>`` prefix component, kept so ids stay \
            sortable by time; distinctness comes from the member hash.
    '''
    canonical = '|'.join(f'{parent}={producer}:{trace}:{seq}'
                         for parent, (producer, trace, seq) in sorted(members.items()))
    digest = hashlib.sha256(f'{window_id or ""}|{canonical}'.encode()).hexdigest()[:12]
    if rounded_micros is None:
        return f'g-{digest}'
    return f'tw-{rounded_micros}-{digest}'

def source_epoch_trace_id(node : str, epoch : str, sequence : int) -> str:
    '''Trace id of a live source's ``sequence``-th capture within capture epoch ``epoch``.'''
    return f'{node}:{epoch}:{sequence}'

def replayable_trace_id(node : str, offset : int, analysis_version : str | None = None) -> str:
    '''Trace id of a replayable source's frame at ``offset``; a new analysis version is a new namespace.'''
    prefix = f'{node}:{analysis_version}' if analysis_version else node
    return f'{prefix}:{offset}'

def members_signature(members : Sequence[str]) -> str:
    return hashlib.sha256('|'.join(sorted(members)).encode()).hexdigest()[:12]


# -- the ledger --------------------------------------------------------------------------

#: How many times a compare-and-swap is retried against a concurrent writer
#: before the caller is told the store would not take the update.
#: How long a partition lease lasts without renewal (``VF_PARTITION_LEASE_SECONDS``):
#: a crashed holder's replacement waits at most this long; a live holder renews
#: every third of it from its own thread. Long enough that a scheduler hiccup does
#: not usurp a paused process, short enough that a crash is not a long outage.
DEFAULT_PARTITION_LEASE_SECONDS = 10.0
PARTITION_LEASE_ENV = 'VF_PARTITION_LEASE_SECONDS'

#: How often a running worker reconciles payload obligations (``BLOB-14`` step 4);
#: ``0`` disables the periodic pass (the start pass always runs).
DEFAULT_RECONCILE_INTERVAL_SECONDS = 60.0
RECONCILE_INTERVAL_ENV = 'VF_RECONCILE_INTERVAL_SECONDS'


def reconcile_interval_from_env(value : str | None) -> float:
    '''``VF_RECONCILE_INTERVAL_SECONDS`` parsed: absent means the default; negative or non-numeric fails fast.'''
    if value is None or not value.strip():
        return DEFAULT_RECONCILE_INTERVAL_SECONDS
    try:
        seconds = float(value)
    except ValueError as e:
        raise ConfigError(f'{RECONCILE_INTERVAL_ENV}={value!r} is not a number of seconds.',
                          remedy = f'Set {RECONCILE_INTERVAL_ENV} to a non-negative number, or unset it.') from e
    if seconds < 0:
        raise ConfigError(f'{RECONCILE_INTERVAL_ENV}={value!r} must not be negative.',
                          remedy = f'Set {RECONCILE_INTERVAL_ENV} to a non-negative number (0 disables), or unset it.')
    return seconds


def partition_lease_from_env(value : str | None) -> float:
    '''``VF_PARTITION_LEASE_SECONDS`` parsed: absent means the default; a non-positive or non-numeric value fails fast.'''
    if value is None or not value.strip():
        return DEFAULT_PARTITION_LEASE_SECONDS
    try:
        seconds = float(value)
    except ValueError as e:
        raise ConfigError(f'{PARTITION_LEASE_ENV}={value!r} is not a number of seconds.',
                          remedy = f'Set {PARTITION_LEASE_ENV} to a positive number, or unset it.') from e
    if seconds <= 0:
        raise ConfigError(f'{PARTITION_LEASE_ENV}={value!r} must be positive.',
                          remedy = f'Set {PARTITION_LEASE_ENV} to a positive number, or unset it.')
    return seconds

#: How often a would-be owner re-reads a held lease while waiting for it to lapse.
LEASE_POLL_SECONDS = 0.5


class _LeaseHeld(Exception):
    '''Internal: the partition record carries another holder's live lease.'''
    def __init__(self, owner : str, lease_until : float) -> None:
        super().__init__(owner)
        self.owner = owner
        self.lease_until = lease_until


CAS_RETRIES = 16

COMPLETION_OPEN = 'open'            # not every replica has ended
COMPLETION_DRAINING = 'draining'    # every terminator seen, deliveries or groups outstanding
COMPLETION_COMPLETE = 'complete'    # every clause of EOS-7 holds
COMPLETION_ABORTED = 'aborted'      # an ABORT is recorded and the parent is drained
COMPLETION_UNKNOWN = 'unknown'      # the broker could not be observed: never complete

OUTCOME_INTENT = 'intent'
OUTCOME_ACCEPTED = 'accepted'
OUTCOME_DUPLICATE = 'duplicate'
OUTCOME_REJECTED = 'rejected'
OUTCOME_UNKNOWN = 'unknown'


@dataclass(frozen = True)
class TerminatorRecord:
    parent : str
    replica_id : int
    kind : str                      # 'eos' | 'abort'
    seq : int
    error : Mapping[str, Any] | None


@dataclass(frozen = True)
class OutboxEntry:
    publication_id : str
    digest : str
    payload_refs : tuple[str, ...]
    outcome : str
    kind : str
    replica_id : int


@dataclass(frozen = True)
class PendingHandoff:
    record_id : str
    headers : Mapping[str, str]
    raw : bytes
    attempts : int


def _dumps(doc : Mapping[str, Any]) -> bytes:
    return json.dumps(doc, sort_keys = True, separators = (',', ':')).encode()


def _loads(raw : bytes | None) -> dict[str, Any]:
    if not raw:
        return {}
    return dict(json.loads(raw.decode()))


def store_shared_and_durable(store : RuntimeStore) -> bool:
    '''Whether a store's records outlive one process *and* are seen by sibling replicas (what makes a ledger a ledger).'''
    caps = store.capabilities()
    return isinstance(caps.durable, Known) and bool(caps.durable.value) and caps.shared_across_processes


class FlowRuntime:
    '''
    The per-node runtime ledger of RFC 0006 ``CTRL-4``, over any ``RuntimeStore``:
    terminators and received sets (the ``EOS-7`` completion barrier), ownership
    epochs with fencing, the publication outbox, attempt counts, the terminal log,
    pending dead-letter handoffs, open join groups, checkpoints and sink-effect
    markers. Every mutation is a compare-and-swap on one key; every read is what
    the store holds, so a replacement process starts from the same facts.

    What it never does: decide policy. The messenger asks it questions
    (``completion_state``, ``attempts_for``) and records facts; the answers hold
    only as far as the store's own ``capabilities()`` — a memory store makes every
    record process-local, and the callers know it (``durable_shared()``).

    - Arguments:
        - store: the ``RuntimeStore`` (``VF_RUNTIME_STORE_URL``).
        - flow_id / run_id / node: the ledger's namespace, ``vf/{flow}/{run}/{node}/``.
        - replica_id / nb_tasks / partition_by: this replica's identity within the node.
        - parent_replicas: ``{parent: nb_tasks}`` from ``VF_PARENT_REPLICAS`` (``ENV-11``); \
            a parent absent from it has no completion barrier.
        - clock: epoch seconds, injectable for tests.

    ``completion_state`` takes the durable's ``(num_pending, num_ack_pending)`` \
        observation as the messenger reads it (``_consumer_pending``).
    '''
    def __init__(self, store : RuntimeStore, flow_id : str, run_id : str, node : str, replica_id : int = 0,
                 nb_tasks : int = 1, partition_by : str | None = None,
                 parent_replicas : Mapping[str, int] | None = None,
                 clock : Callable[[], float] = time.time,
                 lease_seconds : float = DEFAULT_PARTITION_LEASE_SECONDS,
                 sleep : Callable[[float], None] = time.sleep) -> None:
        self._store = store
        self._flow_id = flow_id
        self._run_id = run_id
        self._node = node
        self._replica_id = replica_id
        self._nb_tasks = nb_tasks
        self._partition_by = partition_by if (partition_by and nb_tasks > 1) else None
        self._parent_replicas = dict(parent_replicas or {})
        self._clock = clock
        self._lease_seconds = float(lease_seconds)
        self._sleep = sleep
        self._prefix = f'vf/{flow_id}/{run_id}/{node}/'
        self._lock = threading.Lock()
        # Minted once per process (MSGID-5): a replacement process mints another.
        self._source_epoch = uuid.uuid4().hex[:12]
        # This process's identity as a partition holder: a replacement process is a
        # different holder even at the same replica id (the lease is what tells them apart).
        self._holder = f'{self._node}/r{self._replica_id}/{self._source_epoch}'
        # In-memory mirrors of what this replica wrote, so the hot paths need no re-read.
        self._received : dict[tuple[str, str], set[str]] = {}
        self._published : dict[str, set[str]] = {}
        self._tokens : dict[str, OwnershipToken] = {}
        self._load_mirrors()

    # -- identity -------------------------------------------------------------------------

    @property
    def store(self) -> RuntimeStore:
        return self._store

    @property
    def node(self) -> str:
        return self._node

    @property
    def flow_id(self) -> str:
        return self._flow_id

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def replica_id(self) -> int:
        return self._replica_id

    @property
    def parent_replicas(self) -> Mapping[str, int]:
        return dict(self._parent_replicas)

    def capabilities(self) -> RuntimeCapabilities:
        return self._store.capabilities()

    def durable_shared(self) -> bool:
        '''Whether records here outlive this process *and* are seen by sibling replicas: the EOS-7 and D11 gate.'''
        return store_shared_and_durable(self._store)

    def source_epoch(self) -> str:
        return self._source_epoch

    def partition_id(self) -> str:
        return f'{self._node}/p{self._replica_id}'

    def key(self, *parts : str) -> str:
        return self._prefix + '/'.join(parts)

    # -- store helpers ------------------------------------------------------------------

    def _read(self, key : str) -> tuple[dict[str, Any], str | None]:
        raw, version = self._store.get(key)
        return _loads(raw), version

    def _update(self, key : str, mutate : Callable[[dict[str, Any]], dict[str, Any] | None]) -> dict[str, Any] | None:
        '''
        Read-modify-write under compare-and-swap, retried against concurrent
        writers. ``mutate`` returns the new document, or ``None`` to leave the key
        as it is (the current document is then returned unchanged).
        '''
        for _ in range(CAS_RETRIES):
            current, version = self._read(key)
            updated = mutate(dict(current))
            if updated is None:
                return current
            if self._store.cas(key, version, _dumps(updated)):
                return updated
        raise RuntimeError(f'runtime store: {key} kept changing under {CAS_RETRIES} compare-and-swap attempts')

    def _load_mirrors(self) -> None:
        for key, raw, _version in self._store.scan(self.key('outbox', '')):
            doc = _loads(raw)
            if doc.get('outcome') in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE) and doc.get('kind') == 'data':
                self._published.setdefault(f"r{doc.get('replica', 0)}", set()).add(key.rsplit('/', 1)[-1])

    # -- ownership epochs (RUN-023) -----------------------------------------------------

    def acquire_partition(self, partition_id : str | None = None,
                          expected_epoch : int | None = None, wait : bool = True) -> OwnershipToken:
        '''
        Take (or retake) ownership of a partition: the epoch is CAS-incremented and a
        fresh fencing token minted. ``expected_epoch`` refuses the acquisition when
        someone else has moved the epoch since the caller last saw it.

        A partition is *leased*: the record names its holder and a
        ``lease_until`` the holder renews (``renew_partition``) and clears on a
        graceful close (``release_partition``). Another process asking for a
        partition whose lease is live waits for it to lapse — at most one lease
        length — and is refused the moment the holder renews it (a renewal
        proves the holder alive): a singleton scaled out by adding replicas
        fails explicitly at bind time instead of two owners fencing each other in
        turns (RUN-018, RUN-019), while the replacement for a crashed holder
        takes over as soon as its lease lapses, and at once after a graceful close.
        ``wait = False`` refuses a live lease at once instead of waiting for it
        to lapse (``claim_replica_slot`` probes the free slots that way first).

        - Raises:
            - StaleAuthority: ``expected_epoch`` no longer matches (another owner \
                took over), or the store would not take the update.
            - OwnershipConflict: another live process holds the partition's lease.
        '''
        pid = partition_id or self.partition_id()
        key = self.key('epoch', pid)
        token_id = uuid.uuid4().hex[:16]

        def mutate(doc : dict[str, Any]) -> dict[str, Any]:
            current = int(doc.get('epoch', 0))
            if expected_epoch is not None and current != expected_epoch:
                raise StaleAuthority(
                    f'{self._node}: partition {pid} is at epoch {current}, not {expected_epoch}; another owner took over',
                    remedy = 'Stop this replica; the current owner proceeds alone.', node = self._node)
            now = self._clock()
            holder = str(doc.get('holder', ''))
            lease_until = float(doc.get('lease_until', 0) or 0)
            if holder and holder != self._holder and lease_until > now:
                raise _LeaseHeld(str(doc.get('owner', holder)), lease_until)
            return {'epoch': current + 1, 'token': token_id, 'owner': f'{self._node}/r{self._replica_id}',
                    'holder': self._holder, 'lease_until': now + self._lease_seconds, 'at': now}

        deadline = self._clock() + self._lease_seconds
        observed : float | None = None
        while True:
            try:
                doc = self._update(key, mutate)
                break
            except _LeaseHeld as held:
                now = self._clock()
                renewed = observed is not None and held.lease_until > observed
                if renewed or now >= deadline or not wait:
                    raise OwnershipConflict(
                        f'{self._node}: partition {pid} is held by {held.owner} under a live lease'
                        f'{" (renewed while this replica waited)" if renewed else ""}; a singleton cannot be '
                        f'expanded by starting more replicas',
                        remedy = 'Stop this replica. To run more replicas, redeploy the flow at the owning replica '
                                 'count (nb_tasks) instead of scaling the workload; ownership does not migrate.') from None
                observed = held.lease_until
                self._sleep(min(LEASE_POLL_SECONDS, max(0.0, min(held.lease_until, deadline) - now)) or LEASE_POLL_SECONDS)
        assert doc is not None
        token = OwnershipToken(pid, int(doc['epoch']), str(doc['token']))
        with self._lock:
            self._tokens[pid] = token
        return token

    def held_partition(self, partition_id : str | None = None) -> OwnershipToken | None:
        '''The token this process holds for the partition, if it acquired it already (``claim_replica_slot``).'''
        with self._lock:
            return self._tokens.get(partition_id or self.partition_id())

    def renew_partition(self, token : OwnershipToken) -> float:
        '''
        Extend the lease of a partition this process holds; returns the new
        ``lease_until``. A record that no longer carries the token belongs to
        another owner: ``StaleAuthority``.
        '''
        key = self.key('epoch', token.partition_id)

        def mutate(doc : dict[str, Any]) -> dict[str, Any]:
            if int(doc.get('epoch', 0)) != token.epoch or str(doc.get('token', '')) != token.fencing_token:
                raise StaleAuthority(
                    f'{self._node}: lease renewal under epoch {token.epoch} refused; partition '
                    f'{token.partition_id} is now at epoch {int(doc.get("epoch", 0))}',
                    remedy = 'Stop this replica; a newer owner holds the partition.', node = self._node)
            doc['lease_until'] = self._clock() + self._lease_seconds
            return doc
        doc = self._update(key, mutate)
        assert doc is not None
        return float(doc['lease_until'])

    def release_partition(self, token : OwnershipToken) -> None:
        '''Clear the lease on a graceful close so a replacement takes over at once; a superseded token releases nothing.'''
        key = self.key('epoch', token.partition_id)

        def mutate(doc : dict[str, Any]) -> dict[str, Any] | None:
            if int(doc.get('epoch', 0)) != token.epoch or str(doc.get('token', '')) != token.fencing_token:
                return None
            doc['lease_until'] = 0
            return doc
        self._update(key, mutate)
        with self._lock:
            self._tokens.pop(token.partition_id, None)

    @property
    def lease_seconds(self) -> float:
        return self._lease_seconds

    def current_epoch(self, partition_id : str | None = None) -> int:
        doc, _v = self._read(self.key('epoch', partition_id or self.partition_id()))
        return int(doc.get('epoch', 0))

    def check_authority(self, token : OwnershipToken) -> None:
        '''Refuse a commit under a superseded epoch: the store's epoch and token must be the caller's.'''
        doc, _v = self._read(self.key('epoch', token.partition_id))
        if int(doc.get('epoch', 0)) != token.epoch or str(doc.get('token', '')) != token.fencing_token:
            raise StaleAuthority(
                f'{self._node}: commit under epoch {token.epoch} refused; partition {token.partition_id} is now at '
                f'epoch {int(doc.get("epoch", 0))}',
                remedy = 'Stop this replica; a newer owner holds the partition.', node = self._node)

    # -- attempt counts (STREAM-15, D11) ---------------------------------------------------

    def record_attempt(self, message_id : str, disposition : str) -> int:
        '''One failed attempt of ``message_id`` under ``disposition``; ``worker_fatal`` never counts. Returns the budgeted count.'''
        if disposition == WORKER_FATAL:
            return self.attempts_for(message_id)

        def mutate(doc : dict[str, Any]) -> dict[str, Any]:
            doc[disposition] = int(doc.get(disposition, 0)) + 1
            doc['at'] = self._clock()
            return doc
        doc = self._update(self.key('attempts', message_id), mutate)
        assert doc is not None
        return sum(int(v) for k, v in doc.items() if k not in ('at', WORKER_FATAL))

    def attempts_for(self, message_id : str) -> int:
        '''Failed attempts of ``message_id`` that count against its budget (never ``worker_fatal``).'''
        doc, _v = self._read(self.key('attempts', message_id))
        return sum(int(v) for k, v in doc.items() if k not in ('at', WORKER_FATAL))

    # -- terminal log and pending handoffs (DELIV-15) -------------------------------------

    def record_terminal(self, record : Mapping[str, Any]) -> str:
        '''Append a terminal record (a delivery ended without a dead letter) and return its reference.'''
        position = self._store.append(self.key('terminal'), _dumps(dict(record, at = self._clock())))
        return f'ledger:{self._node}/terminal:{position}'

    def terminal_entries(self) -> list[dict[str, Any]]:
        return [_loads(raw) for raw in self._store.log_entries(self.key('terminal'))]

    def record_pending_handoff(self, record_id : str, headers : Mapping[str, str], raw : bytes) -> None:
        '''A dead letter the broker did not accept: kept so a later attempt (or a restart) re-publishes it under the same id.'''
        def mutate(doc : dict[str, Any]) -> dict[str, Any]:
            return {'headers': dict(headers), 'raw': raw.hex(), 'attempts': int(doc.get('attempts', 0)) + 1,
                    'at': self._clock()}
        self._update(self.key('handoff', record_id), mutate)

    def pending_handoffs(self) -> list[PendingHandoff]:
        out = []
        for key, raw, _v in self._store.scan(self.key('handoff', '')):
            doc = _loads(raw)
            out.append(PendingHandoff(key.rsplit('/', 1)[-1], dict(doc.get('headers', {})),
                                      bytes.fromhex(doc.get('raw', '')), int(doc.get('attempts', 0))))
        return out

    def clear_pending_handoff(self, record_id : str) -> None:
        key = self.key('handoff', record_id)
        _doc, version = self._read(key)
        if version is not None:
            self._store.delete(key, version)

    # -- terminators, received sets, completion (EOS-7) ----------------------------------

    def record_terminator(self, parent : str, replica_id : int, kind : str, seq : int,
                          error : Mapping[str, Any] | None = None) -> bool:
        '''
        Record a parent replica's terminator before it is acked. Idempotent by
        ``(parent, replica_id, kind)``: returns False when that fact was already
        recorded (a duplicate marker), True when it is new.
        '''
        key = self.key('terminator', parent, f'r{replica_id}', kind)
        recorded = {'new': False}

        def mutate(doc : dict[str, Any]) -> dict[str, Any] | None:
            if doc:
                return None
            recorded['new'] = True
            return {'seq': int(seq), 'error': dict(error) if error else None, 'at': self._clock()}
        self._update(key, mutate)
        return recorded['new']

    def terminators(self, parent : str) -> list[TerminatorRecord]:
        out = []
        for key, raw, _v in self._store.scan(self.key('terminator', parent, '')):
            rest = key[len(self.key('terminator', parent, '')):]
            replica, _sep, kind = rest.partition('/')
            doc = _loads(raw)
            out.append(TerminatorRecord(parent, int(replica[1:]), kind, int(doc.get('seq', 0)), doc.get('error')))
        return sorted(out, key = lambda t: (t.replica_id, t.kind))

    def aborted_parents(self) -> dict[str, Mapping[str, Any]]:
        '''Parents with a recorded ABORT, with the error each carried — an ABORT outranks a clean EOS (ABORT-3).'''
        out : dict[str, Mapping[str, Any]] = {}
        for key, raw, _v in self._store.scan(self.key('terminator', '')):
            if key.endswith('/abort'):
                parent = key[len(self.key('terminator', '')):].split('/', 1)[0]
                out.setdefault(parent, _loads(raw).get('error') or {})
        return out

    def record_received(self, parent : str, durable : str, message_id : str) -> bool:
        '''Note a distinct DATA id delivered from ``parent`` on ``durable`` to this replica; False when already noted.'''
        with self._lock:
            seen = self._received.setdefault((parent, durable), set())
            if not seen:
                seen.update(self._own_received(parent, durable))
            if message_id in seen:
                return False
            seen.add(message_id)
        self._store.append(self._received_log(parent, durable, self._replica_id), message_id.encode())
        return True

    def _received_log(self, parent : str, durable : str, replica_id : int) -> str:
        return self.key('received', parent, durable, f'r{replica_id}')

    def _own_received(self, parent : str, durable : str) -> set[str]:
        return {r.decode() for r in self._store.log_entries(self._received_log(parent, durable, self._replica_id))}

    def received_ids(self, parent : str, durable : str) -> set[str]:
        '''The union over this node's replicas of the distinct ids delivered from ``parent`` on ``durable``.'''
        ids : set[str] = set()
        for replica in range(max(1, self._nb_tasks)):
            ids.update(r.decode() for r in self._store.log_entries(self._received_log(parent, durable, replica)))
        return ids

    def completion_state(self, parent : str, durable : str, observation : Observation[tuple[int, int]],
                         pending_halves : bool = False) -> str:
        '''
        Where ``parent`` stands for this node (``EOS-7``): ``open`` until every
        expected replica's terminator is recorded; ``aborted`` once an ABORT is
        recorded and the parent is drained; ``draining`` while the received count is
        short of the terminators' final counts, a join still holds a half, or the
        broker still reports deliveries; ``unknown`` while the broker cannot be
        observed — never ``complete`` on evidence that was never obtained.
        '''
        expected = self._parent_replicas.get(parent)
        recorded = self.terminators(parent)
        by_replica : dict[int, TerminatorRecord] = {}
        for record in recorded:
            # An ABORT outranks an EOS from the same replica.
            if record.replica_id not in by_replica or record.kind == 'abort':
                by_replica[record.replica_id] = record
        aborted = any(r.kind == 'abort' for r in by_replica.values())
        if expected is None:
            return COMPLETION_UNKNOWN if not by_replica else (COMPLETION_ABORTED if aborted else COMPLETION_OPEN)
        if not aborted and any(r not in by_replica for r in range(expected)):
            return COMPLETION_OPEN
        if not isinstance(observation, Known):
            return COMPLETION_UNKNOWN
        available, leased = observation.value           # (num_pending, num_ack_pending) of the durable
        if pending_halves or available or leased:
            return COMPLETION_DRAINING
        if aborted:
            return COMPLETION_ABORTED
        final = sum(r.seq for r in by_replica.values())
        if len(self.received_ids(parent, durable)) < final:
            return COMPLETION_DRAINING
        return COMPLETION_COMPLETE

    def commit_completion(self, parent : str, token : OwnershipToken | None = None) -> CompletionReceipt:
        '''
        Record that ``parent`` is complete for this replica's partition; refused
        under a superseded epoch of *that* partition (StaleAuthority). Partitions
        are independent authorities — a sibling replica's epoch says nothing about
        this one's — so the record is keyed by partition id.
        '''
        if token is not None:
            self.check_authority(token)
        final = sum(r.seq for r in self.terminators(parent))
        epoch = token.epoch if token is not None else 0
        partition = token.partition_id if token is not None else self.partition_id()

        def mutate(doc : dict[str, Any]) -> dict[str, Any] | None:
            if doc and int(doc.get('epoch', -1)) > epoch:
                raise StaleAuthority(
                    f'{self._node}: completion of {parent} already committed under epoch {doc.get("epoch")} of '
                    f'{partition}',
                    remedy = 'Stop this replica; a newer owner committed the completion.', node = self._node)
            return {'epoch': epoch, 'final': final, 'at': self._clock()}
        key = self.key('completion', parent, partition)
        doc = self._update(key, mutate)
        assert doc is not None
        version = self._store.get(key)[1] or ''
        return CompletionReceipt(partition, epoch, final, version)

    def completed_parents(self, partition_id : str | None = None) -> set[str]:
        '''Parents this replica's partition (or ``partition_id``) has committed complete.'''
        partition = partition_id or self.partition_id()
        out : set[str] = set()
        for key, _raw, _v in self._store.scan(self.key('completion', '')):
            rest = key[len(self.key('completion', '')):]
            parent, _sep, owner = rest.partition('/')
            if owner == partition:
                out.add(parent)
        return out

    # -- the outbox (RUN-003, RUN-013, MSG-013/014) ---------------------------------------

    def intend_publication(self, publication_id : str, digest : str, payload_refs : Sequence[str] = (),
                           kind : str = 'data', readers : Sequence[str] = ()) -> str:
        '''
        Record the intent to publish before the send; returns the key's version
        (an existing intent is kept). ``readers`` are the reader obligations the
        put acquired (``VF_BLOB_READER_IDS``), kept with the refs so a reconciler
        can tell which readers still owe a release (``RuntimeObligationLedger``).
        '''
        def mutate(doc : dict[str, Any]) -> dict[str, Any] | None:
            if doc:
                return None
            return {'digest': digest, 'refs': list(payload_refs), 'outcome': OUTCOME_INTENT, 'kind': kind,
                    'replica': self._replica_id, 'at': self._clock(), 'readers': list(readers)}
        self._update(self.key('outbox', publication_id), mutate)
        return self._store.get(self.key('outbox', publication_id))[1] or ''

    def resolve_publication(self, publication_id : str, outcome : PublicationOutcome) -> None:
        '''Record the send's outcome; an ``Accepted`` (duplicate included) DATA id counts once for EOS-7.'''
        if isinstance(outcome, Accepted):
            state = OUTCOME_DUPLICATE if outcome.duplicate else OUTCOME_ACCEPTED
        elif isinstance(outcome, Rejected):
            state = OUTCOME_REJECTED
        else:
            state = OUTCOME_UNKNOWN

        def mutate(doc : dict[str, Any]) -> dict[str, Any] | None:
            if not doc:
                doc = {'digest': '', 'refs': [], 'kind': 'data', 'replica': self._replica_id}
            if doc.get('outcome') in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE) and state == OUTCOME_UNKNOWN:
                return None                             # a definite fact is never downgraded
            doc['outcome'] = state
            doc['resolved_at'] = self._clock()
            if isinstance(outcome, Accepted) and outcome.sequence is not None:
                doc['seq'] = int(outcome.sequence)              # where the channel holds it: eviction is judged by it
            return doc
        doc = self._update(self.key('outbox', publication_id), mutate)
        if doc is not None and doc.get('outcome') in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE) and doc.get('kind') == 'data':
            with self._lock:
                self._published.setdefault(f'r{self._replica_id}', set()).add(publication_id)

    def outbox_of(self, node : str) -> list[dict[str, Any]]:
        '''
        Every outbox document of ``node`` in this run (a parent's, for a reader
        reconciling the obligations it and its siblings owe): the raw records —
        ``refs``, ``readers``, ``outcome``, ``seq`` when accepted.
        '''
        prefix = f'vf/{self._flow_id}/{self._run_id}/{node}/outbox/'
        out = []
        for key, raw, _v in self._store.scan(prefix):
            doc = _loads(raw)
            doc['publication_id'] = key[len(prefix):]
            out.append(doc)
        return out

    def outbox_entry(self, publication_id : str) -> OutboxEntry | None:
        doc, _v = self._read(self.key('outbox', publication_id))
        if not doc:
            return None
        return OutboxEntry(publication_id, str(doc.get('digest', '')), tuple(doc.get('refs', ())),
                           str(doc.get('outcome', OUTCOME_INTENT)), str(doc.get('kind', 'data')),
                           int(doc.get('replica', 0)))

    def unresolved_publications(self) -> list[OutboxEntry]:
        '''Intents whose send was never confirmed (``intent`` or ``unknown``): what a restart must reconcile first.'''
        out = []
        for key, raw, _v in self._store.scan(self.key('outbox', '')):
            doc = _loads(raw)
            if doc.get('outcome') in (OUTCOME_INTENT, OUTCOME_UNKNOWN):
                out.append(OutboxEntry(key.rsplit('/', 1)[-1], str(doc.get('digest', '')), tuple(doc.get('refs', ())),
                                       str(doc.get('outcome')), str(doc.get('kind', 'data')), int(doc.get('replica', 0))))
        return out

    def published_count(self) -> int:
        '''Distinct DATA ids this replica published (accepted or deduplicated): a terminator's ``seq`` (EOS-7).'''
        with self._lock:
            return len(self._published.get(f'r{self._replica_id}', ()))

    # -- open join groups (RUN-001, RUN-002) ----------------------------------------------

    def persist_group(self, group_id : str, members : Mapping[str, tuple[str, str, int]],
                      tokens : Mapping[str, str]) -> RecoveryRecord:
        '''Record a group's members by logical id as they arrive (idempotent; a redelivered member replaces its token).'''
        def mutate(doc : dict[str, Any]) -> dict[str, Any]:
            existing = dict(doc.get('members', {}))
            for parent, (producer, trace, seq) in members.items():
                existing[parent] = [producer, trace, int(seq)]
            held = dict(doc.get('tokens', {}))
            held.update({k: str(v) for k, v in tokens.items()})
            return {'members': existing, 'tokens': held, 'at': doc.get('at', self._clock())}
        doc = self._update(self.key('group', group_id), mutate)
        assert doc is not None
        version = self._store.get(self.key('group', group_id))[1] or ''
        return RecoveryRecord(group_id, {p: f'{m[0]}:{m[1]}:{m[2]}' for p, m in doc['members'].items()}, version)

    def settle_group(self, group_id : str) -> None:
        key = self.key('group', group_id)
        _doc, version = self._read(key)
        if version is not None:
            self._store.delete(key, version)

    def open_groups(self) -> list[RecoveryRecord]:
        out = []
        for key, raw, version in self._store.scan(self.key('group', '')):
            doc = _loads(raw)
            out.append(RecoveryRecord(key.rsplit('/', 1)[-1],
                                      {p: f'{m[0]}:{m[1]}:{m[2]}' for p, m in doc.get('members', {}).items()}, version))
        return out

    def group_members(self, group_id : str) -> dict[str, tuple[str, str, int]]:
        doc, _v = self._read(self.key('group', group_id))
        return {p: (str(m[0]), str(m[1]), int(m[2])) for p, m in doc.get('members', {}).items()}

    # -- checkpoints (MSGID-6, RUN-022) ---------------------------------------------------

    def _checkpoint_key(self) -> str:
        '''
        One checkpoint per replica: a partitioned node's replicas each hold their
        own keys' state, and a competing replica's state is its own too. A
        replacement carries the same replica id (``VF_REPLICA_ID``), so it finds
        exactly the record its predecessor wrote.
        '''
        return self.key('checkpoint', f'r{self._replica_id}')

    def checkpoint(self, state : bytes, replay_position : Mapping[str, Any]) -> str:
        '''
        One CAS write: state and position describe the same committed prefix.
        ``replay_position`` is the node's own vocabulary (``{'offset': 42}`` for a
        replayable source, ``{'group': <input key>}`` for a stateful node, plus
        the ``output`` committed with it). Returns the new version.
        '''
        self._update(self._checkpoint_key(), lambda doc: {'state': state.hex(), 'position': dict(replay_position),
                                                           'at': self._clock()})
        return self._store.get(self._checkpoint_key())[1] or ''

    def restore_checkpoint(self) -> tuple[bytes | None, dict[str, Any]]:
        doc, _v = self._read(self._checkpoint_key())
        if not doc:
            return None, {}
        return bytes.fromhex(doc.get('state', '')), dict(doc.get('position', {}))

    # -- sink effects (RUN-017) ------------------------------------------------------------

    def mark_effect(self, key : str, retention_seconds : float) -> bool:
        '''Mark an effect as applied; False when it already was (within its retention).'''
        now = self._clock()
        marked = {'new': False}

        def mutate(doc : dict[str, Any]) -> dict[str, Any] | None:
            if doc and float(doc.get('until', 0)) > now:
                return None
            marked['new'] = True
            return {'at': now, 'until': now + retention_seconds}
        self._update(self.key('effect', key), mutate)
        return marked['new']

    def effect_seen(self, key : str) -> bool:
        doc, _v = self._read(self.key('effect', key))
        return bool(doc) and float(doc.get('until', 0)) > self._clock()


def claim_replica_slot(store : RuntimeStore, flow_id : str, run_id : str, node : str, nb_tasks : int,
                       partition_by : str | None = None, parent_replicas : Mapping[str, int] | None = None,
                       slots : int | None = None,
                       clock : Callable[[], float] = time.time,
                       lease_seconds : float = DEFAULT_PARTITION_LEASE_SECONDS,
                       sleep : Callable[[float], None] = time.sleep) -> FlowRuntime:
    '''
    A replica id for a process that was given none (ENV-5 step 3): the lowest
    slot ``0..slots-1`` whose partition lease is free or lapsed, claimed
    through the ledger, and the ``FlowRuntime`` that holds it. A Kubernetes
    Deployment pod has no ordinal, so its competing replicas would all report as
    replica 0 — one terminator where the barrier expects N, one lease fought
    over by N pods; claiming a slot gives the live pods distinct, stable
    identities, and the replacement for a crashed pod resumes that pod's slot
    (and its ledger: unresolved intents, terminators) once its lease lapses.

    Free and lapsed slots are taken at once; when none is, each live slot is
    waited on in turn for at most one lease length (a renewal proves its holder
    alive). A process that finds every slot held is an extra replica, refused
    the way a scaled-out singleton is (RUN-018).

    - Arguments:
        - nb_tasks: the node's declared replica count (what the runtime is built with).
        - slots: how many identities may be claimed — ``nb_tasks`` unless a scaler \
            may run more pods (``VF_REPLICA_SLOTS``, its ceiling), which are wanted \
            replicas rather than extras.

    - Raises:
        - OwnershipConflict: every slot is held under a live lease.
    '''
    def runtime_for(slot : int) -> FlowRuntime:
        return FlowRuntime(store, flow_id, run_id, node, slot, nb_tasks, partition_by, parent_replicas,
                           clock = clock, lease_seconds = lease_seconds, sleep = sleep)

    count = max(1, slots if slots is not None else nb_tasks)
    for wait in (False, True):
        for slot in range(count):
            runtime = runtime_for(slot)
            try:
                runtime.acquire_partition(wait = wait)
            except OwnershipConflict:
                continue
            return runtime
    raise OwnershipConflict(
        f'{node}: every one of its {count} replica slots is held under a live lease; this process is an '
        f'extra replica',
        remedy = 'Stop this replica. To run more replicas, redeploy the flow at the wanted nb_tasks instead of '
                 'scaling the workload; ownership does not migrate.')
