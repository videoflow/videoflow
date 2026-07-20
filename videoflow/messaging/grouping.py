'''
Input-group assembly for multi-parent (join) nodes, factored out of the messenger
so the two grouping strategies are testable without a broker:

- ``TraceGroupAssembler`` — groups by exact ``trace_id`` match: inputs that
  descend from the same originating message of a single upstream producer
  (diamond topologies). This is the historical behavior.
- ``TimeGroupAssembler`` — groups by event time: inputs whose ``event_ts`` fall
  within a tolerance of each other, regardless of lineage. This is how streams
  from *independent* producers (multiple cameras, sensors) are fused, with
  optional quorum emission (>= k of N parents at timeout) and per-parent
  "collect" windows for high-rate sensor parents that join many-to-one.

An assembler is fed decoded envelope entries (see
``videoflow.wire.serialization.decode_envelope``) paired with their broker ack
handles. It owns the pending buffers and resolves the handles of anything it
*discards* (evicted, superseded, expired); handles of everything it *emits*
travel out unresolved inside the ``ReadyGroup`` for the task loop to ack/fail
after processing — preserving ack-after-process semantics end to end.

This module also owns the *internal* record types for those entries.
``decode_envelope`` returns a plain ``dict`` and must keep doing so — it is
re-exported through the frozen ``videoflow.serialization`` shim, where a
dead-lettered payload recorded under the old module path still has to decode.
So the dict is adapted **once**, where messaging first receives it
(``NATSMessenger._pull_loop``), into an ``EnvelopeEntry``, and everything from
there down the join/EOS path uses typed attributes. A collected window is a
genuinely different record — its ``message``/``metadata``/``event_ts`` are
*lists* over a parent's samples and it has no lineage of its own — so it is a
separate ``CollectEntry`` rather than an ``EnvelopeEntry`` with lying types.

All methods are called from the single task thread; no locking is needed.
'''
from __future__ import absolute_import, division, print_function

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ..core.policies import JOIN_TIME, MISSING_ERROR, JoinPolicy

logger = logging.getLogger(__package__)

#: How long a buffered collect-parent message may sit unclaimed (beyond the join
#: timeout + settle window) before it is dropped as stale, when no join timeout
#: is configured to derive the bound from.
_DEFAULT_COLLECT_RETENTION_SECONDS = 30.0

@dataclass(slots = True)
class EnvelopeEntry:
    '''
    One decoded envelope as the messaging layer carries it: the exact field set
    ``videoflow.wire.serialization.decode_envelope`` returns, as attributes.

    Built at the messenger's receive boundary via ``from_decoded`` and never
    mutated afterwards — an assembler that supersedes a redelivery swaps the
    whole entry rather than editing one.
    '''
    trace_id : str
    seq : int
    event_ts : float | None
    message : Any
    metadata : dict | None
    is_stop_signal : bool
    type : str = ''
    producer_name : str = ''
    flow_id : str = ''
    run_id : str = ''
    span_id : Any = None
    parent_span_id : Any = None
    replica_id : int = 0
    blob_ref : str | None = None

    @classmethod
    def from_decoded(cls, decoded : dict[str, Any]) -> 'EnvelopeEntry':
        '''
        Adapts a ``decode_envelope`` result to an ``EnvelopeEntry``.

        - Arguments:
            - decoded (dict): the envelope dict from \
                ``videoflow.wire.serialization.decode_envelope``.
        - Returns:
            - The equivalent ``EnvelopeEntry``. Only the fields the join path \
                actually reads are required; the rest are tolerated as absent \
                (carrying their declared default) so a caller can hand over a \
                partial envelope.
        - Raises:
            - KeyError: if ``trace_id`` or ``seq`` is missing — without them a \
                group has no identity to assemble on.
        '''
        return cls(
            trace_id = decoded['trace_id'],
            seq = decoded['seq'],
            event_ts = decoded.get('event_ts'),
            message = decoded.get('message'),
            metadata = decoded.get('metadata'),
            is_stop_signal = decoded.get('is_stop_signal', False),
            type = decoded.get('type', ''),
            producer_name = decoded.get('producer_name', ''),
            flow_id = decoded.get('flow_id', ''),
            run_id = decoded.get('run_id', ''),
            span_id = decoded.get('span_id'),
            parent_span_id = decoded.get('parent_span_id'),
            replica_id = decoded.get('replica_id', 0),
            blob_ref = decoded.get('blob_ref'),
        )

@dataclass(slots = True)
class CollectEntry:
    '''
    The window of samples a *collect* parent contributes to one time group:
    every sample within that parent's window of the group's time, oldest first.

    Deliberately not an ``EnvelopeEntry`` — ``message``, ``metadata`` and
    ``event_ts`` are lists here, and the window has no single trace id or seq of
    its own (which is why ``last_input_info`` reports ``None`` for both).
    '''
    message : list = field(default_factory = list)
    metadata : list = field(default_factory = list)
    event_ts : list = field(default_factory = list)
    is_stop_signal : bool = False

#: What one parent contributes to a ``ReadyGroup``: a normal entry, a collected
#: window, or ``None`` for a parent missing from a quorum emission.
GroupEntry = EnvelopeEntry | CollectEntry | None

class ReadyGroup:
    '''
    A fully assembled input group, ready to hand to the node.

    - Attributes:
        - trace_id: identity the node's output will carry forward (a parent \
            trace id for trace groups; a minted time-window id for time groups).
        - seq: deterministic representative sequence number (stable across \
            redelivery of the same group, for output dedup).
        - event_ts: the group's event time (min over members), carried forward.
        - entries: ``{parent_name: entry}`` where entry is an \
            ``EnvelopeEntry``, ``None`` for a parent missing from a quorum \
            emission, or a ``CollectEntry`` for a collect parent's window.
        - handles: every unresolved ack handle backing this group, in no \
            particular order.
    '''
    def __init__(self, trace_id : str, seq : int, event_ts : float | None,
                entries : dict[str, GroupEntry], handles : list) -> None:
        self.trace_id = trace_id
        self.seq = seq
        self.event_ts = event_ts
        self.entries = entries
        self.handles = handles

def make_assembler(node_name : str, parent_names : list[str], policy : JoinPolicy) -> "GroupAssembler":
    if policy.mode == JOIN_TIME:
        return TimeGroupAssembler(node_name, parent_names, policy)
    return TraceGroupAssembler(node_name, parent_names, policy)

class GroupAssembler:
    '''Base interface: feed entries with ``add``, expire with ``sweep``, drain with ``pop_ready``.'''
    def __init__(self, node_name : str, parent_names : list[str], policy : JoinPolicy) -> None:
        self._node_name = node_name
        self._parent_names = list(parent_names)
        self._policy = policy

    def add(self, parent_name : str, entry : EnvelopeEntry, handle : Any) -> None:
        raise NotImplementedError

    def sweep(self, now : float | None = None) -> None:
        '''Apply the policy's timeout to pending groups (evict or stage for quorum emission).'''
        raise NotImplementedError

    def pop_ready(self, now : float | None = None) -> Optional[ReadyGroup]:
        raise NotImplementedError

    def has_pending_from(self, parent_name : str) -> bool:
        '''Whether any buffered state still holds a message from this parent (EOS drain check).'''
        raise NotImplementedError

class TraceGroupAssembler(GroupAssembler):
    '''
    Groups by exact ``trace_id``. A group is ready when every parent's half with
    the same trace id has arrived; a group that outlives the policy timeout is
    evicted per the missing policy, and the oldest group is evicted (as drop)
    beyond ``max_pending``.
    '''
    def __init__(self, node_name : str, parent_names : list[str], policy : JoinPolicy) -> None:
        super().__init__(node_name, parent_names, policy)
        self._groups: dict[str, dict[str, EnvelopeEntry]] = {}   # trace_id -> {parent: entry}
        self._handles: dict[str, dict[str, Any]] = {}   # trace_id -> {parent: handle}
        self._order: list[str] = []
        self._first_seen: dict[str, float] = {}

    def add(self, parent_name : str, entry : EnvelopeEntry, handle : Any) -> None:
        trace_id = entry.trace_id
        group = self._groups.setdefault(trace_id, {})
        handles = self._handles.setdefault(trace_id, {})
        if parent_name in handles:
            # Redelivery of a half we already buffered (the group hadn't completed
            # yet). Supersede: terminate the stale handle and keep the fresh
            # delivery so its ack deadline restarts.
            handles[parent_name].term()
        elif trace_id not in self._order:
            self._order.append(trace_id)
            self._first_seen[trace_id] = time.monotonic()
        group[parent_name] = entry
        handles[parent_name] = handle
        # Hard cap on buffered groups (last-resort memory guard): drop the oldest.
        while len(self._order) > self._policy.max_pending:
            self._evict(self._order[0], missing = 'drop', reason = 'max_pending exceeded')

    def sweep(self, now : float | None = None) -> None:
        timeout = self._policy.timeout_seconds
        if timeout is None or len(self._parent_names) < 2:
            return
        now = time.monotonic() if now is None else now
        for trace_id in list(self._order):
            if now - self._first_seen.get(trace_id, now) >= timeout:
                self._evict(trace_id, missing = self._policy.missing,
                            reason = f'join timeout ({timeout}s)')

    def _evict(self, trace_id : str, missing : str, reason : str) -> None:
        group = self._groups.pop(trace_id, None)
        if group is None:
            return
        self._order.remove(trace_id)
        self._first_seen.pop(trace_id, None)
        handles = self._handles.pop(trace_id, {})
        for handle in handles.values():
            if missing == MISSING_ERROR:
                handle.nak()   # redeliver — the missing half may still arrive
            else:
                handle.ack()   # DROP: give up on this partial group
        logger.warning(
            f'{self._node_name}: evicting incomplete join group {trace_id} '
            f'(had {list(group.keys())}, needed {self._parent_names}) — {reason}, '
            f'missing policy={missing}.'
        )

    def pop_ready(self, now : float | None = None) -> Optional[ReadyGroup]:
        for trace_id in self._order:
            if len(self._groups[trace_id]) == len(self._parent_names):
                group = self._groups.pop(trace_id)
                handles = self._handles.pop(trace_id)
                self._order.remove(trace_id)
                self._first_seen.pop(trace_id, None)
                # A deterministic representative seq/event_ts (min over the group is
                # stable because the same messages reassemble on retry).
                seq = min(group[name].seq for name in self._parent_names)
                timestamps = [t for t in (group[name].event_ts for name in self._parent_names)
                            if t is not None]
                event_ts = min(timestamps) if timestamps else None
                return ReadyGroup(trace_id, seq, event_ts, dict(group),
                                [handles[name] for name in self._parent_names])
        return None

    def has_pending_from(self, parent_name : str) -> bool:
        return any(parent_name in group for group in self._groups.values())

class _TimeGroup:
    __slots__ = ('gid', 'ts', 'first_seen', 'entries', 'handles')

    def __init__(self, gid : int, ts : float, first_seen : float) -> None:
        self.gid = gid
        self.ts = ts                 # min event_ts over members: the group's time
        self.first_seen = first_seen
        self.entries: dict[str, EnvelopeEntry] = {}   # sync parent -> entry
        self.handles: dict[str, Any] = {}   # sync parent -> handle

class TimeGroupAssembler(GroupAssembler):
    '''
    Groups by event time. A message from a synchronized parent joins the pending
    group whose time is nearest to its ``event_ts`` (and within
    ``tolerance_ms``) among groups not already holding that parent; otherwise it
    seeds a new group. Messages from *collect* parents are buffered per parent
    and attached as a list at emission time (every sample within that parent's
    window of the group's time).

    A group is ready when every sync parent is present — held for the largest
    collect window first, so trailing high-rate samples can arrive — or when the
    policy timeout expires and at least ``quorum`` sync parents are present
    (missing parents emit as ``None``). A timed-out group below quorum is
    evicted per the missing policy.

    Messages without an ``event_ts`` (pre-v3 upstream) fall back to their
    arrival time — correct enough for co-located low-latency flows, but real
    deployments should stamp at the producer.
    '''
    def __init__(self, node_name : str, parent_names : list[str], policy : JoinPolicy) -> None:
        super().__init__(node_name, parent_names, policy)
        unknown = set(policy.collect) - set(parent_names)
        if unknown:
            raise ValueError(f'{node_name}: collect parents {sorted(unknown)} are not '
                            f'parents of this node (parents: {parent_names})')
        self._sync_parents = [p for p in parent_names if p not in policy.collect]
        if not self._sync_parents:
            raise ValueError(f'{node_name}: at least one parent must be synchronized '
                            '(not in collect) to anchor time groups')
        if policy.quorum is not None and policy.quorum > len(self._sync_parents):
            raise ValueError(f'{node_name}: quorum={policy.quorum} exceeds the number '
                            f'of synchronized parents ({len(self._sync_parents)})')
        if policy.tolerance_ms is None:
            raise ValueError(f"{node_name}: time-mode joins require a positive tolerance_ms; "
                            "pass JoinPolicy(mode='time', tolerance_ms=...)")
        self._tolerance_s = policy.tolerance_ms / 1000.0
        self._collect_windows = {p: w / 1000.0 for p, w in policy.collect.items()}
        self._settle_s = max(self._collect_windows.values(), default = 0.0)
        retention = (policy.timeout_seconds
                    if policy.timeout_seconds is not None
                    else _DEFAULT_COLLECT_RETENTION_SECONDS)
        self._collect_retention_s = retention + self._settle_s
        # Collect buffers are bounded per parent; beyond the cap the oldest sample
        # is dropped (acked). Sized so a stalled sync parent can't grow it forever
        # while still holding several seconds of a high-rate sensor.
        self._collect_cap = max(1024, policy.max_pending * 16)

        self._groups: dict[int, _TimeGroup] = {}   # gid -> _TimeGroup
        self._order: list[int] = []               # gids, insertion order
        self._gid_counter = 0
        self._ready: list[ReadyGroup] = []        # ReadyGroups staged by sweep (quorum/timeout emissions)
        # collect parent -> list of [event_ts, arrival_monotonic, entry, handle]
        self._collect_buffers: dict[str, list] = {p: [] for p in self._collect_windows}

    # -- ingestion -----------------------------------------------------

    def add(self, parent_name : str, entry : EnvelopeEntry, handle : Any) -> None:
        now = time.monotonic()
        ts = entry.event_ts
        if ts is None:
            ts = time.time()
        if parent_name in self._collect_windows:
            buf = self._collect_buffers[parent_name]
            buf.append([ts, now, entry, handle])
            while len(buf) > self._collect_cap:
                _, _, _, old_handle = buf.pop(0)
                old_handle.ack()
                logger.warning(f'{self._node_name}: collect buffer for {parent_name} '
                            f'full ({self._collect_cap}); dropping oldest sample')
            return

        # Redelivery of a message already buffered in a pending group: supersede
        # in place so its ack deadline restarts, instead of seeding a duplicate.
        for group in self._groups.values():
            existing = group.entries.get(parent_name)
            if (existing is not None and existing.trace_id == entry.trace_id
                    and existing.seq == entry.seq):
                group.handles[parent_name].term()
                group.entries[parent_name] = entry
                group.handles[parent_name] = handle
                return

        # Nearest pending group within tolerance that doesn't have this parent yet.
        best = None
        best_diff = None
        for group in self._groups.values():
            if parent_name in group.entries:
                continue
            diff = abs(group.ts - ts)
            if diff <= self._tolerance_s and (best_diff is None or diff < best_diff):
                best, best_diff = group, diff
        if best is None:
            self._gid_counter += 1
            best = _TimeGroup(self._gid_counter, ts, now)
            self._groups[best.gid] = best
            self._order.append(best.gid)
        best.entries[parent_name] = entry
        best.handles[parent_name] = handle
        best.ts = min(best.ts, ts)

        while len(self._order) > self._policy.max_pending:
            self._evict(self._order[0], missing = 'drop', reason = 'max_pending exceeded')

    # -- expiry --------------------------------------------------------

    def sweep(self, now : float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self._prune_collect_buffers(now)
        timeout = self._policy.timeout_seconds
        if timeout is None:
            return
        for gid in list(self._order):
            group = self._groups[gid]
            if now - group.first_seen < timeout:
                continue
            complete = len(group.entries) == len(self._sync_parents)
            quorum_met = (self._policy.quorum is not None
                        and len(group.entries) >= self._policy.quorum)
            if complete or quorum_met:
                if not complete:
                    missing = sorted(set(self._sync_parents) - set(group.entries))
                    logger.info(f'{self._node_name}: emitting time group at quorum '
                                f'({len(group.entries)}/{len(self._sync_parents)} sync '
                                f'parents; missing {missing}) after {timeout}s timeout')
                self._ready.append(self._emit(gid))
            else:
                self._evict(gid, missing = self._policy.missing,
                            reason = f'join timeout ({timeout}s), below quorum')

    def _prune_collect_buffers(self, now : float) -> None:
        for parent, buf in self._collect_buffers.items():
            kept = []
            for item in buf:
                if now - item[1] > self._collect_retention_s:
                    item[3].ack()  # stale sample no group claimed: drop it
                else:
                    kept.append(item)
            if len(kept) != len(buf):
                self._collect_buffers[parent] = kept

    def _evict(self, gid : int, missing : str, reason : str) -> None:
        group = self._groups.pop(gid, None)
        if group is None:
            return
        self._order.remove(gid)
        for handle in group.handles.values():
            if missing == MISSING_ERROR:
                handle.nak()
            else:
                handle.ack()
        logger.warning(
            f'{self._node_name}: evicting time group at ts={group.ts:.6f} '
            f'(had {sorted(group.entries.keys())}, needed {self._sync_parents}) — '
            f'{reason}, missing policy={missing}.'
        )

    # -- emission ------------------------------------------------------

    def pop_ready(self, now : float | None = None) -> Optional[ReadyGroup]:
        if self._ready:
            return self._ready.pop(0)
        now = time.monotonic() if now is None else now
        for gid in self._order:
            group = self._groups[gid]
            if len(group.entries) != len(self._sync_parents):
                continue
            # Hold a complete group for the settle window so trailing collect
            # samples (whose event times lag the group's) can still arrive.
            if self._settle_s and now - group.first_seen < self._settle_s:
                continue
            return self._emit(gid)
        return None

    def _emit(self, gid : int) -> ReadyGroup:
        group = self._groups.pop(gid)
        self._order.remove(gid)
        entries: dict[str, GroupEntry] = {}
        handles = list(group.handles.values())
        for parent in self._sync_parents:
            entries[parent] = group.entries.get(parent)  # None if below-quorum missing
        for parent, window_s in self._collect_windows.items():
            claimed: list = []
            rest: list = []
            for item in self._collect_buffers[parent]:
                (claimed if abs(item[0] - group.ts) <= window_s else rest).append(item)
            self._collect_buffers[parent] = rest
            claimed.sort(key = lambda item: item[0])
            entries[parent] = CollectEntry(
                message = [item[2].message for item in claimed],
                metadata = [item[2].metadata for item in claimed],
                event_ts = [item[0] for item in claimed],
            )
            handles.extend(item[3] for item in claimed)
        # Identity of the group is its event time: stable across redelivery (the
        # same members regroup to the same min ts), so downstream dedup holds.
        seq = int(round(group.ts * 1e6))
        return ReadyGroup(f'tw-{seq}', seq, group.ts, entries, handles)

    def has_pending_from(self, parent_name : str) -> bool:
        # Groups staged by sweep but not yet handed to the task still hold this
        # parent's messages — they must keep the parent "pending" so the EOS
        # drain doesn't declare it stopped and strand the staged group.
        for ready in self._ready:
            entry = ready.entries.get(parent_name)
            if parent_name in self._collect_windows:
                if isinstance(entry, CollectEntry) and entry.message:
                    return True
            elif entry is not None:
                return True
        if parent_name in self._collect_buffers:
            return bool(self._collect_buffers[parent_name])
        return any(parent_name in g.entries for g in self._groups.values())
