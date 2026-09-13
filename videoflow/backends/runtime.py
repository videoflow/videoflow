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
from dataclasses import dataclass
from typing import Mapping, Sequence

from .capabilities import RuntimeCapabilities


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
