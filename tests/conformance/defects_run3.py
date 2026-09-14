'''
Negative controls for the Phase-3 RUN cases (credits, scaling admission, the
rollout drain): one reproduction per reviewed defect, applied as a monkeypatch
so the paired oracle can be shown to *fail* against it. Same rules as
``defects.py`` — nothing under ``videoflow/`` imports this, and none of these
is a behaviour anyone should be able to switch on.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
from typing import Any

import pytest

from videoflow.backends import capabilities
from videoflow.backends.memory.messaging import MemoryMessagingBackend
from videoflow.deploy import manifests
from videoflow.messaging import topology
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.runtime import scaling

# -- RUN-024: the historical eight-credit durable, whatever the plan admits -------------------------

def fixed_credit(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``max_ack_pending`` was 8 for every shared durable (``topology.py`` L172-190): ten admitted workers, eight credits.'''
    monkeypatch.setattr(topology, 'consumer_credit',
                        lambda nb_tasks, partitioned, item_credit = 1, prefetch = 4: topology.DEFAULT_MAX_ACK_PENDING)


# -- RUN-025: prefetch bounded by a message count only -----------------------------------------------

def count_only_admission(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed prefetch queue (``nats_messenger.py`` L78, L342-350) was bounded
    by ``_QUEUE_MAXSIZE`` messages and nothing else: a 1 MB frame cost the same
    admission as a 200-byte record, so a slow worker's queue held whatever its
    count allowed. Reproduced at the receive: the byte credit is ignored.
    '''
    real = MemoryMessagingBackend.receive

    def receive(self : MemoryMessagingBackend, subscription : Any, item_credit : int, byte_credit : int,
                deadline : float) -> Any:
        return real(self, subscription, item_credit, 1 << 62, deadline)
    monkeypatch.setattr(MemoryMessagingBackend, 'receive', receive)


# -- RUN-018 / RUN-019: eligibility read off the replica count alone ------------------------------------

def replica_count_only_eligibility(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed admission (``manifests.py`` L162, L204-205, L655-699) knew one
    fixed-scale rule, ``partition_by and nb_tasks > 1``: a two-parent join at one
    replica and a ``partition_by`` node at one replica both got a ScaledObject.
    '''
    real = scaling.scaling_rejections

    def rejections(spec_kind : str, is_partitioned : bool, is_job : bool, *args : Any, **kwargs : Any) -> Any:
        kwargs.pop('is_join', None)
        kwargs.pop('declares_partition', None)
        return real(spec_kind, is_partitioned, is_job, *args, **kwargs)
    monkeypatch.setattr(scaling, 'scaling_rejections', rejections)
    monkeypatch.setattr(manifests, '_keeps_declared_scale', lambda spec: False)


# -- RUN-026: demand read from the queue depth that lossy retention keeps shallow ----------------------------

def lag_only_rate_observer(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed scaler (``manifests.py`` L678-699 over ``topology.py`` L116-141)
    drove replicas from consumer lag alone. Under a one-message retention the lag
    is at most one, so every window read as healthy — and a missing metric read
    as no demand at all.
    '''
    def observe(sample : Any, objective : Any, current_replicas : int, min_replicas : int = 1,
                max_replicas : int = scaling.DEFAULT_MAX_REPLICAS, last_change_at : Any = None,
                now : float = 0.0) -> Any:
        held = max(min_replicas, min(max_replicas, current_replicas))
        window = sample.value if isinstance(sample, scaling.Known) else None
        if window is None:
            return scaling.RateDecision(held, scaling.DEMAND_IDLE)
        lag = 1 if window.offered > window.processed else 0
        fraction = window.processed / max(1, window.processed + lag)
        if fraction >= objective.min_delivered_fraction:
            return scaling.RateDecision(held, scaling.DEMAND_WITHIN_OBJECTIVE, fraction, window)
        return scaling.RateDecision(held, scaling.DEMAND_BREACH, fraction, window)
    monkeypatch.setattr(scaling, 'observe_rate_demand', observe)


# -- RUN-028: every claim counts, whatever the allocator observed ---------------------------------------------

def requests_count_as_capacity(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed scaler (``manifests.py`` L773-775, L831-841) let the HPA raise
    ``replicas`` and called it a scale-out: a Pending pod, a claim still being
    prepared, or a claim nobody could observe all counted as capacity.
    '''
    def reconcile(desired : int, claims : Any) -> Any:
        granted = len(claims)
        return scaling.CapacityDecision(desired, granted, granted, desired, scaling.CAPACITY_ADMITTED)
    monkeypatch.setattr(scaling, 'reconcile_capacity', reconcile)


# -- RUN-030: a quiesce that only raises a flag -------------------------------------------------------------

def flag_only_quiesce(monkeypatch : pytest.MonkeyPatch) -> None:
    '''
    The reviewed SIGTERM path (``worker.py`` L303-315 over ``task.py`` L340-390)
    set a termination flag and died: the inputs the adapter had prefetched stayed
    leased on a process that no longer existed, until ``ack_wait`` lapsed.
    '''
    monkeypatch.setattr(NATSMessenger, 'quiesce', lambda self: self._termination_event.set())


# -- RUN-035 / RUN-036: a planner that never looks at the declared execution shapes -----------------

def shape_blind_planner(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the declarations existed the planner had nothing to refuse: a fused group or a
    batching contract deployed as ordinary nodes, silently. Reproduced by planning without them.'''
    real = capabilities.plan_composition

    def blind(requirements : Any, messaging : Any, **kwargs : Any) -> Any:
        stripped = dataclasses.replace(requirements, execution_groups = {}, batching = {})
        return real(stripped, messaging, **kwargs)
    monkeypatch.setattr(capabilities, 'plan_composition', blind)
