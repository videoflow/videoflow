'''
Negative controls: one reproduction per reviewed defect, applied as a
monkeypatch so the paired conformance oracle can be shown to *fail* against it.
A conformance case is only evidence if its oracle would have caught the bug it
was written for — the ``negative_control(of = ID)`` test in each module asserts
exactly that, and the results writer marks the case INVALID_TEST when the
defect goes undetected.

These live in the test tree on purpose: nothing under ``videoflow/`` may import
them, and none of them is a behaviour anyone should be able to switch on.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
from typing import Any, Callable

import pytest

from videoflow.backends import capabilities, identity
from videoflow.backends.capabilities import LIVE_LATEST, FlowRequirements, ProfileRequest
from videoflow.backends.memory.allocation import MemoryAllocationBackend
from videoflow.backends.memory.messaging import MemoryMessagingBackend
from videoflow.backends.outcomes import known
from videoflow.core.errors import IncompatibleProfile, UnobservableState
from videoflow.deploy import cluster, mig
from videoflow.messaging.topology import stream_name_for


def detects(oracle : Callable[..., Any], *args : Any, **kwargs : Any) -> bool:
    '''True when ``oracle`` fails (an assertion or a pytest failure) — i.e. the defect was caught.'''
    try:
        oracle(*args, **kwargs)
    except (AssertionError, pytest.fail.Exception):
        return True
    return False


# -- MSG-001: a planner that downgrades instead of rejecting -------------------------------

def downgrading_planner() -> Callable[..., Any]:
    '''The reviewed defect: a request the backend cannot honour is quietly served
    at the nearest weaker profile instead of being rejected.'''
    real = capabilities.plan_composition

    def planner(requirements : FlowRequirements, messaging : Any, **kwargs : Any) -> Any:
        try:
            return real(requirements, messaging, **kwargs)
        except (IncompatibleProfile, UnobservableState):
            weaker = dataclasses.replace(
                requirements, restart_safe = False, exactly_once_effects = (),
                profiles = tuple(ProfileRequest(r.channel, LIVE_LATEST) for r in requirements.profiles))
            return real(weaker, messaging, **kwargs)
    return planner


# -- MSG-019: a compiler that never rejects colliding names --------------------------------

def silent_identity_codec(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the fix the graph validator only checked raw names for uniqueness,
    so ``a.b`` and ``a_b`` compiled and then shared one stream.'''
    monkeypatch.setattr(identity, 'collisions', lambda specs, flow_id, run_id: [])
    monkeypatch.setattr(identity, 'node_name_collisions', lambda names: [])


# -- MSG-020: teardown by name prefix ------------------------------------------------------

def prefix_teardown(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``delete_run_streams`` deleted every stream whose name started with
    ``vf-{flow}-{run}-`` — which run ``r-x`` of the same flow also matches.'''
    real = MemoryMessagingBackend.close

    def close(self : MemoryMessagingBackend, owned : Any, expected_generation : str) -> Any:
        prefixes = {stream_name_for(c.flow_id, c.run_id, '')[:-1] + '-' for c in owned}
        victims = [cid for cid in self.channel_ids()
                   if any(stream_name_for(cid.flow_id, cid.run_id, cid.node).startswith(p) for p in prefixes)]
        return real(self, victims, expected_generation)
    monkeypatch.setattr(MemoryMessagingBackend, 'close', close)


# -- ALLOC-001: a solver that budgets compute slices but not memory ---------------------------

def memory_blind_card(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``_Card.fits`` before the fix: slice count and per-profile maximum only.'''
    def fits(self : Any, profile : mig.MigProfile) -> bool:
        if self.whole_owner is not None or self.table is None:
            return False
        if self.slices_used + profile.slices > self.table.total_slices:
            return False
        return self.mig_counts.get(profile.name, 0) < profile.max_per_gpu
    monkeypatch.setattr(mig._Card, 'fits', fits)


# -- ALLOC-007: failed reads that read as "nothing in use" -------------------------------------

def fail_open_occupancy(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``gpu_units_in_use`` returned ``{}`` for a failed listing and for an idle
    cluster alike, and the memory planner's twin ignored a partial snapshot.'''
    monkeypatch.setattr(cluster, 'gpu_units_in_use_observed', lambda kubectl = 'kubectl': known({}))
    monkeypatch.setattr(cluster, 'gpu_inventory_observed',
                        lambda kubectl = 'kubectl', _real = cluster.gpu_inventory_observed:
                        _mark_complete(_real(kubectl)))
    real_plan = MemoryAllocationBackend.plan

    def plan(self : MemoryAllocationBackend, requests : Any, snapshot : Any) -> Any:
        return real_plan(self, requests, dataclasses.replace(snapshot, completeness = 'complete'))
    monkeypatch.setattr(MemoryAllocationBackend, 'plan', plan)


def _mark_complete(observed : Any) -> Any:
    if isinstance(observed, known(0).__class__):
        return known([dataclasses.replace(n, occupancy_known = True) for n in observed.value], observed.generation)
    return observed


# -- ALLOC-008: classification over every node, capacity over the pool -----------------------------

def unscoped_classifier(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the fix ``classify_gpu_resource`` listed *all* nodes while the
    capacity math listed the pool, so an out-of-pool time-sliced node tainted
    the pool's classification.'''
    import json

    def all_nodes(kubectl : str) -> Any:
        out = cluster._kubectl_out(kubectl, 'get', 'nodes', '-o', 'json')
        if not out:
            return None
        try:
            return json.loads(out).get('items', [])
        except ValueError:
            return None
    monkeypatch.setattr(cluster, '_pool_nodes', all_nodes)


# -- ALLOC-009: MPS mistaken for whole devices --------------------------------------------------------

def mps_blind_classifier(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the fix the classifier only knew time-slicing and MIG: an explicit
    ``sharing-strategy=mps`` node without replica labels read as physical.'''
    real = cluster.classify_gpu_resource

    def classify(kubectl : str = 'kubectl', resource : str = 'nvidia.com/gpu', **kwargs : Any) -> str:
        kind = real(kubectl, resource, **kwargs)
        return 'physical' if kind == 'mps' else kind
    monkeypatch.setattr(cluster, 'classify_gpu_resource', classify)


# -- ALLOC-023: validation after the first write ------------------------------------------------------

def eager_deploy(backend : MemoryAllocationBackend, requests : Any, capabilities : Any) -> Any:
    '''An adapter that reserves first and validates second: every rejected
    request leaves an owner stamp (rolled back or not) in the mutation audit.
    The oracle drives this in place of the real validate-then-write path.'''
    from videoflow.backends.allocation import FeasiblePlan, allocation_rejections
    snapshot = backend.inventory({}).value
    plan = backend.plan(requests, snapshot)
    claim = backend.reserve(plan, f'{requests[0].flow_id}:eager', None) if isinstance(plan, FeasiblePlan) else None
    reasons = allocation_rejections(requests, capabilities)
    if reasons:
        if claim is not None:
            backend.release(claim.claim_id, f'{requests[0].flow_id}:undo', claim.desired_generation)
        return reasons
    return claim


# -- RUN-011: progress checked only between messages ------------------------------------------------

def loop_only_watchdog(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the fix the deadline was consulted only by the task loop between
    messages: a process() that never returns is never diagnosed. A watchdog
    whose thread never starts reproduces that.'''
    from videoflow.runtime.watchdog import ProgressWatchdog
    monkeypatch.setattr(ProgressWatchdog, 'start', lambda self: None)


# -- RUN-012: a wall-clock deadline that ignores whether work is pending -----------------------------

def wall_clock_deadline(monkeypatch : pytest.MonkeyPatch) -> None:
    '''A deadline that trips on silence alone restarts idle nodes and slow
    warm-ups; the pending-aware check is what makes those distinct states.'''
    from videoflow.core.errors import ProgressStalled
    from videoflow.core.supervision import ProgressDeadline

    def check(self : ProgressDeadline) -> None:
        if self._timeout > 0 and self.silent_for() >= self._timeout:
            raise ProgressStalled(f'{self._node_name or "node"} was silent for {self.silent_for():.0f}s',
                                  remedy = 'none — this is the defective wall-clock rule', node = self._node_name)
    monkeypatch.setattr(ProgressDeadline, 'check', check)


# -- RUN-048: a supervisor that waits on children in launch order -----------------------------------

def sequential_supervisor(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The committed ``wait_for_completion`` before the concurrent supervisor:
    ``proc.wait()`` on each child in list order, so a dead processor behind an
    unbounded source is not noticed until the source exits.'''
    import signal

    from videoflow.engines import local

    def wait_for_completion(self : Any) -> list:
        stopped = {-signal.SIGINT, -signal.SIGTERM}
        self._failures = []
        pending = list(self._procs)
        self._procs = []
        try:
            while pending:
                name, replica_idx, proc = pending.pop(0)
                proc.wait()
                code = proc.returncode or 0
                if code == 0 or code in stopped:
                    continue
                reason = self._termination_reason(name, replica_idx)
                self._events.emit(local.NodeExited(name, replica_idx, code, reason))
                restarted = self._maybe_restart(name, replica_idx, reason)
                if restarted is not None:
                    pending.append(restarted)
                    continue
                self._failures.append((name, replica_idx, code))
        finally:
            self._stop_abort_announcer()
        failed : list = []
        for name, _replica, _code in self._failures:
            if name not in failed:
                failed.append(name)
        return failed
    monkeypatch.setattr(local.LocalProcessEngine, 'wait_for_completion', wait_for_completion)


# -- RUN-045: an exporter that only knows the mean ---------------------------------------------------

def mean_as_p95(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Count and sum are all this exporter keeps: every observation is bucketed
    at the running mean, so the histogram carries no tail — and a p95 read off
    it is the mean wearing a percentile's name.'''
    from videoflow.runtime.health import HealthState
    real = HealthState.observe
    running : dict = {}

    def observe(self : HealthState, metric : str, value : Any) -> None:
        if value is None:
            return
        count, total = running.get((id(self), metric), (0, 0.0))
        count, total = count + 1, total + float(value)
        running[(id(self), metric)] = (count, total)
        real(self, metric, total / count)
    monkeypatch.setattr(HealthState, 'observe', observe)


# -- RUN-046: demand read off the first declared parent only --------------------------------------------

def first_parent_demand(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The single-trigger scaler: whichever parent came first in the node's
    declaration is the only one whose lag is ever looked at.'''
    from videoflow.runtime import scaling
    real = scaling.observe_demand

    def observe_demand(observations : Any, required_parents : Any = None, **kwargs : Any) -> Any:
        first = dict(list(observations.items())[:1])
        return real(first, required_parents = None, **kwargs)
    monkeypatch.setattr(scaling, 'observe_demand', observe_demand)


# -- RUN-027: autoscaling admitted for a Job-rendered node ---------------------------------------------

def job_blind_admission(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Render the scaler regardless of the workload kind: a ScaledObject whose
    scaleTargetRef names a Deployment that does not exist.'''
    from videoflow.runtime import scaling
    monkeypatch.setattr(scaling, 'admit_autoscaling', lambda *args, **kwargs: [])


# -- ALLOC-010: aggregate-only feasibility -----------------------------------------------------------

def aggregate_only_packing(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The pre-fix preflight arithmetic: total demand <= total free and the
    largest pod <= the largest node, with no per-host placement at all.'''
    from videoflow.deploy import gpu

    def pack(claims : Any, free : Any) -> Any:
        feasible = sum(claims) <= sum(free.values()) and (not claims or max(claims) <= max(free.values(), default = 0))
        return gpu.PodPacking(feasible = feasible, proven = True,
                              placement = {i: sorted(free)[0] for i in range(len(claims))} if feasible and free else {},
                              unplaced = ())
    monkeypatch.setattr(gpu, 'pack_pod_claims', pack)


# -- ALLOC-034: a resolver where the last declaration wins ------------------------------------------

def last_wins_resolver(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The implicit merge before the resolver: whichever source was applied
    last overwrites the field, hard or soft, and nothing is ever rejected.'''
    from videoflow.core import provenance

    def resolve(declarations : Any, subject : str = 'node') -> Any:
        values : dict = {}
        sources : dict = {}
        for d in declarations:
            values[d.field] = d.value
            sources[d.field] = d.source
        return provenance.GpuResolution(values, sources, {})
    monkeypatch.setattr(provenance, 'resolve_gpu_requirements', resolve)


# -- MSG-008: a ladder that blames the message for a worker's failure ---------------------------

def blaming_ladder(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the taxonomy every failure walked the transient ladder: a worker-fatal
    error at the cap dead-lettered a healthy input.'''
    from videoflow.core.errors import TRANSIENT, WORKER_FATAL
    from videoflow.core.policies import DeliveryPolicy
    real = DeliveryPolicy.action_for

    def action_for(self : DeliveryPolicy, disposition : str, num_delivered : int) -> str:
        return real(self, TRANSIENT if disposition == WORKER_FATAL else disposition, num_delivered)
    monkeypatch.setattr(DeliveryPolicy, 'action_for', action_for)


# -- MSG-009: a failed dead-letter publish terminated the delivery anyway ------------------------

def terminating_dlq_failure(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The stranding path: when the dead letter could not be recorded the delivery
    was terminated against a local note, and the input vanished into zero pending.'''
    from videoflow.core.policies import ACTION_DLQ
    from videoflow.messaging.nats_messenger import NATSMessenger
    real = NATSMessenger._settle_failure

    def settle_failure(self : NATSMessenger, handle : Any, error : dict, disposition : str) -> None:
        if self._delivery_policy.action_for(disposition, handle.num_delivered) != ACTION_DLQ:
            return real(self, handle, error, disposition)
        record = self._dlq_publish(handle, error) or None
        handle.term(record or self._terminal_record(handle, error, 'dlq-publish-failed'))
    monkeypatch.setattr(NATSMessenger, '_settle_failure', settle_failure)


# -- MSG-010: a catch-all decode handler ---------------------------------------------------------

def catch_all_decode_terminator(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed handler: any exception on the decode path — a store outage
    included — was undecodable, and undecodable meant TERM with no dead letter.'''
    from videoflow.messaging import nats_messenger
    from videoflow.messaging.nats_messenger import NATSMessenger, _AckHandle

    def discard(self : NATSMessenger, parent_name : str, delivery : Any, error : BaseException,
                handle : Any = None) -> None:
        handle = handle or _AckHandle(delivery.token, self, raw = delivery.envelope_bytes)
        self._count_drop('undecodable')
        handle.term(self._terminal_record(handle, {'code': 'VF_UNKNOWN', 'message': str(error)}, 'undecodable'))
    monkeypatch.setattr(NATSMessenger, '_discard_undecodable', discard)
    monkeypatch.setattr(nats_messenger, '_is_transient_store_failure', lambda error: False)


# -- MSG-011: a settlement that is not fenced by the delivery attempt --------------------------------

def unfenced_settle(monkeypatch : pytest.MonkeyPatch) -> None:
    '''A stale handle's TERM reached the broker and deleted the delivery a newer attempt held.'''
    from videoflow.backends.memory.messaging import MemoryMessagingBackend
    real = MemoryMessagingBackend.settle

    def settle(self : MemoryMessagingBackend, token : Any, outcome : Any, settlement_id : str) -> Any:
        stored, view = self._find(token)
        if view is not None:
            current = dataclasses.replace(token, attempt = view.attempt, generation = view.generation)
            return real(self, current, outcome, settlement_id)
        return real(self, token, outcome, settlement_id)
    monkeypatch.setattr(MemoryMessagingBackend, 'settle', settle)


# -- MSG-012: the (0, 0) fail-open -------------------------------------------------------------------

def zero_on_failure_observation(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``_consumer_pending`` converted every API error to ``(0, 0)`` and no counter
    knew about exhausted work: a stranded input read as an empty, healthy queue.'''
    from videoflow.backends.memory.messaging import MemoryMessagingBackend
    from videoflow.backends.messaging import SubscriptionObservation
    real = MemoryMessagingBackend.observe_subscription

    def observe(self : MemoryMessagingBackend, subscription : Any) -> Any:
        observed = real(self, subscription)
        if not isinstance(observed, known(0).__class__):
            return known(SubscriptionObservation(0, 0, 0, 0, 0, self._clock.now()))
        value = observed.value
        return known(SubscriptionObservation(value.available, value.leased, 0, value.dropped,
                                             value.rejected_publications, value.observed_at, value.generation),
                     observed.generation)
    monkeypatch.setattr(MemoryMessagingBackend, 'observe_subscription', observe)


# -- MSG-005: a provisioner that logs a refused consumer creation and reports ready -------------

def swallowing_subscription_provisioner(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The model's twin of the reviewed ``_ensure_consumer``: every exception caught, logged, ready.'''
    from videoflow.backends.memory.messaging import MemoryMessagingBackend
    from videoflow.backends.messaging import VerifiedSubscription
    real = MemoryMessagingBackend.ensure_subscription

    def ensure_subscription(self : MemoryMessagingBackend, spec : Any, operation_id : str) -> Any:
        try:
            return real(self, spec, operation_id)
        except Exception:  # noqa: BLE001 — the defect: swallowed at debug level
            return VerifiedSubscription(spec, {}, ())
    monkeypatch.setattr(MemoryMessagingBackend, 'ensure_subscription', ensure_subscription)


def swallowing_consumer_provisioner(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``topology._ensure_consumer`` as reviewed: ``add_consumer`` in a bare try, a debug line, no read-back.'''
    from videoflow.messaging import topology

    async def ensure_consumer(js : Any, stream_name : str, config : Any) -> Any:
        try:
            await js.add_consumer(stream_name, config)
        except Exception:  # noqa: BLE001 — the defect
            pass
        return topology.VerifiedConsumer(config, None, ())
    monkeypatch.setattr(topology, '_ensure_consumer', ensure_consumer)


# -- MSG-006: an existing resource accepted on its name ------------------------------------------

def name_match_provisioner(monkeypatch : pytest.MonkeyPatch) -> None:
    '''"Already exists" taken as success: no comparison of the effective configuration.'''
    from videoflow.backends.memory.messaging import MemoryMessagingBackend, _channel_effective, _subscription_effective
    from videoflow.backends.messaging import VerifiedChannel, VerifiedSubscription
    real_channel = MemoryMessagingBackend.ensure_channel
    real_subscription = MemoryMessagingBackend.ensure_subscription

    def ensure_channel(self : MemoryMessagingBackend, spec : Any, operation_id : str) -> Any:
        existing = self._channels.get(spec.id)
        if existing is not None:
            return VerifiedChannel(spec, _channel_effective(existing.spec), ())
        return real_channel(self, spec, operation_id)

    def ensure_subscription(self : MemoryMessagingBackend, spec : Any, operation_id : str) -> Any:
        channel = self._channels.get(spec.id.channel)
        existing = channel.subscriptions.get(spec.id) if channel is not None else None
        if existing is not None:
            return VerifiedSubscription(spec, _subscription_effective(existing.spec), ())
        return real_subscription(self, spec, operation_id)
    monkeypatch.setattr(MemoryMessagingBackend, 'ensure_channel', ensure_channel)
    monkeypatch.setattr(MemoryMessagingBackend, 'ensure_subscription', ensure_subscription)


# -- MSG-004: one competing subscription shared by distinct children ---------------------------------

def shared_child_subscription(monkeypatch : pytest.MonkeyPatch) -> None:
    '''A durable named after the parent alone: X and Y compete for one copy of each input.'''
    from videoflow.backends.memory.messaging import MemoryMessagingBackend
    from videoflow.messaging import topology
    real = MemoryMessagingBackend.ensure_subscription

    def ensure_subscription(self : MemoryMessagingBackend, spec : Any, operation_id : str) -> Any:
        shared = dataclasses.replace(spec, id = dataclasses.replace(spec.id, consumer_node = 'shared'))
        return real(self, shared, operation_id)
    monkeypatch.setattr(MemoryMessagingBackend, 'ensure_subscription', ensure_subscription)
    monkeypatch.setattr(topology, 'durable_name_for',
                        lambda consumer_node_name, parent_node_name: f'shared--from--{topology.sanitize(parent_node_name)}')


# -- MSG-007: a durable per process ----------------------------------------------------------------------

def process_scoped_subscription(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Every bind creates its own subscription: a replacement abandons the one the dead worker held.'''
    import uuid

    from videoflow.backends.memory.messaging import MemoryMessagingBackend
    real = MemoryMessagingBackend.ensure_subscription

    def ensure_subscription(self : MemoryMessagingBackend, spec : Any, operation_id : str) -> Any:
        scoped = dataclasses.replace(spec, id = dataclasses.replace(spec.id, instance = uuid.uuid4().hex[:6]))
        return real(self, scoped, operation_id)
    monkeypatch.setattr(MemoryMessagingBackend, 'ensure_subscription', ensure_subscription)


# -- MSG-017: max_ack_pending fixed at eight -----------------------------------------------------------------

def fixed_credit(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The pre-RFC credit: eight for every durable, whatever the replica count.'''
    from videoflow.messaging import topology
    monkeypatch.setattr(topology, 'consumer_credit', lambda nb_tasks, partitioned, item_credit = 1, prefetch = 4: 8)
    import test_msg_subscriptions
    monkeypatch.setattr(test_msg_subscriptions, 'consumer_credit', topology.consumer_credit)


# -- MSG-013: a publisher that reports a guess -------------------------------------------------------------

def guessing_publisher(monkeypatch : pytest.MonkeyPatch) -> None:
    '''A send the broker never answered reported as a definite rejection — while the
    bytes were still on their way and landed later.'''
    from videoflow.backends.memory.messaging import MemoryMessagingBackend
    from videoflow.backends.outcomes import PublicationUnknown, Rejected
    real = MemoryMessagingBackend.publish

    def publish(self : MemoryMessagingBackend, envelope : Any, deadline : float) -> Any:
        outcome = real(self, envelope, deadline)
        if isinstance(outcome, PublicationUnknown):
            return Rejected(outcome.publication_id, 'timed out: assumed not published', True)
        return outcome
    monkeypatch.setattr(MemoryMessagingBackend, 'publish', publish)
    monkeypatch.setattr(MemoryMessagingBackend, 'cancel_publication', lambda self, channel_id, pid: False)


# -- MSG-014: an unlimited idempotence claim that only relies on the window ----------------------------------

def unlimited_idempotence_claim(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The capabilities advertise a persistent publication ledger; the backend keeps the window.'''
    from videoflow.backends.capabilities import LEDGER_PERSISTENT
    from videoflow.backends.memory.messaging import MemoryMessagingBackend
    real = MemoryMessagingBackend.capabilities

    def capabilities(self : MemoryMessagingBackend) -> Any:
        return dataclasses.replace(real(self), publication_ledger = LEDGER_PERSISTENT)
    monkeypatch.setattr(MemoryMessagingBackend, 'capabilities', capabilities)


# -- MSG-015: a live publish that returns success from every caught exception ------------------------------

def swallowed_live_failure(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed REALTIME ``_do_publish``: whatever happened, the frame was counted as sent.'''
    from videoflow.backends.outcomes import Accepted
    from videoflow.core.constants import REALTIME
    from videoflow.messaging.nats_messenger import NATSMessenger
    real = NATSMessenger._publish_envelope

    def publish_envelope(self : NATSMessenger, envelope : Any, is_abort : bool) -> Any:
        outcome = real(self, envelope, is_abort)
        if self._flow_type == REALTIME and not isinstance(outcome, Accepted):
            self._count('accepted')
        return outcome
    monkeypatch.setattr(NATSMessenger, '_publish_envelope', publish_envelope)


# -- MSG-002: requirements blind to a node's delivery override ---------------------------------------------

def override_blind_requirements(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the planner, the flow type decided for every node: an at-least-once
    alert sink in a REALTIME flow got a retry-enabled consumer on a one-slot stream.'''
    from videoflow.backends.capabilities import LIVE_LATEST, RELIABLE_WORK
    from videoflow.core.constants import BATCH
    from videoflow.core.policies import DeliveryPolicy

    def profile_for_edge(flow_type : str, delivery : Any) -> str:
        DeliveryPolicy.resolve(flow_type, None)
        return RELIABLE_WORK if flow_type == BATCH else LIVE_LATEST
    monkeypatch.setattr(capabilities, 'profile_for_edge', profile_for_edge)


# -- MSG-003: latest-per-key served by one global slot ---------------------------------------------------

def global_slot_latest(monkeypatch : pytest.MonkeyPatch) -> None:
    '''One ``max_msgs`` per node, whatever the key: camera A's traffic evicts camera B's only frame.'''
    monkeypatch.setattr(MemoryMessagingBackend, '_subject_key', lambda self, channel, envelope: envelope.kind)


# -- MSG-016: an envelope-count bound and nothing else ---------------------------------------------------

def count_only_backpressure(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The byte budget silently dropped from the channel: only ``max_msgs`` ever bounds anything.'''
    real = MemoryMessagingBackend.ensure_channel

    def ensure_channel(self : MemoryMessagingBackend, spec : Any, operation_id : str) -> Any:
        return real(self, dataclasses.replace(spec, max_bytes = None), operation_id)
    monkeypatch.setattr(MemoryMessagingBackend, 'ensure_channel', ensure_channel)


# -- MSG-018: control and data in one retention slot ------------------------------------------------------

def shared_control_slot(monkeypatch : pytest.MonkeyPatch) -> None:
    '''EOS and data on one ``max_msgs = 1`` stream: the terminator evicts the final frame, the next frame the terminator.'''
    from videoflow.backends.messaging import KIND_DATA
    monkeypatch.setattr(MemoryMessagingBackend, '_subject_key', lambda self, channel, envelope: KIND_DATA)


# -- MSG-024: the working queue posing as the archive -------------------------------------------------------

def working_queue_as_archive(monkeypatch : pytest.MonkeyPatch) -> None:
    '''"Replay" read the INTEREST stream: nothing is there once the working consumers acknowledged.'''
    def replay(self : MemoryMessagingBackend, channel_id : Any, event_id : str) -> Any:
        for envelope in self.stored(channel_id):
            if envelope.event_id == event_id:
                return envelope
        return None
    monkeypatch.setattr(MemoryMessagingBackend, 'replay', replay)


# -- MSG-025: a core transport that claims a backlog -----------------------------------------------------------

def core_claims_backlog(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Capabilities that promise retained, recoverable delivery over a transport that keeps nothing.'''
    real = MemoryMessagingBackend.capabilities

    def caps(self : MemoryMessagingBackend) -> Any:
        return dataclasses.replace(real(self), retained_backlog = True, recoverable_delivery = True)
    monkeypatch.setattr(MemoryMessagingBackend, 'capabilities', caps)
