'''
Autoscaling demand as a pure function of what the broker says about each of a
node's parents, and the rules that say when a node must not be autoscaled at all.

Why this exists. ``videoflow deploy --autoscaling`` renders a KEDA
``ScaledObject`` per processor that scales its Deployment on JetStream consumer
lag. Two things were wrong with how that demand was derived, and both are the
kind of bug no test fails on because the wrong answer is a plausible number:

- **Demand was read from one parent.** The scaler was keyed on the node's first
  declared parent only, so a join whose first parent was drained while its
  second backed up reported no lag at all — and which parent was "first" was an
  accident of the ``process(a, b)`` declaration, so two equivalent graphs scaled
  differently. ``observe_demand`` evaluates every parent it is given, returns a
  canonical (name-sorted) decision that is identical for any permutation of the
  input, and takes the replica recommendation as the maximum over parents. That
  is also exactly what a ``ScaledObject`` with one trigger per parent does — the
  HPA "will calculate metrics for every scaler and use the highest desired
  replica count" (KEDA FAQ) — so the pure function and the rendered scaler agree
  by construction.
- **A failed read was a zero.** A ``consumer_info`` that timed out looked like an
  empty consumer. Here a parent whose observation is ``Unknown`` makes the whole
  demand unknown: ``DemandDecision.replicas`` is ``None`` and the parent is
  named. Never a zero that would scale a busy node down.

Demand is *diagnosed* as well as sized, because RUN-046's second question is
whether extra workers can help at all. A multi-parent node is a join: every
group needs a message from each required parent. When every parent has work
waiting, the backlog is a processing-capacity shortfall that replicas repair.
When some parents are empty while others back up, the backed-up messages are
waiting for counterparts that have not arrived — a missing-input stall that no
replica count repairs, and that a quorum or timeout join policy, not a scaler,
resolves. The decision carries both the recommendation and the diagnosis; the
caller that knows the join policy decides what a starved join gets.

Eligibility is separate from demand. ``scaling_rejections`` names every reason
a node stays at a fixed scale, mirroring the rules ``deploy.manifests`` applies
when it renders: only processors; not partitioned nodes, whose key ownership
would rehash; not a multi-parent join at a single replica, which a scaler would
turn into competing multi-worker joins (RUN-018); not a node that *declares*
``partition_by`` at ``nb_tasks = 1``, because the intent is not erased by the
replica count — scaling it from one binds every new replica to the same
competing durable and splits one key's history among unfenced states
(RUN-019); GPU nodes only on explicit opt-in. One rule is an error rather
than a silent skip. A BATCH flow renders every node as a Kubernetes **Job**, and
a Job's ``parallelism`` is fixed when it is created — it is not what a scaler
scales: KEDA's ``ScaledObject`` and the HPA drive the ``/scale`` subresource of
a Deployment or StatefulSet, and a Job has none. A ``ScaledObject`` whose
``scaleTargetRef`` names the Deployment a Job-rendered node never gets is a
dangling scaler that does nothing while the operator believes it scales
(RUN-027), so ``job_autoscaling_error`` is raised at render time instead.

Two more observers live here because they are the same kind of pure decision:

- ``observe_rate_demand`` sizes demand from *throughput* rather than lag, for the
  live-video path whose lossy retention conceals overload (RUN-026): a
  ``live_latest`` channel keeps one message per subject, so a node at a third of
  the offered rate shows a lag of one and a scaler keyed on lag never fires,
  while two frames in three are evicted before delivery. The health counters —
  offered, processed, dropped over a control window — see the loss the queue
  depth hides, and the declared objective (``RateObjective``) says how much of
  it is acceptable. A missing counter is ``DEMAND_UNKNOWN``, never idle.
- ``reconcile_capacity`` keeps *desired* concurrency (what demand asks for)
  apart from *granted* (claims the allocator holds) and *ready* (workloads
  observed processing) (RUN-028): a Pending pod or an allocation request is
  not processing capacity, a shortfall is a named ``capacity_constrained``
  decision rather than a silently smaller scale-out, and a claim that could not
  be observed is listed as unknown instead of counted either way.

Nothing here touches a broker or a cluster: the inputs are
``backends.messaging.SubscriptionObservation`` values wrapped in
``backends.outcomes.Known``/``Unknown``, and every function is deterministic.
'''
from __future__ import absolute_import, division, print_function

import math
from dataclasses import dataclass
from typing import Iterable, Mapping

from ..backends.allocation import CLAIM_ALLOCATED, CLAIM_PREPARED, CLAIM_READY, ClaimObservation
from ..backends.messaging import SubscriptionObservation
from ..backends.outcomes import Known, Observation
from ..core.compiler import NODE_KIND_PROCESSOR
from ..core.errors import CapabilityError, ConfigError

#: Diagnoses a ``DemandDecision`` carries alongside its replica recommendation.
DEMAND_UNKNOWN = 'unknown'    # a parent could not be observed: the decision is no decision
DEMAND_IDLE = 'idle'          # no parent has work waiting
DEMAND_BACKLOG = 'backlog'    # every parent has work waiting: a processing-capacity signal
DEMAND_STARVED = 'starved'    # some parents have work, others none: a missing-input stall for a join that needs them
DIAGNOSES = (DEMAND_UNKNOWN, DEMAND_IDLE, DEMAND_BACKLOG, DEMAND_STARVED)

#: Diagnoses of a ``RateDecision`` (``observe_rate_demand``), alongside ``DEMAND_UNKNOWN`` and ``DEMAND_IDLE``.
DEMAND_WITHIN_OBJECTIVE = 'within_objective'   # the delivered fraction meets the declared objective
DEMAND_BREACH = 'breach'                       # the objective is breached: work is lost faster than it is processed
DEMAND_STABILIZING = 'stabilizing'             # a breach, but inside the stabilization window of the last change: held
DEMAND_STALLED = 'stalled'                     # work offered, nothing processed: not a capacity question
RATE_DIAGNOSES = (DEMAND_UNKNOWN, DEMAND_IDLE, DEMAND_WITHIN_OBJECTIVE, DEMAND_BREACH, DEMAND_STABILIZING,
                  DEMAND_STALLED)

#: Diagnoses of a ``CapacityDecision`` (``reconcile_capacity``).
CAPACITY_ADMITTED = 'admitted'                  # every desired replica holds a grant
CAPACITY_CONSTRAINED = 'capacity_constrained'   # fewer grants than desired: partial admission, named as such
CAPACITY_UNKNOWN = 'unknown'                    # a claim could not be observed: neither counted nor discounted
CAPACITY_DIAGNOSES = (CAPACITY_ADMITTED, CAPACITY_CONSTRAINED, CAPACITY_UNKNOWN)

#: The rule's defaults, mirroring the rendered scaler: ``lagThreshold: '10'`` in
#: ``deploy.manifests.scaled_object`` and the CLI's ``--max-replicas`` default.
DEFAULT_TARGET_LAG_PER_REPLICA = 10
DEFAULT_MAX_REPLICAS = 10

#: ``ParentDemand.unknown_reason`` for a required parent nobody observed at all.
MISSING_OBSERVATION = 'missing'

#: The one rejection that is an error rather than a fixed-scale skip (RUN-027).
JOB_REJECTION = ('the node renders as a Kubernetes Job, whose parallelism is fixed when it is created and is not '
                 "what a scaler scales: KEDA's ScaledObject and the HPA drive the /scale subresource of a "
                 'Deployment or StatefulSet, and a Job has none.')
#: A singleton join must not become competing multi-worker joins (RUN-018).
JOIN_REJECTION = ('the node joins several parents at a single replica: a scaler would turn that singleton join into '
                  'competing multi-worker joins, delivering the halves of one group to different replicas where '
                  'neither can assemble it, and no runtime store advertises the elastic join state that would let '
                  'group ownership follow the replica count.')
#: Partition intent declared at nb_tasks = 1 is intent, not an accident of the replica count (RUN-019).
PARTITION_INTENT_REJECTION = ('the node declares partition_by: a replica count of 1 does not erase that intent, and '
                              'scaling it from 1 would bind every new replica to the same competing durable, '
                              "splitting one key's history among independent, unfenced states — a partitioned "
                              'node scales only by redeploying with the nb_tasks it should own its keys at.')

@dataclass(frozen = True)
class ParentDemand:
    '''
    One parent's contribution to a decision.

    - Arguments:
        - parent: the parent node's name.
        - lag: the messages the node still owes this parent — not yet delivered \
            plus delivered-but-unacked — or ``None`` when the observation was \
            ``Unknown``.
        - replicas: what this parent alone asks for under the rule, or ``None`` \
            when unknown.
        - unknown_reason: the ``Unknown.reason`` (``'timeout'``, \
            ``MISSING_OBSERVATION`` …); empty when the parent was observed.
    '''
    parent : str
    lag : int | None
    replicas : int | None
    unknown_reason : str = ''

@dataclass(frozen = True)
class DemandDecision:
    '''
    The outcome of ``observe_demand``. Canonical: two calls over the same
    observations handed over in any order compare (and hash) equal.

    - Arguments:
        - replicas: the recommendation — the maximum over parents, clamped to \
            ``[min_replicas, max_replicas]`` — or ``None`` when the diagnosis is \
            ``DEMAND_UNKNOWN``. Never ``0`` for "could not tell".
        - diagnosis: one of ``DIAGNOSES``.
        - parents: every evaluated parent, sorted by name.
        - unknown_parents: the parents that could not be observed, sorted.
        - starved_parents: the parents with nothing waiting while another parent \
            has a backlog, sorted; empty unless the diagnosis is ``DEMAND_STARVED``.
    '''
    replicas : int | None
    diagnosis : str
    parents : tuple[ParentDemand, ...]
    unknown_parents : tuple[str, ...] = ()
    starved_parents : tuple[str, ...] = ()

    @property
    def known(self) -> bool:
        '''Whether every parent was observed, i.e. whether ``replicas`` is a number.'''
        return self.diagnosis != DEMAND_UNKNOWN

def validate_scaling_rule(target_lag_per_replica : int, min_replicas : int, max_replicas : int) -> None:
    '''
    - Raises:
        - ConfigError: a target below 1 (a replica must absorb some lag), a \
            minimum below 1 (a processor at zero replicas never observes its \
            end-of-stream), or a maximum below the minimum.
    '''
    if target_lag_per_replica < 1:
        raise ConfigError(f'target_lag_per_replica must be >= 1, got {target_lag_per_replica}.',
                          remedy = 'Set the messages of lag one replica should absorb before another is added '
                                   f'(the rendered scaler uses {DEFAULT_TARGET_LAG_PER_REPLICA}).')
    if min_replicas < 1:
        raise ConfigError(f'min_replicas must be >= 1, got {min_replicas}.',
                          remedy = "Keep at least one replica: a processor scaled to zero cannot observe its "
                                   "parents' end-of-stream, so the flow never completes.")
    if max_replicas < min_replicas:
        raise ConfigError(f'max_replicas ({max_replicas}) is below min_replicas ({min_replicas}).',
                          remedy = 'Raise --max-replicas to at least the node\'s nb_tasks, or lower nb_tasks.')

def parent_lag(observation : SubscriptionObservation) -> int:
    '''
    The messages the node still owes one parent: ``available`` (not yet
    delivered) plus ``leased`` (delivered, not yet acked) — the same sum the
    KEDA nats-jetstream scaler reads (``num_pending + num_ack_pending``), so a
    decision here matches the rendered trigger. ``unresolved`` is excluded on
    purpose: work the broker can no longer deliver is not work another replica
    could take.
    '''
    return observation.available + observation.leased

def replicas_for_lag(lag : int, target_lag_per_replica : int, min_replicas : int, max_replicas : int) -> int:
    '''
    The per-parent rule: enough replicas for each to carry at most
    ``target_lag_per_replica`` messages — ``ceil(lag / target)``, the HPA's
    ``AverageValue`` arithmetic — clamped to ``[min_replicas, max_replicas]``.
    '''
    wanted = -(-lag // target_lag_per_replica)      # ceil, without a float in the way
    return min(max_replicas, max(min_replicas, wanted))

def observe_demand(observations : Mapping[str, Observation[SubscriptionObservation]],
                   required_parents : Iterable[str] | None = None,
                   target_lag_per_replica : int = DEFAULT_TARGET_LAG_PER_REPLICA,
                   min_replicas : int = 1,
                   max_replicas : int = DEFAULT_MAX_REPLICAS) -> DemandDecision:
    '''
    Size and diagnose a node's demand from every parent's subscription observation.

    - Arguments:
        - observations: parent name -> what the broker said about the node's \
            subscription on that parent (``Known`` or ``Unknown``). Iteration \
            order is irrelevant: the decision is canonical.
        - required_parents: the parents that must be evaluated. Defaults to the \
            observed ones; a required parent with no observation at all counts \
            as ``Unknown(MISSING_OBSERVATION)``, because "nobody asked" is not \
            "no work".
        - target_lag_per_replica, min_replicas, max_replicas: the rule; see \
            ``replicas_for_lag``.

    - Returns:
        - a ``DemandDecision``: ``replicas`` is the maximum over parents when \
            every one was observed, else ``None`` with the unknown parents named.

    - Raises:
        - ConfigError: an invalid rule (see ``validate_scaling_rule``), or no \
            parents at all to evaluate.
    '''
    validate_scaling_rule(target_lag_per_replica, min_replicas, max_replicas)
    names = sorted(set(observations) | set(required_parents or ()))
    if not names:
        raise ConfigError('observe_demand was given no parents to evaluate.',
                          remedy = 'Pass one observation per parent of the node (its NodeSpec.parents); '
                                   'a processor always has at least one.')
    parents : list[ParentDemand] = []
    for name in names:
        observation = observations.get(name)
        if isinstance(observation, Known):
            lag = parent_lag(observation.value)
            parents.append(ParentDemand(
                name, lag, replicas_for_lag(lag, target_lag_per_replica, min_replicas, max_replicas)))
        else:
            reason = MISSING_OBSERVATION if observation is None else observation.reason
            parents.append(ParentDemand(name, None, None, reason))
    unknown = tuple(p.parent for p in parents if p.lag is None)
    if unknown:
        return DemandDecision(None, DEMAND_UNKNOWN, tuple(parents), unknown_parents = unknown)
    replicas = max(p.replicas for p in parents if p.replicas is not None)
    starved = tuple(p.parent for p in parents if p.lag == 0)
    if len(starved) == len(parents):
        return DemandDecision(replicas, DEMAND_IDLE, tuple(parents))
    if starved:
        return DemandDecision(replicas, DEMAND_STARVED, tuple(parents), starved_parents = starved)
    return DemandDecision(replicas, DEMAND_BACKLOG, tuple(parents))

def scaling_rejections(spec_kind : str, is_partitioned : bool, is_job : bool,
                       has_parents : bool = True, device_type : str = 'cpu',
                       gpu_autoscaling : bool = False, is_join : bool = False,
                       declares_partition : bool = False) -> list[str]:
    '''
    Every reason this node stays at a fixed scale — empty means a scaler may be
    rendered. Mirrors the rules ``deploy.manifests`` applies, in one place, so
    an admission check and the renderer cannot drift.

    - Arguments:
        - spec_kind: ``NodeSpec.kind``.
        - is_partitioned: ``partition_by`` set with ``nb_tasks > 1`` \
            (``manifests._is_partitioned``).
        - is_job: whether the workload renders as a Kubernetes Job — every node \
            of a BATCH flow, and a finite producer of a REALTIME one \
            (``manifests.workload``).
        - has_parents: whether the node consumes anything, i.e. has a stream \
            whose lag could be measured.
        - device_type, gpu_autoscaling: ``NodeSpec.device_type`` and the \
            deploy's ``--gpu-autoscaling`` opt-in; GPU nodes are excluded \
            without it.
        - is_join: the node has more than one parent. A join at one replica is \
            a singleton join; scaled, its groups' halves land on different \
            replicas (``JOIN_REJECTION``, RUN-018).
        - declares_partition: ``partition_by`` is set, whatever ``nb_tasks`` \
            is. Distinct from ``is_partitioned`` on purpose: at ``nb_tasks = 1`` \
            the node binds a competing durable, and a scaler would add \
            replicas to *that* durable (``PARTITION_INTENT_REJECTION``, RUN-019).

    - Returns:
        - the applicable reasons, each a sentence naming the rule and its \
            consequence. ``JOB_REJECTION`` is the one that must not be skipped \
            silently: ``admit_autoscaling`` raises for it.
    '''
    reasons : list[str] = []
    if spec_kind != NODE_KIND_PROCESSOR:
        reasons.append(f'{spec_kind} nodes are not autoscaled: only processors are (a producer owns its source '
                       'and a consumer runs at its declared nb_tasks).')
    elif not has_parents:
        reasons.append('the node has no parent stream whose lag could drive a scaler.')
    if is_partitioned:
        reasons.append('the node is partitioned (partition_by with nb_tasks > 1): rehashing key ownership on a '
                       'replica-count change would double- or zero-process messages, so it runs at a fixed scale.')
    elif declares_partition:
        reasons.append(PARTITION_INTENT_REJECTION)
    if is_join:
        reasons.append(JOIN_REJECTION)
    if is_job:
        reasons.append(JOB_REJECTION)
    if device_type == 'gpu' and not gpu_autoscaling:
        reasons.append('the node runs on GPU and --gpu-autoscaling was not given: each extra replica claims '
                       'whole GPUs, so lag-driven scale-out can strand pods Pending.')
    return reasons

def job_autoscaling_error(node_name : str, flow_type : str) -> CapabilityError:
    '''
    The RUN-027 refusal, built in one place so the renderer and an admission
    check raise the same words: autoscaling was requested for a node that
    renders as a Job. Raised *before* anything is applied — the alternative is
    a ScaledObject whose ``scaleTargetRef`` names a Deployment that does not
    exist, which the API server accepts and the HPA then cannot resolve, so the
    flow runs at ``nb_tasks`` forever while the operator believes it scales.
    '''
    return CapabilityError(
        f'Node {node_name!r} cannot be autoscaled: in a {flow_type} flow it renders as a Kubernetes Job, and a '
        "Job's parallelism is fixed at creation — it is not what a scaler scales (a ScaledObject drives the "
        '/scale subresource of a Deployment or StatefulSet; a Job has none).',
        remedy = f'Drop --autoscaling and set nb_tasks on {node_name!r} to the parallelism you want, or run the '
                 'flow as REALTIME, where processors render as Deployments that KEDA can scale.',
        node = node_name, flow_type = flow_type)

def admit_autoscaling(node_name : str, flow_type : str, spec_kind : str, is_partitioned : bool, is_job : bool,
                      has_parents : bool = True, device_type : str = 'cpu',
                      gpu_autoscaling : bool = False, is_join : bool = False,
                      declares_partition : bool = False) -> list[str]:
    '''
    The render-time admission for one node when ``--autoscaling`` is on: the
    fixed-scale rejections to log (render no scaler, keep the declared scale),
    or the Job refusal raised.

    - Returns:
        - ``scaling_rejections(...)`` for this node; empty means render a scaler.

    - Raises:
        - CapabilityError: the node is a processor that renders as a Job — the \
            flag cannot mean anything for it, and a scaler would dangle.
    '''
    reasons = scaling_rejections(spec_kind, is_partitioned, is_job, has_parents = has_parents,
                                 device_type = device_type, gpu_autoscaling = gpu_autoscaling,
                                 is_join = is_join, declares_partition = declares_partition)
    if is_job and spec_kind == NODE_KIND_PROCESSOR:
        raise job_autoscaling_error(node_name, flow_type)
    return reasons

# -- throughput-based demand (RUN-026) ---------------------------------------------------

@dataclass(frozen = True)
class ThroughputSample:
    '''
    One control window of a node's throughput, from the health counters and the
    subscription observation.

    - Arguments:
        - offered: inputs the source offered into the node's channel during the \
            window — what the node was handed (``messages_offered_total``) *plus* \
            what the channel evicted before delivery (``SubscriptionObservation.dropped``). \
            Under lossy retention the second term is the whole story.
        - processed: inputs the node acknowledged during the window (``messages_processed_total``).
        - dropped: inputs lost during the window — evicted before delivery, or \
            given up on by policy (``messages_dropped_total``).
        - window_seconds: the window's length.
    '''
    offered : int
    processed : int
    dropped : int
    window_seconds : float

    @property
    def delivered_fraction(self) -> float | None:
        '''``processed / offered``, or ``None`` when nothing was offered.'''
        if self.offered <= 0:
            return None
        return min(1.0, self.processed / self.offered)

@dataclass(frozen = True)
class RateObjective:
    '''
    The declared delivery objective a live node must meet, per control window.

    - Arguments:
        - min_delivered_fraction: the least ``processed / offered`` that is \
            acceptable over one window (``1.0`` means every offered frame).
        - control_window_seconds: how long a window is; a sample shorter than \
            this is not yet a decision.
        - stabilization_seconds: how long after a replica change a further \
            change is held back, so a decision is not taken against a fleet \
            that has not finished starting.
    '''
    min_delivered_fraction : float
    control_window_seconds : float
    stabilization_seconds : float = 0.0

@dataclass(frozen = True)
class RateDecision:
    '''
    The outcome of ``observe_rate_demand``.

    - Arguments:
        - replicas: the recommendation, or ``None`` when the diagnosis is \
            ``DEMAND_UNKNOWN`` or ``DEMAND_STALLED``. Never ``0`` for "could not tell".
        - diagnosis: one of ``RATE_DIAGNOSES``.
        - delivered_fraction: what the window measured, ``None`` when unknown or idle.
        - sample: the sample decided on, ``None`` when unknown.
        - unknown_reason: why the sample could not be used (``Unknown.reason``, \
            ``MISSING_OBSERVATION``, or ``'window'`` for a sample shorter than \
            the control window).
    '''
    replicas : int | None
    diagnosis : str
    delivered_fraction : float | None = None
    sample : ThroughputSample | None = None
    unknown_reason : str = ''

def validate_rate_objective(objective : RateObjective) -> None:
    '''
    - Raises:
        - ConfigError: a delivered fraction outside ``(0, 1]``, a control window \
            that is not positive, or a negative stabilization window.
    '''
    if not 0.0 < objective.min_delivered_fraction <= 1.0:
        raise ConfigError(f'min_delivered_fraction must be in (0, 1], got {objective.min_delivered_fraction}.',
                          remedy = 'Declare the fraction of offered frames one control window must deliver, e.g. 0.9.')
    if objective.control_window_seconds <= 0:
        raise ConfigError(f'control_window_seconds must be positive, got {objective.control_window_seconds}.',
                          remedy = 'Declare the window the counters are read over (the scaler polling interval).')
    if objective.stabilization_seconds < 0:
        raise ConfigError(f'stabilization_seconds must not be negative, got {objective.stabilization_seconds}.',
                          remedy = 'Use 0 for no stabilization, or the seconds a replica takes to become ready.')

def throughput_sample(offered_before : int, offered_after : int, processed_before : int, processed_after : int,
                      dropped_before : int, dropped_after : int, evicted_before : int, evicted_after : int,
                      window_seconds : float) -> ThroughputSample:
    '''
    A ``ThroughputSample`` from two readings of the counters: the node's offered
    / processed / dropped totals (``HealthState.throughput``) and the channel's
    evicted-before-delivery count (``SubscriptionObservation.dropped``), which is
    folded into both ``offered`` and ``dropped`` because the node never saw
    those inputs at all. Counters only grow, so a reading that went *down* is a
    restarted exporter: the sample is taken from zero for that counter.
    '''
    def delta(before : int, after : int) -> int:
        return after if after < before else after - before
    evicted = delta(evicted_before, evicted_after)
    return ThroughputSample(
        offered = delta(offered_before, offered_after) + evicted,
        processed = delta(processed_before, processed_after),
        dropped = delta(dropped_before, dropped_after) + evicted,
        window_seconds = window_seconds)

def observe_rate_demand(sample : Observation[ThroughputSample] | None, objective : RateObjective,
                        current_replicas : int, min_replicas : int = 1,
                        max_replicas : int = DEFAULT_MAX_REPLICAS,
                        last_change_at : float | None = None, now : float = 0.0) -> RateDecision:
    '''
    Size and diagnose a live node's demand from its throughput over one control
    window, against the declared objective.

    - Arguments:
        - sample: the window's ``ThroughputSample``, as ``Known``/``Unknown``; \
            ``None`` when nobody read the counters (``MISSING_OBSERVATION``).
        - objective: the declared ``RateObjective``.
        - current_replicas: how many replicas produced ``sample.processed``; the \
            per-replica capacity is derived from it.
        - min_replicas, max_replicas: the admissible range; the recommendation \
            is clamped, never invented beyond it.
        - last_change_at, now: when the replica count last changed and the \
            current time on the same clock; a breach within \
            ``objective.stabilization_seconds`` of the change is \
            ``DEMAND_STABILIZING`` and holds ``current_replicas``.

    - Returns:
        - a ``RateDecision``. A breach asks for \
            ``ceil(offered / (processed / current_replicas))`` replicas — enough \
            for the measured per-replica capacity to absorb the offered rate — \
            clamped to the range.

    - Raises:
        - ConfigError: an invalid objective or range.
    '''
    validate_rate_objective(objective)
    validate_scaling_rule(1, min_replicas, max_replicas)
    if current_replicas < 1:
        raise ConfigError(f'current_replicas must be >= 1, got {current_replicas}.',
                          remedy = 'Pass the replica count that produced the sample.')
    if sample is None:
        return RateDecision(None, DEMAND_UNKNOWN, unknown_reason = MISSING_OBSERVATION)
    if not isinstance(sample, Known):
        return RateDecision(None, DEMAND_UNKNOWN, unknown_reason = sample.reason)
    window = sample.value
    if window.window_seconds < objective.control_window_seconds:
        return RateDecision(None, DEMAND_UNKNOWN, sample = window, unknown_reason = 'window')
    fraction = window.delivered_fraction
    if fraction is None:
        return RateDecision(max(min_replicas, min(max_replicas, current_replicas)), DEMAND_IDLE, sample = window)
    if fraction >= objective.min_delivered_fraction:
        return RateDecision(max(min_replicas, min(max_replicas, current_replicas)), DEMAND_WITHIN_OBJECTIVE,
                            fraction, window)
    if window.processed <= 0:
        return RateDecision(None, DEMAND_STALLED, fraction, window)
    if last_change_at is not None and now - last_change_at < objective.stabilization_seconds:
        return RateDecision(max(min_replicas, min(max_replicas, current_replicas)), DEMAND_STABILIZING,
                            fraction, window)
    per_replica = window.processed / current_replicas
    wanted = math.ceil(window.offered * objective.min_delivered_fraction / per_replica)
    return RateDecision(min(max_replicas, max(min_replicas, wanted)), DEMAND_BREACH, fraction, window)

# -- desired versus granted versus ready (RUN-028) ---------------------------------------------

@dataclass(frozen = True)
class CapacityDecision:
    '''
    The outcome of ``reconcile_capacity``.

    - Arguments:
        - desired: the replicas demand asked for.
        - granted: claims observed holding a grant (allocated, prepared or ready).
        - ready: claims whose workload is observed ready — the only count that \
            is processing capacity.
        - admitted: ``min(desired, granted)``: what may be scheduled now.
        - diagnosis: one of ``CAPACITY_DIAGNOSES``.
        - reason: the sentence a report prints for a constrained or unknown decision.
        - unknown_claims: claims whose observation was ``Unknown``, sorted; \
            neither granted nor ready, and named.
    '''
    desired : int
    granted : int
    ready : int
    admitted : int
    diagnosis : str
    reason : str = ''
    unknown_claims : tuple[str, ...] = ()

def reconcile_capacity(desired : int, claims : Mapping[str, Observation[ClaimObservation]]) -> CapacityDecision:
    '''
    Reconcile the replicas demand wants with what the allocator actually holds.

    - Arguments:
        - desired: the demand decision's replica count (``>= 0``).
        - claims: claim id -> the allocator's ``observe(claim_id)``; one claim per \
            replica the plan asked for.

    - Returns:
        - a ``CapacityDecision``. A claim counts as *granted* only when observed \
            ``allocated``/``prepared``/``ready`` with a grant, as *ready* only when \
            observed ``ready``; a pending or failed claim is neither, and an \
            ``Unknown`` observation is listed, never counted. ``desired > granted`` \
            is ``capacity_constrained`` with the shortfall named; a shortfall \
            that unknown claims could cover is ``unknown``.
    '''
    if desired < 0:
        raise ConfigError(f'desired replicas must be >= 0, got {desired}.',
                          remedy = 'Pass the replica count a demand decision produced.')
    granted = ready = 0
    unknown : list[str] = []
    for claim_id in sorted(claims):
        observed = claims[claim_id]
        if not isinstance(observed, Known):
            unknown.append(claim_id)
            continue
        claim = observed.value
        if claim.status in (CLAIM_ALLOCATED, CLAIM_PREPARED, CLAIM_READY) and claim.grant:
            granted += 1
            if claim.status == CLAIM_READY:
                ready += 1
    admitted = min(desired, granted)
    if desired <= granted:
        return CapacityDecision(desired, granted, ready, admitted, CAPACITY_ADMITTED, unknown_claims = tuple(unknown))
    shortfall = desired - granted
    if unknown:
        return CapacityDecision(
            desired, granted, ready, admitted, CAPACITY_UNKNOWN,
            reason = (f'{shortfall} of {desired} desired replicas hold no observed grant and {len(unknown)} claim(s) '
                      f'could not be observed ({", ".join(unknown)}): the shortfall cannot be told from a read failure.'),
            unknown_claims = tuple(unknown))
    return CapacityDecision(
        desired, granted, ready, admitted, CAPACITY_CONSTRAINED,
        reason = (f'{desired} replicas desired, {granted} granted: {shortfall} cannot be scheduled until the allocator '
                  f'grants more capacity; {ready} of the granted are ready to process.'))
