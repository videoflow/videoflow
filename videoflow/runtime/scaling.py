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
would rehash; GPU nodes only on explicit opt-in. One rule is an error rather
than a silent skip. A BATCH flow renders every node as a Kubernetes **Job**, and
a Job's ``parallelism`` is fixed when it is created — it is not what a scaler
scales: KEDA's ``ScaledObject`` and the HPA drive the ``/scale`` subresource of
a Deployment or StatefulSet, and a Job has none. A ``ScaledObject`` whose
``scaleTargetRef`` names the Deployment a Job-rendered node never gets is a
dangling scaler that does nothing while the operator believes it scales
(RUN-027), so ``job_autoscaling_error`` is raised at render time instead.

Nothing here touches a broker or a cluster: the inputs are
``backends.messaging.SubscriptionObservation`` values wrapped in
``backends.outcomes.Known``/``Unknown``, and every function is deterministic.
'''
from __future__ import absolute_import, division, print_function

from dataclasses import dataclass
from typing import Iterable, Mapping

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
                       gpu_autoscaling : bool = False) -> list[str]:
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
                      gpu_autoscaling : bool = False) -> list[str]:
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
                                 device_type = device_type, gpu_autoscaling = gpu_autoscaling)
    if is_job and spec_kind == NODE_KIND_PROCESSOR:
        raise job_autoscaling_error(node_name, flow_type)
    return reasons
