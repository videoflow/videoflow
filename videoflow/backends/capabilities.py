'''
Capability profiles and the composition planner.

A backend's name is not a guarantee. "JetStream" can be a single server on an
emptyDir or a three-replica cluster on persistent disks; "Redis" can be an
evictable cache or an append-only durable store; "GPU" can be a whole device, a
MIG slice with hardware isolation, or a time-share that only the scheduler
accounts for. So every adapter reports a versioned *capabilities* record, the
flow (or its defaults) states what it *requires* as profiles, and the planner
compares the two before a single stream is provisioned, a single worker starts,
or a single label is written. A requirement the composition cannot meet is
rejected with the channel, the profile and the missing guarantee named; it is
never quietly downgraded to whatever the backend does offer.

Messaging profiles (ARCHITECTURE §2):

- ``live_latest``: bounded backlog and age, explicit drop policy and scope
  (latest per key versus global), observable gaps; no offline replay promise.
- ``reliable_work``: every accepted item stays recoverable for each required
  logical consumer until committed or durably failed; overflow backpressures,
  rejects before acceptance, or records a terminal outcome — never evicts.
- ``durable_control``: reconciled run state and durable terminal records across
  controller and worker restarts; a transient notification may wake workers but
  cannot be the sole record of stop, abort or completion.
- ``replay_archive``: retention independent of working consumers, with payloads
  and provenance; a replay is a new execution with explicit recipients.

Allocation guarantees name the mechanism behind a promise, because "reserved"
means three different things: ``enforcement`` says whether a memory bound is
enforced by hardware (MIG), by a runtime cap (MPS pinned memory), by scheduler
accounting only (a consumable capacity or a whole-device count), or not at all.

``FlowRequirements`` is deliberately **not** a field of ``NodeSpec``: the spec is
serialised with ``asdict`` into every specs ConfigMap, so a new field changes
bytes for every existing flow. Requirements travel as a separate document beside
the specs and as environment variables that are emitted only when set.
'''
from __future__ import absolute_import, division, print_function

import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ..core.compiler import NodeSpec
from ..core.constants import BATCH, REALTIME
from ..core.errors import SEVERITY_ERROR, ConfigError, Diagnostic, IncompatibleProfile, UnobservableState
from ..core.policies import AT_LEAST_ONCE, BEST_EFFORT, DeliveryPolicy
from .outcomes import Observation, Unknown

# -- messaging profiles --------------------------------------------------------------

LIVE_LATEST = 'live_latest'
RELIABLE_WORK = 'reliable_work'
DURABLE_CONTROL = 'durable_control'
REPLAY_ARCHIVE = 'replay_archive'
MESSAGING_PROFILES = (LIVE_LATEST, RELIABLE_WORK, DURABLE_CONTROL, REPLAY_ARCHIVE)

#: Retention classes a channel can be compiled to. A channel has exactly one.
RETENTION_LIMITS = 'limits'
RETENTION_INTEREST = 'interest'

#: What a backend knows about the fate of a publication after the fact.
LEDGER_NONE = 'none'
LEDGER_WINDOW = 'window'
LEDGER_PERSISTENT = 'persistent'

# -- allocation guarantees -----------------------------------------------------------

EXCLUSIVE_DEVICE = 'exclusive_device'
ISOLATED_MIG = 'isolated_mig'
COOPERATIVE_SHARE = 'cooperative_share'
HARD_MEMORY_LIMIT = 'hard_memory_limit'
MULTI_DEVICE = 'multi_device'
ELASTIC_CAPACITY = 'elastic_capacity'
ALLOCATION_GUARANTEES = (EXCLUSIVE_DEVICE, ISOLATED_MIG, COOPERATIVE_SHARE, HARD_MEMORY_LIMIT,
                         MULTI_DEVICE, ELASTIC_CAPACITY)

ENFORCEMENT_NONE = 'none'
ENFORCEMENT_ACCOUNTING = 'accounting'
ENFORCEMENT_RUNTIME_CAP = 'runtime_cap'
ENFORCEMENT_HARDWARE = 'hardware'
ENFORCEMENTS = (ENFORCEMENT_NONE, ENFORCEMENT_ACCOUNTING, ENFORCEMENT_RUNTIME_CAP, ENFORCEMENT_HARDWARE)

# -- capability records ----------------------------------------------------------------

@dataclass(frozen = True)
class MessagingCapabilities:
    '''
    What a messaging adapter, *as configured*, can guarantee. Observations that
    depend on the deployed broker (replication, storage, payload limit) are
    ``Observation`` values: an adapter that could not read them says so, and the
    planner treats that as a reason to reject a profile that depends on them.
    '''
    adapter : str
    version : str
    retained_backlog : bool
    recoverable_delivery : bool
    latest_per_key : bool
    dedup_window_seconds : int | None
    publication_ledger : str
    replication_factor : Observation[int]
    persistent_storage : Observation[bool]
    max_payload_bytes : Observation[int]
    credit_resizable : bool
    control_shares_data_slot : bool
    durable_control : bool = False
    archive : bool = False
    mixed_retention_per_channel : bool = False

@dataclass(frozen = True)
class PayloadCapabilities:
    adapter : str
    durable : Observation[bool]
    evictable : Observation[bool]
    atomic_multikey : Observation[bool]
    max_object_bytes : int | None
    reader_identities : bool

@dataclass(frozen = True)
class RuntimeCapabilities:
    store : str
    durable : Observation[bool]
    shared_across_processes : bool
    restart_safe_joins : bool
    elastic_state : bool

@dataclass(frozen = True)
class AllocationCapabilities:
    adapter : str
    authority : str
    exclusive_device : bool
    isolated_mig : bool
    cooperative_sharing : bool
    memory_enforcement : str
    multi_device : bool
    topology_verification : bool
    elastic : bool
    admission_boundary : bool
    version_matrix : Mapping[str, str] = field(default_factory = dict)

@dataclass(frozen = True)
class ExecutionCapabilities:
    engine : str
    restart_supervision : bool
    readiness_states : tuple[str, ...]
    pvc_mounts : bool
    autoscaling_controllers : tuple[str, ...] = ()

# -- what a flow requires ---------------------------------------------------------------

@dataclass(frozen = True)
class ProfileRequest:
    '''
    - Arguments:
        - channel: the producing node whose output channel the profile applies to.
        - profile: one of ``MESSAGING_PROFILES``.
        - options: profile-specific knobs — ``latest_per_key`` (bool), \
            ``horizon_seconds`` (int, the recovery/replay horizon), ``key`` (str).
    '''
    channel : str
    profile : str
    options : Mapping[str, Any] = field(default_factory = dict)

    def to_dict(self) -> dict[str, Any]:
        return {'channel': self.channel, 'profile': self.profile, 'options': dict(self.options)}

    @staticmethod
    def from_dict(d : Mapping[str, Any]) -> 'ProfileRequest':
        return ProfileRequest(str(d['channel']), str(d['profile']), dict(d.get('options') or {}))

@dataclass(frozen = True)
class FlowRequirements:
    '''
    Everything a flow asks of its backends beyond the graph itself. Serialised
    beside the specs (``compile_to_dict()['requirements']``) only when non-empty,
    so flows that never touch it produce byte-identical documents.
    '''
    profiles : tuple[ProfileRequest, ...] = ()
    restart_safe : bool = False
    exactly_once_effects : tuple[str, ...] = ()
    resources : Mapping[str, Mapping[str, str]] = field(default_factory = dict)
    priority_class : str | None = None
    rollout_policy : str | None = None
    #: Broker pod losses accepted work must survive (the deployment's declared
    #: fault model, MSG-021): ``f`` needs persistent storage and ``2f + 1`` stream
    #: copies, read back — never inferred from durable names or pod counts.
    tolerated_failures : int = 0

    def is_empty(self) -> bool:
        return (not self.profiles and not self.restart_safe and not self.exactly_once_effects
                and not self.resources and self.priority_class is None and self.rollout_policy is None
                and not self.tolerated_failures)

    def to_dict(self) -> dict[str, Any]:
        d = {
            'profiles': [p.to_dict() for p in self.profiles],
            'restart_safe': self.restart_safe,
            'exactly_once_effects': list(self.exactly_once_effects),
            'resources': {node: dict(res) for node, res in self.resources.items()},
            'priority_class': self.priority_class,
            'rollout_policy': self.rollout_policy,
        }
        if self.tolerated_failures:
            # Only when set: a document without it is exactly what it always was.
            d['tolerated_failures'] = self.tolerated_failures
        return d

    @staticmethod
    def from_dict(d : Mapping[str, Any] | None) -> 'FlowRequirements':
        if not d:
            return FlowRequirements()
        return FlowRequirements(
            profiles = tuple(ProfileRequest.from_dict(p) for p in d.get('profiles') or ()),
            restart_safe = bool(d.get('restart_safe', False)),
            exactly_once_effects = tuple(d.get('exactly_once_effects') or ()),
            resources = {str(k): dict(v) for k, v in (d.get('resources') or {}).items()},
            priority_class = d.get('priority_class'),
            rollout_policy = d.get('rollout_policy'),
            tolerated_failures = int(d.get('tolerated_failures') or 0),
        )

def profile_for_edge(flow_type : str, delivery : Mapping[str, Any] | None) -> str:
    '''
    The profile an edge requests under today's presets: BATCH is reliable work,
    REALTIME is live-latest, and a node's ``delivery`` override flips its own
    inputs (an at-least-once sink inside a REALTIME flow asks for reliable work).
    '''
    policy = DeliveryPolicy.resolve(flow_type, dict(delivery) if delivery else None)
    if policy.delivery == AT_LEAST_ONCE:
        return RELIABLE_WORK
    if policy.delivery == BEST_EFFORT:
        return LIVE_LATEST
    return RELIABLE_WORK if flow_type == BATCH else LIVE_LATEST

def default_requirements(flow_type : str, specs : Sequence[NodeSpec]) -> FlowRequirements:
    '''
    The requirements a flow makes without saying anything: one profile request
    per edge, derived from the flow type and each consuming node's delivery
    override. Two consumers of one channel may therefore ask for different
    profiles; whether one channel can serve both is the planner's decision.
    '''
    requests : list[ProfileRequest] = []
    seen : set[tuple[str, str]] = set()
    for spec in specs:
        profile = profile_for_edge(flow_type, spec.delivery)
        for parent in spec.parents:
            if (parent, profile) in seen:
                continue
            seen.add((parent, profile))
            requests.append(ProfileRequest(parent, profile, {'consumer': spec.name}))
    return FlowRequirements(profiles = tuple(requests))

# -- the plan ---------------------------------------------------------------------------------

@dataclass(frozen = True)
class CompositionPlan:
    '''
    The admitted composition: which profile each channel got, the retention class
    it compiles to, and the notes a human reads to see why. An instance exists
    only if nothing was rejected — a rejection is an exception, not a field.
    '''
    channel_profiles : Mapping[str, str]
    channel_retention : Mapping[str, str]
    restart_safe : bool
    notes : tuple[str, ...] = ()

    def render(self) -> str:
        lines = [f'{channel}: {profile} -> {self.channel_retention.get(channel, "?")}'
                 for channel, profile in sorted(self.channel_profiles.items())]
        return '\n'.join(lines + list(self.notes))

def _retention_for(profile : str) -> str:
    return RETENTION_INTEREST if profile in (RELIABLE_WORK, REPLAY_ARCHIVE) else RETENTION_LIMITS

def plan_composition(requirements : FlowRequirements, messaging : MessagingCapabilities,
                     payload : PayloadCapabilities | None = None,
                     runtime : RuntimeCapabilities | None = None,
                     allocation : AllocationCapabilities | None = None,
                     execution : ExecutionCapabilities | None = None,
                     payload_refs_in_use : bool = False) -> CompositionPlan:
    '''
    Admit the composition or reject it — never downgrade it.

    - Arguments:
        - requirements: what the flow asks for (``default_requirements`` when it \
            says nothing).
        - messaging / payload / runtime / allocation / execution: what the composed \
            backends advertise. ``None`` means that backend is not part of the \
            composition; a profile needing it is then rejected.
        - payload_refs_in_use: whether any channel offloads payloads to the store, \
            which makes the store's durability part of the messaging guarantee.

    - Raises:
        - IncompatibleProfile: a requested guarantee is definitely not provided; the \
            diagnostics list every one.
        - UnobservableState: a requested guarantee depends on a capability the \
            adapter could not observe (replication, persistence, store durability). \
            Unknown is not a pass.
    '''
    definite : list[Diagnostic] = []
    unobservable : list[Diagnostic] = []
    channel_profiles : dict[str, str] = {}
    notes : list[str] = []

    def reject(channel : str, profile : str, missing : str, remedy : str) -> None:
        definite.append(Diagnostic(SEVERITY_ERROR, 'VF_INCOMPATIBLE_PROFILE', channel,
                                   f'channel {channel!r} requests {profile}: {missing}', remedy))

    def cannot_observe(channel : str, profile : str, what : str, observation : Unknown) -> None:
        # The remedy names the backend whose state went unread, which is not
        # always the transport's.
        if 'payload' in what:
            adapter = payload.adapter if payload is not None else 'payload store'
        elif 'runtime' in what:
            adapter = runtime.store if runtime is not None else 'runtime store'
        else:
            adapter = messaging.adapter
        unobservable.append(Diagnostic(SEVERITY_ERROR, 'VF_STATE_UNKNOWN', channel,
                                       f'channel {channel!r} requests {profile}: {what} could not be '
                                       f'observed ({observation.reason}: {observation.detail})',
                                       f'Restore access to the {adapter} configuration and '
                                       f're-run; an unobservable guarantee is not an available one.'))

    for request in requirements.profiles:
        channel, profile = request.channel, request.profile
        if profile not in MESSAGING_PROFILES:
            reject(channel, profile, 'unknown profile', f'Use one of {", ".join(MESSAGING_PROFILES)}.')
            continue
        previous = channel_profiles.get(channel)
        if previous is not None and _retention_for(previous) != _retention_for(profile):
            if not messaging.mixed_retention_per_channel:
                reject(channel, profile,
                       f'its consumers also request {previous}, and {messaging.adapter} keeps one '
                       f'retention class per channel',
                       'Give the channel one delivery class: move the reliable consumer behind its '
                       'own producer, or drop the per-node delivery override.')
                continue
            notes.append(f'{channel}: mixed retention ({previous} + {profile}) served by separate channels')
        if profile == RELIABLE_WORK:
            if not (messaging.retained_backlog and messaging.recoverable_delivery):
                reject(channel, profile, f'{messaging.adapter} offers no retained, recoverable delivery',
                       'Use a JetStream-backed broker (INTEREST retention with redelivery) for this channel.')
                continue
            if payload_refs_in_use:
                if payload is None:
                    reject(channel, profile, 'payload references are in use but no payload store is composed',
                           'Configure a payload store (VF_BLOB_REDIS_URL / --blob-redis-url).')
                    continue
                if isinstance(payload.durable, Unknown):
                    cannot_observe(channel, profile, 'payload-store durability', payload.durable)
                    continue
                if not payload.durable.value:
                    reject(channel, profile, f'the {payload.adapter} payload store is evictable, so an '
                           f'accepted envelope could outlive its bytes',
                           'Use a durable payload store (Redis with appendonly and noeviction) or lower '
                           'the profile to live_latest.')
                    continue
        elif profile == LIVE_LATEST:
            if request.options.get('latest_per_key') and not messaging.latest_per_key:
                reject(channel, profile, f'{messaging.adapter} has no per-key latest-value queue',
                       'Request a global latest slot, or use an adapter that implements per-key retention.')
                continue
        elif profile == DURABLE_CONTROL:
            if not messaging.durable_control:
                reject(channel, profile, f'{messaging.adapter} keeps control state in transient notifications only',
                       'Enable the durable control store (RFC 0006 run-state bucket).')
                continue
            if runtime is None or isinstance(runtime.durable, Unknown) or not runtime.durable.value:
                if runtime is not None and isinstance(runtime.durable, Unknown):
                    cannot_observe(channel, profile, 'runtime store durability', runtime.durable)
                else:
                    reject(channel, profile, 'no durable runtime store is composed',
                           'Set VF_RUNTIME_STORE_URL to a durable store (redis:// with persistence).')
                continue
        elif profile == REPLAY_ARCHIVE:
            if not messaging.archive:
                reject(channel, profile, f'{messaging.adapter} has no archive independent of working consumers',
                       'Compose an archive adapter; an ACK-drained work queue is not an archive.')
                continue
        channel_profiles[channel] = profile if previous is None else previous

    if requirements.restart_safe:
        if runtime is None:
            reject('*', 'restart_safe', 'no runtime store is composed',
                   'Set VF_RUNTIME_STORE_URL (file:// for one host, redis:// across hosts).')
        elif isinstance(runtime.durable, Unknown):
            cannot_observe('*', 'restart_safe', 'runtime store durability', runtime.durable)
        elif not (runtime.durable.value and runtime.shared_across_processes):
            reject('*', 'restart_safe', f'the {runtime.store} runtime store is not durable across processes',
                   'Use file:// on a single host or redis:// with persistence enabled.')

    if requirements.tolerated_failures > 0:
        f = requirements.tolerated_failures
        needed = 2 * f + 1
        what = f'tolerated_failures={f}'
        if isinstance(messaging.persistent_storage, Unknown):
            cannot_observe('*', what, 'broker storage persistence', messaging.persistent_storage)
        elif not messaging.persistent_storage.value:
            reject('*', what, f'{messaging.adapter} keeps streams on ephemeral storage, so a pod loss loses '
                   'accepted work (three brokers on emptyDir are three ways to lose it)',
                   'Deploy the broker with persistent volumes (--broker-profile durable) before asking it to '
                   'survive a pod loss.')
        if isinstance(messaging.replication_factor, Unknown):
            cannot_observe('*', what, 'stream replication', messaging.replication_factor)
        elif messaging.replication_factor.value < needed:
            reject('*', what, f'streams keep {messaging.replication_factor.value} copy(ies); surviving {f} '
                   f'loss(es) needs {needed} — broker pods are not stream replicas',
                   f'Provision streams with num_replicas >= {needed} (--broker-replicas {needed} sets '
                   'jetstream_replicas accordingly).')
        if payload_refs_in_use:
            if payload is None:
                reject('*', what, 'payload references are in use but no payload store is composed',
                       'Configure a durable payload store (VF_BLOB_REDIS_URL / --blob-redis-url).')
            elif isinstance(payload.durable, Unknown):
                cannot_observe('*', what, 'payload-store durability', payload.durable)
            elif not payload.durable.value:
                reject('*', what, f'the {payload.adapter} payload store is not durable: broker survival does '
                       'not substitute for independent payload durability',
                       'Use a durable payload store (Redis with appendonly and noeviction on a volume).')

    for sink in requirements.exactly_once_effects:
        reject(sink, 'exactly_once_effects', 'no backend can make an external effect exactly-once; only a '
               'sink using its external system\'s idempotency key or transaction can',
               f'Declare {sink!r} idempotent with an external key, or accept at-least-once effects.')

    if definite:
        raise IncompatibleProfile(
            'The requested profiles cannot be satisfied by the composed backends:\n'
            + '\n'.join(f'  - {d.message}' for d in definite),
            remedy = definite[0].remedy, diagnostics = definite + unobservable,
            channels = sorted({d.node for d in definite if d.node}))
    if unobservable:
        raise UnobservableState(
            'A requested guarantee depends on state that could not be observed:\n'
            + '\n'.join(f'  - {d.message}' for d in unobservable),
            remedy = unobservable[0].remedy)

    retention = {channel: _retention_for(profile) for channel, profile in channel_profiles.items()}
    return CompositionPlan(channel_profiles, retention, requirements.restart_safe, tuple(notes))

def realtime_default_capabilities_note(flow_type : str) -> str:
    '''One line for `videoflow explain`: what the flow type's preset implies.'''
    return (f'{flow_type}: ' + (f'{RELIABLE_WORK} on every channel' if flow_type == BATCH
                                else f'{LIVE_LATEST} on every channel')) if flow_type in (BATCH, REALTIME) else flow_type


#: The environment variable carrying an operator's explicit channel-profile
#: requests to a worker (``deploy --require-profile``); absent when none were made,
#: so the default worker environment is unchanged (D8).
PROFILE_REQUESTS_ENV = 'VF_PROFILE_REQUESTS_JSON'

def requests_env(explicit : Sequence[ProfileRequest]) -> dict[str, str]:
    '''The worker environment entry for explicit requests — empty when there are none.'''
    if not explicit:
        return {}
    return {PROFILE_REQUESTS_ENV: json.dumps([r.to_dict() for r in explicit], sort_keys = True)}

def requests_from_env(value : str | None) -> list[ProfileRequest]:
    '''The inverse of ``requests_env`` on the worker side; ``[]`` when unset.'''
    if not value:
        return []
    return [ProfileRequest.from_dict(d) for d in json.loads(value)]

#: How long the in-container admission checks (the provision entrypoint before it
#: creates anything, a worker before it opens its node) give the broker and the
#: payload store to answer a read-back, connect included. Unset ⇒ the default;
#: the answer past the deadline is ``Unknown('timeout')``, never a guess.
ADMISSION_TIMEOUT_ENV = 'VF_ADMISSION_TIMEOUT_SECONDS'
DEFAULT_ADMISSION_TIMEOUT_SECONDS = 60.0

def admission_timeout_from_env(value : str | None) -> float:
    '''
    ``VF_ADMISSION_TIMEOUT_SECONDS`` as seconds: the default when unset.

    - Raises:
        - ConfigError: a value that is not a positive number.
    '''
    if value in (None, ''):
        return DEFAULT_ADMISSION_TIMEOUT_SECONDS
    try:
        seconds = float(value)   # type: ignore[arg-type]  # narrowed by the check above
    except ValueError as e:
        raise ConfigError(f'{ADMISSION_TIMEOUT_ENV}={value!r} is not a number.',
                          remedy = 'Set it to the seconds a read-back of the broker may take, or unset it.') from e
    if seconds <= 0:
        raise ConfigError(f'{ADMISSION_TIMEOUT_ENV}={value!r} is not positive.',
                          remedy = 'Set it to the seconds a read-back of the broker may take, or unset it.')
    return seconds
