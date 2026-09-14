'''
Composition admission: does the broker and payload store a flow is about to run
on actually provide the delivery guarantees its channels ask for?

Every channel of a flow carries a messaging *profile* (``live_latest`` for a
REALTIME flow, ``reliable_work`` for a BATCH one, unless the operator asks for
another with ``--require-profile``), and every backend advertises what it can
honour (``videoflow.backends.capabilities``). The planner either admits the
composition or rejects it by name — it never downgrades a request to whatever
the backend happens to offer, which is how a batch job used to end up on an
evictable store with nobody told.

This module is the deploy-time half of that: it turns the broker and store a
flow will run on into capability records, parses the operator's explicit
requests, and runs the planner. An auto-provisioned broker or store is judged by
its *declared* profile (``jetstream_capabilities`` / ``redis_payload_capabilities``
— the render is what the profile says it is); a bring-your-own ``--nats`` /
``--blob-redis-url`` is *read back live* before anything is created
(``jetstream_capabilities_observed`` / ``redis_payload_capabilities_observed``:
the connection's payload limit, the JetStream account's storage allowance, the
run's streams when they already exist; Redis' persistence and eviction
settings), and whatever the probe could not read stays ``Unknown`` with the
reason — never assumed. A definite incompatibility is binding (RFC 0006,
accepted); an unobservable capability of a bring-your-own broker or store is a
warning unless the operator named a profile explicitly. Explicit requests also
travel to the provision Job and the workers as ``VF_PROFILE_REQUESTS_JSON``,
emitted only when there are any (D8); there they bind again, against the
streams as provisioned (``runtime.provision``) and as bound (``runtime.worker``,
before ``open()``), through ``messaging.topology.verify_channel_profiles``.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import dataclasses
import logging
import os
import re
import sys
from typing import Callable, List, Optional, Sequence

from ..backends.capabilities import (
    LEDGER_NONE,
    LEDGER_WINDOW,
    MESSAGING_PROFILES,
    PROFILE_REQUESTS_ENV,
    CompositionPlan,
    ExecutionCapabilities,
    FlowRequirements,
    MessagingCapabilities,
    PayloadCapabilities,
    ProfileRequest,
    RuntimeCapabilities,
    combined_limit,
    default_requirements,
    graph_limits_from_env,
    plan_composition,
    requests_env,
    requests_from_env,
)
from ..backends.outcomes import Observation, Unknown, known, unknown
from ..core.compiler import NodeSpec
from ..core.errors import ConfigError, IncompatibleProfile, UnobservableState, VideoflowError
from .broker_profiles import BrokerProfile, RedisProfile

logger = logging.getLogger(__package__)

#: JetStream's default duplicate-detection window (2 minutes), which
#: ``topology.stream_config_for`` leaves at the server default.
JETSTREAM_DEDUP_WINDOW_SECONDS = 120

#: Redis' default ``proto-max-bulk-len``: the largest value one SET may carry.
REDIS_MAX_OBJECT_BYTES = 512 << 20


def jetstream_capabilities(profile : Optional[BrokerProfile],
                           unread : str = 'the broker configuration was not read back (bring-your-own --nats)') \
        -> MessagingCapabilities:
    '''
    What a JetStream broker offers, from its *declared* profile: retained,
    recoverable delivery always (INTEREST retention with redelivery is what the
    topology provisions); persistence and replication as the profile says.
    With no profile (a bring-your-own ``--nats``, or a Service the namespace
    already had with no profile record — ``unread`` says which) those two are
    ``Unknown`` — the broker was not read back, and unknown is not "yes"; the
    CLI reads a bring-your-own broker back with ``jetstream_capabilities_observed``
    instead, and the provision Job reads a reused one back in-cluster.
    '''
    persistence : Observation[bool]
    replication : Observation[int]
    if profile is None:
        persistence = unknown('unread', unread)
        replication = unknown('unread', unread)
    else:
        persistence = known(bool(profile.persistence))
        replication = known(int(profile.jetstream_replicas))
    declared_streams, declared_consumers = graph_limits_from_env(os.environ)
    return MessagingCapabilities(
        adapter = 'jetstream', version = '2.10', retained_backlog = True, recoverable_delivery = True,
        latest_per_key = False, dedup_window_seconds = JETSTREAM_DEDUP_WINDOW_SECONDS,
        publication_ledger = 'window', replication_factor = replication, persistent_storage = persistence,
        max_payload_bytes = unknown('unread', 'nc.max_payload is read when the client connects'),
        credit_resizable = True, control_shares_data_slot = True,
        max_streams = combined_limit(None, declared_streams), max_consumers = combined_limit(None, declared_consumers))


def redis_payload_capabilities(profile : Optional[RedisProfile],
                               unread : str = 'the store configuration was not read back (bring-your-own --blob-redis-url)') \
        -> PayloadCapabilities:
    '''
    What a Redis payload store offers, from its declared profile: durable only
    with append-only persistence *and* ``noeviction`` (either alone lets an
    accepted envelope outlive its bytes); ``persistent_storage`` only on a
    claim (the dev profile's emptyDir is durable across a container restart,
    gone with the pod). No profile (bring-your-own
    ``--blob-redis-url``, or a reused Service with no profile record — ``unread``
    says which): ``Unknown`` until read back, which the CLI does with
    ``redis_payload_capabilities_observed`` for a bring-your-own store and the
    provision Job does in-cluster for a reused one.
    '''
    durable : Observation[bool]
    evictable : Observation[bool]
    atomic : Observation[bool]
    storage : Observation[bool]
    if profile is None:
        durable = unknown('unread', unread)
        evictable = unknown('unread', unread)
        atomic = unknown('unread', 'CLUSTER KEYSLOT of the obligation keys was not checked')
        storage = unknown('unread', unread)
    else:
        durable = known(profile.persistent and profile.eviction == 'noeviction')
        evictable = known(profile.eviction != 'noeviction')
        atomic = known(True)                     # one node: every key shares its slot
        storage = known(profile.stateful)        # a claim outlives the pod; an emptyDir does not
    return PayloadCapabilities('redis', durable = durable, evictable = evictable, atomic_multikey = atomic,
                               max_object_bytes = REDIS_MAX_OBJECT_BYTES, reader_identities = False,
                               persistent_storage = storage)


#: How long a deploy-time probe of a bring-your-own broker or store waits, connect
#: included, before reporting what it could not observe. Short on purpose: it runs
#: on the operator's machine before anything is created, and an unreachable service
#: is reported as ``Unknown('unreachable')`` for the planner to rule on, not waited for.
PROBE_TIMEOUT_SECONDS = 5.0

_SERVER_VERSION_RE = re.compile(r'v?(\d+\.\d+\.\d+[0-9A-Za-z.+-]*)')


def _unobserved_jetstream(reason : str, detail : str) -> MessagingCapabilities:
    '''The JetStream adapter's static guarantees with every broker-dependent observation ``Unknown``.'''
    unobserved = unknown(reason, detail)
    return dataclasses.replace(jetstream_capabilities(None), replication_factor = unobserved,
                               persistent_storage = unobserved, max_payload_bytes = unobserved)


def _core_nats_only(version : str, max_payload : Observation[int]) -> MessagingCapabilities:
    '''
    A NATS server that answered but has no JetStream: nothing is retained, nothing
    is redelivered, no copies are kept — definite observations, not unknowns, so
    a ``reliable_work`` channel is rejected by name rather than left to fail at
    the first publish.
    '''
    return MessagingCapabilities(
        adapter = 'nats (JetStream not enabled)', version = version, retained_backlog = False,
        recoverable_delivery = False, latest_per_key = False, dedup_window_seconds = None,
        publication_ledger = LEDGER_NONE, replication_factor = known(0), persistent_storage = known(False),
        max_payload_bytes = max_payload, credit_resizable = False, control_shares_data_slot = True)


def _server_version(rendered : str) -> str:
    '''``'2.10.29'`` out of nats-py's ``ServerVersion`` rendering (``'<nats server v2.10.29>'``), else the rendering itself.'''
    match = _SERVER_VERSION_RE.search(rendered)
    return match.group(1) if match else rendered


def jetstream_capabilities_observed(nats_url : str, timeout : float = PROBE_TIMEOUT_SECONDS,
                                    stream_names : Sequence[str] = (),
                                    fail_fast : bool = True) -> MessagingCapabilities:
    '''
    What a bring-your-own JetStream broker offers, *read back from the live
    server* rather than declared:

    - ``max_payload_bytes``: the connection's ``max_payload`` (nats-py 2.15.0, \
        ``nats/aio/client.py:1285``, the server's INFO ``max_payload``).
    - ``persistent_storage``: from ``js.account_info()`` (``nats/js/manager.py:69`` \
        -> ``api.AccountInfo``, ``nats/js/api.py:761``): ``limits.max_storage`` \
        (``AccountLimits``, ``api.py:720``) is the account's file-store allowance, \
        ``-1`` unlimited and ``0`` none — a stream provisioned with ``storage`` unset \
        is a file stream by server default, so the allowance says whether this \
        run's streams will be file-backed before any exists. Once a stream of the \
        run exists, its applied ``config.storage`` (``stream_info``, \
        ``manager.py:85`` -> ``api.StreamInfo.config``) is the answer instead. \
        File storage is what the JetStream API can see; whether the directory \
        behind it survives a pod loss is the deployment's business (an \
        auto-provisioned broker answers that through its declared profile).
    - ``replication_factor``: the smallest ``config.num_replicas`` over the run's \
        existing streams; ``Unknown('unread')`` while none exists, because the \
        copies a stream keeps are decided when it is created (``VF_STREAM_REPLICAS``).

    A server that answers without JetStream is reported as offering no retained,
    recoverable delivery (``_core_nats_only``). Anything that could not be read
    is ``Unknown`` with the reason — ``timeout``, ``auth`` (credentials refused,
    or a permissions violation on the API subjects, which the server reports
    through the error callback while the request itself times out),
    ``unreachable``, ``malformed`` — and never a guess.

    - Arguments:
        - timeout: overall bound, connect included.
        - stream_names: the run's stream names (``run_stream_names``), read when \
            they exist.
        - fail_fast: no reconnects — the operator's machine. False lets the client \
            retry a broker that is still starting (the in-cluster provision Job).
    '''
    # Optional dependency (distributed extra): the CLI imports this module at
    # module scope and must load without a broker client installed.
    import nats
    import nats.js.errors
    from nats.js.api import StorageType

    # Deferred for the same reason: topology imports ``nats`` at module scope.
    from ..messaging.topology import connect_options_for, observation_failure

    reported : list[str] = []

    async def _record(e : BaseException) -> None:
        reported.append(f'{type(e).__name__}: {e}')

    async def _probe() -> MessagingCapabilities:
        nc = await asyncio.wait_for(nats.connect(nats_url, **connect_options_for(timeout, fail_fast, _record)),
                                    timeout)
        try:
            js = nc.jetstream(timeout = timeout)
            max_payload : Observation[int] = known(int(nc.max_payload))
            version = _server_version(str(nc.connected_server_version))
            replication : Observation[int]
            persistence : Observation[bool]
            account_streams : Observation[int] | None = None
            account_consumers : Observation[int] | None = None
            try:
                account = await js.account_info()
            except nats.js.errors.ServiceUnavailableError as e:
                # A 503 with no description is the no-responders case (nothing
                # serves ``$JS.API``); ``JetStream not enabled for account`` is the
                # per-account one. Any other 503 (a cluster without a meta leader)
                # is transient: the server is there, its answer is not.
                if e.description is None or 'not enabled' in e.description:
                    return _core_nats_only(version, max_payload)
                replication = persistence = unknown('unreachable', f'JetStream API unavailable: {e}')
            except Exception as e:  # noqa: BLE001 — every failure is reported as the observation it prevented
                reason, detail = observation_failure(e, reported)
                replication = persistence = unknown(reason, f'account info: {detail}')
            else:
                persistence = known(account.limits.max_storage != 0)
                replication = unknown('unread', 'no stream of this run exists yet; the copies each stream keeps '
                                                'are read back once it is provisioned')
                # The account's stream and consumer allowances (``AccountLimits``,
                # nats-py 2.15.0 ``api.py:720``; -1 = unlimited): the graph-size
                # limit the adapter reports, MSG-026.
                account_streams = known(int(account.limits.max_streams))
                account_consumers = known(int(account.limits.max_consumers))
            file_backed : list[bool] = []
            copies : list[int] = []
            failed : Unknown | None = None
            for name in stream_names:
                try:
                    info = await js.stream_info(name)
                except nats.js.errors.NotFoundError:
                    continue
                except Exception as e:  # noqa: BLE001
                    reason, detail = observation_failure(e, reported)
                    failed = unknown(reason, f'stream {name}: {detail}')
                    break
                file_backed.append(info.config.storage in (None, StorageType.FILE))
                copies.append(int(info.config.num_replicas or 1))
            if failed is not None:
                replication = persistence = failed
            elif file_backed:
                persistence = known(all(file_backed))
                replication = known(min(copies))
            declared_streams, declared_consumers = graph_limits_from_env(os.environ)
            return MessagingCapabilities(
                adapter = 'jetstream', version = version, retained_backlog = True, recoverable_delivery = True,
                latest_per_key = False, dedup_window_seconds = JETSTREAM_DEDUP_WINDOW_SECONDS,
                publication_ledger = LEDGER_WINDOW, replication_factor = replication,
                persistent_storage = persistence, max_payload_bytes = max_payload,
                credit_resizable = True, control_shares_data_slot = True,
                max_streams = combined_limit(account_streams, declared_streams),
                max_consumers = combined_limit(account_consumers, declared_consumers))
        finally:
            await nc.close()

    try:
        return asyncio.run(_probe())
    except Exception as e:  # noqa: BLE001 — the connect failed: nothing was observed, and the reason says why
        reason, detail = observation_failure(e, reported)
        logger.info(f'broker {nats_url} not read back ({reason}): {detail}')
        return _unobserved_jetstream(reason, f'{nats_url}: {detail}')


def redis_payload_capabilities_observed(url : str, timeout : float = PROBE_TIMEOUT_SECONDS) -> PayloadCapabilities:
    '''
    What a bring-your-own Redis payload store offers, read back live
    (``wire.redis_payload_store.redis_capabilities_observed``: ``CONFIG GET
    appendonly / save / maxmemory-policy``, ``INFO cluster``, ``CLUSTER KEYSLOT``
    of the obligation keys). Refused credentials are ``Unknown('auth')``; the
    delegate reports every other failure with its reason.

    - Arguments:
        - url: ``redis://`` / ``rediss://`` (``redis.Redis.from_url``, redis-py 8.0.1).
        - timeout: socket connect and read timeout per command.
    '''
    import redis  # optional dependency (extra): only the Redis stores need it
    import redis.exceptions

    # Deferred for the same reason: the module pulls serialization (msgpack/protobuf).
    from ..wire.redis_payload_store import redis_capabilities_observed

    client = redis.Redis.from_url(url, socket_timeout = timeout, socket_connect_timeout = timeout)
    try:
        try:
            client.ping()
        except redis.exceptions.AuthenticationError as e:
            refused = unknown('auth', f'{url}: credentials refused: {e}')
            return PayloadCapabilities('redis', durable = refused, evictable = refused, atomic_multikey = refused,
                                       max_object_bytes = REDIS_MAX_OBJECT_BYTES, reader_identities = True)
        except redis.exceptions.RedisError:
            pass    # the delegate observes and reports the failure per command
        return redis_capabilities_observed(client)
    finally:
        client.close()


def run_stream_names(flow_id : str, run_id : str, specs : Sequence[NodeSpec]) -> list[str]:
    '''The stream names a run's nodes publish on, for a probe to read back when they already exist.'''
    # Deferred: topology imports the optional ``nats`` client at module scope.
    from ..messaging.topology import stream_name_for
    return [stream_name_for(flow_id, run_id, spec.name) for spec in specs]


def graph_size(specs : Sequence[NodeSpec], flow_id : str, run_id : str) -> tuple[int, int]:
    '''
    ``(streams, consumers)`` one run of the flow provisions and binds: every node's
    stream plus the flow's dead-letter stream; every data durable (one per
    competing child, one per replica of a partitioned child), every EOS anchor,
    and the per-process EOS durables each replica binds at start
    (``identity.derived_names`` for what provisioning creates; the EOS durables
    are ``nb_tasks`` per parent edge, minted by the workers).
    '''
    # Deferred: identity pulls topology (the optional nats extra) at call time.
    from ..backends.identity import derived_names
    names = derived_names(specs, flow_id, run_id)
    kinds = [identity.kind for identities in names.values() for identity in identities]
    streams = kinds.count('stream') + kinds.count('dlq_stream')
    consumers = kinds.count('durable') + kinds.count('partitioned_durable') + kinds.count('eos_anchor')
    by_name = {spec.name: spec for spec in specs}
    consumers += sum(spec.nb_tasks for spec in specs for parent in spec.parents if parent in by_name)
    return streams, consumers


def verify_graph_size(specs : Sequence[NodeSpec], flow_id : str, run_id : str,
                      messaging : MessagingCapabilities) -> None:
    '''
    Refuse a graph larger than the adapter supports before anything is provisioned
    (MSG-026): a run that creates its first hundred streams and then fails on the
    account limit is a partially provisioned, apparently healthy run. ``-1`` (an
    unlimited account) and an undeclared limit admit everything; an ``Unknown``
    limit is left to the read-back at provisioning.

    - Raises:
        - IncompatibleProfile: the run derives more streams or consumers than the \
            adapter's declared or read-back limit.
    '''
    streams, consumers = graph_size(specs, flow_id, run_id)
    findings = []
    for what, count, limit in (('streams', streams, messaging.max_streams),
                               ('consumers', consumers, messaging.max_consumers)):
        if limit is None or isinstance(limit, Unknown) or limit.value < 0:
            continue
        if count > limit.value:
            findings.append(f'one run of this flow provisions {count} {what}, and the {messaging.adapter} adapter '
                            f'supports {limit.value} (the account limit, or the declared VF_MAX_{what.upper()})')
    if findings:
        raise IncompatibleProfile(
            'The graph exceeds the supported size of the composed broker:\n' + '\n'.join(f'  - {f}' for f in findings),
            remedy = 'Split the flow into smaller flows, reduce replicas or fan-out, raise the account limit, or '
                     'declare a larger measured limit (VF_MAX_STREAMS / VF_MAX_CONSUMERS) once a benchmark backs it.',
            channels = [])


def verify_topology_shape(flow_type : str, flow_id : str, run_id : str,
                          explicit : Sequence[ProfileRequest]) -> None:
    '''
    Reject an explicit request the flow's own topology cannot carry, before
    anything is built, provisioned or applied. Streams are shaped by the flow
    type (``topology.stream_config_for``: REALTIME ⇒ limits / discard-old, BATCH
    ⇒ interest / discard-new), so ``reliable_work`` on a REALTIME channel or
    ``live_latest`` on a BATCH one contradicts the stream that will exist
    whatever the broker can do — the same finding the provision Job and the
    workers report from the read-back, an hour earlier and with nothing to tear
    down.

    - Raises:
        - IncompatibleProfile: naming every such channel.
    '''
    if not explicit:
        return
    # Deferred: topology imports the optional ``nats`` client at module scope.
    from ..messaging.topology import profile_mismatches, stream_config_for
    findings = []
    for request in explicit:
        shape = stream_config_for(flow_id, run_id, request.channel, flow_type)
        mismatches = profile_mismatches(request.profile, shape)
        if mismatches:
            findings.append(f'channel {request.channel!r} requests {request.profile}, but a {flow_type.upper()} '
                            f'flow provisions its streams as: ' + '; '.join(mismatches))
    if findings:
        raise IncompatibleProfile(
            'The requested profiles contradict the streams this flow type provisions:\n'
            + '\n'.join(f'  - {f}' for f in findings),
            remedy = 'Streams are shaped by the flow type: run the flow as BATCH for reliable_work channels '
                     'and REALTIME for live_latest ones, or drop the --require-profile entry.',
            channels = [r.channel for r in explicit])


def local_dev_capabilities() -> tuple[MessagingCapabilities, PayloadCapabilities]:
    '''
    What ``videoflow run-local``'s docker dev containers offer
    (``deploy.localinfra``): a JetStream server without a volume and the Redis
    ``RedisProfile.dev()`` describes — an append-only file, ``noeviction`` — which
    ``localinfra`` starts with exactly those arguments.
    '''
    return (jetstream_capabilities(BrokerProfile.dev()), redis_payload_capabilities(RedisProfile.dev()))


def parse_profile_requests(values : Optional[Sequence[str]], specs : Sequence[NodeSpec]) -> List[ProfileRequest]:
    '''
    ``--require-profile CHANNEL=PROFILE`` entries as requests. A channel is the
    name of the node whose output it carries; the profile is one of
    ``MESSAGING_PROFILES``.

    - Raises:
        - ConfigError: a malformed entry, a channel no node publishes, an \
            unknown profile, or the same channel named twice.
    '''
    channels = sorted(s.name for s in specs if s.has_children)
    requests : List[ProfileRequest] = []
    seen : set[str] = set()
    for value in values or ():
        channel, sep, profile = value.partition('=')
        if not sep or not channel or not profile:
            raise ConfigError(f'--require-profile expects CHANNEL=PROFILE, got {value!r}.',
                              remedy = f'Name a publishing node and a profile, e.g. --require-profile '
                                       f'{channels[0] if channels else "producer"}=reliable_work.')
        if channel not in channels:
            raise ConfigError(f'--require-profile names channel {channel!r}, which no node in this flow publishes.',
                              remedy = f'Use one of: {", ".join(channels) or "(the flow has no channels)"}.')
        if profile not in MESSAGING_PROFILES:
            raise ConfigError(f'--require-profile names unknown profile {profile!r}.',
                              remedy = f'Use one of: {", ".join(MESSAGING_PROFILES)}.')
        if channel in seen:
            raise ConfigError(f'--require-profile names channel {channel!r} twice.',
                              remedy = 'A channel has one profile; drop one of the entries.')
        seen.add(channel)
        requests.append(ProfileRequest(channel, profile))
    return requests


def requirements_for(flow_type : str, specs : Sequence[NodeSpec],
                     explicit : Sequence[ProfileRequest] = (),
                     declared : FlowRequirements | None = None) -> FlowRequirements:
    '''
    The flow-type presets for every channel, with the operator's explicit
    requests replacing theirs, plus what the nodes themselves declared
    (``deploy.compile.declared_requirements``: sink guarantees, execution
    groups, batching contracts) when the caller has the compiled document.
    '''
    base = default_requirements(flow_type, specs)
    overridden = {r.channel: r for r in explicit}
    profiles = tuple(overridden.pop(r.channel, r) for r in base.profiles) + tuple(overridden.values())
    if declared is None:
        return dataclasses.replace(base, profiles = profiles)
    return dataclasses.replace(base, profiles = profiles, sink_guarantees = dict(declared.sink_guarantees),
                               execution_groups = dict(declared.execution_groups), batching = dict(declared.batching),
                               exactly_once_effects = tuple(declared.exactly_once_effects),
                               effect_retention_seconds = declared.effect_retention_seconds,
                               replay_horizon_seconds = declared.replay_horizon_seconds)


def admit(requirements : FlowRequirements, messaging : MessagingCapabilities,
          payload : Optional[PayloadCapabilities], *, payload_refs_in_use : bool,
          enforce : bool, unknown_is_fatal : bool, where : str,
          runtime : Optional[RuntimeCapabilities] = None,
          execution : Optional[ExecutionCapabilities] = None) -> Optional[CompositionPlan]:
    '''
    Run the planner. A rejection that is not binding is printed as a warning and
    ``None`` returned — today's behaviour, with the reason on record; a binding
    one propagates the planner's error (exit 2 or 3).

    Two kinds of rejection, bound separately: a *definite* incompatibility (the
    declared store is evictable, the transport retains nothing) binds under
    ``enforce``; an *unobservable* capability (a bring-your-own broker or store
    whose probe could not read the setting — unreachable, refused, timed out)
    binds only under ``unknown_is_fatal``: with the switch on, a broker that
    happens to be slow must not turn every deploy into a rejection, so it is a
    warning there; an explicit request is the one case where "unobserved" must
    not pass.

    - Arguments:
        - payload_refs_in_use: a payload store is configured, so envelopes over \
            the inline threshold offload to it and its durability is part of \
            the channel guarantee.
        - enforce: definite rejections are binding (explicit requests, or the \
            RFC 0006 switch — ``enforce_admission``).
        - unknown_is_fatal: unobservable capabilities are binding too (explicit \
            requests only — ``unknown_admission``).
        - where: ``deploy`` / ``run-local``, for the message.
        - runtime: the runtime store's read-back (``VF_RUNTIME_STORE_URL``), which \
            ``restart_safe`` and ``durable_control`` are admitted against; None \
            when no store is configured.
        - execution: what the engine advertises (fused groups, batching); None \
            when the caller is not deploying through an engine.
    '''
    try:
        return plan_composition(requirements, messaging, payload = payload, runtime = runtime,
                                execution = execution, payload_refs_in_use = payload_refs_in_use)
    except IncompatibleProfile as e:
        if enforce:
            raise
        _warn(where, e, 'advisory until RFC 0006 is accepted; pass --require-profile CHANNEL=PROFILE '
                        'to make it binding')
        return None
    except UnobservableState as e:
        if unknown_is_fatal:
            raise
        _warn(where, e, 'the setting could not be read back from the live service; '
                        'pass --require-profile CHANNEL=PROFILE to reject an unobserved guarantee')
        return None


def _warn(where : str, error : VideoflowError, standing : str) -> None:
    lines = error.message.splitlines()
    print(f'WARNING: {where}: the composition does not provide every requested guarantee — '
          f'{lines[0]} {"; ".join(line.strip() for line in lines[1:])} ({standing}). '
          f'Remedy: {error.remedy}', file = sys.stderr)


def enforce_admission(explicit : Sequence[ProfileRequest]) -> bool:
    '''Whether a definite rejection is binding: always, since RFC 0006 was accepted (kept for its callers' symmetry).'''
    return True


def unknown_admission(explicit : Sequence[ProfileRequest]) -> bool:
    '''Whether an unobservable capability is binding: only an explicit request asks for that.'''
    return bool(explicit)

__all__ = [
    'PROBE_TIMEOUT_SECONDS', 'PROFILE_REQUESTS_ENV', 'admit', 'enforce_admission', 'jetstream_capabilities',
    'jetstream_capabilities_observed', 'local_dev_capabilities', 'parse_profile_requests',
    'redis_payload_capabilities', 'redis_payload_capabilities_observed', 'requests_env', 'requests_from_env',
    'requirements_for', 'run_stream_names', 'unknown_admission', 'verify_topology_shape',
]


def runtime_capabilities_observed(url : str | None) -> RuntimeCapabilities:
    '''
    What the runtime store behind ``url`` (``VF_RUNTIME_STORE_URL``) can promise,
    read back: a Redis store probes its persistence, a file store is durable on
    its host, a memory store never is. An unset URL is the memory store.
    '''
    # Deferred: the store registry imports the wire package (optional `msgpack`/`protobuf`).
    from ..runtime.runtime_stores import make_runtime_store
    scheme = (url or 'memory://').split(':', 1)[0].lower() or 'memory'
    try:
        return make_runtime_store(url).capabilities()
    except ValueError:
        raise
    except Exception as e:  # noqa: BLE001 — an unreachable or refused store is Unknown, never assumed durable
        name = type(e).__name__
        reason = 'auth' if 'Permission' in name or 'Authentication' in name else \
            'timeout' if 'Timeout' in name else 'unreachable'
        return RuntimeCapabilities(scheme, durable = unknown(reason, f'{name}: {e}'),
                                   shared_across_processes = True, restart_safe_joins = False,
                                   elastic_state = False)


# -- placement admission (plan Phase 4) ---------------------------------------------------------

@dataclasses.dataclass(frozen = True)
class ReplicaAdmission:
    '''
    Three numbers a scale decision keeps apart (ALLOC-030, RUN-028): what was
    asked for, what the allocator can place on the capacity it observed, and
    what is actually ready to process — with the reason the rest is not
    admitted. A desired count is never reported as capacity.
    '''
    desired : int
    admitted : int
    ready : int
    reasons : tuple[str, ...] = ()

    @property
    def unadmitted(self) -> int:
        return max(0, self.desired - self.admitted)


def replica_admission(desired : int, feasible : Callable[[int], list[str]], ready : int) -> ReplicaAdmission:
    '''
    Admits the largest replica count ``feasible`` places without objections
    (``feasible(n)`` returns the reasons ``n`` replicas do not fit, empty when
    they do), reporting the reasons ``desired`` did not. ``ready`` is observed,
    never inferred from the admitted count.
    '''
    reasons = tuple(feasible(desired)) if desired > 0 else ()
    if not reasons:
        return ReplicaAdmission(desired, desired, min(ready, desired), ())
    admitted = 0
    for n in range(desired - 1, 0, -1):
        if not feasible(n):
            admitted = n
            break
    return ReplicaAdmission(desired, admitted, min(ready, admitted), reasons)


def rollout_problems(rollout_policy : Optional[str], specs : Sequence[NodeSpec], flow_type : str,
                     free_devices : Observation[int]) -> list[str]:
    '''
    Why a declared rollout policy cannot be honoured on the observed pool
    (ALLOC-029, RUN-029): ``surge`` replaces a GPU Deployment's pods by starting
    one extra first, which needs that replica's devices free somewhere in the
    pool; on a pool with none, the rollout would wait forever behind the old
    pod. ``drain`` needs nothing (the old pod stops first). No declared policy
    keeps the API default, which on a full pool stalls the same way — reported
    as advice, since the flow deployed that way before. An unobservable pool
    cannot admit ``surge``.
    '''
    # Function-level: manifests imports yaml at module scope (optional dep).
    from .manifests import _renders_as_job

    rolling = [s for s in specs if s.device_type == 'gpu' and not _renders_as_job(s, flow_type)]
    if not rolling:
        return []
    needed = max(s.gpu_count for s in rolling)
    if rollout_policy == 'surge':
        if isinstance(free_devices, Unknown):
            return [f'--rollout-policy surge needs {needed} spare GPU device(s) for the replacement replica, '
                    f'and the pool could not be observed ({free_devices.reason}: {free_devices.detail})']
        if free_devices.value < needed:
            return [f'--rollout-policy surge needs {needed} spare GPU device(s) for the replacement replica; the '
                    f'pool has {free_devices.value} free. Use --rollout-policy drain (the old replica stops first) '
                    f'or free capacity']
        return []
    if rollout_policy is None and not isinstance(free_devices, Unknown) and free_devices.value < needed:
        return [f'no --rollout-policy declared and the pool has {free_devices.value} free GPU device(s): the '
                f'default rolling update starts the replacement before stopping the old replica and would wait '
                f'behind it. Declare --rollout-policy drain (interrupt) or surge (reserve capacity)']
    return []


def free_gpu_devices_observed(kubectl : str = 'kubectl') -> Observation[int]:
    '''Whole GPU devices no running pod holds, across the videoflow pool — Unknown when either read failed.'''
    # Function-level: cluster imports gpu (get_gpu_mode) at module scope — the same cycle gpu.py defers.
    from .cluster import gpu_inventory_observed

    observed = gpu_inventory_observed(kubectl)
    if isinstance(observed, Unknown):
        return observed
    if any(not n.occupancy_known for n in observed.value):
        return unknown('failed', 'the pod listing behind the occupancy could not be read')
    free = 0
    for node in observed.value:
        held = sum(units for resource, units in node.used_units.items() if '/' in resource)
        free += max(0, node.card_count - held)
    return known(free, observed.generation)
