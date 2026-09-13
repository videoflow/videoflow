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
reason — never assumed. Until RFC 0006 is accepted the verdict is *advisory*
for a flow that asked for nothing explicitly — a warning, today's behaviour
otherwise — and binding as soon as the operator names a profile or the
``VF_RFC0006`` switch is on (decision D1). Explicit requests also travel to the
provision Job and the workers as ``VF_PROFILE_REQUESTS_JSON``, emitted only when
there are any so the default environment is unchanged (D8); there they bind
again, against the streams as provisioned (``runtime.provision``) and as bound
(``runtime.worker``, before ``open()``), through
``messaging.topology.verify_channel_profiles``.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import dataclasses
import logging
import re
import sys
from typing import List, Optional, Sequence

from ..backends.capabilities import (
    LEDGER_NONE,
    LEDGER_WINDOW,
    MESSAGING_PROFILES,
    PROFILE_REQUESTS_ENV,
    CompositionPlan,
    FlowRequirements,
    MessagingCapabilities,
    PayloadCapabilities,
    ProfileRequest,
    default_requirements,
    plan_composition,
    requests_env,
    requests_from_env,
)
from ..backends.outcomes import Observation, Unknown, known, unknown
from ..core import constants
from ..core.compiler import NodeSpec
from ..core.errors import ConfigError, IncompatibleProfile, UnobservableState, VideoflowError
from .broker_profiles import BrokerProfile, RedisProfile

logger = logging.getLogger(__package__)

#: JetStream's default duplicate-detection window (2 minutes), which
#: ``topology.stream_config_for`` leaves at the server default.
JETSTREAM_DEDUP_WINDOW_SECONDS = 120

#: Redis' default ``proto-max-bulk-len``: the largest value one SET may carry.
REDIS_MAX_OBJECT_BYTES = 512 << 20


def jetstream_capabilities(profile : Optional[BrokerProfile]) -> MessagingCapabilities:
    '''
    What a JetStream broker offers, from its *declared* profile: retained,
    recoverable delivery always (INTEREST retention with redelivery is what the
    topology provisions); persistence and replication as the profile says.
    With no profile (a bring-your-own ``--nats``) those two are ``Unknown`` —
    the broker was not read back, and unknown is not "yes"; the CLI reads such
    a broker back with ``jetstream_capabilities_observed`` instead.
    '''
    persistence : Observation[bool]
    replication : Observation[int]
    if profile is None:
        persistence = unknown('unread', 'the broker configuration was not read back (bring-your-own --nats)')
        replication = unknown('unread', 'the broker configuration was not read back (bring-your-own --nats)')
    else:
        persistence = known(bool(profile.persistence))
        replication = known(int(profile.jetstream_replicas))
    return MessagingCapabilities(
        adapter = 'jetstream', version = '2.10', retained_backlog = True, recoverable_delivery = True,
        latest_per_key = False, dedup_window_seconds = JETSTREAM_DEDUP_WINDOW_SECONDS,
        publication_ledger = 'window', replication_factor = replication, persistent_storage = persistence,
        max_payload_bytes = unknown('unread', 'nc.max_payload is read when the client connects'),
        credit_resizable = True, control_shares_data_slot = True)


def redis_payload_capabilities(profile : Optional[RedisProfile]) -> PayloadCapabilities:
    '''
    What a Redis payload store offers, from its declared profile: durable only
    with append-only persistence *and* ``noeviction`` (either alone lets an
    accepted envelope outlive its bytes). No profile (bring-your-own
    ``--blob-redis-url``): ``Unknown`` until read back, which the CLI does with
    ``redis_payload_capabilities_observed``.
    '''
    durable : Observation[bool]
    evictable : Observation[bool]
    atomic : Observation[bool]
    if profile is None:
        durable = unknown('unread', 'the store configuration was not read back (bring-your-own --blob-redis-url)')
        evictable = unknown('unread', 'the store configuration was not read back (bring-your-own --blob-redis-url)')
        atomic = unknown('unread', 'CLUSTER KEYSLOT of the obligation keys was not checked')
    else:
        persistent = profile.persistence != 'none'
        durable = known(persistent and profile.eviction == 'noeviction')
        evictable = known(profile.eviction != 'noeviction')
        atomic = known(True)                     # one node: every key shares its slot
    return PayloadCapabilities('redis', durable = durable, evictable = evictable, atomic_multikey = atomic,
                               max_object_bytes = REDIS_MAX_OBJECT_BYTES, reader_identities = False)


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
            return MessagingCapabilities(
                adapter = 'jetstream', version = version, retained_backlog = True, recoverable_delivery = True,
                latest_per_key = False, dedup_window_seconds = JETSTREAM_DEDUP_WINDOW_SECONDS,
                publication_ledger = LEDGER_WINDOW, replication_factor = replication,
                persistent_storage = persistence, max_payload_bytes = max_payload,
                credit_resizable = True, control_shares_data_slot = True)
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
    (``deploy.localinfra``): a JetStream server without a volume and a Redis
    with ``volatile-lru`` eviction and no persistence.
    '''
    return (jetstream_capabilities(BrokerProfile.dev()),
            redis_payload_capabilities(RedisProfile(persistence = 'none', eviction = 'volatile-lru')))


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
                     explicit : Sequence[ProfileRequest] = ()) -> FlowRequirements:
    '''The flow-type presets for every channel, with the operator's explicit requests replacing theirs.'''
    base = default_requirements(flow_type, specs)
    overridden = {r.channel: r for r in explicit}
    profiles = tuple(overridden.pop(r.channel, r) for r in base.profiles) + tuple(overridden.values())
    return dataclasses.replace(base, profiles = profiles)


def admit(requirements : FlowRequirements, messaging : MessagingCapabilities,
          payload : Optional[PayloadCapabilities], *, payload_refs_in_use : bool,
          enforce : bool, unknown_is_fatal : bool, where : str) -> Optional[CompositionPlan]:
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
    '''
    try:
        return plan_composition(requirements, messaging, payload = payload,
                                payload_refs_in_use = payload_refs_in_use)
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
    '''Whether a definite rejection is binding: an explicit request, or the RFC 0006 switch.'''
    return bool(explicit) or bool(constants.RFC0006)


def unknown_admission(explicit : Sequence[ProfileRequest]) -> bool:
    '''Whether an unobservable capability is binding: only an explicit request asks for that.'''
    return bool(explicit)

__all__ = [
    'PROBE_TIMEOUT_SECONDS', 'PROFILE_REQUESTS_ENV', 'admit', 'enforce_admission', 'jetstream_capabilities',
    'jetstream_capabilities_observed', 'local_dev_capabilities', 'parse_profile_requests',
    'redis_payload_capabilities', 'redis_payload_capabilities_observed', 'requests_env', 'requests_from_env',
    'requirements_for', 'run_stream_names', 'unknown_admission', 'verify_topology_shape',
]
