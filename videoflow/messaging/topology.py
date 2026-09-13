'''
Broker topology: naming, JetStream stream/consumer configuration, and up-front
provisioning of a flow's streams and durable consumers.

Everything that decides *how a message routes* — subject names, stream names,
durable names, and the retention/discard policies that give REALTIME vs BATCH
their semantics — lives here, so the messenger, the compiler, the manifests, and
the provisioning entrypoint all agree on one source of truth.

Naming is scoped by ``flow_id`` **and** ``run_id`` so that re-running or
redeploying a flow gets a fresh set of streams instead of colliding with the
previous run's durables.

Ownership is a separate question from naming, and this module answers it too.
Names are hyphen-joined and hyphens are legal inside every part, so a stream
name alone cannot say where the run ends and the node begins: ``vf-f-r-x-n`` is
node ``x-n`` of run ``r`` *and* node ``n`` of run ``r-x``. Two things make
ownership exact anyway. Under RFC 0006 (``VF_RFC0006=1``) every stream and
durable is created with owner labels in its JetStream ``metadata``
(``videoflow.io/flow-id``, ``run-id``, ``node``, ``kind``, ``generation``) that
teardown compares verbatim. For streams that predate the labels, the stream's own
data subject is the authority: subjects are dot-delimited and ``sanitize`` never
lets a dot through, so ``vf.f.r.x-n`` and ``vf.f.r-x.n`` are distinct strings —
teardown reads the run out of the subject token, never out of a name prefix.
Provisioning also reads every stream and consumer back after creating it and
reports the fields the broker did not honour, because a REALTIME stream the
broker silently kept at INTEREST retention is a flow with the wrong semantics,
not a provisioned one.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import logging
import re
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional, Sequence

import nats
import nats.errors
import nats.js.errors
from nats.aio.client import Client
from nats.js import JetStreamContext
from nats.js.api import (
    ConsumerConfig,
    ConsumerInfo,
    DiscardPolicy,
    RetentionPolicy,
    StreamConfig,
    StreamInfo,
    StreamsListIterator,
)
from nats.js.errors import NotFoundError

from ..backends.capabilities import LIVE_LATEST, RELIABLE_WORK, ProfileRequest
from ..backends.identity import LABEL_NODE, flow_labels, has_owner_labels, owner_labels, owns
from ..backends.outcomes import CleanupObservation, Unknown, unknown

# The module, not the symbol: ``constants.RFC0006`` is read at call time, so a test
# can flip the switch with monkeypatch instead of re-importing this module.
from ..core import constants
from ..core.compiler import NodeSpec
from ..core.constants import REALTIME
from ..core.errors import BrokerUnavailable, IncompatibleProfile, UnobservableState
from ..core.policies import DEFAULT_MAX_RETRIES, DeliveryPolicy

logger = logging.getLogger(__package__)

_SANITIZE_RE = re.compile(r'[^A-Za-z0-9_-]+')

def sanitize(value : str) -> str:
    return _SANITIZE_RE.sub('_', value)

# -- naming ----------------------------------------------------------------

def subject_for(flow_id : str, run_id : str, node_name : str) -> str:
    return f'vf.{sanitize(flow_id)}.{sanitize(run_id)}.{sanitize(node_name)}'

def stream_name_for(flow_id : str, run_id : str, node_name : str) -> str:
    return f'vf-{sanitize(flow_id)}-{sanitize(run_id)}-{sanitize(node_name)}'

def eos_subject_for(flow_id : str, run_id : str, node_name : str) -> str:
    # End-of-stream markers ride a separate subject on the node's own stream, so
    # data consumers (which filter to the data subject) never see them and every
    # consuming replica can observe EOS via its own dedicated consumer.
    return f'vf.{sanitize(flow_id)}.{sanitize(run_id)}.{sanitize(node_name)}._eos'

def control_subject_for(flow_id : str, run_id : str) -> str:
    return f'vf.{sanitize(flow_id)}.{sanitize(run_id)}._control.stop'

def durable_name_for(consumer_node_name : str, parent_node_name : str) -> str:
    # Durables are scoped to the (run-scoped) parent stream, so they need no
    # run_id of their own. Replicas of one consuming node share this durable
    # (competing consumers); distinct children of one parent get distinct
    # durables (broadcast fan-out).
    return f'{sanitize(consumer_node_name)}--from--{sanitize(parent_node_name)}'

def partitioned_durable_name_for(consumer_node_name : str, parent_node_name : str, replica_id : int) -> str:
    # Each replica of a partitioned node gets its own durable, so every replica
    # receives every message and keeps only the ones it owns (hash(key)%N==id).
    return f'{durable_name_for(consumer_node_name, parent_node_name)}--p{replica_id}'

def eos_durable_name_for(consumer_node_name : str, parent_node_name : str, instance_id : str) -> str:
    # Per-consuming-replica durable so *every* replica observes EOS (unlike the
    # shared data durable, where only one replica would). instance_id makes it
    # unique per replica (a stable replica ordinal, or a per-process uuid).
    return f'{sanitize(consumer_node_name)}--eos--{sanitize(parent_node_name)}--{sanitize(instance_id)}'

def eos_anchor_durable_name_for(node_name : str) -> str:
    '''The provision-time interest anchor on a node's EOS subject (see eos_anchor_config).'''
    return f'{sanitize(node_name)}--eos--anchor'

def dlq_stream_name(flow_id : str) -> str:
    '''
    The dead-letter stream, scoped to the **flow** rather than the run.

    Everything else about a run is disposable and is deleted with it; dead letters
    are the opposite — they are the forensic record of what went wrong, and they
    are most wanted precisely after a run that failed and was torn down. Run
    scoping meant ``delete_run_streams`` destroyed the evidence on the way out and
    the stream's week-long retention never applied to anybody. The run id lives in
    the *subject* instead, so entries stay attributable and filterable.
    '''
    return f'vf-{sanitize(flow_id)}-dlq'

def dlq_subject_for(flow_id : str, run_id : str, node_name : str) -> str:
    '''``vf.{flow}._dlq.{run}.{node}`` — filterable by run, by node, or by both.'''
    return f'vf.{sanitize(flow_id)}._dlq.{sanitize(run_id)}.{sanitize(node_name)}'

def dlq_subject_filter(flow_id : str, run_id : str | None = None,
                    node_name : str | None = None) -> str:
    '''
    A subject wildcard selecting dead letters for inspection: the whole flow, one
    run of it, or one node of one run. Used by ``videoflow dlq``.
    '''
    run = sanitize(run_id) if run_id else '*'
    node = sanitize(node_name) if node_name else '*'
    if run_id is None and node_name is None:
        return f'vf.{sanitize(flow_id)}._dlq.>'
    return f'vf.{sanitize(flow_id)}._dlq.{run}.{node}'

_TOKEN_RE = re.compile(r'[A-Za-z0-9_-]+')

def subject_owner(subject : str) -> tuple[str, str, str] | None:
    '''
    The ``(flow, run, node)`` tokens a data or EOS subject encodes — sanitized,
    exactly as ``subject_for``/``eos_subject_for`` wrote them — or None for any
    other subject (control, DLQ, wildcards, foreign).

    This is the one place a run boundary is exact without metadata: subject
    tokens are dot-delimited and ``sanitize`` never lets a dot through, so
    ``vf.f.r.x-n`` (run ``r``, node ``x-n``) and ``vf.f.r-x.n`` (run ``r-x``,
    node ``n``) are different strings although both hyphen-join to the stream
    name ``vf-f-r-x-n``. ``delete_run_streams`` attributes streams that predate
    owner labels with it.
    '''
    tokens = subject.split('.')
    if len(tokens) == 5 and tokens[4] == '_eos':
        tokens = tokens[:4]
    if len(tokens) != 4 or tokens[0] != 'vf' or not all(_TOKEN_RE.fullmatch(t) for t in tokens[1:]):
        return None
    return tokens[1], tokens[2], tokens[3]

def stream_label_selector(flow_id : str, run_id : str) -> str:
    '''
    Prefix shared by every stream of one run. Diagnostic only: it is also the
    prefix of every stream of run ``{run}-x``, so ``delete_run_streams`` never
    selects on it — ownership comes from owner metadata or from the subject
    tokens (``subject_owner``). Enumerated by ``backends.identity.derived_names``.
    '''
    return f'vf-{sanitize(flow_id)}-{sanitize(run_id)}-'

# -- stream / consumer config ---------------------------------------------

#: REALTIME keeps only the freshest N messages per node and never blocks the
#: producer (a new publish evicts the oldest). BATCH uses INTEREST retention so
#: acked messages are freed, bounding the backlog to unacked messages; a full
#: stream then *rejects* new publishes (DiscardPolicy.NEW), which the publisher
#: turns into blocking backpressure instead of silent loss.
DEFAULT_REALTIME_BUFFER = 1
DEFAULT_BATCH_MAX_MSGS = 10_000
DUPLICATE_WINDOW_SECONDS = 120
#: How long dead letters are retained (``STREAM-8``): the forensic and replay horizon a
#: dead letter's payload obligation must cover too (``BLOB-14`` step 3).
DLQ_RETENTION_SECONDS = 7 * 24 * 3600
#: Deliveries a worker parks locally per subscription beyond the one it is
#: processing, so the next input is already in hand when the current one settles.
DEFAULT_PREFETCH = 4
#: Inputs one replica holds in processing at a time.
DEFAULT_ITEM_CREDIT = 1
#: The broker-side ``max_ack_pending`` provisioning used before RFC 0006 (``STREAM-15``).
DEFAULT_MAX_ACK_PENDING = 8
#: The ``max_ack_pending`` a worker bound its durable with before RFC 0006 (its
#: local queue depth plus two).
LEGACY_BIND_CREDIT = DEFAULT_PREFETCH + 2

def consumer_credit(nb_tasks : int, partitioned : bool, item_credit : int = DEFAULT_ITEM_CREDIT,
                    prefetch : int = DEFAULT_PREFETCH) -> int:
    '''
    ``STREAM-15``: the broker-side ``max_ack_pending`` a subscription needs so
    every replica can hold ``item_credit`` inputs in processing plus ``prefetch``
    parked, without one replica's un-acked work starving another's — a shared
    (competing) durable multiplies by the replica count, a per-replica
    (partitioned) durable does not. Provisioning and the worker call this so the
    durable created and the durable bound agree (MSG-017).
    '''
    per_replica = item_credit + prefetch
    return per_replica if partitioned else nb_tasks * per_replica

def _labels(flow_id : str, run_id : str, node : str, kind : str,
            generation : str | None) -> dict[str, str] | None:
    '''
    Owner labels for a stream or durable, or None while RFC 0006 is off. A None
    ``metadata`` is left out of the JetStream request entirely (nats-py 2.15.0,
    ``nats/js/api.py`` ``Base.as_dict`` skips None fields), which is what keeps
    the default path byte-identical to the pre-RFC configs.
    '''
    if not constants.RFC0006:
        return None
    return owner_labels(flow_id, run_id, node = node, kind = kind, generation = generation)

def stream_config_for(flow_id : str, run_id : str, node_name : str, flow_type : str,
                    subjects : list[str] | None = None, realtime_buffer : int = DEFAULT_REALTIME_BUFFER,
                    batch_max_msgs : int = DEFAULT_BATCH_MAX_MSGS,
                    generation : str | None = None, replicas : int = 1) -> StreamConfig:
    '''
    - Arguments:
        - generation: provisioning generation recorded in the owner labels \
            (RFC 0006); None leaves that label out.
        - replicas: copies the stream keeps (a replicated broker profile's \
            ``jetstream_replicas``); 1 leaves the field to the server default, so \
            a single-server request is exactly what it always was.
    '''
    name = stream_name_for(flow_id, run_id, node_name)
    metadata = _labels(flow_id, run_id, node_name, 'stream', generation)
    num_replicas = replicas if replicas > 1 else None
    if subjects is None:
        # Data and EOS ride the same stream on distinct subjects.
        subjects = [subject_for(flow_id, run_id, node_name),
                    eos_subject_for(flow_id, run_id, node_name)]
    if flow_type == REALTIME:
        return StreamConfig(
            name = name, subjects = subjects,
            retention = RetentionPolicy.LIMITS,
            max_msgs = max(1, realtime_buffer),
            discard = DiscardPolicy.OLD,
            duplicate_window = DUPLICATE_WINDOW_SECONDS,
            metadata = metadata,
            num_replicas = num_replicas,
        )
    return StreamConfig(
        name = name, subjects = subjects,
        retention = RetentionPolicy.INTEREST,
        max_msgs = batch_max_msgs,
        discard = DiscardPolicy.NEW,
        duplicate_window = DUPLICATE_WINDOW_SECONDS,
        metadata = metadata,
        num_replicas = num_replicas,
    )

def dlq_stream_config(flow_id : str, replicas : int = 1) -> StreamConfig:
    # Flow-level labels only (no run id): no run's teardown can ever own it.
    return StreamConfig(
        name = dlq_stream_name(flow_id),
        subjects = [f'vf.{sanitize(flow_id)}._dlq.>'],
        retention = RetentionPolicy.LIMITS,
        discard = DiscardPolicy.OLD,
        max_age = DLQ_RETENTION_SECONDS,  # keep dead-lettered messages for a week
        metadata = flow_labels(flow_id, 'dlq') if constants.RFC0006 else None,
        num_replicas = replicas if replicas > 1 else None,
    )

def max_deliver_for(flow_type : str, max_retries : int = DEFAULT_MAX_RETRIES,
                    delivery : dict | None = None) -> int:
    '''
    Broker-side delivery cap for one node's durables.

    Derived from the node's effective ``DeliveryPolicy`` rather than from the flow
    type alone, because delivery is overridable per node: an at-least-once sink in
    a REALTIME flow needs a cap above 1 or its retries would be silently
    impossible, and a best-effort node in a BATCH flow should not be retried at
    all. Provisioning and the messenger both call this so the durable they create
    and the durable they bind agree.

    - Arguments:
        - flow_type: supplies the preset.
        - max_retries: deployment-level retry count (``VF_MAX_RETRIES``).
        - delivery: the node's own override, as a dict (``NodeSpec.delivery``).
    '''
    return DeliveryPolicy.resolve(flow_type, delivery, max_retries).max_deliver

def consumer_config_for(flow_id : str, run_id : str, consumer_node_name : str,
                        parent_node_name : str, ack_wait : int = 60, max_deliver : int = 1,
                        max_ack_pending : int = 8, generation : str | None = None) -> ConsumerConfig:
    '''
    Durable pull-consumer config for one (child, parent) edge. Filters to the
    parent's *data* subject so EOS markers (on the ``_eos`` subject of the same
    stream) are handled by a separate per-replica consumer instead. ``max_deliver``
    is 1 for REALTIME (no redelivery — freshest wins) and ``retries + 1`` for BATCH;
    ``max_ack_pending`` bounds how many un-acked messages the broker will hand out
    before it stops delivering (this is the server-side half of prefetch bounding).
    Under RFC 0006 the durable is labelled with its *consuming* node as owner.
    '''
    durable = durable_name_for(consumer_node_name, parent_node_name)
    return ConsumerConfig(
        durable_name = durable,
        filter_subject = subject_for(flow_id, run_id, parent_node_name),
        ack_wait = ack_wait,
        max_deliver = max_deliver,
        max_ack_pending = max_ack_pending,
        metadata = _labels(flow_id, run_id, consumer_node_name, 'durable', generation),
    )

def eos_consumer_config(flow_id : str, run_id : str, consumer_node_name : str,
                        parent_node_name : str, instance_id : str,
                        inactive_threshold : int = 3600, generation : str | None = None) -> ConsumerConfig:
    '''
    Per-replica durable pull-consumer for a parent's EOS subject. ``inactive_threshold``
    lets the server clean it up automatically some time after the flow ends, so
    per-process (uuid-suffixed) EOS consumers don't accumulate.
    '''
    durable = eos_durable_name_for(consumer_node_name, parent_node_name, instance_id)
    return ConsumerConfig(
        durable_name = durable,
        filter_subject = eos_subject_for(flow_id, run_id, parent_node_name),
        ack_wait = 30,
        inactive_threshold = inactive_threshold,
        metadata = _labels(flow_id, run_id, consumer_node_name, 'eos_durable', generation),
    )

def eos_anchor_config(flow_id : str, run_id : str, node_name : str,
                      generation : str | None = None) -> ConsumerConfig:
    '''
    Provision-time durable on a node's EOS subject that exists purely to create
    *interest*, so an EOS marker is retained by the BATCH (INTEREST-retention)
    stream no matter when it is published.

    The real EOS consumers are per-process (uuid-suffixed) durables created in each
    worker's setup — they cannot be pre-provisioned, so without this anchor an EOS
    published by a fast-finishing parent *before* a slow-starting child registers
    its EOS consumer is silently discarded (no interest at publish time), and the
    child then waits for EOS forever: the flow never terminates. The anchor is
    never fetched from and never acks, so the marker stays retained for any number
    of late-created consumers (their default DeliverPolicy.ALL replays it); the
    run's stream teardown deletes the anchor with everything else. No
    ``inactive_threshold``: it must not be reaped while the run is alive.
    '''
    return ConsumerConfig(
        durable_name = eos_anchor_durable_name_for(node_name),
        filter_subject = eos_subject_for(flow_id, run_id, node_name),
        ack_wait = 30,
        metadata = _labels(flow_id, run_id, node_name, 'eos_anchor', generation),
    )

# -- provisioning ----------------------------------------------------------

@dataclass(frozen = True)
class VerifiedStream:
    '''
    What the broker holds after ``_ensure_stream``: the config that was asked
    for, the config read back (None when the read-back itself failed), and each
    requested field the two differ on, rendered ``'field: requested X, effective Y'``.
    Empty ``mismatches`` means the broker gave the flow exactly what it asked for.
    '''
    requested : StreamConfig
    effective : Optional[StreamConfig]
    mismatches : tuple[str, ...] = ()

@dataclass(frozen = True)
class VerifiedConsumer:
    '''``VerifiedStream``'s counterpart for a durable consumer.'''
    requested : ConsumerConfig
    effective : Optional[ConsumerConfig]
    mismatches : tuple[str, ...] = ()

#: The fields a read-back compares, per config type. Only fields the request set
#: explicitly (not None) take part, so server-filled defaults (``storage``,
#: ``num_replicas``, ``max_waiting``...) never count as mismatches unless they were
#: asked for. ``metadata`` is a subset check: nats-server adds its own ``_nats.*`` keys.
_STREAM_FIELDS = ('name', 'subjects', 'retention', 'discard', 'max_msgs', 'max_msgs_per_subject', 'max_bytes',
                  'max_age', 'duplicate_window', 'storage', 'num_replicas', 'metadata')
_CONSUMER_FIELDS = ('durable_name', 'filter_subject', 'deliver_policy', 'ack_policy', 'replay_policy',
                    'ack_wait', 'max_deliver', 'max_ack_pending', 'inactive_threshold', 'metadata')

def _plain(value : Any) -> Any:
    '''Enum members to their wire value: nats-py's policies are ``str`` enums, but a message should read ``'interest'``.'''
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list):
        return [_plain(v) for v in value]
    return value

def _mismatches(requested : Any, effective : Any, fields : Sequence[str]) -> tuple[str, ...]:
    '''
    The requested fields the effective config does not honour.

    Both sides are compared as plain attribute values (``dataclasses.asdict``),
    not as nats-py's ``as_dict()`` wire form, which inserts ``max_age = 0`` and
    ``duplicate_window = 0`` for unset fields (nats-py 2.15.0, ``nats/js/api.py``
    ``StreamConfig.as_dict``) and would make every unset field look requested.
    ``duplicate_window = 0`` is the dataclass default meaning "server default",
    so it is skipped like None. Durations round-trip exactly: the client sends
    whole nanoseconds and ``from_response`` divides back to seconds, so a
    requested ``60`` reads back as ``60.0``.
    '''
    want_all = asdict(requested)
    got_all = asdict(effective)
    found : list[str] = []
    for field in fields:
        want = want_all.get(field)
        if want is None or (field == 'duplicate_window' and not want):
            continue
        got = got_all.get(field)
        if field == 'metadata':
            missing = {k: v for k, v in want.items() if (got or {}).get(k) != v}
            if missing:
                found.append(f'metadata: requested {missing!r} not carried by the broker (effective {got!r})')
            continue
        if _plain(want) != _plain(got):
            found.append(f'{field}: requested {_plain(want)!r}, effective {_plain(got)!r}')
    return tuple(found)

def _report_mismatches(kind : str, name : str, mismatches : tuple[str, ...]) -> None:
    '''
    A read-back that differs from the request means the broker did not give the
    flow what it asked for: an existing stream whose retention/storage/replicas an
    update cannot change, a value clamped to an account limit, or a server too old
    to carry metadata. Under RFC 0006 that is an ``IncompatibleProfile`` — the
    alternative is a REALTIME stream silently running with BATCH retention. With
    the switch off it is a warning, and provisioning proceeds as it always has.
    '''
    if not mismatches:
        return
    detail = '; '.join(mismatches)
    if constants.RFC0006:
        raise IncompatibleProfile(
            f'{kind} {name} exists with a configuration the broker did not change to the requested one: {detail}.',
            remedy = ('Tear the run down (`videoflow teardown --flow-id <flow> --run-id <run> --nats <url>`) '
                      'or provision under a fresh run id so the resource is created rather than updated. '
                      'A value the broker clamped needs its JetStream account limit raised or the request '
                      'lowered; missing metadata means the server predates JetStream metadata (nats-server '
                      '2.10) — upgrade it, or run without VF_RFC0006.'),
            resource = name, mismatches = list(mismatches))
    logger.warning(f'{kind} {name}: broker configuration differs from the requested one ({detail}); '
                   'the flow may not get the semantics it asked for')

def _unverifiable(kind : str, name : str, error : Exception) -> tuple[str, ...]:
    '''
    The resource could be neither created/updated nor read back, so nothing can
    be said about what the broker holds. Under RFC 0006 that is an error in its
    own right (``BrokerUnavailable``: an unobserved config is not a verified one);
    with the switch off it is the debug line provisioning has always emitted.
    '''
    if constants.RFC0006:
        raise BrokerUnavailable(
            f'{kind} {name} could not be provisioned or read back: {error}',
            remedy = 'Check that the broker is reachable and that the parent stream exists, then re-run provisioning.',
            resource = name) from error
    logger.debug(f'{kind} {name} could not be read back after provisioning: {error}')
    return (f'read-back failed: {error!r}',)

def _stream_exists(error : Exception) -> bool:
    # nats-py 2.15.0 raises ``nats.js.errors.BadRequestError`` for a create that
    # collides (``nats/js/errors.py`` ``APIError.from_error`` maps only the HTTP-style
    # ``code`` to a class; it exposes no JetStream ``err_code`` table), so the server's
    # description text is the one stable signal the client gives.
    msg = str(error)
    return 'already in use' in msg or 'name already in use' in msg

async def _ensure_stream(js : JetStreamContext, config : StreamConfig) -> VerifiedStream:
    '''
    Create the stream, or bring an existing one in line, then read it back.

    ``add_stream``/``update_stream`` return the server's ``StreamInfo``, whose
    ``config`` is the configuration actually applied (nats-py 2.15.0,
    ``nats/js/manager.py`` ``add_stream``/``update_stream`` build it from the
    create/update response), so a successful call is its own read-back;
    ``stream_info`` is consulted only when JetStream refused the update — which
    is exactly when the existing stream differs on a field an update cannot
    change (retention, storage).

    - Raises:
        - ``IncompatibleProfile``: a requested field is not what the broker holds (RFC 0006 on).
        - ``BrokerUnavailable``: the stream could be neither updated nor read back (RFC 0006 on).
    '''
    name = config.name or ''
    info : StreamInfo | None = None
    failure : Exception | None = None
    try:
        info = await js.add_stream(config)
    except Exception as e:  # noqa: BLE001 — nats raises a generic error on conflict
        if not _stream_exists(e):
            raise
        # Exists already; bring the mutable fields in line. JetStream rejects an
        # update that changes retention or storage — the read-back below reports
        # that instead of hiding it (run-scoped names make it rare, not impossible).
        try:
            info = await js.update_stream(config)
        except Exception as ue:  # noqa: BLE001
            failure = ue
            logger.debug(f'stream {name} exists; update refused: {ue}')
    if info is None:
        try:
            info = await js.stream_info(name)
        except Exception as e:  # noqa: BLE001
            return VerifiedStream(config, None, _unverifiable('stream', name, failure or e))
    verified = VerifiedStream(config, info.config, _mismatches(config, info.config, _STREAM_FIELDS))
    _report_mismatches('stream', name, verified.mismatches)
    return verified

async def _ensure_consumer(js : JetStreamContext, stream_name : str, config : ConsumerConfig) -> VerifiedConsumer:
    '''
    Create the durable, or accept the existing one, then read it back.

    ``add_consumer`` is idempotent for an identical config and returns the applied
    ``ConsumerInfo`` (nats-py 2.15.0, ``nats/js/manager.py`` ``add_consumer``);
    JetStream rejects a create whose config differs from the existing durable's,
    and then ``consumer_info`` shows what the durable actually is.

    - Raises:
        - ``IncompatibleProfile``: a requested field is not what the broker holds (RFC 0006 on).
        - ``BrokerUnavailable``: the durable could be neither created nor read back (RFC 0006 on).
    '''
    durable = config.durable_name or ''
    label = f'{stream_name}/{durable}'
    info : ConsumerInfo | None = None
    failure : Exception | None = None
    try:
        info = await js.add_consumer(stream_name, config)
    except Exception as e:  # noqa: BLE001
        failure = e
        logger.debug(f'add_consumer({stream_name}, {durable}) skipped: {e}')
    if info is None:
        try:
            info = await js.consumer_info(stream_name, durable)
        except Exception as e:  # noqa: BLE001
            return VerifiedConsumer(config, None, _unverifiable('consumer', label, failure or e))
    verified = VerifiedConsumer(config, info.config, _mismatches(config, info.config, _CONSUMER_FIELDS))
    _report_mismatches('consumer', label, verified.mismatches)
    return verified

async def provision_flow(nc : Client, specs : list[NodeSpec], flow_id : str, run_id : str, flow_type : str,
                        max_retries : int = DEFAULT_MAX_RETRIES, ack_wait : int = 60,
                        max_ack_pending : int | None = None, generation : str | None = None,
                        replicas : int = 1) -> None:
    '''
    Idempotently create every stream and durable consumer a flow needs, before any
    worker publishes. Required for BATCH: under INTEREST retention a message
    published with no registered consumer interest is discarded immediately, so
    the durables must exist first. Every resource is read back after creation
    (``_ensure_stream``/``_ensure_consumer``), so a broker that did not honour a
    requested field is reported rather than trusted.

    - Arguments:
        - nc: a connected ``nats`` client.
        - specs: list of ``videoflow.core.compiler.NodeSpec``.
        - max_ack_pending: broker-side credit per durable. None derives it: under \
            RFC 0006 from the consuming node's replica count (``consumer_credit``, \
            STREAM-15), otherwise the fixed ``DEFAULT_MAX_ACK_PENDING`` it always was.
        - generation: provisioning generation recorded in the owner labels \
            (RFC 0006), so ``delete_run_streams`` can be asked to remove only what \
            this provisioning created. None leaves that label out.
        - replicas: stream copies to request (a replicated broker profile's \
            ``jetstream_replicas``); read back and reported like every other field.
    '''
    js = nc.jetstream()
    by_name = {spec.name: spec for spec in specs}

    # 1. One stream per node — plus, for any node something consumes from, an
    #    interest *anchor* on its EOS subject. The per-process EOS durables are
    #    created only in worker setup, so without the anchor an EOS published
    #    before a slow-starting child registers loses the interest race and is
    #    discarded — leaving the child waiting for EOS forever (see eos_anchor_config).
    for spec in specs:
        await _ensure_stream(js, stream_config_for(flow_id, run_id, spec.name, flow_type, generation = generation,
                                                   replicas = replicas))
        if spec.has_children:
            await _ensure_consumer(js, stream_name_for(flow_id, run_id, spec.name),
                                   eos_anchor_config(flow_id, run_id, spec.name, generation = generation))

    # 2. DLQ stream — where messages that exhaust their retries land. Flow-scoped,
    #    so tearing this run down does not delete the record of what it lost.
    await _ensure_stream(js, dlq_stream_config(flow_id, replicas = replicas))

    # 3. Durable consumers per (child, parent) edge, on the parent's stream. A
    #    partitioned child gets one durable *per replica* (broadcast + client-side
    #    ownership); everything else gets one shared durable (competing consumers).
    for spec in specs:
        partition_by = spec.partition_by
        nb_tasks = spec.nb_tasks
        # Per node, not per flow: a node may override its delivery mode, and the
        # durable created here must match the one its worker binds.
        max_deliver = max_deliver_for(flow_type, max_retries, spec.delivery)
        partitioned = bool(partition_by and nb_tasks > 1)
        if max_ack_pending is not None:
            credit = max_ack_pending
        elif constants.RFC0006:
            credit = consumer_credit(nb_tasks, partitioned)
        else:
            credit = DEFAULT_MAX_ACK_PENDING
        for parent_name in spec.parents:
            if parent_name not in by_name:
                continue
            parent_stream = stream_name_for(flow_id, run_id, parent_name)
            base = consumer_config_for(flow_id, run_id, spec.name, parent_name, ack_wait = ack_wait,
                                    max_deliver = max_deliver, max_ack_pending = credit,
                                    generation = generation)
            if partitioned:
                for replica_id in range(nb_tasks):
                    cfg = ConsumerConfig(
                        durable_name = partitioned_durable_name_for(spec.name, parent_name, replica_id),
                        filter_subject = base.filter_subject, ack_wait = base.ack_wait,
                        max_deliver = base.max_deliver, max_ack_pending = base.max_ack_pending,
                        metadata = _labels(flow_id, run_id, spec.name, 'partitioned_durable', generation))
                    await _ensure_consumer(js, parent_stream, cfg)
            else:
                await _ensure_consumer(js, parent_stream, base)

async def provision_flow_connect(nats_url : str, specs : list[NodeSpec], flow_id : str, run_id : str,
                                flow_type : str, connect_options : dict[str, Any] | None = None,
                                **kwargs : Any) -> None:
    '''
    Connects to NATS, provisions, and drains — a self-contained entrypoint.

    - Arguments:
        - connect_options: extra kwargs for ``nats.connect``. The default (retry \
            forever) is right for the in-cluster provision Job, whose broker may \
            still be starting; a local run passes fail-fast options instead so an \
            unreachable broker reports itself rather than hanging.
    '''
    nc = await nats.connect(nats_url, **(connect_options or {}))
    try:
        await provision_flow(nc, specs, flow_id, run_id, flow_type, **kwargs)
    finally:
        await nc.drain()

def provision_flow_sync(nats_url : str, specs : list[NodeSpec], flow_id : str, run_id : str, flow_type : str,
                        connect_options : dict[str, Any] | None = None, timeout : float | None = None,
                        **kwargs : Any) -> None:
    '''
    Synchronous wrapper for callers outside an event loop (the local engine, the
    init entrypoint).

    - Arguments:
        - timeout: overall bound in seconds, or None (the default) to wait \
            indefinitely. ``nats.connect`` retries an unreachable server forever — \
            ``allow_reconnect``/``max_reconnect_attempts`` only govern reconnects \
            *after* a successful connect — so a caller that would rather report the \
            problem than block must set this.

    - Raises:
        - ``TimeoutError`` when ``timeout`` elapses first.
    '''
    async def _go() -> None:
        await provision_flow_connect(nats_url, specs, flow_id, run_id, flow_type,
                                     connect_options = connect_options, **kwargs)

    async def _bounded() -> None:
        await asyncio.wait_for(_go(), timeout = timeout)

    asyncio.run(_bounded() if timeout is not None else _go())

async def _list_streams(js : JetStreamContext) -> list[StreamInfo]:
    '''
    Every stream in the account, across pages.

    ``$JS.API.STREAM.LIST`` is paged (nats-py 2.15.0 documents the 256-entry page
    for the sibling ``consumers_info``, ``nats/js/manager.py:326``) and
    ``streams_info`` returns one page with no way to tell whether it was the last
    (``manager.py:177``). ``streams_info_iterator`` (``manager.py:192``) wraps the
    same page in a ``StreamsListIterator`` carrying the server's ``total``
    (``nats/js/api.py:506``), which makes the stop condition exact — a teardown
    that saw page one of two would otherwise report a complete cleanup.
    '''
    infos : list[StreamInfo] = []
    offset = 0
    while True:
        page = await js.streams_info_iterator(offset = offset)
        if not isinstance(page, StreamsListIterator):   # annotated Iterable; the total lives on the iterator
            raise BrokerUnavailable(f'stream listing returned {type(page).__name__}, not a StreamsListIterator',
                                    remedy = "Pass the nats client's own jetstream() context.")
        got = list(page)
        infos.extend(got)
        offset += len(got)
        if offset >= page.total:
            return infos
        if not got:
            raise BrokerUnavailable(f'stream listing stopped after {offset} of {page.total} streams',
                                    remedy = 'Retry the teardown; the broker returned a short page.')

def _owned_stream(config : StreamConfig, flow_id : str, run_id : str, generation : str | None,
                  node_names : frozenset[str] | None, exact_names : frozenset[str] | None) -> bool:
    '''
    Whether a listed stream belongs to this run — exactly, never by prefix.

    Labelled streams (RFC 0006) are decided by their owner metadata alone: the
    labels hold the raw ids, so ``r`` and ``r-x`` differ verbatim, and a stream
    whose name is in this run's set but whose labels name another run is not
    touched. Unlabelled streams are decided by their data subject
    (``subject_owner``): the run token is delimited by dots no id can contain.
    When the caller names the nodes, the stream must additionally be one of theirs.
    '''
    metadata = config.metadata
    if has_owner_labels(metadata):
        if not owns(metadata, flow_id, run_id, generation = generation):
            return False
        if node_names is None:
            return True
        node = (metadata or {}).get(LABEL_NODE)
        return node in node_names if node is not None else config.name in (exact_names or frozenset())
    owners = [o for o in (subject_owner(s) for s in (config.subjects or [])) if o is not None]
    if not owners or any(o[:2] != (sanitize(flow_id), sanitize(run_id)) for o in owners):
        return False
    return exact_names is None or config.name in exact_names

async def delete_run_streams(nc : Client, flow_id : str, run_id : str,
                             node_names : Iterable[str] | None = None,
                             generation : str | None = None) -> CleanupObservation:
    '''
    Teardown: delete the streams this run owns — by exact ownership, never by
    name prefix — and report truthfully what happened.

    Ownership, per stream (``_owned_stream``): a stream carrying RFC 0006 owner
    labels is this run's iff the labels equal ``(flow_id, run_id)`` verbatim (and
    ``generation``, when given); an unlabelled stream is this run's iff its data
    subject names this run token for token. Neither can mistake run ``r-x`` for
    run ``r`` the way ``startswith('vf-{flow}-{run}-')`` did, and neither needs
    the graph: ``node_names`` (``[spec.name for spec in specs]``) only narrows the
    deletion to those nodes' streams, for a caller tearing down its own
    provisioning.

    What is *not* attributed — and is therefore left standing, and not reported,
    since nothing on it says whose it is: an unlabelled stream provisioned with
    custom subjects by something other than this module. Nothing in-tree does that.

    The flow's dead-letter stream is deliberately never one of them. Teardown
    runs in a ``finally`` on success, failure, stall and Ctrl-C alike, so deleting
    the DLQ here destroyed exactly the evidence an operator wants after a failed
    run. It ages out on its own retention instead, and ``videoflow dlq purge``
    removes it on purpose. (It carries flow-level labels with no run id, and its
    subject ``vf.{flow}._dlq.>`` is not a data subject, so neither rule can match it.)

    - Returns:
        - ``CleanupObservation``: ``removed`` are confirmed gone (a stream that \
            was already gone counts), ``remaining`` are owned streams whose delete \
            failed, and ``complete`` is False whenever anything remains **or the \
            inventory could not be read** — a listing failure deletes nothing and \
            is not "nothing to delete". Re-running is safe, and is the remedy.
    '''
    js = nc.jetstream()
    try:
        infos = await _list_streams(js)
    except Exception as e:  # noqa: BLE001 — whatever failed, the caller must hear it, not infer an empty inventory
        logger.warning(f'stream listing failed during teardown of flow {flow_id} run {run_id}: {e}')
        return CleanupObservation(complete = False, removed = (), remaining = (),
                                  reason = f'stream listing failed: {e}')
    nodes = frozenset(node_names) if node_names is not None else None
    exact_names = (frozenset(stream_name_for(flow_id, run_id, n) for n in nodes)
                   if nodes is not None else None)
    removed : list[str] = []
    remaining : list[str] = []
    reasons : list[str] = []
    for info in infos:
        name = info.config.name
        if not name or not _owned_stream(info.config, flow_id, run_id, generation, nodes, exact_names):
            continue
        try:
            # nats-py 2.15.0 (nats/js/manager.py:136): returns the server's ``success`` flag.
            ok = await js.delete_stream(name)
        except NotFoundError:
            ok = True      # already gone (a concurrent teardown): absent is the goal
        except Exception as e:  # noqa: BLE001
            remaining.append(name)
            reasons.append(f'{name}: {e}')
            logger.debug(f'delete_stream({name}) failed during teardown: {e}')
            continue
        if ok:
            removed.append(name)
        else:
            remaining.append(name)
            reasons.append(f'{name}: broker answered success=false')
    return CleanupObservation(complete = not remaining, removed = tuple(removed),
                              remaining = tuple(remaining), reason = '; '.join(reasons))

# -- read-back: what a run's streams carry, for admission after provisioning ------------

#: How long ``read_back_streams`` gives the broker, connect included, before every
#: stream it was asked about is reported ``Unknown('timeout')``.
DEFAULT_READ_BACK_TIMEOUT_SECONDS = 30.0

#: Retention policies that keep a message until a consumer acknowledges it — what
#: ``reliable_work`` needs. ``limits`` keeps by count/size/age and evicts regardless.
_ACK_RETENTION = (RetentionPolicy.INTEREST.value, RetentionPolicy.WORK_QUEUE.value)

def profile_mismatches(profile : str, effective : StreamConfig) -> tuple[str, ...]:
    '''
    The ways a stream's *effective* configuration contradicts a messaging profile
    — the semantic half of a read-back, beside the field-level ``_mismatches``.
    A field can match the request and still not carry the profile (the request
    asked for the wrong shape), and a stream nobody here provisioned can carry
    the profile with fields no request ever set (a byte limit instead of a
    message limit), so the two checks are separate. Each finding reads
    ``'<field>: <effective value> <what that means>'``; empty means the stream
    carries the profile.

    ``reliable_work`` needs retention that keeps a message until it is
    acknowledged (INTEREST or WORK_QUEUE) and ``DiscardPolicy.NEW`` at the
    limit — ``OLD`` on a full stream evicts an unacknowledged message, which is
    exactly the loss the profile forbids. ``live_latest`` needs LIMITS retention,
    ``DiscardPolicy.OLD`` (the freshest message wins) and a bound on the backlog
    (``max_msgs``, ``max_bytes`` or ``max_age``), or "latest" is whatever fits on
    the disk. ``durable_control`` and ``replay_archive`` are not stream-level
    promises on JetStream (the planner rejects them before a stream exists), so
    they have no findings here.
    '''
    retention = _plain(effective.retention) or RetentionPolicy.LIMITS.value
    discard = _plain(effective.discard) or DiscardPolicy.OLD.value
    found : list[str] = []
    if profile == RELIABLE_WORK:
        if retention not in _ACK_RETENTION:
            found.append(f'retention: {retention} keeps messages by count, size or age, not until acknowledged')
        if discard != DiscardPolicy.NEW.value:
            found.append(f'discard: {discard} evicts unacknowledged messages when the stream is full')
    elif profile == LIVE_LATEST:
        if retention != RetentionPolicy.LIMITS.value:
            found.append(f'retention: {retention} holds messages for consumers instead of keeping the latest')
        if discard != DiscardPolicy.OLD.value:
            found.append(f'discard: {discard} rejects new publishes at the limit instead of dropping the oldest')
        if not ((effective.max_msgs or 0) > 0 or (effective.max_bytes or 0) > 0 or (effective.max_age or 0) > 0):
            found.append('limits: no max_msgs, max_bytes or max_age bounds the backlog')
    return tuple(found)

async def quiet_error_cb(_e : BaseException) -> None:
    '''Silences the client's per-attempt error logging; the read-back reports the failure itself.'''

def connect_options_for(timeout : float, fail_fast : bool,
                        error_cb : Callable[[BaseException], Awaitable[None]]) -> dict[str, Any]:
    '''
    ``nats.connect`` keyword arguments for a short-lived observation connection.

    ``fail_fast`` (the operator's machine) disables reconnects; note that nats-py
    2.15.0 still cycles the server pool on a refused connect
    (``nats/aio/client.py`` ``_select_next_server`` loops until a server answers,
    and ``max_reconnect_attempts = 0`` never discards one), so the caller bounds
    the connect with ``asyncio.wait_for`` and reads the refusal out of
    ``error_cb`` — which is also where a server-side permissions violation on a
    JetStream API subject arrives (``client.py`` ``_process_err``), while the
    request itself merely times out. Without ``fail_fast`` (an in-cluster
    entrypoint whose broker may still be starting) the client's own retry
    schedule applies: 60 attempts two seconds apart.
    '''
    options : dict[str, Any] = {'error_cb': error_cb}
    if fail_fast:
        options.update(allow_reconnect = False, connect_timeout = timeout, max_reconnect_attempts = 0)
    return options

def observation_failure(error : BaseException, reported : Sequence[str] = ()) -> tuple[str, str]:
    '''
    ``(reason, detail)`` for a failed observation of the broker, in the ``Unknown``
    vocabulary: ``auth`` (credentials refused, or a permissions violation the
    error callback saw while the request timed out), ``unreachable`` (no server
    answered — a refused connect shows up as a timeout with a
    ``ConnectionRefusedError`` in ``reported``), ``timeout`` (the server is there
    and slow), ``malformed`` (an answer the client could not interpret).
    '''
    seen = ' '.join(reported).lower()
    text = f'{type(error).__name__}: {error}'
    if 'authorization' in seen or 'permissions violation' in seen or 'authorization' in str(error).lower():
        return 'auth', text if 'authorization' in str(error).lower() else f'{text} (server reported: {"; ".join(reported)})'
    # Timeouts first: ``TimeoutError`` is an ``OSError`` since Python 3.11, and
    # nats-py's ``TimeoutError`` subclasses ``asyncio.TimeoutError``.
    if isinstance(error, (nats.errors.TimeoutError, asyncio.TimeoutError, TimeoutError)):
        if 'connectionrefused' in seen.replace(' ', '') or 'oserror' in seen or 'gaierror' in seen \
                or 'connect call failed' in seen:
            return 'unreachable', f'{text} (the client reported: {reported[-1]})'
        return 'timeout', text
    if isinstance(error, (nats.errors.NoServersError, OSError)):
        return 'unreachable', text
    if isinstance(error, nats.js.errors.APIError):
        return 'api', text
    if isinstance(error, nats.errors.Error):
        return 'unreachable', text
    return 'malformed', text

async def read_back_streams_async(js : JetStreamContext, flow_id : str, run_id : str, node_names : Sequence[str],
                                  flow_type : str, replicas : int = 1, generation : str | None = None,
                                  reported : Sequence[str] = ()) -> dict[str, VerifiedStream | Unknown]:
    '''
    Read the streams of the named nodes back from a JetStream context, keyed by
    node. Each entry is a ``VerifiedStream`` whose ``requested`` is what
    provisioning asks for under this flow type (``stream_config_for``) and whose
    ``mismatches`` are the requested fields the broker does not honour, or an
    ``Unknown`` saying why nothing can be said: ``missing`` (the stream does not
    exist — a definite observation, kept apart from the unobservable ones), or
    ``timeout`` / ``auth`` / ``unreachable`` / ``api`` / ``malformed``
    (``observation_failure``).

    ``stream_info`` returns the applied configuration (nats-py 2.15.0,
    ``nats/js/manager.py`` ``stream_info`` -> ``api.StreamInfo.config``), and a
    stream that does not exist is ``nats.js.errors.NotFoundError`` (err_code 10059).
    '''
    result : dict[str, VerifiedStream | Unknown] = {}
    for node in node_names:
        name = stream_name_for(flow_id, run_id, node)
        requested = stream_config_for(flow_id, run_id, node, flow_type, generation = generation, replicas = replicas)
        try:
            info = await js.stream_info(name)
        except NotFoundError:
            result[node] = unknown('missing', f'stream {name} does not exist')
        except Exception as e:  # noqa: BLE001 — whatever failed, the caller must hear it, not infer a config
            reason, detail = observation_failure(e, reported)
            result[node] = unknown(reason, f'stream {name}: {detail}')
        else:
            result[node] = VerifiedStream(requested, info.config, _mismatches(requested, info.config, _STREAM_FIELDS))
    return result

def read_back_streams(nats_url : str, flow_id : str, run_id : str, node_names : Sequence[str], flow_type : str,
                      timeout : float = DEFAULT_READ_BACK_TIMEOUT_SECONDS, replicas : int = 1,
                      generation : str | None = None, fail_fast : bool = True) -> dict[str, VerifiedStream | Unknown]:
    '''
    ``read_back_streams_async`` over a short-lived connection of its own — for a
    worker checking its channels before it opens, or the provision entrypoint
    checking what it just created — so nothing reaches into a messenger's live
    connection. A connect that fails within ``timeout`` reports every node as the
    same ``Unknown`` (``observation_failure``); nothing is ever inferred.

    - Arguments:
        - node_names: the nodes whose output streams to read.
        - flow_type / replicas / generation: what provisioning requested, so \
            ``VerifiedStream.mismatches`` is meaningful.
        - timeout: overall bound, connect included.
        - fail_fast: no reconnects (the operator's machine, a worker whose \
            broker is already up); False lets the client retry a broker that \
            is still starting, as the in-cluster provision Job does.
    '''
    reported : list[str] = []

    async def _record(e : BaseException) -> None:
        reported.append(f'{type(e).__name__}: {e}')

    async def _go() -> dict[str, VerifiedStream | Unknown]:
        nc = await asyncio.wait_for(nats.connect(nats_url, **connect_options_for(timeout, fail_fast, _record)), timeout)
        try:
            js = nc.jetstream(timeout = timeout)
            return await asyncio.wait_for(
                read_back_streams_async(js, flow_id, run_id, node_names, flow_type, replicas = replicas,
                                        generation = generation, reported = reported), timeout)
        finally:
            await nc.close()

    try:
        return asyncio.run(_go())
    except Exception as e:  # noqa: BLE001 — the connect or the whole read failed: every node is unobserved
        reason, detail = observation_failure(e, reported)
        return {node: unknown(reason, f'{nats_url}: {detail}') for node in node_names}

def verify_channel_profiles(read_back : Mapping[str, VerifiedStream | Unknown], requests : Sequence[ProfileRequest],
                            *, unknown_is_fatal : bool, where : str, config_mismatches : bool = False) -> None:
    '''
    Bind explicit profile requests to what the broker actually holds. For every
    request whose channel was read back: a stream whose effective configuration
    contradicts the profile (``profile_mismatches``) is a definite rejection; a
    stream that does not exist is a broker problem (provisioning has not run, or
    ran against another run id); a stream that could not be observed is fatal
    only when ``unknown_is_fatal`` — an explicit request must not pass on an
    unread guarantee — and a logged warning otherwise. Requests for channels
    the caller did not read back are not this caller's to judge.

    - Arguments:
        - config_mismatches: also count the field-level ``VerifiedStream.mismatches`` \
            (a ``num_replicas`` below the requested count, a clamped limit). The \
            provision entrypoint knows the full request and passes True; a worker, \
            which does not know the replica count asked for, judges the profile only.
        - where: ``provision`` / ``worker <node>``, for the message.

    - Raises:
        - IncompatibleProfile: a stream definitely does not carry its requested profile.
        - BrokerUnavailable: a requested channel's stream does not exist.
        - UnobservableState: a requested channel could not be read back and ``unknown_is_fatal``.
    '''
    contradictions : list[str] = []
    missing : list[str] = []
    unobservable : list[str] = []
    for request in requests:
        observed = read_back.get(request.channel)
        if observed is None:
            continue
        if isinstance(observed, Unknown):
            line = f'channel {request.channel!r} ({request.profile}): {observed.detail or observed.reason}'
            if observed.reason == 'missing':
                missing.append(line)
            elif unknown_is_fatal:
                unobservable.append(f'{line} [{observed.reason}]')
            else:
                logger.warning(f'{where}: {line} [{observed.reason}]; proceeding, the guarantee is unverified')
            continue
        name = observed.requested.name or request.channel
        if observed.effective is None:
            line = f'channel {request.channel!r} ({request.profile}): stream {name} was not read back'
            if unknown_is_fatal:
                unobservable.append(line)
            else:
                logger.warning(f'{where}: {line}; proceeding, the guarantee is unverified')
            continue
        findings = list(profile_mismatches(request.profile, observed.effective))
        if config_mismatches:
            findings.extend(observed.mismatches)
        if findings:
            contradictions.append(f'channel {request.channel!r} requests {request.profile}, but its stream {name} '
                                  f'carries: ' + '; '.join(findings))
    if contradictions:
        raise IncompatibleProfile(
            f'{where}: the provisioned streams do not carry the requested profiles:\n'
            + '\n'.join(f'  - {c}' for c in contradictions),
            remedy = ('Streams are shaped by the flow type (REALTIME: limits/discard-old, BATCH: interest/'
                      'discard-new); run the flow as the type that provides the profile, or drop the request. '
                      'A stream that pre-dates this run keeps its shape: tear the run down or use a fresh run id. '
                      'A clamped field needs its JetStream account limit raised.'),
            channels = [r.channel for r in requests if any(f'channel {r.channel!r}' in c for c in contradictions)])
    if missing:
        raise BrokerUnavailable(
            f'{where}: requested channels have no stream on the broker:\n' + '\n'.join(f'  - {m}' for m in missing),
            remedy = 'Provision the run first (the provision Job / `videoflow run-local` does this) and check '
                     'that VF_FLOW_ID / VF_RUN_ID match the provisioned run.')
    if unobservable:
        raise UnobservableState(
            f'{where}: requested channels could not be read back from the broker:\n'
            + '\n'.join(f'  - {u}' for u in unobservable),
            remedy = 'Restore access to the broker (reachability, credentials, JetStream API permissions) and '
                     're-run; an explicit profile request does not pass on an unobserved guarantee.')
