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
'''
from __future__ import absolute_import, division, print_function

import logging
import re

from nats.js.api import ConsumerConfig, DiscardPolicy, RetentionPolicy, StreamConfig

from ..core.constants import REALTIME

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

def dlq_stream_name(flow_id : str, run_id : str) -> str:
    return f'vf-{sanitize(flow_id)}-{sanitize(run_id)}-dlq'

def dlq_subject_for(flow_id : str, run_id : str, node_name : str) -> str:
    return f'vf.{sanitize(flow_id)}.{sanitize(run_id)}._dlq.{sanitize(node_name)}'

def stream_label_selector(flow_id : str, run_id : str) -> str:
    '''Prefix shared by every stream of one run — used for teardown by name prefix.'''
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

def stream_config_for(flow_id : str, run_id : str, node_name : str, flow_type : str,
                    subjects = None, realtime_buffer : int = DEFAULT_REALTIME_BUFFER,
                    batch_max_msgs : int = DEFAULT_BATCH_MAX_MSGS) -> StreamConfig:
    name = stream_name_for(flow_id, run_id, node_name)
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
        )
    return StreamConfig(
        name = name, subjects = subjects,
        retention = RetentionPolicy.INTEREST,
        max_msgs = batch_max_msgs,
        discard = DiscardPolicy.NEW,
        duplicate_window = DUPLICATE_WINDOW_SECONDS,
    )

def dlq_stream_config(flow_id : str, run_id : str) -> StreamConfig:
    return StreamConfig(
        name = dlq_stream_name(flow_id, run_id),
        subjects = [f'vf.{sanitize(flow_id)}.{sanitize(run_id)}._dlq.>'],
        retention = RetentionPolicy.LIMITS,
        discard = DiscardPolicy.OLD,
        max_age = 7 * 24 * 3600,  # keep dead-lettered messages for a week
    )

#: Default number of times a BATCH message is *retried* (redelivered) after the
#: first delivery attempt before it is dead-lettered. max_deliver = retries + 1.
DEFAULT_MAX_RETRIES = 3

def max_deliver_for(flow_type : str, max_retries : int = DEFAULT_MAX_RETRIES) -> int:
    '''
    REALTIME never redelivers (freshest wins; a failed frame is dropped), so
    max_deliver is 1. BATCH redelivers up to ``max_retries`` times, then the
    message is dead-lettered.
    '''
    if flow_type == REALTIME:
        return 1
    return max_retries + 1

def consumer_config_for(flow_id : str, run_id : str, consumer_node_name : str,
                        parent_node_name : str, ack_wait : int = 60, max_deliver : int = 1,
                        max_ack_pending : int = 8) -> ConsumerConfig:
    '''
    Durable pull-consumer config for one (child, parent) edge. Filters to the
    parent's *data* subject so EOS markers (on the ``_eos`` subject of the same
    stream) are handled by a separate per-replica consumer instead. ``max_deliver``
    is 1 for REALTIME (no redelivery — freshest wins) and ``retries + 1`` for BATCH;
    ``max_ack_pending`` bounds how many un-acked messages the broker will hand out
    before it stops delivering (this is the server-side half of prefetch bounding).
    '''
    durable = durable_name_for(consumer_node_name, parent_node_name)
    return ConsumerConfig(
        durable_name = durable,
        filter_subject = subject_for(flow_id, run_id, parent_node_name),
        ack_wait = ack_wait,
        max_deliver = max_deliver,
        max_ack_pending = max_ack_pending,
    )

def eos_consumer_config(flow_id : str, run_id : str, consumer_node_name : str,
                        parent_node_name : str, instance_id : str,
                        inactive_threshold : int = 3600) -> ConsumerConfig:
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
    )

def eos_anchor_config(flow_id : str, run_id : str, node_name : str) -> ConsumerConfig:
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
    )

# -- provisioning ----------------------------------------------------------

async def _ensure_stream(js, config : StreamConfig) -> None:
    try:
        await js.add_stream(config)
    except Exception as e:  # noqa: BLE001 — nats raises a generic error on conflict
        msg = str(e)
        if 'already in use' in msg or 'name already in use' in msg:
            # Exists already; best-effort bring config in line (retention changes
            # are rejected by JetStream, but run-scoped names make that unreachable).
            try:
                await js.update_stream(config)
            except Exception:
                logger.debug(f'stream {config.name} exists; update skipped')
        else:
            raise

async def _ensure_consumer(js, stream_name : str, config : ConsumerConfig) -> None:
    try:
        await js.add_consumer(stream_name, config)
    except Exception as e:  # noqa: BLE001
        logger.debug(f'add_consumer({stream_name}, {config.durable_name}) skipped: {e}')

async def provision_flow(nc, specs, flow_id : str, run_id : str, flow_type : str,
                        max_retries : int = DEFAULT_MAX_RETRIES, ack_wait : int = 60,
                        max_ack_pending : int = 8) -> None:
    '''
    Idempotently create every stream and durable consumer a flow needs, before any
    worker publishes. Required for BATCH: under INTEREST retention a message
    published with no registered consumer interest is discarded immediately, so
    the durables must exist first.

    - Arguments:
        - nc: a connected ``nats`` client.
        - specs: list of ``videoflow.compiler.NodeSpec``.
    '''
    js = nc.jetstream()
    by_name = {spec.name: spec for spec in specs}
    max_deliver = max_deliver_for(flow_type, max_retries)

    # 1. One stream per node — plus, for any node something consumes from, an
    #    interest *anchor* on its EOS subject. The per-process EOS durables are
    #    created only in worker setup, so without the anchor an EOS published
    #    before a slow-starting child registers loses the interest race and is
    #    discarded — leaving the child waiting for EOS forever (see eos_anchor_config).
    for spec in specs:
        await _ensure_stream(js, stream_config_for(flow_id, run_id, spec.name, flow_type))
        if getattr(spec, 'has_children', True):
            await _ensure_consumer(js, stream_name_for(flow_id, run_id, spec.name),
                                   eos_anchor_config(flow_id, run_id, spec.name))

    # 2. DLQ stream — where messages that exhaust their retries land.
    await _ensure_stream(js, dlq_stream_config(flow_id, run_id))

    # 3. Durable consumers per (child, parent) edge, on the parent's stream. A
    #    partitioned child gets one durable *per replica* (broadcast + client-side
    #    ownership); everything else gets one shared durable (competing consumers).
    for spec in specs:
        partition_by = getattr(spec, 'partition_by', None)
        nb_tasks = getattr(spec, 'nb_tasks', 1)
        for parent_name in spec.parents:
            if parent_name not in by_name:
                continue
            parent_stream = stream_name_for(flow_id, run_id, parent_name)
            base = consumer_config_for(flow_id, run_id, spec.name, parent_name, ack_wait = ack_wait,
                                    max_deliver = max_deliver, max_ack_pending = max_ack_pending)
            if partition_by and nb_tasks > 1:
                for replica_id in range(nb_tasks):
                    cfg = ConsumerConfig(
                        durable_name = partitioned_durable_name_for(spec.name, parent_name, replica_id),
                        filter_subject = base.filter_subject, ack_wait = base.ack_wait,
                        max_deliver = base.max_deliver, max_ack_pending = base.max_ack_pending)
                    await _ensure_consumer(js, parent_stream, cfg)
            else:
                await _ensure_consumer(js, parent_stream, base)

async def provision_flow_connect(nats_url : str, specs, flow_id : str, run_id : str,
                                flow_type : str, **kwargs) -> None:
    '''Connects to NATS, provisions, and drains — a self-contained entrypoint.'''
    import nats
    nc = await nats.connect(nats_url)
    try:
        await provision_flow(nc, specs, flow_id, run_id, flow_type, **kwargs)
    finally:
        await nc.drain()

def provision_flow_sync(nats_url : str, specs, flow_id : str, run_id : str, flow_type : str, **kwargs) -> None:
    '''Synchronous wrapper for callers outside an event loop (the local engine, the init entrypoint).'''
    import asyncio
    asyncio.run(provision_flow_connect(nats_url, specs, flow_id, run_id, flow_type, **kwargs))

async def delete_run_streams(nc, flow_id : str, run_id : str) -> None:
    '''Best-effort teardown: delete every stream belonging to this run.'''
    js = nc.jetstream()
    prefix = stream_label_selector(flow_id, run_id)
    dlq = dlq_stream_name(flow_id, run_id)
    try:
        names = await js.streams_info()
    except Exception:
        return
    for info in names:
        name = info.config.name
        if name and (name.startswith(prefix) or name == dlq):
            try:
                await js.delete_stream(name)
            except Exception:
                logger.debug(f'delete_stream({name}) failed during teardown')
