'''
Broker profiles: the sizing knobs ``videoflow deploy`` provisions its in-cluster
NATS and Redis with when the user brings no broker of their own.

``deploy.infra`` renders the dev-grade single-replica Deployments this project
has always shipped, and those must stay byte-identical — the k8s integration
tests and every cluster that already runs them depend on that shape. The
durability work (multi-node JetStream, file-store persistence, an append-only
Redis) needs a second shape, and threading six positional knobs through
``ensure_infra``/``nats_manifests``/``redis_manifests`` would have turned every
call site into a lie waiting to happen. So the knobs travel as two small records
this module owns, with one constructor per named profile:

  - ``BrokerProfile.dev()``      — today's render: one server, emptyDir, no
                                   persistence. ``profile = None`` everywhere in
                                   ``deploy.infra`` means exactly this.
  - ``BrokerProfile.durable()``  — a NATS StatefulSet with ``cluster { routes }``
                                   and a PVC per pod, so a stream with
                                   ``jetstream_replicas`` copies survives a pod
                                   and a node.
  - ``RedisProfile.dev()``       — the transport-only cache (no persistence,
                                   volatile-lru).
  - ``RedisProfile.durable()``   — append-only file on a PVC, never evicts.

The records are dataclasses rather than dicts because we own their shape (the
Kubernetes objects they become stay dicts, per ``deploy.manifests``). Validation
happens at construction so a bad profile fails at the CLI, before any manifest is
rendered — a JetStream cluster with an even replica count or a stream replicated
more times than there are servers cannot elect a leader, and the only symptom in
the cluster would be a provision Job that never completes.

Priority: ``priority_class`` lands on every pod a profile renders. The
``--priority-class`` flag sets it on the workers and on the infra alike, so a
deploy told to yield to higher-priority work yields *everything* it created.
'''
from __future__ import absolute_import, division, print_function

from dataclasses import dataclass

from ..core.errors import ConfigError

#: The profile names ``--broker-profile`` accepts, in the order the help shows them.
BROKER_PROFILE_NAMES = ('dev', 'durable')

#: What ``RedisProfile.persistence`` may be. ``none`` renders ``--appendonly no``
#: with RDB snapshots off (transport, not storage); ``appendonly`` renders an AOF
#: on a PersistentVolumeClaim.
REDIS_PERSISTENCE_MODES = ('none', 'appendonly')

#: Redis ``maxmemory-policy`` values, as the server spells them.
REDIS_EVICTION_POLICIES = ('noeviction', 'volatile-lru', 'allkeys-lru', 'volatile-lfu',
                           'allkeys-lfu', 'volatile-random', 'allkeys-random', 'volatile-ttl')

#: JetStream caps stream replication at five copies regardless of cluster size.
_MAX_JETSTREAM_REPLICAS = 5

#: The default StorageClass the durable profiles claim from. ``local-path`` is
#: what k3s (and kind) ship; a cluster without it names its own through
#: ``--broker-storage-class``.
DEFAULT_STORAGE_CLASS = 'local-path'

@dataclass(frozen = True)
class BrokerProfile:
    '''
    How the auto-provisioned NATS is shaped.

    - Arguments:
        - replicas: NATS server pods. ``1`` renders the Deployment of \
            ``k8s/nats.yaml``; more renders a StatefulSet whose pods route to each \
            other over a headless Service (``cluster { routes }``).
        - jetstream_replicas: copies each stream should keep — what the provisioner \
            asks JetStream for. At most ``replicas`` and at most 5.
        - storage_class: StorageClass of the per-pod PersistentVolumeClaim when \
            ``persistence`` is on; ``None`` takes the cluster default.
        - storage_size: size of that claim (a Kubernetes quantity, ``10Gi``).
        - persistence: keep the JetStream file store on a PersistentVolumeClaim \
            instead of an emptyDir that dies with the pod.
        - max_file_store: the ``jetstream { max_file_store }`` server limit; keep \
            it under ``storage_size`` when persisting.
        - priority_class: ``priorityClassName`` for the NATS pods, or none.
    '''
    replicas : int = 1
    jetstream_replicas : int = 1
    storage_class : str | None = None
    storage_size : str = '10Gi'
    persistence : bool = False
    max_file_store : str = '10GB'
    priority_class : str | None = None

    def __post_init__(self) -> None:
        if self.replicas < 1:
            raise ConfigError(f'a broker profile needs at least one NATS replica, got {self.replicas}.',
                              remedy = 'Pass --broker-replicas 1 (dev) or 3 (durable).')
        if not 1 <= self.jetstream_replicas <= min(self.replicas, _MAX_JETSTREAM_REPLICAS):
            raise ConfigError(
                f'jetstream_replicas={self.jetstream_replicas} cannot be satisfied by '
                f'{self.replicas} NATS server(s) (JetStream allows 1..{_MAX_JETSTREAM_REPLICAS} '
                f'copies, never more than there are servers).',
                remedy = 'Raise --broker-replicas or lower the stream replication.')
        if self.replicas > 1 and self.replicas % 2 == 0:
            raise ConfigError(
                f'a NATS cluster of {self.replicas} servers has no majority quorum for JetStream '
                f'leader election.',
                remedy = 'Use an odd replica count: --broker-replicas 3 (or 5).')
        if not self.storage_size or not self.max_file_store:
            raise ConfigError('storage_size and max_file_store must be non-empty quantities.',
                              remedy = "Use Kubernetes/NATS quantities such as '10Gi' and '10GB'.")

    @property
    def stateful(self) -> bool:
        '''
        Whether the profile renders a StatefulSet. True as soon as the pods need
        a stable identity: a persistent claim per pod, or route peers that must
        find each other by a predictable DNS name.
        '''
        return self.persistence or self.replicas > 1

    @classmethod
    def dev(cls, priority_class : str | None = None) -> 'BrokerProfile':
        '''Today's single-replica, emptyDir NATS — what ``profile = None`` means.'''
        return cls(priority_class = priority_class)

    @classmethod
    def durable(cls, replicas : int = 3, storage_class : str | None = DEFAULT_STORAGE_CLASS,
                priority_class : str | None = None) -> 'BrokerProfile':
        '''
        A JetStream cluster whose streams keep up to three copies on persistent
        volumes. ``jetstream_replicas`` is ``min(replicas, 3)``: three copies is
        the standard JetStream deployment and five buys little at this scale.
        '''
        return cls(replicas = replicas, jetstream_replicas = min(replicas, 3),
                   storage_class = storage_class, persistence = True,
                   priority_class = priority_class)

@dataclass(frozen = True)
class RedisProfile:
    '''
    How the auto-provisioned Redis (the large-payload blob store) is shaped.

    - Arguments:
        - persistence: ``'none'`` (RDB and AOF off; the store is transport) or \
            ``'appendonly'`` (an AOF under ``/data`` on a PersistentVolumeClaim).
        - eviction: the ``maxmemory-policy``. The dev profile evicts TTL'd blobs \
            oldest-first under pressure (every videoflow key carries a TTL, \
            PROTOCOL.md BLOB-7); a durable store refuses writes instead.
        - storage_class: StorageClass of the claim when persisting; ``None`` takes \
            the cluster default.
        - storage_size: size of that claim.
        - priority_class: ``priorityClassName`` for the Redis pod, or none.
    '''
    persistence : str = 'none'
    eviction : str = 'noeviction'
    storage_class : str | None = None
    storage_size : str = '10Gi'
    priority_class : str | None = None

    def __post_init__(self) -> None:
        if self.persistence not in REDIS_PERSISTENCE_MODES:
            raise ConfigError(
                f'unknown Redis persistence mode {self.persistence!r}. Known modes: '
                f'{", ".join(REDIS_PERSISTENCE_MODES)}.',
                remedy = "Use persistence = 'none' for a transport-only cache or 'appendonly' "
                         'for a store that survives a restart.')
        if self.eviction not in REDIS_EVICTION_POLICIES:
            raise ConfigError(
                f'unknown Redis eviction policy {self.eviction!r}. Known policies: '
                f'{", ".join(REDIS_EVICTION_POLICIES)}.',
                remedy = "Spell it the way redis-server's --maxmemory-policy does.")
        if not self.storage_size:
            raise ConfigError('storage_size must be a non-empty Kubernetes quantity.',
                              remedy = "Use a quantity such as '10Gi'.")

    @property
    def stateful(self) -> bool:
        '''Whether the profile keeps its data on a PersistentVolumeClaim.'''
        return self.persistence != 'none'

    @classmethod
    def dev(cls, priority_class : str | None = None) -> 'RedisProfile':
        '''Today's transport-only cache: no persistence, volatile-lru eviction.'''
        return cls(persistence = 'none', eviction = 'volatile-lru', priority_class = priority_class)

    @classmethod
    def durable(cls, storage_class : str | None = DEFAULT_STORAGE_CLASS,
                priority_class : str | None = None) -> 'RedisProfile':
        '''An append-only Redis on a claim that never evicts a blob.'''
        return cls(persistence = 'appendonly', eviction = 'noeviction',
                   storage_class = storage_class, priority_class = priority_class)

def broker_profiles(name : str, replicas : int | None = None, storage_class : str | None = None,
                    priority_class : str | None = None) -> tuple[BrokerProfile, RedisProfile]:
    '''
    The ``(BrokerProfile, RedisProfile)`` pair a profile *name* denotes — the
    one lookup ``--broker-profile`` goes through, so the CLI and any programmatic
    caller resolve a name the same way.

    - Arguments:
        - name: one of ``BROKER_PROFILE_NAMES``.
        - replicas: NATS replica override (``--broker-replicas``); durable only.
        - storage_class: claim StorageClass override (``--broker-storage-class``); \
            durable only.
        - priority_class: ``priorityClassName`` for every infra pod.

    - Raises:
        - ConfigError: an unknown name, or a durable-only override given with \
            the dev profile (which has nothing to apply it to).
    '''
    if name not in BROKER_PROFILE_NAMES:
        raise ConfigError(f'unknown broker profile {name!r}. Known profiles: '
                          f'{", ".join(BROKER_PROFILE_NAMES)}.',
                          remedy = 'Pass --broker-profile dev or --broker-profile durable.')
    if name == 'dev':
        if replicas is not None or storage_class is not None:
            raise ConfigError('--broker-replicas and --broker-storage-class only apply to the '
                              'durable broker profile; the dev profile is one emptyDir server.',
                              remedy = 'Add --broker-profile durable, or drop the override.')
        return BrokerProfile.dev(priority_class), RedisProfile.dev(priority_class)
    storage = DEFAULT_STORAGE_CLASS if storage_class is None else storage_class
    return (BrokerProfile.durable(replicas = 3 if replicas is None else replicas,
                                  storage_class = storage, priority_class = priority_class),
            RedisProfile.durable(storage_class = storage, priority_class = priority_class))
