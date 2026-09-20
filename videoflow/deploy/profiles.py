'''
Cluster profiles: the per-cluster values of a deploy, kept in one file instead
of eight flags (or eight environment variables) per cluster.

A laptop cluster needs none of this — the flavor is detected, the image is
side-loaded, a dev broker is provisioned. A shared or multi-node cluster has a
handful of properties that are the same for every solution and every run on
it: the namespace, the registry the nodes pull from (and how to push to it),
the RWX claim and the directory it is served at, where the caches live, the
PriorityClass, the GPU nodes that are yours. Those belong with the cluster, so
they live under its name, matched to the kubectl context they describe::

    # ~/.config/videoflow/clusters.yaml  (or $VF_CLUSTERS_FILE, or --clusters-file)
    docker:                          # machine-level; every docker build / run
      build_args: '--build-arg http_proxy=http://proxy:3128'
      run_args: ''
    clusters:
      lab:                           # `--cluster lab`, or matched by `context`
        context: default             # the kubectl context this profile belongs to
        namespace: videoflow
        registry: 10.0.0.1:5000
        push_tool: crane
        mount_pvc: ['work-share:/shared/videoflow']
        mount_home: /shared/videoflow/home
        priority_class: cluster-batch
        gpu_nodes: [gpu-01]          # optional
        nats: null                   # optional bring-your-own broker (+ blob_redis_url)

Every cluster key is a ``deploy`` flag with underscores (``registry`` is
``--registry``); lists stand for repeatable flags. ``deploy`` takes all of
them, ``teardown`` the ones it has (``namespace``, ``nats``, ``kubectl``,
``gpu_mode``, ``broker_profile``), ``run-local`` only the ``docker`` section.
Precedence, everywhere: an explicit flag, then an environment variable (the
``docker`` keys have ``VF_DOCKER_BUILD_ARGS`` / ``VF_DOCKER_RUN_ARGS``), then
the profile, then the built-in default — nothing in the file overrides what
was typed.
'''
from __future__ import absolute_import, division, print_function

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..core.errors import ConfigError
from .broker_profiles import BROKER_PROFILE_NAMES
from .build import BUILD_ARGS_ENV, PUSH_TOOLS, RUN_ARGS_ENV
from .images import IMAGE_PULL_POLICIES

PROFILES_FILE_ENV = 'VF_CLUSTERS_FILE'
PROFILES_FILE_NAME = 'clusters.yaml'

#: The cluster keys, each a deploy flag; ``context`` is the selector, not a flag.
LIST_KEYS = ('mount_pvc', 'mount', 'gpu_nodes')
INT_KEYS = ('broker_replicas',)
CLUSTER_KEYS = ('context', 'namespace', 'registry', 'push_tool', 'mount_pvc', 'mount', 'mount_home',
                'priority_class', 'gpu_nodes', 'gpu_runtime_class', 'gpu_mode', 'broker_profile',
                'broker_replicas', 'broker_storage_class', 'image_pull_policy', 'nats', 'blob_redis_url',
                'kubectl')
DOCKER_KEYS = ('build_args', 'run_args')
#: Which profile keys each command consumes (``run-local`` takes only the docker
#: section). ``teardown`` never takes ``broker_profile`` from the file: the
#: Service it deletes records the profile that rendered it, which is the truth
#: even after the file moved on to another profile.
COMMAND_KEYS = {
    'deploy': tuple(k for k in CLUSTER_KEYS if k != 'context'),
    'teardown': ('namespace', 'nats', 'kubectl', 'gpu_mode'),
    'run-local': (),
}
_CHOICES = {'push_tool': PUSH_TOOLS, 'broker_profile': tuple(BROKER_PROFILE_NAMES),
            'image_pull_policy': IMAGE_PULL_POLICIES}


@dataclass
class Profiles:
    '''The parsed file: where it came from, the docker section, the clusters by name.'''
    path : Optional[str] = None
    docker : Dict[str, str] = field(default_factory = dict)
    clusters : Dict[str, Dict[str, Any]] = field(default_factory = dict)


def default_profiles_path() -> str:
    '''``$VF_CLUSTERS_FILE``, else ``$XDG_CONFIG_HOME/videoflow/clusters.yaml`` (``~/.config`` by default).'''
    explicit = os.environ.get(PROFILES_FILE_ENV)
    if explicit:
        return os.path.expanduser(explicit)
    base = os.environ.get('XDG_CONFIG_HOME') or os.path.join(os.path.expanduser('~'), '.config')
    return os.path.join(base, 'videoflow', PROFILES_FILE_NAME)


def _cluster(name : str, raw : Any, path : str) -> Dict[str, Any]:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError(f'cluster profile {name!r} in {path} must be a mapping, got {type(raw).__name__}.',
                          remedy = 'Write it as `name:` followed by indented `key: value` lines.')
    unknown = sorted(set(raw) - set(CLUSTER_KEYS))
    if unknown:
        raise ConfigError(f'cluster profile {name!r} in {path} has unknown key(s): {", ".join(unknown)}.',
                          remedy = f'Valid keys (each a deploy flag): {", ".join(CLUSTER_KEYS)}.')
    profile : Dict[str, Any] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if key in LIST_KEYS:
            values = value if isinstance(value, list) else [value]
            if not all(isinstance(v, (str, int, float)) for v in values):
                raise ConfigError(f'cluster profile {name!r}: {key} must be a list of strings.',
                                  remedy = f"Example: {key}: ['{'work-share:/shared' if key == 'mount_pvc' else 'value'}']")
            profile[key] = [str(v) for v in values]
            continue
        if isinstance(value, (list, dict)):
            raise ConfigError(f'cluster profile {name!r}: {key} must be a single value, got {type(value).__name__}.',
                              remedy = f'Write `{key}: <value>`; only {", ".join(LIST_KEYS)} take lists.')
        if key in INT_KEYS:
            # argparse defaults bypass the flag's type: hand the parser the int it expects.
            if isinstance(value, bool) or not isinstance(value, int) and not (isinstance(value, str) and value.strip().lstrip('-').isdigit()):
                raise ConfigError(f'cluster profile {name!r}: {key} must be an integer, got {value!r}.',
                                  remedy = f'Write `{key}: 1` (the NATS servers of the durable profile).')
            profile[key] = int(value)
            continue
        value = str(value)
        if key in _CHOICES and value not in _CHOICES[key]:
            raise ConfigError(f'cluster profile {name!r}: {key} is {value!r}.',
                              remedy = f'Use one of: {", ".join(_CHOICES[key])}.')
        profile[key] = value
    return profile


def load_profiles(path : Optional[str] = None) -> Profiles:
    '''
    The clusters file, validated. A missing file is not an error — most machines
    have none — unless ``path`` was given explicitly.

    - Raises:
        - ConfigError: the file is malformed, or a profile has an unknown key or \
            a value of the wrong shape or outside a flag's choices.
    '''
    explicit = path is not None
    path = path or default_profiles_path()
    if not os.path.isfile(path):
        if explicit:
            raise ConfigError(f'Clusters file not found: {path}',
                              remedy = 'Create it (see the videoflow README, "Multi-node clusters"), or drop --clusters-file.')
        return Profiles()
    import yaml  # optional dep (deploy extra)
    with open(path) as f:
        try:
            raw = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigError(f'Clusters file {path} is not valid YAML: {e}') from e
    if not isinstance(raw, dict):
        raise ConfigError(f'Clusters file {path} must be a mapping with `docker` and `clusters` sections.')
    unknown = sorted(set(raw) - {'docker', 'clusters'})
    if unknown:
        raise ConfigError(f'Clusters file {path} has unknown top-level key(s): {", ".join(unknown)}.',
                          remedy = 'Only `docker` and `clusters` are read; cluster profiles go under `clusters`.')
    docker_raw = raw.get('docker') or {}
    if not isinstance(docker_raw, dict) or set(docker_raw) - set(DOCKER_KEYS):
        raise ConfigError(f'Clusters file {path}: the docker section takes only {", ".join(DOCKER_KEYS)}.')
    docker = {k: str(v) for k, v in docker_raw.items() if v is not None}
    clusters_raw = raw.get('clusters') or {}
    if not isinstance(clusters_raw, dict):
        raise ConfigError(f'Clusters file {path}: `clusters` must map profile names to their settings.')
    clusters = {str(name): _cluster(str(name), body, path) for name, body in clusters_raw.items()}
    return Profiles(path = path, docker = docker, clusters = clusters)


def select_profile(profiles : Profiles, name : Optional[str],
                   current_context : Callable[[], str]) -> Optional[tuple]:
    '''
    ``(name, profile)`` for ``--cluster NAME``, else for the profile whose
    ``context`` is the current kubectl context (asked only when some profile
    names one), else None.

    - Raises:
        - ConfigError: ``name`` is not in the file.
    '''
    if name is not None:
        if name not in profiles.clusters:
            known = ', '.join(sorted(profiles.clusters)) or 'none'
            raise ConfigError(f'No cluster profile named {name!r} in {profiles.path or default_profiles_path()} '
                              f'(known: {known}).',
                              remedy = 'Add it under `clusters:` in that file, or pass the flags directly.')
        return name, profiles.clusters[name]
    with_context = {n: p for n, p in profiles.clusters.items() if p.get('context')}
    if not with_context:
        return None
    context = current_context()
    for candidate, profile in with_context.items():
        if profile['context'] == context:
            return candidate, profile
    return None


def command_defaults(profile : Dict[str, Any], command : str) -> Dict[str, Any]:
    '''The argparse defaults ``command`` takes from ``profile`` (its dests, with lists joined where the flag is a string).'''
    defaults : Dict[str, Any] = {}
    for key in COMMAND_KEYS.get(command, ()):
        if key not in profile:
            continue
        value = profile[key]
        defaults[key] = ','.join(value) if key == 'gpu_nodes' else value
    return defaults


def apply_docker_env(profiles : Profiles, environ : Any = os.environ) -> List[str]:
    '''
    Exports the ``docker`` section as ``VF_DOCKER_BUILD_ARGS`` / ``VF_DOCKER_RUN_ARGS``
    for variables not already set (the environment wins). Returns what was set.
    '''
    applied = []
    for key, env_name in (('build_args', BUILD_ARGS_ENV), ('run_args', RUN_ARGS_ENV)):
        value = profiles.docker.get(key)
        if value and not environ.get(env_name):
            environ[env_name] = value
            applied.append(env_name)
    return applied
