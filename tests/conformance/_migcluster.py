'''
A stateful kubectl fake for the managed-MIG lifecycle cases (ALLOC-003/005/006/
011/033): the GPU Operator's objects as live state, with the API server's
compare-and-swap rules and a MIG manager that reacts *late*.

``support_kubectl.FakeKubectl`` already serves nodes with resourceVersion CAS.
This adds what ``videoflow.deploy.gpu.MixGpu`` also writes and reads:

- the published ``videoflow-mig-parted-config`` ConfigMap (create / replace
  with ``resourceVersion`` / annotate / delete), so two flows' entries merge
  and a stale write conflicts;
- the ClusterPolicy (get / merge-patch / annotate), so the pointer and its
  restore record are real state;
- the operator's base ConfigMap, the MIG manager pods and DaemonSet (canned);
- the MIG manager itself: a node whose ``nvidia.com/mig.config`` label changed
  keeps its **old** ``mig.config.state`` and allocatable until ``tick()`` (or
  ``manager_latency`` reads later) — the stale-status interval ALLOC-003 is about
  — then reports ``success`` and advertises the entry's geometry, or ``failed``
  for a node in ``failing``.

Everything is in-process and observable: ``configmap_entries()``,
``pointer()``, ``labels(node)``, ``calls``.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess
from typing import Any, Dict, List, Optional

from support_kubectl import FakeKubectl

from videoflow.deploy import gpu

BASE_CONFIG = ('version: v1\n'
               'mig-configs:\n'
               '  all-disabled:\n'
               '    - devices: all\n'
               '      mig-enabled: false\n')
OPERATOR_NAMESPACE = 'gpu-operator'
POLICY_NAME = 'cluster-policy'
DEFAULT_CONFIG_NAME = 'default-mig-parted-config'


def _daemonset() -> str:
    return json.dumps({'items': [{
        'metadata': {'generation': 2},
        'spec': {'template': {'spec': {'volumes': [{'name': 'mig-parted-config',
                                                    'configMap': {'name': gpu.MIG_CONFIGMAP_NAME}}]}}},
        'status': {'observedGeneration': 2, 'desiredNumberScheduled': 1, 'updatedNumberScheduled': 1, 'numberReady': 1},
    }]})


class MigCluster(FakeKubectl):
    '''See the module docstring. ``nodes``: name -> {'labels', 'annotations', 'allocatable', 'cards'}.'''
    def __init__(self, nodes : Dict[str, Dict[str, Any]], manager_latency : int = 3) -> None:
        super().__init__({}, nodes = {n: {k: v for k, v in s.items() if k != 'cards'} for n, s in nodes.items()})
        self.cards = {n: int(s.get('cards', 2)) for n, s in nodes.items()}
        for name, state in self.nodes.items():
            state['labels'].setdefault(gpu.MIG_CONFIG_STATE_LABEL, 'success')
            state['allocatable'].setdefault('nvidia.com/gpu', str(self.cards[name]))
        self.configmap : Optional[Dict[str, Any]] = None
        self.configmap_rv = 10
        self.policy : Dict[str, Any] = {'metadata': {'name': POLICY_NAME, 'annotations': {}},
                                        'spec': {'migManager': {'config': {'name': DEFAULT_CONFIG_NAME}}}}
        self.manager_latency = manager_latency
        self.failing_nodes : set = set()
        #: Running pods holding GPU units, as the occupancy read lists them: (node, resource, units).
        self.pods : List[tuple] = []
        self.pending : Dict[str, int] = {}          # node -> reads left before the manager reacts
        self.mutation_log : List[str] = []

    # -- what the tests read ------------------------------------------------------

    def configmap_entries(self) -> List[str]:
        if self.configmap is None:
            return []
        import yaml
        parsed = yaml.safe_load(self.configmap['data']['config.yaml']) or {}
        return sorted((parsed.get('mig-configs') or {}).keys())

    def pointer(self) -> str:
        return self.policy['spec']['migManager']['config']['name']

    def tick(self) -> None:
        '''The MIG manager reacts to every pending label change now.'''
        for node in list(self.pending):
            self._apply_manager(node)
        self.pending.clear()

    # -- the manager -----------------------------------------------------------------

    def _apply_manager(self, node : str) -> None:
        state = self.nodes[node]
        entry = state['labels'].get(gpu.MIG_CONFIG_LABEL, '')
        if node in self.failing_nodes:
            state['labels'][gpu.MIG_CONFIG_STATE_LABEL] = 'failed'
            return
        geometry = self._entry_geometry(entry)
        state['allocatable'] = {k: v for k, v in state['allocatable'].items() if not k.startswith('nvidia.com/')}
        if geometry is None:
            state['allocatable']['nvidia.com/gpu'] = str(self.cards[node])
        else:
            for resource, count in geometry.items():
                state['allocatable'][resource] = str(count)
        state['labels'][gpu.MIG_CONFIG_STATE_LABEL] = 'success'

    def _entry_geometry(self, entry : str) -> Optional[Dict[str, int]]:
        '''Advertised resources for a mig-parted entry name; None for a disabled/absent one.'''
        if not entry or self.configmap is None:
            return None
        import yaml
        parsed = yaml.safe_load(self.configmap['data']['config.yaml']) or {}
        spec = (parsed.get('mig-configs') or {}).get(entry)
        if not spec:
            return None
        resources : Dict[str, int] = {}
        for card in spec:
            if card.get('mig-enabled'):
                for profile, count in (card.get('mig-devices') or {}).items():
                    resources[f'nvidia.com/mig-{profile}'] = resources.get(f'nvidia.com/mig-{profile}', 0) + int(count)
            else:
                devices = card.get('devices')
                n = self.cards[next(iter(self.cards))] if devices == 'all' else len(devices)
                resources['nvidia.com/gpu'] = resources.get('nvidia.com/gpu', 0) + n
        return resources or None

    # -- kubectl -------------------------------------------------------------------------

    def __call__(self, cmd : List[str], **kwargs : Any) -> subprocess.CompletedProcess:
        args = list(cmd[1:])
        joined = ' '.join(cmd)
        for needle in self.failing:
            if needle in joined:
                self.calls.append((list(cmd), kwargs.get('input')))
                return subprocess.CompletedProcess(cmd, 1, '', 'Error from server (Forbidden): denied by test')
        served = self._serve(args, cmd, kwargs.get('input'))
        if served is not None:
            self.calls.append((list(cmd), kwargs.get('input')))
            return served
        return super().__call__(cmd, **kwargs)

    def _serve(self, args : List[str], cmd : List[str], stdin : Optional[str]) -> Optional[subprocess.CompletedProcess]:
        ok = lambda out = '': subprocess.CompletedProcess(cmd, 0, out, '')   # noqa: E731
        fail = lambda err: subprocess.CompletedProcess(cmd, 1, '', err)     # noqa: E731
        # The state-label poll: the manager's latency is counted in reads.
        if args[:2] == ['get', 'node'] and any(a.startswith('jsonpath={.metadata.labels.nvidia') for a in args):
            node = args[2]
            self._maybe_react(node)
            return ok(self.nodes[node]['labels'].get(gpu.MIG_CONFIG_STATE_LABEL, ''))
        if args[:2] == ['get', 'node'] and len(args) >= 5 and args[3:5] == ['-o', 'json']:
            self._maybe_react(args[2])
            return None                                     # FakeKubectl serves the doc
        if args[:2] == ['label', 'node']:
            node = args[2]
            before = dict(self.nodes[node]['labels'])
            result = super()._serve_node_state(cmd)
            after = self.nodes[node]['labels']
            if result is not None and result.returncode == 0 and before.get(gpu.MIG_CONFIG_LABEL) != after.get(gpu.MIG_CONFIG_LABEL):
                self.pending[node] = self.manager_latency  # a change: the manager will react, later
                self.mutation_log.append(f'label {node} {gpu.MIG_CONFIG_LABEL}={after.get(gpu.MIG_CONFIG_LABEL)}')
            return result
        if args[:2] == ['get', 'pods'] and '-l' in args and 'app=nvidia-mig-manager' in args:
            return ok(f'{OPERATOR_NAMESPACE} mig-manager-abc\n')
        if args[:2] == ['get', 'pods'] and '-A' in args and '-o' in args:
            return ok(json.dumps({'items': [
                {'spec': {'nodeName': node, 'containers': [{'resources': {'limits': {resource: str(units)}}}]},
                 'status': {'phase': 'Running'}} for node, resource, units in self.pods]}))
        if args[:2] == ['get', 'clusterpolicies.nvidia.com']:
            return ok(json.dumps({'items': [json.loads(json.dumps(self.policy))]}))
        if args[:2] == ['annotate', 'clusterpolicies.nvidia.com']:
            for a in args[3:]:
                if '=' in a and not a.startswith('--'):
                    key, value = a.split('=', 1)
                    self.policy['metadata'].setdefault('annotations', {})[key] = value
                elif a.endswith('-') and not a.startswith('--'):
                    self.policy['metadata'].get('annotations', {}).pop(a[:-1], None)
            self.mutation_log.append('annotate clusterpolicy ' + ' '.join(args[3:]))
            return ok('clusterpolicy annotated')
        if args[:2] == ['patch', 'clusterpolicies.nvidia.com']:
            patch = json.loads(args[args.index('-p') + 1])
            name = patch['spec']['migManager']['config']['name']
            self.policy['spec']['migManager']['config']['name'] = name
            self.mutation_log.append(f'point clusterpolicy {name}')
            return ok('clusterpolicy patched')
        if args[:2] == ['get', 'daemonsets']:
            return ok(_daemonset())
        if args[:2] == ['get', 'configmap']:
            name = args[2]
            if name == DEFAULT_CONFIG_NAME:
                return ok(json.dumps({'data': {'config.yaml': BASE_CONFIG}}))
            if name == gpu.MIG_CONFIGMAP_NAME:
                if self.configmap is None:
                    return ok('') if '--ignore-not-found' in args else fail('Error from server (NotFound): configmaps not found')
                if '-o' in args and args[args.index('-o') + 1] == 'name':
                    return ok(f'configmap/{gpu.MIG_CONFIGMAP_NAME}')
                return ok(json.dumps(self.configmap))
            return ok('') if '--ignore-not-found' in args else fail('NotFound')
        if args[:1] in (['create'], ['replace']) and stdin:
            payload = json.loads(stdin)
            if payload.get('kind') != 'ConfigMap':
                return ok('applied')
            if args[0] == 'create':
                if self.configmap is not None:
                    return fail('Error from server (AlreadyExists): configmaps already exists')
            else:
                if self.configmap is None:
                    return fail('Error from server (NotFound): configmaps not found')
                if payload['metadata'].get('resourceVersion') != self.configmap['metadata']['resourceVersion']:
                    return fail('Error from server (Conflict): Operation cannot be fulfilled on configmaps: the object has been modified')
            self.configmap_rv += 1
            self.configmap = {'metadata': {'name': gpu.MIG_CONFIGMAP_NAME, 'namespace': OPERATOR_NAMESPACE,
                                           'resourceVersion': str(self.configmap_rv),
                                           'annotations': dict((self.configmap or {}).get('metadata', {}).get('annotations', {}))},
                              'data': dict(payload['data'])}
            self.mutation_log.append(f'{args[0]} configmap entries={self.configmap_entries()}')
            return ok('configmap written')
        if args[:2] == ['annotate', 'configmap']:
            for a in args[3:]:
                if '=' in a and not a.startswith('--') and self.configmap is not None:
                    key, value = a.split('=', 1)
                    self.configmap['metadata'].setdefault('annotations', {})[key] = value
            self.mutation_log.append('tombstone configmap')
            return ok('configmap annotated')
        if args[:2] == ['delete', 'configmap']:
            self.configmap = None
            self.mutation_log.append('DELETE configmap')
            return ok('configmap deleted')
        return None

    def _maybe_react(self, node : str) -> None:
        '''The manager's late reaction, in reads: the stale terminal state for ``manager_latency``
        reads, then ``pending`` for one read, then the terminal verdict with the geometry applied.'''
        if node in self.pending:
            self.pending[node] -= 1
            if self.pending[node] == 0:
                self.nodes[node]['labels'][gpu.MIG_CONFIG_STATE_LABEL] = 'pending'
            elif self.pending[node] < 0:
                del self.pending[node]
                self._apply_manager(node)
