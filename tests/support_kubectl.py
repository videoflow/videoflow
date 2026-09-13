'''
Fakes for the kubectl subprocess boundary, shared by the unit tests of
``videoflow.deploy.cluster`` / ``videoflow.deploy.gpu`` and by the conformance
suite. videoflow talks to Kubernetes exclusively through ``subprocess.run``
(no client library), so a fake at that boundary exercises every line of the
real code including argv construction and stdout parsing.

Two flavours:

- ``fake_run(responses, failing = ())``: canned stdout keyed by a command
  substring; commands matching a ``failing`` needle exit 1 like a forbidden API
  call. An unmatched pod listing answers with an empty list, because kubectl
  never prints '' for a successful ``get pods -o json`` and the occupancy reader
  rightly treats '' as unreadable — so a test that means "nothing runs" gets the
  honest answer and only tests that put ``pods -A`` in ``failing`` see the
  unreadable case.
- ``FakeKubectl(responses, nodes = None)``: the same lookup plus a call log, and,
  with ``nodes``, live node state served with the API server's
  compare-and-swap semantics — a ``--resource-version`` that is not the node's
  current one is a 409 Conflict, ``k=v`` without ``--overwrite`` on a different
  value fails client-side, every successful write bumps the resourceVersion.
  That is what lets ownership tests prove a claim is a CAS and not a hope.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

_FORBIDDEN = ('Error from server (Forbidden): pods is forbidden: User "vf" cannot list '
              'resource "pods" at the cluster scope')


def fake_run(responses : Mapping[str, str], failing : Iterable[str] = ()) -> Callable[..., subprocess.CompletedProcess]:
    '''A ``subprocess.run`` stand-in; see the module docstring.'''
    failing = tuple(failing)

    def run(cmd : List[str], **kwargs : Any) -> subprocess.CompletedProcess:
        joined = ' '.join(cmd)
        for needle in failing:
            if needle in joined:
                return subprocess.CompletedProcess(cmd, 1, '', _FORBIDDEN)
        for needle, out in responses.items():
            if needle in joined:
                return subprocess.CompletedProcess(cmd, 0, out, '')
        if 'pods -A' in joined:
            return subprocess.CompletedProcess(cmd, 0, '{"items": []}', '')
        return subprocess.CompletedProcess(cmd, 0, '', '')
    return run


def nodes_json(*nodes : Tuple[str, Mapping[str, str], Mapping[str, str]]) -> str:
    '''A ``kubectl get nodes -o json`` body from ``(name, labels, allocatable)`` triples.'''
    return json.dumps({'items': [
        {'metadata': {'name': name, 'labels': dict(labels)}, 'status': {'allocatable': dict(allocatable)}}
        for name, labels, allocatable in nodes]})


def pods_json(*pods : Tuple[Optional[str], str, List[Mapping[str, str]]]) -> str:
    '''A ``kubectl get pods -A -o json`` body from ``(node, phase, [limits, ...])`` triples.'''
    return json.dumps({'items': [
        {'spec': {'nodeName': node, 'containers': [{'resources': {'limits': dict(l)}} for l in limits]},
         'status': {'phase': phase}}
        for node, phase, limits in pods]})


class FakeKubectl:
    '''subprocess.run stand-in with a call log and optional live node state; see the module docstring.'''
    def __init__(self, responses : Mapping[str, str], nodes : Optional[Mapping[str, Mapping[str, Any]]] = None,
                 failing : Iterable[str] = ()) -> None:
        self.responses = dict(responses)
        self.failing = tuple(failing)
        self.calls : List[Tuple[List[str], Optional[str]]] = []
        self.nodes : Optional[Dict[str, Dict[str, Any]]] = None if nodes is None else {
            name: {'labels': dict(state.get('labels') or {}),
                   'annotations': dict(state.get('annotations') or {}), 'rv': 1}
            for name, state in nodes.items()}

    def __call__(self, cmd : List[str], **kwargs : Any) -> subprocess.CompletedProcess:
        self.calls.append((list(cmd), kwargs.get('input')))
        joined = ' '.join(cmd)
        for needle in self.failing:
            if needle in joined:
                return subprocess.CompletedProcess(cmd, 1, '', _FORBIDDEN)
        if self.nodes is not None:
            served = self._serve_node_state(cmd)
            if served is not None:
                return served
        for needle, out in self.responses.items():
            if needle in joined:
                return subprocess.CompletedProcess(cmd, 0, out, '')
        return subprocess.CompletedProcess(cmd, 0, '', '')

    def joined_calls(self) -> List[str]:
        return [' '.join(c) for c, _stdin in self.calls]

    def labels(self, node : str) -> Dict[str, str]:
        assert self.nodes is not None, 'FakeKubectl was built without node state'
        return dict(self.nodes[node]['labels'])

    def mutations(self) -> List[str]:
        '''Every call that would change cluster state (anything but a read).'''
        return [' '.join(c) for c, _stdin in self.calls
                if len(c) > 1 and c[1] not in ('get', 'version', 'config', 'auth', 'explain')]

    def _node_doc(self, name : str) -> Dict[str, Any]:
        assert self.nodes is not None
        state = self.nodes[name]
        return {'metadata': {'name': name, 'resourceVersion': str(state['rv']),
                             'labels': dict(state['labels']),
                             'annotations': dict(state['annotations'])}}

    def _serve_node_state(self, cmd : List[str]) -> Optional[subprocess.CompletedProcess]:
        assert self.nodes is not None
        args = list(cmd[1:])
        if args[:2] == ['get', 'nodes'] and args[-2:] == ['-o', 'json']:
            items = [self._node_doc(name) for name in self.nodes]
            return subprocess.CompletedProcess(cmd, 0, json.dumps({'items': items}), '')
        if args[:2] == ['get', 'node'] and len(args) >= 5 and args[3:5] == ['-o', 'json']:
            name = args[2]
            if name not in self.nodes:
                return subprocess.CompletedProcess(cmd, 1, '', f'nodes "{name}" not found')
            return subprocess.CompletedProcess(cmd, 0, json.dumps(self._node_doc(name)), '')
        if args[:2] in (['label', 'node'], ['annotate', 'node']):
            field = 'labels' if args[0] == 'label' else 'annotations'
            name = args[2]
            if name not in self.nodes:
                return subprocess.CompletedProcess(cmd, 1, '', f'nodes "{name}" not found')
            state = self.nodes[name]
            expected = next((a.split('=', 1)[1] for a in args if a.startswith('--resource-version=')),
                            None)
            if expected is not None and expected != str(state['rv']):
                return subprocess.CompletedProcess(
                    cmd, 1, '', f'Error from server (Conflict): Operation cannot be fulfilled on '
                                f'nodes "{name}": the object has been modified; please apply your '
                                f'changes to the latest version and try again')
            overwrite = '--overwrite' in args
            for arg in args[3:]:
                if arg.startswith('--'):
                    continue
                if '=' in arg:
                    key, value = arg.split('=', 1)
                    if key in state[field] and state[field][key] != value and not overwrite:
                        return subprocess.CompletedProcess(
                            cmd, 1, '', f"error: '{key}' already has a value "
                                        f"({state[field][key]}), and --overwrite is false")
                    state[field][key] = value
                elif arg.endswith('-'):
                    state[field].pop(arg[:-1], None)
            state['rv'] += 1
            return subprocess.CompletedProcess(cmd, 0, f'node/{name} {args[0]}ed', '')
        return None
