'''
Byte-identity goldens for everything videoflow renders or names.

CLAUDE.md is explicit that a behaviour-identity check, not a green suite, is the
acceptance test for a refactor: byte-compare the rendered YAML / emitted JSON
before and after. These tests are that check, recorded once on the base commit and
compared on every run. A golden may change only in a commit that explains why
(an RFC, a deliberate rendering change) — never as a side effect.

What is pinned, and why each:

- **Compiled specs** of the four toy solutions (``NodeSpec.to_dict()`` through the
  same ``python -m videoflow.compile`` subprocess the deploy path uses): any new
  ``NodeSpec`` field changes every specs ConfigMap byte, which is why the plan
  keeps new requirements off ``NodeSpec``.
- **Rendered manifests** of the toys plus an in-process GPU/partitioned/autoscaled
  demo flow (exclusive and mix modes): names, labels, env, resources, affinity,
  probes, Jobs vs Deployments vs StatefulSets, KEDA objects.
- **Broker names and configs** over a 200-tuple corpus of awkward identifiers:
  subjects, streams, durables, EOS/DLQ names, ``k8s_name``, and the JetStream
  stream/consumer configs for both flow types.

Each toy is compiled in its own subprocess because all four ship a ``common.py``
and importing two into one interpreter collides (see CLAUDE.md). Absolute staging
paths are normalised to ``<ROOT>`` so the goldens are machine-independent.

Record or refresh the goldens deliberately with ``VF_UPDATE_GOLDENS=1``.
'''
from __future__ import absolute_import, division, print_function

import difflib
import json
import os
import pathlib
import random
import subprocess
import sys
from typing import Any, Dict, List

import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.compiler import NodeSpec, compile_flow
from videoflow.core.constants import BATCH, GPU, REALTIME
from videoflow.core.supervision import SupervisionPolicy
from videoflow.deploy.manifests import dump_manifests, k8s_name, parse_mounts, render_manifests
from videoflow.messaging import topology
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

HERE = pathlib.Path(__file__).parent
GOLDEN = HERE / 'golden'
INTEGRATION = HERE / 'integration'
if str(INTEGRATION) not in sys.path:
    sys.path.insert(0, str(INTEGRATION))

from support_solutions import K8S_FUSION_CONFIG, SOLUTION_CONFIGS, stage_solution  # noqa: E402

UPDATE = os.environ.get('VF_UPDATE_GOLDENS') == '1'
NATS_URL = 'nats://nats.videoflow-test.svc:4222'
IMAGE = 'videoflow-base:py3.12'
NAMESPACE = 'videoflow-test'

def _check(name : str, text : str) -> None:
    '''Compare ``text`` with the golden file ``name``, or record it under VF_UPDATE_GOLDENS=1.'''
    path = GOLDEN / name
    if UPDATE or not path.exists():
        if not UPDATE:
            pytest.fail(f'golden {path} is missing; record it with VF_UPDATE_GOLDENS=1')
        path.parent.mkdir(parents = True, exist_ok = True)
        path.write_text(text)
        return
    expected = path.read_text()
    if expected != text:
        diff = '\n'.join(list(difflib.unified_diff(
            expected.splitlines(), text.splitlines(), fromfile = f'golden/{name}',
            tofile = 'rendered', lineterm = ''))[:80])
        pytest.fail(f'{name} differs from its golden (a rendering/naming change needs an RFC or a '
                    f'deliberate golden update with VF_UPDATE_GOLDENS=1):\n{diff}')

def _normalise(text : str, root : pathlib.Path) -> str:
    return text.replace(str(root), '<ROOT>')

# -- toy solutions -------------------------------------------------------------

TOYS = {
    'toy_calculator': SOLUTION_CONFIGS['toy_calculator'],
    'toy_router': SOLUTION_CONFIGS['toy_router'],
    'toy_recovery': SOLUTION_CONFIGS['toy_recovery'],
    'toy_fusion': SOLUTION_CONFIGS['toy_fusion'],
    'toy_fusion_k8s': K8S_FUSION_CONFIG,
}

def _compile_in_subprocess(graph : pathlib.Path) -> Dict[str, Any]:
    proc = subprocess.run([sys.executable, '-m', 'videoflow.compile', str(graph)],
                          cwd = str(graph.parent), capture_output = True, text = True,
                          timeout = 120, check = False)
    assert proc.returncode == 0, f'compile failed:\n{proc.stdout[-2000:]}\n{proc.stderr[-4000:]}'
    return json.loads(proc.stdout)

@pytest.mark.parametrize('name', sorted(TOYS))
def test_toy_specs_and_manifests_are_byte_identical(name : str, tmp_path : pathlib.Path) -> None:
    solution = name.removesuffix('_k8s')
    work = stage_solution(tmp_path, solution, TOYS[name], with_template = True)
    document = _compile_in_subprocess(work / f'{solution}.py')
    # compile_flow's topological order is not stable across processes for nodes at
    # the same depth (hash order of node objects); pin a name order so the golden
    # measures rendering, not the sort. Making the sort deterministic is a Phase 1 item.
    document['specs'] = sorted(document['specs'], key = lambda d: d['name'])
    specs_text = json.dumps(document['specs'], indent = 2, sort_keys = True) + '\n'
    _check(f'specs/{name}.json', _normalise(specs_text, tmp_path))

    specs = [NodeSpec.from_dict(d) for d in document['specs']]
    manifests = render_manifests(
        specs, f'golden-{name}', document['flow_type'], NATS_URL, 'run0',
        namespace = NAMESPACE, default_image = IMAGE, mounts = parse_mounts([str(work)]),
        supervision = SupervisionPolicy(), blob_redis_url = 'redis://redis.videoflow-test.svc:6379/0')
    _check(f'manifests/{name}.yaml', _normalise(dump_manifests(manifests), tmp_path))

# -- an in-process demo covering GPU, partitioning, joins and autoscaling ---------

def _demo_flow(flow_type : str) -> Flow:
    producer = IntProducer(0, 40, 0.1, name = 'numbers')
    gpu = IdentityProcessor(name = 'gpu-two', device_type = GPU, gpu_count = 2)(producer)
    sharer = IdentityProcessor(name = 'gpu-sharer', device_type = GPU, nb_tasks = 2)(producer)
    part = IdentityProcessor(name = 'part', nb_tasks = 3, partition_by = 'trace_id')(gpu)
    joined = IdentityProcessor(name = 'joined')(part, sharer)
    printer = CommandlineConsumer(name = 'printer')(joined)
    return Flow([printer], flow_type = flow_type, flow_id = 'golden-demo')

@pytest.mark.parametrize('flow_type,mode,autoscaling', [
    (REALTIME, 'exclusive', True),
    (REALTIME, 'mix', False),
    (BATCH, 'exclusive', False),
])
def test_demo_manifests_are_byte_identical(flow_type : str, mode : str, autoscaling : bool) -> None:
    specs = sorted(compile_flow(_demo_flow(flow_type)), key = lambda s: s.name)
    manifests = render_manifests(
        specs, 'golden-demo', flow_type, NATS_URL, 'run0', namespace = NAMESPACE,
        default_image = IMAGE, gpu_runtime_class = 'nvidia', gpu_mode = mode,
        autoscaling = autoscaling, gpu_autoscaling = autoscaling,
        supervision = SupervisionPolicy())
    _check(f'manifests/demo_{flow_type}_{mode}{"_autoscaled" if autoscaling else ""}.yaml',
           dump_manifests(manifests))
    _check(f'specs/demo_{flow_type}.json',
           json.dumps([s.to_dict() for s in specs], indent = 2, sort_keys = True) + '\n')

# -- names and broker configs over an adversarial corpus --------------------------

_ALPHABET = ['a', 'b', 'c', 'x', 'y', 'z', '0', '1', '9', '-', '_', '.', ' ', '/', '*', '>',
             'Ä', 'é', 'ß', '中', '!', '@', '#', 'flow', 'run', 'node', 'eos', 'dlq', 'p1', '--']

def _corpus(n : int = 200, seed : int = 1729) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    tuples = []
    for _ in range(n):
        parts = [''.join(rng.choice(_ALPHABET) for _ in range(rng.randint(1, 12))) for _ in range(4)]
        tuples.append({'flow_id': parts[0], 'run_id': parts[1], 'node': parts[2], 'parent': parts[3],
                       'replica': rng.randint(0, 5), 'instance': f'r{rng.randint(0, 3)}-{rng.randbytes(4).hex()}'})
    # Known collision pairs from the design package, placed deterministically.
    tuples.append({'flow_id': 'a.b', 'run_id': 'r', 'node': 'a_b', 'parent': 'a.b', 'replica': 0, 'instance': 'r0-00000000'})
    tuples.append({'flow_id': 'a', 'run_id': 'b-c', 'node': 'n', 'parent': 'p', 'replica': 0, 'instance': 'r0-00000000'})
    tuples.append({'flow_id': 'a-b', 'run_id': 'c', 'node': 'n', 'parent': 'p', 'replica': 0, 'instance': 'r0-00000000'})
    tuples.append({'flow_id': 'f', 'run_id': 'r', 'node': 'n', 'parent': 'p', 'replica': 0, 'instance': 'r0-00000000'})
    tuples.append({'flow_id': 'f', 'run_id': 'r-x', 'node': 'n', 'parent': 'p', 'replica': 0, 'instance': 'r0-00000000'})
    return tuples

def _names_for(t : Dict[str, Any]) -> Dict[str, str]:
    f, r, n, p = t['flow_id'], t['run_id'], t['node'], t['parent']
    return {
        'subject': topology.subject_for(f, r, n),
        'stream': topology.stream_name_for(f, r, n),
        'eos_subject': topology.eos_subject_for(f, r, n),
        'control_subject': topology.control_subject_for(f, r),
        'durable': topology.durable_name_for(n, p),
        'partitioned_durable': topology.partitioned_durable_name_for(n, p, t['replica']),
        'eos_durable': topology.eos_durable_name_for(n, p, t['instance']),
        'eos_anchor': topology.eos_anchor_durable_name_for(n),
        'dlq_stream': topology.dlq_stream_name(f),
        'dlq_subject': topology.dlq_subject_for(f, r, n),
        'dlq_filter_flow': topology.dlq_subject_filter(f),
        'dlq_filter_run_node': topology.dlq_subject_filter(f, r, n),
        'stream_label_selector': topology.stream_label_selector(f, r),
        'k8s_workload': k8s_name('vf', f, n),
        'k8s_run_label': k8s_name(r),
    }

def test_names_over_corpus_are_byte_identical() -> None:
    rows = [{'input': t, 'names': _names_for(t)} for t in _corpus()]
    _check('names/corpus.json', json.dumps(rows, indent = 1, ensure_ascii = False, sort_keys = True) + '\n')

def test_broker_configs_are_byte_identical() -> None:
    configs : Dict[str, Any] = {}
    for flow_type in (REALTIME, BATCH):
        configs[f'stream/{flow_type}'] = topology.stream_config_for('f', 'r', 'n', flow_type).as_dict()
        for delivery in (None, {'delivery': 'at-least-once'}, {'delivery': 'best-effort'}):
            key = 'preset' if delivery is None else delivery['delivery']
            configs[f'max_deliver/{flow_type}/{key}'] = topology.max_deliver_for(flow_type, 3, delivery)
    configs['consumer'] = topology.consumer_config_for('f', 'r', 'child', 'parent', ack_wait = 60,
                                                        max_deliver = 4, max_ack_pending = 8).as_dict()
    configs['eos_consumer'] = topology.eos_consumer_config('f', 'r', 'child', 'parent', 'r0-abcd1234').as_dict()
    configs['eos_anchor'] = topology.eos_anchor_config('f', 'r', 'n').as_dict()
    configs['dlq_stream'] = topology.dlq_stream_config('f').as_dict()
    _check('broker/configs.json', json.dumps(configs, indent = 2, sort_keys = True, default = str) + '\n')
