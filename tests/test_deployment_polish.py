'''
Phase 6 deployment/observability polish: init Job + spec ConfigMap, PDB,
startupProbe, the `explain` CLI, structured logging, and sink idempotency logic.
Mostly pure/unit (no broker); the idempotency logic is exercised with in-memory
fakes so it stays deterministic.
'''
import io
import json
import os
import tempfile
from contextlib import redirect_stdout

import pytest

from videoflow.core import Flow
from videoflow.core.constants import REALTIME
from videoflow.core.engine import Messenger
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor
from videoflow.consumers import CommandlineConsumer, VoidConsumer
from videoflow.compiler import compile_flow
from videoflow.manifests import render_manifests
from videoflow.idempotency import idempotency_key, IdempotencyStore
from videoflow.core.task import ConsumerTask

def _demo_flow():
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'identity', nb_tasks = 2)(p)
    out = CommandlineConsumer(name = 'printer')(a)
    return Flow([out], flow_type = REALTIME, flow_id = 'demo')

# -- manifests -------------------------------------------------------------

def test_provision_init_job_and_spec_configmap_present():
    manifests = render_manifests(compile_flow(_demo_flow()), 'demo', 'realtime', 'nats://x:4222', 'run1')
    by = {(m['kind'], m['metadata']['name']): m for m in manifests}
    assert ('Job', 'vf-demo-provision') in by
    assert ('ConfigMap', 'vf-demo-specs') in by
    # The init Job mounts the specs and runs the provision entrypoint.
    job = by[('Job', 'vf-demo-provision')]
    container = job['spec']['template']['spec']['containers'][0]
    assert container['command'] == ['python', '-m', 'videoflow.provision']

def test_pdb_for_multi_replica_and_startup_probe():
    manifests = render_manifests(compile_flow(_demo_flow()), 'demo', 'realtime', 'nats://x:4222', 'run1')
    by = {(m['kind'], m['metadata']['name']): m for m in manifests}
    # identity has nb_tasks=2 → gets a PodDisruptionBudget; producer/consumer don't.
    assert ('PodDisruptionBudget', 'vf-demo-identity-pdb') in by
    assert ('PodDisruptionBudget', 'vf-demo-producer-pdb') not in by
    dep = by[('Deployment', 'vf-demo-identity')]
    container = dep['spec']['template']['spec']['containers'][0]
    assert 'startupProbe' in container

# -- explain CLI -----------------------------------------------------------

_GRAPH_SRC = '''
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor
from videoflow.consumers import CommandlineConsumer

def build_flow():
    p = IntProducer(0, 5, name='producer')
    a = IdentityProcessor(name='identity')(p)
    out = CommandlineConsumer(name='printer')(a)
    return Flow([out], flow_type=BATCH, flow_id='explaindemo')
'''

def test_explain_prints_nodes_and_dlq():
    from videoflow.cli import build_parser
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, 'graph.py')
        with open(path, 'w') as f:
            f.write(_GRAPH_SRC)
        parser = build_parser()
        args = parser.parse_args(['explain', path, '--run-id', 'r1'])
        buf = io.StringIO()
        with redirect_stdout(buf):
            args.func(args)
        out = buf.getvalue()
        assert 'explaindemo' in out
        assert 'producer' in out and 'identity' in out and 'printer' in out
        assert 'vf-explaindemo-r1-dlq' in out

# -- structured logging ----------------------------------------------------

def test_json_formatter_includes_context_fields():
    import logging
    from videoflow.logging_config import JsonFormatter
    rec = logging.LogRecord('n', logging.INFO, __file__, 1, 'hello', None, None)
    rec.flow_id = 'f'
    rec.node_name = 'detector'
    line = JsonFormatter().format(rec)
    obj = json.loads(line)
    assert obj['msg'] == 'hello'
    assert obj['flow_id'] == 'f'
    assert obj['node_name'] == 'detector'

# -- sink idempotency logic ------------------------------------------------

def test_idempotency_key_is_deterministic():
    a = idempotency_key('flow', 'sink', 'mid-1')
    b = idempotency_key('flow', 'sink', 'mid-1')
    c = idempotency_key('flow', 'sink', 'mid-2')
    assert a == b and a != c

class _MemStore(IdempotencyStore):
    def __init__(self):
        self._seen = set()

    def seen(self, key):
        return key in self._seen

    def mark(self, key):
        self._seen.add(key)

class _ReplayMessenger(Messenger):
    '''Delivers the same input twice (a redelivery), then stops.'''
    def __init__(self):
        self._deliveries = 2

    def receive_message(self):
        if self._deliveries > 0:
            self._deliveries -= 1
            return {'p': {'message': 7, 'metadata': None, 'is_stop_signal': False}}
        return {'p': {'message': None, 'metadata': None, 'is_stop_signal': True}}

    def last_input_key(self):
        return 'same-key'  # both deliveries are the same logical event

    def ack_inputs(self):
        pass

    def fail_inputs(self, exc):
        pass

def test_idempotent_sink_consumes_once_across_redelivery():
    consumed = []

    class RecordingConsumer(VoidConsumer):
        def consume(self, item):
            consumed.append(item)

    node = RecordingConsumer(name = 'sink', idempotent = True)
    task = ConsumerTask(node, _ReplayMessenger(), has_children = False,
                        parent_names = ['p'], idempotency_store = _MemStore())
    task.run()
    # The event was delivered twice but the side effect happened once.
    assert consumed == [7]

if __name__ == '__main__':
    pytest.main([__file__])
