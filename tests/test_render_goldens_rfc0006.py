'''
The switch-on goldens: what the demo flow renders and provisions with
``VF_RFC0006=1`` (plan Phase 3, RFC 0006 "Goldens"). Recorded under
``tests/golden/rfc0006/`` and compared on every run, exactly like the base
set — the difference between the two directories *is* the RFC's observable
footprint, and it must change only in a commit that names the requirement id
behind the change. At acceptance (Phase 6) this set becomes the base.

The switch is flipped with ``monkeypatch`` on ``core.constants`` (the one way
every module reads it), so the base goldens in ``test_render_goldens.py`` stay
what a default process renders.
'''
from __future__ import absolute_import, division, print_function

import difflib
import json
import os
import pathlib
from typing import Any, Dict

import pytest
from test_render_goldens import IMAGE, NAMESPACE, NATS_URL, _demo_flow

from videoflow.core import constants
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.supervision import SupervisionPolicy
from videoflow.deploy.manifests import dump_manifests, render_manifests
from videoflow.messaging import topology

HERE = pathlib.Path(__file__).parent
GOLDEN = HERE / 'golden' / 'rfc0006'
UPDATE = os.environ.get('VF_UPDATE_GOLDENS') == '1'


def _check(name : str, text : str) -> None:
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
            expected.splitlines(), text.splitlines(), fromfile = f'golden/rfc0006/{name}',
            tofile = 'rendered', lineterm = ''))[:80])
        pytest.fail(f'{name} differs from its RFC 0006 golden (name the requirement id behind the change and '
                    f'update deliberately with VF_UPDATE_GOLDENS=1):\n{diff}')


@pytest.fixture
def switched(monkeypatch : pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)


@pytest.mark.parametrize('flow_type', [REALTIME, BATCH])
def test_demo_manifests_under_the_switch(flow_type : str, switched : None) -> None:
    specs = sorted(compile_flow(_demo_flow(flow_type)), key = lambda s: s.name)
    manifests = render_manifests(
        specs, 'golden-demo', flow_type, NATS_URL, 'run0', namespace = NAMESPACE,
        default_image = IMAGE, gpu_runtime_class = 'nvidia', supervision = SupervisionPolicy(),
        blob_redis_url = 'redis://redis.videoflow-test.svc:6379/0')
    _check(f'manifests/demo_{flow_type}.yaml', dump_manifests(manifests))


def test_broker_configs_under_the_switch(switched : None) -> None:
    configs : Dict[str, Any] = {}
    for flow_type in (REALTIME, BATCH):
        configs[f'stream/{flow_type}'] = topology.stream_config_for('f', 'r', 'n', flow_type,
                                                                   generation = 'gen1').as_dict()
        for delivery in (None, {'delivery': 'at-least-once'}, {'delivery': 'best-effort'}):
            key = 'preset' if delivery is None else delivery['delivery']
            configs[f'max_deliver/{flow_type}/{key}'] = topology.max_deliver_for(flow_type, 3, delivery)
            configs[f'max_deliver_ledger/{flow_type}/{key}'] = topology.max_deliver_for(flow_type, 3, delivery,
                                                                                        ledger_budget = True)
    configs['credit/shared/1'] = topology.consumer_credit(1, False)
    configs['credit/shared/10'] = topology.consumer_credit(10, False)
    configs['credit/partitioned/10'] = topology.consumer_credit(10, True)
    configs['consumer'] = topology.consumer_config_for('f', 'r', 'child', 'parent', ack_wait = 60,
                                                        max_deliver = 4, max_ack_pending = 8,
                                                        generation = 'gen1').as_dict()
    configs['stream/replicated'] = topology.stream_config_for('f', 'r', 'n', BATCH, replicas = 3).as_dict()
    configs['dlq_stream'] = topology.dlq_stream_config('f').as_dict()
    _check('broker/configs.json', json.dumps(configs, indent = 2, sort_keys = True, default = str) + '\n')
