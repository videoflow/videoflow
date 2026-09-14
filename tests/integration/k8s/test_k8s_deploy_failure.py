'''
What ``videoflow deploy`` does when the flow does not work.

The exit codes are a public contract — CI and scripts branch on them, and they are
documented as such in README.md — but nothing in the suite has ever produced one
from a real cluster. These do: a node that always fails must end the deploy with
``EXIT_FLOW_FAILED``, print the failing worker's own logs *before* tearing anything
down, and leave nothing behind.

The teardown-before-exit ordering is the part worth guarding. A Job's pods carry
the only account of why the node died, and ``delete_resources`` removes them; the
CLI dumps the logs first for exactly that reason, and a refactor that reorders the
two would turn every cluster failure into an exit code with no explanation.

The slowest test in the bucket: ``max_restarts`` defaults to 3, so the Job
controller walks its 10/20/40s backoff ladder before the Job reaches its terminal
Failed condition. That wait is the behaviour under test — it is what a user's
deploy actually costs when a node is broken — so it is not shortened here.
'''
from __future__ import absolute_import, division, print_function

import pathlib
import shutil

import pytest
import support_k8s
from support_k8s import FIXTURE_IMAGE, resources_for, videoflow

from videoflow.core.errors import EXIT_FLOW_FAILED

pytestmark = pytest.mark.timeout(900)

FIXTURE_NODES = pathlib.Path(__file__).parent / 'fixture_nodes.py'

GRAPH = '''\
# A graph whose middle node fails on every message, for the deploy exit-code test.
from fixture_nodes import BoomProcessor

from videoflow.consumers import VoidConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer

def build_flow():
    producer = IntProducer(0, 5, 0.01, name = 'producer')
    boom = BoomProcessor(name = 'boom')(producer)
    return Flow([VoidConsumer(name = 'sink')(boom)], flow_type = BATCH)
'''

def stage_failing_graph(root : pathlib.Path) -> pathlib.Path:
    '''
    A graph module and the node module it imports, staged together.

    Both are needed, on different machines and for different reasons — which is the
    node contract in miniature. The graph is compiled *here*, in the CLI subprocess,
    where ``_load_flow`` puts this directory on ``sys.path``, so ``fixture_nodes``
    has to be next to it or the compile fails before the cluster is ever contacted.
    The pods never see either file: they rebuild the node from
    ``fixture_nodes.BoomProcessor``, which resolves inside the fixture image.

    No Dockerfile and no config template — this deploy runs ``--image --no-build``.
    '''
    root.mkdir(parents = True, exist_ok = True)
    shutil.copy(FIXTURE_NODES, root / 'fixture_nodes.py')
    (root / 'failing_flow.py').write_text(GRAPH)
    return root

def test_a_failing_node_exits_four_and_says_why(k8s_work_root, k8s_namespace, flow_ids):
    flow_id, run_id = flow_ids
    work = stage_failing_graph(k8s_work_root / flow_id)

    proc = videoflow(['deploy', str(work / 'failing_flow.py'),
                      '--namespace', k8s_namespace,
                      '--flow-id', flow_id, '--run-id', run_id,
                      '--image', FIXTURE_IMAGE, '--no-build',
                      '--non-interactive', *support_k8s.deploy_extra_args()], cwd = work)

    assert proc.returncode == EXIT_FLOW_FAILED, (
        f'expected exit {EXIT_FLOW_FAILED} (flow failed), got {proc.returncode}\n'
        f'--- stdout ---\n{proc.stdout[-6000:]}\n'
        f'--- stderr ---\n{proc.stderr[-6000:]}')

    combined = proc.stdout + proc.stderr
    # It names the node, not just "something failed".
    assert 'boom' in combined, combined[-4000:]
    # And it dumped that node's logs while its pods still existed.
    assert 'logs: node boom' in combined, combined[-4000:]

    # A failed BATCH deploy still cleans up after itself.
    assert resources_for(k8s_namespace, flow_id) == []

if __name__ == '__main__':
    pytest.main([__file__])
