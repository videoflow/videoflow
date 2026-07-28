'''
``KubernetesExecutionEngine`` against a real cluster, with no CLI in the way.

This is the tightest seam the Kubernetes path has: compile a flow, hand the specs
to the engine, let it apply the manifests, and ask it whether the flow finished.
Everything above it (config discovery, image building, the prepare hook, solution
conventions) is skipped — which is the point, because when a toy-solution test
fails those layers make it hard to say *where*.

What only a cluster can show:

- The two-phase apply works. ``_wait_provision`` blocks on a real provision Job
  creating real streams before any worker starts, which is what stops a fast finite
  producer from publishing end-of-stream before its consumers have interest.
- ``wait_for_completion`` reads Job completion correctly. Its unit tests
  (tests/test_k8s_watchdog.py) feed it canned kubectl output, so they pin the
  parsing but not the premise — and the premise was wrong until recently: reading
  ``.status.failed`` called a Job dead on its first pod failure, while the
  controller was still retrying it.
- A pod really can write through a hostPath mount onto the host filesystem, which
  is what every artifact assertion in this bucket rests on.

The flows here use built-in nodes plus fixture_nodes.py, which is baked into
``videoflow-k8s-fixtures`` (see the Dockerfile next to this file) — the way a worker
is actually meant to reach a class the framework does not ship. None of the solution
images are needed.
'''
from __future__ import absolute_import, division, print_function

import pytest
from fixture_nodes import BoomProcessor, LineWriterConsumer
from support_k8s import FIXTURE_IMAGE, resources_for

from videoflow.consumers import VoidConsumer
from videoflow.core import Flow
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.supervision import SupervisionPolicy
from videoflow.deploy.infra import infra_urls
from videoflow.deploy.manifests import parse_mounts
from videoflow.engines.kubernetes import KubernetesExecutionEngine
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

pytestmark = pytest.mark.timeout(600)

def make_engine(namespace : str, specs : list, mounts = None,
                supervision = None) -> KubernetesExecutionEngine:
    '''The engine as the deploy CLI builds it: precompiled specs, no live graph.'''
    return KubernetesExecutionEngine(
        nats_url = infra_urls(namespace)['nats'],
        namespace = namespace,
        default_image = FIXTURE_IMAGE,
        specs = specs,
        mounts = mounts,
        supervision = supervision or SupervisionPolicy(),
    )

def test_a_clean_batch_flow_completes_and_its_output_reaches_the_host(
        k8s_namespace, k8s_work_root, flow_ids):
    '''
    The happy path, end to end: provision, run one Job per node, drain, complete.

    The output file is the second assertion and the more interesting one — it was
    written by a consumer pod, through a hostPath volume, into a directory bind-
    mounted from the host. Every solution test in this bucket depends on that round
    trip, and this is where it is stated plainly instead of assumed.
    '''
    flow_id, run_id = flow_ids
    out = k8s_work_root / flow_id / 'out' / 'seen.txt'
    out.parent.mkdir(parents = True, exist_ok = True)

    producer = IntProducer(0, 10, 0.01, name = 'producer')
    identity = IdentityProcessor(name = 'identity')(producer)
    sink = LineWriterConsumer(str(out), name = 'sink')(identity)
    specs = compile_flow(Flow([sink], flow_type = BATCH, flow_id = flow_id))

    # Mounted at the same absolute path on both sides, because that is the path
    # baked into the sink's params when the graph was compiled here on the host.
    engine = make_engine(k8s_namespace, specs, mounts = parse_mounts([str(out.parent)]))
    try:
        engine.allocate_and_run_tasks(None, flow_id, BATCH, run_id)
        assert engine.wait_for_completion() == []
        got = sorted(int(line) for line in out.read_text().splitlines() if line.strip())
        assert got == list(range(11))
    finally:
        engine.teardown()

    assert resources_for(k8s_namespace, flow_id) == []

def test_a_node_that_never_recovers_is_reported_by_name(k8s_namespace, flow_ids):
    '''
    A node whose Job exhausts its backoffLimit comes back from
    ``wait_for_completion`` by node name, not by Job name — that is what the CLI
    prints and what decides exit code 4.

    ``max_restarts = 0`` keeps it to a single pod. With the default 3 the verdict is
    the same but arrives about seventy seconds later, through the Job controller's
    10/20/40s backoff ladder.
    '''
    flow_id, run_id = flow_ids
    producer = IntProducer(0, 5, 0.01, name = 'producer')
    boom = BoomProcessor(name = 'boom')(producer)
    specs = compile_flow(Flow([VoidConsumer(name = 'sink')(boom)], flow_type = BATCH,
                              flow_id = flow_id))

    engine = make_engine(k8s_namespace, specs,
                         supervision = SupervisionPolicy(max_restarts = 0))
    try:
        engine.allocate_and_run_tasks(None, flow_id, BATCH, run_id)
        assert engine.wait_for_completion() == ['boom']
    finally:
        engine.teardown()

def test_a_realtime_rollout_reports_healthy(k8s_namespace, flow_ids):
    '''
    A REALTIME deploy never completes, so its success signal is the rollout check:
    every pod Ready on two consecutive polls, well inside the deadline. Nothing here
    terminates, which is why the teardown is not optional.
    '''
    flow_id, run_id = flow_ids
    # is_finite=False is what makes the producer a Deployment rather than a Job:
    # a REALTIME flow whose producer completes would have its pods restarted by
    # the kubelet, which rollout_report correctly reports as a failing deploy.
    producer = IntProducer(0, None, 0.05, name = 'producer', is_finite = False)
    identity = IdentityProcessor(name = 'identity')(producer)
    specs = compile_flow(Flow([VoidConsumer(name = 'sink')(identity)], flow_type = REALTIME,
                              flow_id = flow_id))

    engine = make_engine(k8s_namespace, specs)
    try:
        engine.allocate_and_run_tasks(None, flow_id, REALTIME, run_id)
        report = engine.rollout_report()
        assert report.failing == [], report.failing
    finally:
        engine.teardown()

    assert resources_for(k8s_namespace, flow_id) == []

if __name__ == '__main__':
    pytest.main([__file__])
