'''
`deploy` wiring that does not need a cluster: which Dockerfile the autobuild
gets (the flow decides, never the docker daemon), the resolved config reaching
the in-image compile, and — further down — the registry push and the GPU
RuntimeClass defaulting.

Pure/unit: the graph load, the build, docker and kubectl are monkeypatched; every
command runs with --dry-run, which renders and returns before any cluster step.
'''
import json

import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH, GPU
from videoflow.deploy import build, cli, infra, solution
from videoflow.deploy.compile import compile_to_dict
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

GRAPH = '''
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer

def build_flow(cfg = None):
    p = IntProducer(0, 3, name = 'numbers')
    out = CommandlineConsumer(name = 'printer')(p)
    return Flow([out], flow_type = BATCH, flow_id = 'render')
'''


def _cpu_flow():
    return Flow([CommandlineConsumer(name = 'printer')(IntProducer(0, 3, name = 'numbers'))],
                flow_type = BATCH, flow_id = 'render')


def _gpu_flow():
    p = IntProducer(0, 3, name = 'numbers')
    work = IdentityProcessor(name = 'work', device_type = GPU)(p)
    return Flow([CommandlineConsumer(name = 'printer')(work)], flow_type = BATCH, flow_id = 'render')


@pytest.fixture
def harness(tmp_path, monkeypatch):
    '''A staged graph plus stubs; returns (graph dir, recorder).'''
    graph = tmp_path / 'mygraph.py'
    graph.write_text(GRAPH)
    seen = {'build': [], 'docker': [], 'flow': _cpu_flow}
    monkeypatch.setattr(cli, '_load_flow', lambda target: seen['flow']())
    monkeypatch.setattr(build, 'docker_gpus_available', lambda: False)
    monkeypatch.setattr(cli, 'docker_gpus_available', lambda: False)
    monkeypatch.setattr(cli, 'autobuild',
                        lambda graph_dir, **kw: seen['build'].append(kw) or 'videoflow-mygraph:latest')
    monkeypatch.setattr(cli, 'image_exists', lambda ref: False)
    monkeypatch.setenv(solution.CONFIG_ENV, 'reset-by-fixture')
    monkeypatch.delenv(solution.CONFIG_ENV)
    return tmp_path, seen


def _deploy(tmp_path, *extra):
    return cli.main(['deploy', str(tmp_path / 'mygraph.py'), '--non-interactive', '--run-id', 'r1',
                     '--dry-run', *extra])


def _both_dockerfiles(tmp_path):
    (tmp_path / 'Dockerfile').write_text('ARG BASE_IMAGE=videoflow-base:py3.12\nFROM ${BASE_IMAGE}\n')
    (tmp_path / 'gpu.Dockerfile').write_text('ARG BASE_IMAGE=videoflow-base:py3.12-cuda\nFROM ${BASE_IMAGE}\n')


def test_x_gpu_decides_the_dockerfile_not_the_docker_daemon(harness):
    tmp_path, seen = harness
    _both_dockerfiles(tmp_path)
    (tmp_path / 'config.template.yaml').write_text("device: cpu\nx-questions: []\nx-gpu: ['{device}']\n")
    (tmp_path / 'config.yaml').write_text('device: gpu\n')
    assert _deploy(tmp_path) == 0
    assert seen['build'] == [{'needs_gpu': True, 'context_override': None}]
    (tmp_path / 'config.yaml').write_text('device: cpu\n')
    seen['build'].clear()
    assert _deploy(tmp_path) == 0
    assert seen['build'] == [{'needs_gpu': False, 'context_override': None}]


def test_without_x_gpu_the_compiled_graph_decides(harness):
    tmp_path, seen = harness
    _both_dockerfiles(tmp_path)
    seen['flow'] = _gpu_flow
    assert _deploy(tmp_path) == 0
    assert seen['build'] == [{'needs_gpu': True, 'context_override': None}]


def test_an_undecidable_choice_builds_cpu_with_a_note(harness, monkeypatch, capsys):
    tmp_path, seen = harness
    _both_dockerfiles(tmp_path)
    # The graph does not import here and no template says which image: CPU, and
    # the operator is told how to declare it.
    document = json.dumps(compile_to_dict(str(tmp_path / 'mygraph.py')))

    def missing_deps(target):
        raise ImportError('no torch')
    monkeypatch.setattr(cli, '_load_flow', missing_deps)
    monkeypatch.setattr(cli, 'run_in_image', lambda *a, **kw: seen['docker'].append(kw) or document)
    assert _deploy(tmp_path) == 0
    assert seen['build'] == [{'needs_gpu': False, 'context_override': None}]
    assert 'x-gpu' in capsys.readouterr().err


def test_the_resolved_config_reaches_the_in_image_compile(harness, monkeypatch):
    tmp_path, seen = harness
    document = json.dumps(compile_to_dict(str(tmp_path / 'mygraph.py')))
    (tmp_path / 'config.yaml').write_text('device: cpu\n')

    def missing_deps(target):
        raise ImportError('no torch')
    monkeypatch.setattr(cli, '_load_flow', missing_deps)
    monkeypatch.setattr(cli, 'run_in_image', lambda *a, **kw: seen['docker'].append(kw) or document)
    assert _deploy(tmp_path, '--image', 'ghcr.io/acme/sol:1', '--no-build') == 0
    assert seen['docker'][-1]['env'] == {solution.CONFIG_ENV: str(tmp_path / 'config.yaml')}
    assert seen['docker'][-1]['capture'] is True


# -- the registry push (multi-node clusters) --------------------------------------------

def test_registry_pushes_the_built_image_and_the_pods_use_the_registry_ref(harness, monkeypatch, capsys):
    tmp_path, seen = harness
    (tmp_path / 'Dockerfile').write_text('FROM x\n')
    pushed = []
    monkeypatch.setattr(cli, 'image_exists', lambda ref: ref == 'videoflow-mygraph:latest')
    monkeypatch.setattr(cli, 'push_image',
                        lambda ref, registry, tool: pushed.append((ref, registry, tool)) or f'{registry}/{ref}')
    # A render-only output is meant to be applied, so the image is pushed for it.
    out_dir = tmp_path / 'manifests'
    code = cli.main(['deploy', str(tmp_path / 'mygraph.py'), '--non-interactive', '--run-id', 'r1',
                     '--render-only', '--output', str(out_dir), '--registry', '10.0.0.1:5000', '--push-tool', 'crane'])
    assert code == 0 and pushed == [('videoflow-mygraph:latest', '10.0.0.1:5000', 'crane')]
    assert any('10.0.0.1:5000/videoflow-mygraph:latest' in p.read_text() for p in out_dir.glob('job-*.yaml'))
    # A dry run has no side effects: nothing is pushed, the manifests still name the registry ref.
    pushed.clear()
    assert _deploy(tmp_path, '--registry', '10.0.0.1:5000', '--push-tool', 'crane') == 0
    out, err = capsys.readouterr()
    assert pushed == [] and 'not pushed' in err
    assert 'image: 10.0.0.1:5000/videoflow-mygraph:latest' in out
    # An image that already names a registry is the operator's to have pushed.
    code = cli.main(['deploy', str(tmp_path / 'mygraph.py'), '--non-interactive', '--run-id', 'r1',
                     '--render-only', '--output', str(out_dir), '--registry', '10.0.0.1:5000',
                     '--no-build', '--image', 'ghcr.io/acme/x:1'])
    assert code == 0 and pushed == []


# -- the GPU RuntimeClass -----------------------------------------------------------------

def test_resolve_gpu_runtime_class_detects_asks_or_opts_out(monkeypatch, capsys):
    probes = []
    monkeypatch.setattr(cli, 'nvidia_runtimeclass', lambda kubectl: probes.append(kubectl) or 'nvidia')
    gpu = [object()]
    assert cli._resolve_gpu_runtime_class(None, gpu, 'kubectl') == 'nvidia'
    assert probes == ['kubectl'] and 'RuntimeClass' in capsys.readouterr().err
    assert cli._resolve_gpu_runtime_class('nvidia-legacy', gpu, 'kubectl') == 'nvidia-legacy'
    assert cli._resolve_gpu_runtime_class('none', gpu, 'kubectl') is None
    assert cli._resolve_gpu_runtime_class('', gpu, 'kubectl') is None
    assert cli._resolve_gpu_runtime_class(None, [], 'kubectl') is None        # a CPU flow never probes
    assert probes == ['kubectl']
    monkeypatch.setattr(cli, 'nvidia_runtimeclass', lambda kubectl: None)
    assert cli._resolve_gpu_runtime_class(None, gpu, 'kubectl') is None     # nothing registered: nothing set


def test_a_render_never_probes_the_cluster_for_the_runtime_class(harness, monkeypatch, capsys):
    tmp_path, seen = harness
    seen['flow'] = _gpu_flow
    def boom(kubectl):
        raise AssertionError('a render must not touch the cluster')
    monkeypatch.setattr(cli, 'nvidia_runtimeclass', boom)
    assert _deploy(tmp_path, '--gpu-runtime-class', 'none') == 0
    assert 'runtimeClassName' not in capsys.readouterr().out
    assert _deploy(tmp_path, '--gpu-runtime-class', 'nvidia') == 0
    assert 'runtimeClassName: nvidia' in capsys.readouterr().out


class _Stop(cli.ConfigError):
    '''Raised from the stubbed ``admit`` so the command stops right after the GPU preflight.'''


def test_the_detected_runtime_class_reaches_the_preflight(harness, monkeypatch):
    from videoflow.backends.outcomes import known
    tmp_path, seen = harness
    seen['flow'] = _gpu_flow
    preflight = {}
    monkeypatch.setattr(cli, 'detect_cluster', lambda kubectl: 'k3s')
    monkeypatch.setattr(cli, 'hostpath_warning', lambda flavor: None)
    # No --nats: the command would otherwise `kubectl get svc` the developer's cluster.
    monkeypatch.setattr(infra, 'reused_infra',
                        lambda kubectl, namespace, need_redis: infra.ReusedInfra(nats = None, redis = None))
    monkeypatch.setattr(cli, 'nvidia_runtimeclass', lambda kubectl: 'nvidia')
    monkeypatch.setattr(cli, 'gpu_preflight', lambda kubectl, **kw: preflight.update(kw) or [])
    monkeypatch.setattr(cli, 'free_gpu_devices_observed', lambda kubectl: known(4))

    def admit(*a, **kw):
        raise _Stop('stopped after admission')
    monkeypatch.setattr(cli, 'admit', admit)
    code = cli.main(['deploy', str(tmp_path / 'mygraph.py'), '--non-interactive', '--run-id', 'r1', '--no-build',
                     '--image', 'ghcr.io/acme/x:1'])
    assert code == 2 and preflight['gpu_runtime_class'] == 'nvidia'
    code = cli.main(['deploy', str(tmp_path / 'mygraph.py'), '--non-interactive', '--run-id', 'r1', '--no-build',
                     '--image', 'ghcr.io/acme/x:1', '--gpu-runtime-class', 'none'])
    assert code == 2 and preflight['gpu_runtime_class'] is None


# -- <repo>://<name> solution references ---------------------------------------
# The reference resolves to a cached checkout; its root is the build context
# (solution Dockerfiles COPY sibling packages), unless --build-context says otherwise.

def _stub_resolver(monkeypatch, tmp_path):
    from videoflow.deploy import solution_refs

    def resolve(arg, cache_root = None):
        assert arg == 'videoflow://mygraph'
        return solution_refs.ResolvedSolution(graph_path = str(tmp_path / 'mygraph.py'),
                                              factory = None, build_context = '/clone/root')
    monkeypatch.setattr(solution_refs, 'resolve_solution_ref', resolve)


@pytest.mark.parametrize('extra_argv, context', [
    ([], '/clone/root'),                                   # the checkout root, where the Dockerfile's COPYs resolve
    (['--build-context', '{tmp_path}'], '{tmp_path}'),     # unless the operator says otherwise
], ids = ['checkout-root', 'explicit-build-context'])
def test_a_solution_ref_builds_from_its_checkout_root_unless_told_otherwise(harness, monkeypatch, extra_argv, context):
    tmp_path, seen = harness
    _both_dockerfiles(tmp_path)
    _stub_resolver(monkeypatch, tmp_path)
    argv = [a.format(tmp_path = tmp_path) for a in extra_argv]
    assert cli.main(['deploy', 'videoflow://mygraph', '--non-interactive', '--run-id', 'r1', '--dry-run', *argv]) == 0
    assert seen['build'] == [{'needs_gpu': False, 'context_override': context.format(tmp_path = tmp_path)}]


def test_explain_accepts_a_solution_ref(harness, monkeypatch, capsys):
    tmp_path, seen = harness
    _stub_resolver(monkeypatch, tmp_path)
    assert cli.main(['explain', 'videoflow://mygraph']) == 0
    assert 'numbers' in capsys.readouterr().out


def test_unresolvable_solution_ref_is_a_clean_error(harness, monkeypatch, capsys):
    tmp_path, seen = harness
    from videoflow.core.errors import ResourceUnavailable
    from videoflow.deploy import solution_refs

    def resolve(arg, cache_root = None):
        raise ResourceUnavailable('could not fetch videoflow at v9.9.9', remedy = 'clone by hand')
    monkeypatch.setattr(solution_refs, 'resolve_solution_ref', resolve)
    assert cli.main(['deploy', 'videoflow://mygraph', '--non-interactive', '--run-id', 'r1', '--dry-run']) == 3
    assert 'clone by hand' in capsys.readouterr().err


# -- no --image, no Dockerfile: the base image ----------------------------------
# A flow of built-in nodes needs nothing but videoflow, so the base image for this
# version is the default; a graph it cannot run is refused with the fix, before a
# pod finds out.

def _without_dockerfile(harness, monkeypatch, image = 'ghcr.io/videoflow/videoflow-base:9.9.9'):
    tmp_path, seen = harness
    monkeypatch.setattr(cli, 'autobuild', lambda graph_dir, **kw: seen['build'].append(kw) or None)
    monkeypatch.setattr(cli, 'default_image', lambda needs_gpu: seen.setdefault('default', []).append(needs_gpu) or image)
    return tmp_path, seen


def test_no_dockerfile_deploys_the_base_image(harness, monkeypatch, capsys):
    tmp_path, seen = _without_dockerfile(harness, monkeypatch)
    assert _deploy(tmp_path) == 0
    out, err = capsys.readouterr()
    assert 'image: ghcr.io/videoflow/videoflow-base:9.9.9' in out
    assert 'Using the videoflow base image' in err and seen['default'] == [False]


def test_no_build_still_requires_an_image(harness, monkeypatch, capsys):
    # --no-build says the operator manages images: nothing is built and nothing is assumed.
    tmp_path, seen = _without_dockerfile(harness, monkeypatch)
    assert _deploy(tmp_path, '--no-build') == 2
    assert 'has no container image' in capsys.readouterr().err and 'default' not in seen


def test_a_graph_with_its_own_nodes_is_refused_for_the_base_image(harness, monkeypatch, capsys):
    tmp_path, seen = _without_dockerfile(harness, monkeypatch)

    class Twice(IdentityProcessor):
        pass

    def own_flow():
        p = IntProducer(0, 3, name = 'numbers')
        return Flow([CommandlineConsumer(name = 'printer')(Twice(name = 'twice')(p))],
                    flow_type = BATCH, flow_id = 'render')
    seen['flow'] = own_flow
    assert _deploy(tmp_path) == 2
    err = capsys.readouterr().err
    assert 'twice' in err and 'Twice' in err and 'docker/user-image.example.Dockerfile' in err


def test_a_graph_that_does_not_import_here_is_refused_for_the_base_image(harness, monkeypatch, capsys):
    # Without the default, the graph would be compiled inside the image; the base
    # image cannot hold what this machine lacks, so say so instead of trying.
    tmp_path, seen = _without_dockerfile(harness, monkeypatch)
    compiled = []
    monkeypatch.setattr(cli, '_compile_in_image', lambda *a, **kw: compiled.append(a))

    def missing_dep():
        raise ImportError("No module named 'torch'")
    seen['flow'] = missing_dep
    assert _deploy(tmp_path) == 2
    err = capsys.readouterr().err
    assert 'torch' in err and 'docker/user-image.example.Dockerfile' in err and compiled == []
