'''
The toy-solution fixtures both whole-flow buckets share.

Parity between ``local/test_toy_solutions.py`` (``videoflow run-local``, workers as
host subprocesses) and ``k8s/test_k8s_solutions.py`` (``videoflow deploy``, one Job
per node on kind) is the entire point of this module: the **same config dict** and
the **same assertion function** run against both engines, so a divergence between
them fails as a framework bug rather than quietly becoming two tests that drifted
apart. Anything asserted here is a claim about the solution's answer, not about the
engine that produced it.

What deliberately does *not* live here is the argv. ``run-local`` and ``deploy``
share no flags worth abstracting over — one takes ``--no-infra``, the other takes a
namespace, an image and a flow id — and a single builder with a branch for each
would be harder to read than the two it replaced. Each bucket keeps its own.

The configs are smaller and faster than the shipped ``config.example.yaml`` (short
streams, high pacing): the point is the framework path, and a CI run should not
spend a minute on arithmetic.
'''
from __future__ import absolute_import, division, print_function

import json
import pathlib
import shutil
from typing import Any, Dict

import yaml

SOLUTIONS_DIR = pathlib.Path(__file__).resolve().parents[2] / 'solutions'

SOLUTION_CONFIGS : Dict[str, Dict[str, Any]] = {
    'toy_calculator': {
        'work_dir': './out',
        'start_value': 1,
        'end_value': 40,
        'producer_fps': 200,
        'delay_fps': 150,
        'flow_type': 'batch',
        'square': {'workers': 2},
        'join': {'timeout_s': None, 'missing': 'wait', 'max_pending': 100000},
    },
    'toy_router': {
        'work_dir': './out',
        'seed': 7,
        'events': 120,
        'sensors': 6,
        'rate_fps': 400,
        'idempotent_sink': True,
        'enrich': {'threshold': 50.0, 'lookup_ms': 1.0},
        'counter': {'partitions': 3, 'partition_by': '_partition_key'},
    },
    'toy_recovery': {
        'work_dir': './out',
        'events': 24,
        'rate_fps': 400,
        'max_retries': 3,
        'fragile': {'poison_values': [7, 19], 'crash_at': 12},
    },
    'toy_fusion': {
        'work_dir': './out',
        'cameras': 2,
        'camera_fps': 10,
        'phase_step_ms': 3.0,
        'sensor_hz': 100,
        'duration_s': 3,
        'flow_type': 'realtime',
        'fusion': {'tolerance_ms': 15, 'timeout_s': 0.25, 'quorum': 1,
                   'sensor_window_ms': 40},
    },
}

# The REALTIME shape a cluster deploy must have. With duration_s > 0 the producers
# are finite, so manifests renders them as Jobs, the consumer Deployment reaches EOS
# and exits, and restartPolicy: Always restarts it — which is exactly what
# rollout_report's "restarted twice and still not ready" check flags as a failing
# deploy. duration_s = 0 gives the genuinely unbounded flow the REALTIME path is for.
K8S_FUSION_CONFIG = {**SOLUTION_CONFIGS['toy_fusion'], 'duration_s': 0}

def stage_solution(root : pathlib.Path, name : str, config : Dict[str, Any],
                   with_template : bool = False,
                   with_dockerfile : bool = False) -> pathlib.Path:
    '''
    Copies solution ``name`` into ``root``, writes ``config`` as its config.yaml,
    and returns the staged directory.

    Only the Python modules are copied — the graph, its glue nodes, the config
    loader and the prep hook. ``build_flow()`` is called by ``load_flow`` with no
    arguments, so a solution always reads the ``config.yaml`` next to its own
    module (``--config`` reaches only ``prepare.py``); staging a copy is the only
    way to give a run its own config and work_dir without writing into the repo.

    - Arguments:
        - root: the directory to stage into; the copy and every artifact live \
            under ``root/name``.
        - name: the directory name under ``solutions/``.
        - config: the solution's config.yaml as a dict.
        - with_template: also copy ``config.template.yaml``. A cluster deploy \
            needs it — ``_cmd_deploy`` only collects the hostPath mounts a \
            solution declares in ``x-mounts`` when it can find the template, and \
            without them the pods write their artifacts into a container-local \
            directory nobody can read. It is safe because ``ensure_config`` \
            returns the config.yaml written below before it ever looks at the \
            template. A local run needs neither.
        - with_dockerfile: also copy the ``Dockerfile``, which makes ``deploy`` \
            build the image instead of taking one. Only the autobuild test wants \
            this: the Dockerfile copies paths relative to the repo root, so a \
            build from here needs an explicit ``--build-context``.

    - Returns:
        - the staged solution directory (its work_dir is ``<staged>/out``).
    '''
    source = SOLUTIONS_DIR / name
    assert source.is_dir(), f'solution not found: {source}'
    work = root / name
    work.mkdir(parents = True)
    for module in source.glob('*.py'):
        shutil.copy(module, work / module.name)
    if with_template:
        shutil.copy(source / 'config.template.yaml', work / 'config.template.yaml')
    if with_dockerfile:
        shutil.copy(source / 'Dockerfile', work / 'Dockerfile')
    (work / 'config.yaml').write_text(yaml.safe_dump(config, sort_keys = False))
    # Create the work dir as the test user before anything else can. A hostPath
    # volume with no `type` and the prepare container both create a missing path as
    # root, and then the non-root test user cannot clean it up.
    (work / 'out').mkdir(exist_ok = True)
    return work

def read_artifact(work_dir : pathlib.Path, filename : str) -> Dict[str, Any]:
    '''The named JSON artifact, failing with the directory listing when absent.'''
    path = work_dir / filename
    assert path.is_file(), (f'{filename} was not written; work_dir holds: '
                            f'{sorted(p.name for p in work_dir.iterdir()) if work_dir.is_dir() else "nothing"}')
    with open(path) as f:
        return json.load(f)

# -- the assertions, one per solution ----------------------------------------
#
# Each of these is the solution's own success claim, not a claim about the engine.
# Both buckets call the same function on the same artifact.

def assert_calculator_report(report : Dict[str, Any]) -> None:
    '''
    ``matches_expected`` compares the flow's final statistics against the
    closed-form values prepare.py baked before the run, so it is true only if every
    integer crossed every edge exactly once and the join re-aligned all of them —
    the loss-free guarantee BATCH exists to provide.
    '''
    assert report['matches_expected'] is True, report
    assert report['pairs_seen'] == 40, report
    assert report['final_stats']['count'] == 40, report

def assert_router_counts(counts : Dict[str, Any]) -> None:
    '''
    Both assertions matter and they are different claims. ``matches_expected`` says
    every event was counted exactly once; ``sticky`` says each sensor was owned by
    exactly one replica — the property that makes replicating a stateful node
    correct at all. A routing bug can easily preserve the totals while losing
    stickiness.
    '''
    assert counts['matches_expected'] is True, counts
    assert counts['sticky'] is True, counts
    assert sum(counts['totals'].values()) == 120, counts

def assert_recovery_report(report : Dict[str, Any]) -> None:
    '''
    A conservation claim, and the only one that matters: **every event either
    arrived exactly once or was dead-lettered — never both, never neither.** The
    poison events are absent because they were quarantined; the crash event is
    present because the message was handed back rather than blamed, the worker was
    restarted, and the redelivery succeeded.
    '''
    assert report['matches_expected'] is True, report
    assert report['dead_lettered'] == [7, 19], report
    # The crash event came back: a sick worker never blames its message.
    assert 12 in report['delivered'], report

def assert_fusion_latest(latest : Dict[str, Any]) -> None:
    '''
    The live-state artifact, rewritten on every fused moment. The cameras and the
    IMU share no lineage, so nothing here could be grouped by trace id — a moment
    exists only because each producer stamps an event timestamp and the join groups
    on it.
    '''
    assert latest['moment'] > 0, latest
    assert latest['sensor_samples'] > 0, latest
