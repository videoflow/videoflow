'''
`videoflow --version`, and the solution config convention applied by the
commands that only load a graph (`explain`, `provision`): --config / the
config.yaml beside the graph / the template's Q&A, exactly as `deploy` does, so a
solution whose build_flow reads its config gets the same treatment before the
first deploy has written one. Also pins the toy solutions honouring
VF_SOLUTION_CONFIG, which is what makes --config reach them.
'''
import json
import os
import subprocess
import sys

import pytest

import videoflow
from videoflow.deploy import cli, solution

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# A graph that reads its config the way the shipped solutions do.
_CONFIG_GRAPH = '''
import os
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer

def build_flow(cfg = None):
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.environ.get('VF_SOLUTION_CONFIG') or os.path.join(here, 'config.yaml')
    with open(path) as f:
        end = int(f.read().split(':')[1])
    return Flow([CommandlineConsumer(name = 'printer')(IntProducer(0, end, name = 'numbers'))],
                flow_type = BATCH, flow_id = 'cfgdemo')
'''
_TEMPLATE = 'end_value: 5\nx-questions:\n  - {key: end_value, prompt: Last integer, type: int, default: 5}\n'


@pytest.fixture
def graph(tmp_path, monkeypatch):
    (tmp_path / 'graph.py').write_text(_CONFIG_GRAPH)
    (tmp_path / 'config.template.yaml').write_text(_TEMPLATE)
    # cli.main exports the resolved config into os.environ; set-then-delete makes
    # monkeypatch restore the variable to absent afterwards (delenv alone records
    # nothing for a variable that is not set), so later tests never see it.
    monkeypatch.setenv(solution.CONFIG_ENV, 'reset-by-fixture')
    monkeypatch.delenv(solution.CONFIG_ENV)
    return tmp_path


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(['--version'])
    assert e.value.code == 0
    assert capsys.readouterr().out.strip() == f'videoflow {videoflow.__version__}'


def test_explain_uses_an_explicit_config(graph, capsys):
    (graph / 'mine.yaml').write_text('end_value: 7\n')
    assert cli.main(['explain', str(graph / 'graph.py'), '--config', str(graph / 'mine.yaml')]) == 0
    assert 'cfgdemo' in capsys.readouterr().out
    assert not (graph / 'config.yaml').exists()


def test_explain_without_a_config_lists_the_inputs_instead_of_a_traceback(graph):
    with pytest.raises(SystemExit) as e:
        cli.main(['explain', str(graph / 'graph.py'), '--non-interactive'])
    message = str(e.value)
    assert 'Inputs needed' in message and 'end_value' in message
    assert not (graph / 'config.yaml').exists()


def test_explain_generates_the_config_from_the_template_when_interactive(graph, monkeypatch, capsys):
    monkeypatch.setattr('sys.stdin.isatty', lambda: True)
    monkeypatch.setattr(solution, 'ask_questions', lambda questions, graph_dir, input_fn = None: {'end_value': 9})
    assert cli.main(['explain', str(graph / 'graph.py')]) == 0
    assert (graph / 'config.yaml').exists()
    assert 'cfgdemo' in capsys.readouterr().out


TOYS = ('toy_calculator', 'toy_router', 'toy_recovery', 'toy_fusion')

# Loads each toy in turn with VF_SOLUTION_CONFIG pointing at a copy of its example
# config, and prints {name: flow_id}. The toys all ship a sibling ``common.py``, so
# between loads every module that came from the previous toy's directory is
# evicted (a stale ``common`` would silently serve the next toy); the check that
# ``common`` really is the toy's own is what makes one process as honest as four.
_LOAD_TOYS = '''
import json, os, sys
from videoflow.deploy.compile import load_flow
flow_ids = {}
for name, graph, config in json.loads(sys.argv[1]):
    os.environ['VF_SOLUTION_CONFIG'] = config
    solution_dir = os.path.dirname(graph)
    flow_ids[name] = load_flow(graph).flow_id
    assert os.path.dirname(os.path.abspath(sys.modules['common'].__file__)) == solution_dir, name
    for module_name, module in list(sys.modules.items()):
        file = getattr(module, '__file__', None)
        if file and os.path.dirname(os.path.abspath(file)) == solution_dir:
            del sys.modules[module_name]
    sys.path.remove(solution_dir)
print(json.dumps(flow_ids))
'''


def test_toy_solutions_honour_vf_solution_config(tmp_path):
    jobs = []
    for name in TOYS:
        solution_dir = os.path.join(ROOT, 'solutions', name)
        assert not os.path.exists(os.path.join(solution_dir, 'config.yaml')), \
            'this test relies on no config.yaml being checked in'
        # The config's work_dir resolves next to the config file, so a copy under
        # tmp_path keeps each solution's out/ directory out of the checkout.
        config_dir = tmp_path / name
        config_dir.mkdir()
        (config_dir / 'config.yaml').write_text(open(os.path.join(solution_dir, 'config.example.yaml')).read())
        jobs.append((name, os.path.join(solution_dir, f'{name}.py'), str(config_dir / 'config.yaml')))
    proc = subprocess.run([sys.executable, '-c', _LOAD_TOYS, json.dumps(jobs)],
                          cwd = tmp_path, env = dict(os.environ), capture_output = True, text = True)
    assert proc.returncode == 0, proc.stderr
    flow_ids = json.loads(proc.stdout)
    assert set(flow_ids) == set(TOYS) and all(flow_ids.values())
    for name in TOYS:
        assert (tmp_path / name / 'out').is_dir(), name   # ... and it was the copy that was honoured
