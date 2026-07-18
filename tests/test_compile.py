'''
The videoflow.compile entrypoint: a graph module compiles to a JSON document
that round-trips back into equivalent NodeSpec objects — the contract that lets
deploy compile inside the solution container and render manifests on the host.
'''
import json
import subprocess
import sys

from videoflow.compile import compile_to_dict, specs_from_document

GRAPH = '''
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer
from helper import NAME

def build_flow(cfg = None):
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = NAME, nb_tasks = 2, partition_by = 'trace_id')(p)
    out = CommandlineConsumer(name = 'printer')(a)
    return Flow([out], flow_type = BATCH, flow_id = 'demo')
'''


def _write_graph(tmp_path):
    (tmp_path / 'graph.py').write_text(GRAPH)
    # A sibling module: proves load_flow adds the graph dir to sys.path.
    (tmp_path / 'helper.py').write_text("NAME = 'work'\n")
    return str(tmp_path / 'graph.py')


def test_compile_to_dict_round_trips(tmp_path):
    document = compile_to_dict(_write_graph(tmp_path))
    # The document must be pure JSON (what the container prints to stdout).
    document = json.loads(json.dumps(document))
    flow_id, flow_type, specs = specs_from_document(document)
    assert (flow_id, flow_type) == ('demo', 'batch')
    by_name = {s.name: s for s in specs}
    assert set(by_name) == {'producer', 'work', 'printer'}
    assert by_name['work'].nb_tasks == 2
    assert by_name['work'].partition_by == 'trace_id'


def test_cli_module_prints_json_document(tmp_path):
    graph = _write_graph(tmp_path)
    proc = subprocess.run([sys.executable, '-m', 'videoflow.compile', graph],
                          capture_output = True, text = True, check = True)
    flow_id, flow_type, specs = specs_from_document(proc.stdout)
    assert flow_id == 'demo' and flow_type == 'batch' and len(specs) == 3
