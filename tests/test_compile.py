'''
The videoflow.compile entrypoint: a graph module compiles to a JSON document
that round-trips back into equivalent NodeSpec objects — the contract that lets
deploy compile inside the solution container and render manifests on the host.
'''
import json
import subprocess
import sys

from videoflow.deploy.compile import compile_to_dict, specs_from_document

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


INLINE_GRAPH = '''
import videoflow
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer

class InlineNode(videoflow.core.node.ProcessorNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def process(self, x):
        return x

def build_flow(cfg = None):
    p = IntProducer(0, 3, name = 'producer')
    n = InlineNode(name = 'inline')(p)
    out = CommandlineConsumer(name = 'printer')(n)
    return Flow([out], flow_type = BATCH, flow_id = 'inline')
'''


def _node_class(document, name):
    return [s['node_class'] for s in document['specs'] if s['name'] == name][0]


def test_inline_node_class_path_is_importable(tmp_path):
    '''
    A node class defined in the graph module must compile to a class path a worker
    can actually import. Loading the graph under a synthetic module name produced
    `_videoflow_user_graph.InlineNode`, which resolves to no file anywhere.
    '''
    graph = tmp_path / 'mygraph.py'
    graph.write_text(INLINE_GRAPH)
    document = compile_to_dict(str(graph))
    assert _node_class(document, 'inline') == 'mygraph.InlineNode'
    # ...and the recorded module really is importable, which is the whole point.
    import importlib
    import sys
    sys.path.insert(0, str(tmp_path))
    try:
        assert hasattr(importlib.import_module('mygraph'), 'InlineNode')
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop('mygraph', None)


def test_graph_named_after_an_installed_module_does_not_shadow_it(tmp_path):
    '''A graph called json.py must never displace the stdlib module.'''
    graph = tmp_path / 'json.py'
    graph.write_text(INLINE_GRAPH)
    document = compile_to_dict(str(graph))
    # Falls back to the private name rather than claiming `json`.
    assert _node_class(document, 'inline') == 'json.InlineNode'.replace('json', '_videoflow_user_graph')
    import json as stdlib_json
    assert stdlib_json.dumps({'ok': True}) == '{"ok": true}'


def test_non_identifier_filename_falls_back(tmp_path):
    graph = tmp_path / 'my-graph.py'          # hyphen: not importable as a name
    graph.write_text(INLINE_GRAPH)
    document = compile_to_dict(str(graph))
    assert _node_class(document, 'inline') == '_videoflow_user_graph.InlineNode'


def test_cli_module_prints_json_document(tmp_path):
    graph = _write_graph(tmp_path)
    proc = subprocess.run([sys.executable, '-m', 'videoflow.compile', graph],
                          capture_output = True, text = True, check = True)
    flow_id, flow_type, specs = specs_from_document(proc.stdout)
    assert flow_id == 'demo' and flow_type == 'batch' and len(specs) == 3
