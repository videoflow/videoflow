'''
Compiles a graph module to a JSON specs document, for use where the operator
machine cannot import the graph (its ML dependencies live only in the solution
image). ``videoflow deploy`` runs this *inside* that image::

    docker run --rm -v <graph_dir>:<graph_dir> -w <graph_dir> <image> \
        python -m videoflow.compile graph.py[:factory]

and rebuilds the ``NodeSpec`` list on the host with ``NodeSpec.from_dict`` —
the same serialization the provision Job's specs ConfigMap uses.

Output (stdout): ``{"flow_id": ..., "flow_type": ..., "specs": [...]}``.
'''
from __future__ import absolute_import, division, print_function

import argparse
import importlib.util
import json
import os
import sys
from typing import Any


def load_flow(target : str) -> Any:
    '''
    - Arguments:
        - target: ``path/to/graph.py`` or ``path/to/graph.py:factory_name`` \
            (factory defaults to ``build_flow``).

    - Returns:
        - a built ``Flow`` produced by calling the factory.
    '''
    if ':' in target:
        path, factory_name = target.rsplit(':', 1)
    else:
        path, factory_name = target, 'build_flow'

    if not os.path.isfile(path):
        raise SystemExit(f'Graph module not found: {path}')

    # Make the graph's sibling modules importable (e.g. `from common import ...`)
    # regardless of the caller's cwd.
    graph_dir = os.path.dirname(os.path.abspath(path))
    if graph_dir not in sys.path:
        sys.path.insert(0, graph_dir)

    spec = importlib.util.spec_from_file_location('_videoflow_user_graph', path)
    if spec is None or spec.loader is None:
        raise SystemExit(f'Could not load graph module: {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, factory_name):
        raise SystemExit(f"Module '{path}' has no factory '{factory_name}'. "
                        f"Define '{factory_name}() -> Flow'.")
    factory = getattr(module, factory_name)
    return factory()

def compile_to_dict(target, envelope_version = None, allow_pickle = False) -> dict:
    from .compiler import compile_flow

    flow = load_flow(target)
    specs = compile_flow(flow, envelope_version = envelope_version, allow_pickle = allow_pickle)
    return {
        'flow_id': flow.flow_id,
        'flow_type': flow.flow_type,
        'specs': [s.to_dict() for s in specs],
    }

def specs_from_document(document) -> tuple:
    '''``(flow_id, flow_type, specs)`` from a compile-JSON document (dict or JSON string).'''
    from .compiler import NodeSpec

    if isinstance(document, str):
        document = json.loads(document)
    specs = [NodeSpec.from_dict(d) for d in document['specs']]
    return document['flow_id'], document['flow_type'], specs

def main(argv = None) -> None:
    ap = argparse.ArgumentParser(prog = 'python -m videoflow.compile',
                                 description = 'Compile a graph module to a JSON specs document on stdout.')
    ap.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    ap.add_argument('--envelope-version', type = int, default = None)
    ap.add_argument('--allow-pickle', action = 'store_true')
    args = ap.parse_args(argv)
    try:
        document = compile_to_dict(args.graph, envelope_version = args.envelope_version,
                                   allow_pickle = args.allow_pickle)
    except ValueError as e:
        raise SystemExit(str(e)) from e
    json.dump(document, sys.stdout)
    sys.stdout.write('\n')

if __name__ == '__main__':
    main()
