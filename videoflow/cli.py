'''
Command-line entrypoint for deploying a videoflow graph to Kubernetes.

    videoflow deploy path/to/graph.py:build_flow --nats nats://nats:4222 --namespace videoflow

The graph module must expose a factory (default name ``build_flow``) that returns a
built ``videoflow.core.flow.Flow`` without calling ``.run()`` on it — the CLI needs
the graph, not a running flow.
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import argparse
import importlib.util
import os
import sys

def _load_flow(target : str):
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

    spec = importlib.util.spec_from_file_location('_videoflow_user_graph', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, factory_name):
        raise SystemExit(f"Module '{path}' has no factory '{factory_name}'. "
                        f"Define '{factory_name}() -> Flow'.")
    factory = getattr(module, factory_name)
    return factory()

def _cmd_deploy(args):
    from .compiler import compile_flow
    from .image_registry import set_override
    from .manifests import render_manifests, dump_manifests

    for override in args.image_override or []:
        if '=' not in override:
            raise SystemExit(f'--image-override must be name=family, got: {override}')
        node_name, family = override.split('=', 1)
        set_override(node_name, family)

    import uuid

    flow = _load_flow(args.graph)
    if args.flow_id:
        flow._flow_id = args.flow_id
    run_id = args.run_id or uuid.uuid4().hex[:12]
    # Building the Flow already ran GraphEngine's cycle/uniqueness validation.
    specs = compile_flow(flow)
    manifests = render_manifests(
        specs, flow.flow_id, flow.flow_type, args.nats, run_id,
        namespace = args.namespace, registry = args.registry,
        image_tag = args.image_tag, blob_redis_url = args.blob_redis_url,
        autoscaling = args.autoscaling, max_replicas = args.max_replicas,
    )
    yaml_str = dump_manifests(manifests)

    if args.dry_run:
        sys.stdout.write(yaml_str)
        return

    os.makedirs(args.output, exist_ok = True)
    for m in manifests:
        fname = f"{m['kind'].lower()}-{m['metadata']['name']}.yaml"
        with open(os.path.join(args.output, fname), 'w') as f:
            f.write(dump_manifests([m]))

    kustomization = {
        'apiVersion': 'kustomize.config.k8s.io/v1beta1',
        'kind': 'Kustomization',
        'namespace': args.namespace,
        'resources': [
            f"{m['kind'].lower()}-{m['metadata']['name']}.yaml" for m in manifests
        ],
    }
    import yaml
    with open(os.path.join(args.output, 'kustomization.yaml'), 'w') as f:
        f.write(yaml.dump(kustomization, default_flow_style = False, sort_keys = False))

    print(f'Wrote {len(manifests)} manifests + kustomization.yaml to {args.output}')
    print(f'Apply with:  kubectl apply -k {args.output}')
    print(f'Flow id: {flow.flow_id}   Run id: {run_id}')

def _cmd_run_local(args):
    from .engines.local import LocalProcessEngine

    flow = _load_flow(args.graph)
    engine = LocalProcessEngine(nats_url = args.nats, blob_redis_url = args.blob_redis_url)
    flow.run(engine)
    print(f'Flow {flow.flow_id} running locally against {args.nats}. Ctrl-C to stop.')
    try:
        flow.join()
    except KeyboardInterrupt:
        flow.stop()

def _cmd_explain(args):
    from .compiler import compile_flow
    from .messaging.topology import subject_for, dlq_stream_name

    flow = _load_flow(args.graph)
    run_id = args.run_id or '<run-id>'
    specs = compile_flow(flow)
    lines = [f'Flow: {flow.flow_id}   type={flow.flow_type}   run={run_id}',
            f'Nodes ({len(specs)}):']
    for s in specs:
        bits = [f'replicas={s.nb_tasks}']
        if s.device_type == 'gpu':
            bits.append('device=gpu')
        if s.partition_by:
            bits.append(f'partition_by={s.partition_by}')
        if s.join_policy:
            bits.append(f"join={s.join_policy.get('missing')}")
        lines.append(f'  {s.name}  [{s.kind}]  image={s.image_family}  ' + '  '.join(bits))
        lines.append(f'      subject: {subject_for(flow.flow_id, run_id, s.name)}')
        if s.parents:
            lines.append(f'      from: {", ".join(s.parents)}')
    lines.append(f'DLQ stream: {dlq_stream_name(flow.flow_id, run_id)}')
    print('\n'.join(lines))

def _cmd_provision(args):
    import uuid
    from .compiler import compile_flow
    from .messaging.topology import provision_flow_sync

    flow = _load_flow(args.graph)
    run_id = args.run_id or uuid.uuid4().hex[:12]
    specs = compile_flow(flow)
    provision_flow_sync(args.nats, specs, flow.flow_id, run_id, flow.flow_type)
    print(f'Provisioned {len(specs)} node streams for flow {flow.flow_id} run {run_id}')

def _cmd_teardown(args):
    import asyncio
    import nats
    from .messaging.topology import control_subject_for, delete_run_streams

    async def _go():
        nc = await nats.connect(args.nats)
        try:
            await nc.publish(control_subject_for(args.flow_id, args.run_id), b'stop')
            await nc.flush()
            await delete_run_streams(nc, args.flow_id, args.run_id)
        finally:
            await nc.drain()

    asyncio.run(_go())
    print(f'Sent stop + deleted run streams for flow {args.flow_id} run {args.run_id}')
    if args.namespace:
        import subprocess
        from .manifests import LABEL_FLOW_ID, k8s_name
        selector = f'{LABEL_FLOW_ID}={k8s_name(args.flow_id)}'
        subprocess.run(['kubectl', 'delete', '-n', args.namespace,
                        'deployment,statefulset,job,configmap,service,networkpolicy,poddisruptionbudget,scaledobject',
                        '-l', selector], check = False)
        print(f'Deleted workloads in namespace {args.namespace} for flow {args.flow_id}')

def build_parser():
    parser = argparse.ArgumentParser(prog = 'videoflow', description = 'Deploy videoflow graphs.')
    sub = parser.add_subparsers(dest = 'command', required = True)

    deploy = sub.add_parser('deploy', help = 'Render/apply Kubernetes manifests for a graph.')
    deploy.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    deploy.add_argument('--nats', required = True, help = 'NATS URL reachable from inside the cluster.')
    deploy.add_argument('--namespace', default = 'default')
    deploy.add_argument('--flow-id', default = None,
                        help = 'Stable flow id for naming resources (overrides the graph module\'s). '
                               'Use the same value to redeploy/update an existing flow.')
    deploy.add_argument('--run-id', default = None,
                        help = 'Per-run id that scopes this run\'s broker streams (auto-generated if omitted). '
                               'A new run id gives fresh streams; reuse it to target the same run.')
    deploy.add_argument('--output', default = './manifests')
    deploy.add_argument('--registry', default = '', help = 'Image registry prefix, e.g. ghcr.io/acme.')
    deploy.add_argument('--image-tag', default = 'latest')
    deploy.add_argument('--blob-redis-url', default = None, help = 'Redis URL for the large-payload blob store.')
    deploy.add_argument('--image-override', action = 'append', metavar = 'NAME=FAMILY',
                        help = 'Override the Docker image family for a node. Repeatable.')
    deploy.add_argument('--autoscaling', action = 'store_true',
                        help = 'Emit a KEDA ScaledObject per processor node (requires KEDA in-cluster).')
    deploy.add_argument('--max-replicas', type = int, default = 10,
                        help = 'Upper bound for autoscaled processors (default 10).')
    deploy.add_argument('--dry-run', action = 'store_true', help = 'Print manifests to stdout, write nothing.')
    deploy.set_defaults(func = _cmd_deploy)

    run = sub.add_parser('run-local', help = 'Run a graph as local subprocesses against a NATS server.')
    run.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    run.add_argument('--nats', default = 'nats://localhost:4222')
    run.add_argument('--blob-redis-url', default = None)
    run.set_defaults(func = _cmd_run_local)

    explain = sub.add_parser('explain', help = 'Print a human-readable summary of a compiled graph.')
    explain.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    explain.add_argument('--run-id', default = None)
    explain.set_defaults(func = _cmd_explain)

    prov = sub.add_parser('provision', help = 'Create a flow\'s streams/durables on the broker (usually run automatically).')
    prov.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    prov.add_argument('--nats', required = True)
    prov.add_argument('--run-id', default = None)
    prov.set_defaults(func = _cmd_provision)

    teardown = sub.add_parser('teardown', help = 'Stop a run and delete its broker streams (and, with --namespace, its K8s workloads).')
    teardown.add_argument('--flow-id', required = True)
    teardown.add_argument('--run-id', required = True)
    teardown.add_argument('--nats', required = True)
    teardown.add_argument('--namespace', default = None, help = 'If set, also kubectl-delete the flow\'s workloads.')
    teardown.set_defaults(func = _cmd_teardown)

    return parser

def main(argv = None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)

if __name__ == '__main__':
    main()
