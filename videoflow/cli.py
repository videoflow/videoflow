'''
Command-line entrypoint for deploying a videoflow graph to Kubernetes.

    videoflow deploy path/to/graph.py:build_flow --nats nats://nats:4222 --namespace videoflow

The graph module must expose a factory (default name ``build_flow``) that returns a
built ``videoflow.core.flow.Flow`` without calling ``.run()`` on it — the CLI needs
the graph, not a running flow.
'''
from __future__ import absolute_import, division, print_function

import argparse
import importlib.util
import os
import sys
from typing import Any


def _load_flow(target : str) -> Any:
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
    if spec is None or spec.loader is None:
        raise SystemExit(f'Could not load graph module: {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, factory_name):
        raise SystemExit(f"Module '{path}' has no factory '{factory_name}'. "
                        f"Define '{factory_name}() -> Flow'.")
    factory = getattr(module, factory_name)
    return factory()

def _cmd_deploy(args) -> None:
    import uuid

    from .compiler import compile_flow
    from .images import parse_override
    from .manifests import dump_manifests, render_manifests

    overrides = {}
    for override in args.image_override or []:
        try:
            name, ref = parse_override(override)
        except ValueError as e:
            raise SystemExit(str(e)) from e
        overrides[name] = ref

    flow = _load_flow(args.graph)
    if args.flow_id:
        flow._flow_id = args.flow_id
    run_id = args.run_id or uuid.uuid4().hex[:12]
    # Building the Flow already ran GraphEngine's cycle/uniqueness validation.
    try:
        specs = compile_flow(flow, envelope_version = args.envelope_version, allow_pickle = args.allow_pickle)
        manifests = render_manifests(
            specs, flow.flow_id, flow.flow_type, args.nats, run_id,
            namespace = args.namespace, default_image = args.image,
            image_overrides = overrides, blob_redis_url = args.blob_redis_url,
            autoscaling = args.autoscaling, max_replicas = args.max_replicas,
            envelope_version = args.envelope_version, allow_pickle = args.allow_pickle,
            provision_image = args.provision_image,
        )
    except ValueError as e:
        raise SystemExit(str(e)) from e
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

def _cmd_component_validate(args) -> None:
    from .component import load_descriptor

    ok = True
    for path in args.path:
        try:
            desc = load_descriptor(path)
        except Exception as e:
            print(f'INVALID  {path}: {e}')
            ok = False
            continue
        images = ', '.join(f'{k}={v}' for k, v in sorted(desc.images.items()))
        print(f'OK       {desc.name} v{desc.version}  role={desc.role}  protocol={desc.protocol}  '
            f'device={desc.device}  images[{images}]')
    if not ok:
        raise SystemExit(1)

def _cmd_component_push(args) -> None:
    from .oci import push_component

    try:
        target = push_component(args.path, args.ref)
    except Exception as e:
        raise SystemExit(f'push failed: {e}') from e
    print(f'Pushed component descriptor {args.path} -> oci://{target}')

def _cmd_component_pull(args) -> None:
    from .oci import pull_component

    try:
        path = pull_component(args.ref, force = args.force, verify = args.verify,
                            cosign_args = args.cosign_arg or None)
    except Exception as e:
        raise SystemExit(f'pull failed: {e}') from e
    print(f'Resolved {args.ref} -> {path}')

def _cmd_component_inspect(args) -> None:
    from .oci import inspect_component

    try:
        d = inspect_component(args.ref, verify = args.verify)
    except Exception as e:
        raise SystemExit(f'inspect failed: {e}') from e
    images = ', '.join(f'{k}={v}' for k, v in sorted(d.images.items()))
    kind = 'python' if not d.is_native else 'native'
    print(f'{d.name} v{d.version}  ({kind}, role={d.role}, protocol={d.protocol})')
    print(f'  device: {d.device}')
    print(f'  images: {images}')
    if d.python_class:
        print(f'  pythonClass: {d.python_class}')
    if d.params_schema.get('properties'):
        print(f'  params: {", ".join(sorted(d.params_schema["properties"]))}')

def _cmd_run_local(args) -> None:
    from .engines.local import LocalProcessEngine

    flow = _load_flow(args.graph)
    engine = LocalProcessEngine(nats_url = args.nats, blob_redis_url = args.blob_redis_url,
                                allow_pickle = args.allow_pickle,
                                local_docker_nats_url = args.local_docker_nats_url)
    flow.run(engine)
    print(f'Flow {flow.flow_id} running locally against {args.nats}. Ctrl-C to stop.')
    try:
        flow.join()
    except KeyboardInterrupt:
        flow.stop()

def _cmd_explain(args) -> None:
    from .compiler import specs_from_tasks_data
    from .messaging.topology import dlq_stream_name, subject_for

    flow = _load_flow(args.graph)
    run_id = args.run_id or '<run-id>'
    # Describe the graph without enforcing wire compatibility (that is a deploy-time
    # concern; explain must work for any flow, including remote-on-default-wire).
    specs = specs_from_tasks_data(flow.tasks_data())
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
        image = s.image or '«--image default»'
        kind = f'{s.kind}, remote' if s.is_remote else s.kind
        lines.append(f'  {s.name}  [{kind}]  image={image}  ' + '  '.join(bits))
        if s.is_remote:
            lines.append(f'      component: {s.component_ref}  (protocol v{s.protocol_version})')
        lines.append(f'      subject: {subject_for(flow.flow_id, run_id, s.name)}')
        if s.parents:
            lines.append(f'      from: {", ".join(s.parents)}')
    lines.append(f'DLQ stream: {dlq_stream_name(flow.flow_id, run_id)}')
    print('\n'.join(lines))

def _cmd_provision(args) -> None:
    import uuid

    from .compiler import specs_from_tasks_data
    from .messaging.topology import provision_flow_sync

    flow = _load_flow(args.graph)
    run_id = args.run_id or uuid.uuid4().hex[:12]
    # Provisioning only needs stream/durable names (language-neutral); no wire check.
    specs = specs_from_tasks_data(flow.tasks_data())
    provision_flow_sync(args.nats, specs, flow.flow_id, run_id, flow.flow_type)
    print(f'Provisioned {len(specs)} node streams for flow {flow.flow_id} run {run_id}')

def _cmd_teardown(args) -> None:
    import asyncio

    import nats

    from .messaging.topology import control_subject_for, delete_run_streams

    async def _go() -> None:
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

def _format_payload(message : Any) -> str:
    '''One-line human summary of a decoded payload for the debug inspector.'''
    if message is None:
        return 'None (EOS or empty)'
    import numpy as np
    if isinstance(message, np.ndarray):
        return f'ndarray shape={tuple(message.shape)} dtype={message.dtype}'
    from .serialization import RawPayload
    if isinstance(message, RawPayload):
        return f'RawPayload type={message.payload_type} ({len(message.data)} bytes, opaque)'
    if hasattr(message, 'DESCRIPTOR'):
        return f'{message.DESCRIPTOR.full_name}: ' + str(message).replace('\n', ' ').strip()[:200]
    return repr(message)[:200]

def _print_decoded(buf : bytes, headers : dict = None) -> None:
    from .serialization import decode_envelope
    d = decode_envelope(buf)
    if headers:
        interesting = {k: v for k, v in headers.items() if k.startswith('VF-') or k == 'Nats-Msg-Id'}
        if interesting:
            print('  headers: ' + '  '.join(f'{k}={v}' for k, v in interesting.items()))
    print(f"  {d['type']}  producer={d['producer_name']}  trace={d['trace_id']}  seq={d['seq']}  "
        f"replica={d['replica_id']}  event_ts={d['event_ts']}")
    if d['metadata']:
        print(f"  metadata: {d['metadata']}")
    print(f"  payload: {_format_payload(d['message'])}")

def _cmd_debug_decode(args) -> None:
    if args.file:
        with open(args.file, 'rb') as f:
            buf = f.read()
        print(f'Envelope from {args.file} ({len(buf)} bytes):')
        _print_decoded(buf)
        return
    if not args.dlq:
        raise SystemExit('Provide a FILE of raw envelope bytes, or --dlq with --flow-id/--run-id.')
    if not (args.flow_id and args.run_id):
        raise SystemExit('--dlq requires --flow-id and --run-id.')

    import asyncio

    import nats

    from .messaging.topology import dlq_stream_name

    async def _go() -> None:
        nc = await nats.connect(args.nats)
        js = nc.jetstream()
        stream = dlq_stream_name(args.flow_id, args.run_id)
        subject = f'vf.{args.flow_id}.{args.run_id}._dlq.>'
        try:
            # Ephemeral, no-ack inspection: the messages stay in the DLQ.
            sub = await js.pull_subscribe(subject, stream = stream)
            printed = 0
            while printed < args.limit:
                try:
                    msgs = await sub.fetch(batch = min(10, args.limit - printed), timeout = 2.0)
                except (nats.errors.TimeoutError, TimeoutError):
                    break
                for msg in msgs:
                    print(f'--- DLQ message {printed + 1} (subject {msg.subject}) ---')
                    _print_decoded(msg.data, headers = dict(msg.headers or {}))
                    printed += 1
            if printed == 0:
                print(f'No messages in DLQ stream {stream}.')
            else:
                print(f'Decoded {printed} DLQ message(s) from {stream} (left in place).')
        finally:
            await nc.drain()

    asyncio.run(_go())

def build_parser() -> argparse.ArgumentParser:
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
    deploy.add_argument('--image', default = None,
                        help = 'Default container image ref for nodes that do not declare their own '
                               '(e.g. ghcr.io/acme/app:v1). Build it FROM videoflow-base with your code + deps.')
    deploy.add_argument('--blob-redis-url', default = None, help = 'Redis URL for the large-payload blob store.')
    deploy.add_argument('--image-override', action = 'append', metavar = 'NAME=IMAGE',
                        help = 'Override the container image for one node (wins over --image and the node\'s '
                               'own image=). Repeatable.')
    deploy.add_argument('--autoscaling', action = 'store_true',
                        help = 'Emit a KEDA ScaledObject per processor node (requires KEDA in-cluster).')
    deploy.add_argument('--max-replicas', type = int, default = 10,
                        help = 'Upper bound for autoscaled processors (default 10).')
    deploy.add_argument('--envelope-version', type = int, default = None,
                        help = 'Wire envelope version (3 msgpack | 4 protobuf). A flow with any '
                               'remote component is forced to 4. Defaults to the build default.')
    deploy.add_argument('--allow-pickle', action = 'store_true',
                        help = 'Permit the legacy Python-only pickle payload codec (rejected for '
                               'flows containing remote components).')
    deploy.add_argument('--provision-image', default = None,
                        help = 'Image the provision init Job runs on (needs videoflow + broker client). '
                               'Set this when --image is a non-Python vendor image.')
    deploy.add_argument('--dry-run', action = 'store_true', help = 'Print manifests to stdout, write nothing.')
    deploy.set_defaults(func = _cmd_deploy)

    run = sub.add_parser('run-local', help = 'Run a graph as local subprocesses against a NATS server.')
    run.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    run.add_argument('--nats', default = 'nats://localhost:4222')
    run.add_argument('--blob-redis-url', default = None)
    run.add_argument('--allow-pickle', action = 'store_true',
                    help = 'Permit the legacy Python-only pickle payload codec.')
    run.add_argument('--local-docker-nats-url', default = None,
                    help = 'NATS URL a docker-run remote component connects to '
                           '(default rewrites localhost -> host.docker.internal).')
    run.set_defaults(func = _cmd_run_local)

    comp = sub.add_parser('component', help = 'Work with component descriptors.')
    comp_sub = comp.add_subparsers(dest = 'component_command', required = True)
    validate = comp_sub.add_parser('validate', help = 'Validate one or more component.yaml descriptors.')
    validate.add_argument('path', nargs = '+', help = 'Path(s) to a component.yaml or its directory.')
    validate.set_defaults(func = _cmd_component_validate)

    push = comp_sub.add_parser('push', help = 'Push a component descriptor to an OCI registry as an artifact.')
    push.add_argument('path', help = 'Path to a component.yaml or its directory.')
    push.add_argument('ref', help = 'Target ref, e.g. oci://ghcr.io/vendor/name:1.2.0')
    push.set_defaults(func = _cmd_component_push)

    pull = comp_sub.add_parser('pull', help = 'Pull + cache a component descriptor from an OCI registry.')
    pull.add_argument('ref', help = 'Component ref, e.g. oci://ghcr.io/vendor/name:1.2.0')
    pull.add_argument('--force', action = 'store_true', help = 'Re-pull even if cached.')
    pull.add_argument('--verify', action = 'store_true', help = 'Verify the artifact signature with cosign before trusting it.')
    pull.add_argument('--cosign-arg', action = 'append', metavar = 'ARG',
                    help = 'Extra arg passed to `cosign verify` (e.g. --key=cosign.pub). Repeatable.')
    pull.set_defaults(func = _cmd_component_pull)

    inspect = comp_sub.add_parser('inspect', help = 'Show a remote component descriptor (pulls + caches it).')
    inspect.add_argument('ref', help = 'Component ref, e.g. oci://ghcr.io/vendor/name:1.2.0')
    inspect.add_argument('--verify', action = 'store_true', help = 'Verify the signature with cosign first.')
    inspect.set_defaults(func = _cmd_component_inspect)

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

    debug = sub.add_parser('debug', help = 'Inspect wire messages (envelopes, DLQ).')
    debug_sub = debug.add_subparsers(dest = 'debug_command', required = True)
    decode = debug_sub.add_parser('decode', help = 'Decode and print videoflow envelope(s) from a file or a run\'s DLQ.')
    decode.add_argument('file', nargs = '?', help = 'Path to a file of raw envelope bytes.')
    decode.add_argument('--dlq', action = 'store_true', help = 'Read from a run\'s DLQ stream instead of a file.')
    decode.add_argument('--nats', default = 'nats://localhost:4222')
    decode.add_argument('--flow-id', default = None)
    decode.add_argument('--run-id', default = None)
    decode.add_argument('--limit', type = int, default = 20, help = 'Max DLQ messages to decode (default 20).')
    decode.set_defaults(func = _cmd_debug_decode)

    return parser

def main(argv = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)

if __name__ == '__main__':
    main()
