'''
Command-line entrypoint for deploying a videoflow graph to Kubernetes.

    videoflow deploy path/to/graph.py

``deploy`` is one command that does everything: it generates the solution config
(asking the template's questions when none exists), runs the solution's prepare
hook, builds and loads the node image into the detected local cluster, provisions
the broker (an in-cluster dev NATS + Redis when ``--nats`` is omitted), applies
the flow, and — for a BATCH flow — waits for it to run to completion and then
tears down every resource (Kubernetes workloads + broker streams + owned infra).
A REALTIME flow is applied and left running; stop it later with ``videoflow
teardown``. Each automatic step has an explicit override (``--config``,
``--image``, ``--nats``, ``--mount``, ``--no-prepare``, ``--no-build``, ...).
Nothing is written to disk (beyond a generated ``config.yaml``) unless
``--render-only`` (write manifest files + kustomization) or ``--dry-run`` (print
YAML to stdout) is given.

The graph module must expose a factory (default name ``build_flow``) that returns a
built ``videoflow.core.flow.Flow`` without calling ``.run()`` on it — the CLI needs
the graph, not a running flow. When the graph's dependencies are not importable on
this machine, deploy compiles it inside the solution image instead.
'''
from __future__ import absolute_import, division, print_function

import argparse
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
    from .compile import load_flow
    return load_flow(target)

def _cmd_deploy(args) -> None:
    import subprocess
    import uuid

    from .core.constants import BATCH
    from .images import parse_override, resolve_image
    from .manifests import parse_mounts

    overrides = {}
    for override in args.image_override or []:
        try:
            name, ref = parse_override(override)
        except ValueError as e:
            raise SystemExit(str(e)) from e
        overrides[name] = ref

    graph_path = args.graph.rsplit(':', 1)[0] if ':' in args.graph else args.graph
    if not os.path.isfile(graph_path):
        raise SystemExit(f'Graph module not found: {graph_path}')
    graph_dir = os.path.dirname(os.path.abspath(graph_path))
    graph_target = os.path.abspath(graph_path) + \
        (':' + args.graph.rsplit(':', 1)[1] if ':' in args.graph else '')

    # 0. Solution conventions: ensure a config (interactive Q&A over the solution's
    # config.template.yaml when none exists) and collect its x-mounts.
    from .solution import ensure_config, find_template, load_template, resolve_mounts
    interactive = not args.non_interactive and sys.stdin.isatty()
    config_path = ensure_config(graph_dir, args.config, interactive)
    template_path = find_template(graph_dir)
    template_mounts = []
    if template_path and config_path:
        import yaml
        with open(config_path) as f:
            template_mounts = resolve_mounts(load_template(template_path), yaml.safe_load(f),
                                             graph_dir)
    try:
        mounts = parse_mounts((args.mount or []) + template_mounts)
        # Prep/compile containers additionally see the solution directory itself
        # (config, prepare.py, work dir) at its host path.
        container_mounts = parse_mounts([graph_dir]) + mounts
    except ValueError as e:
        raise SystemExit(str(e)) from e

    # 1. Image: --image wins; else build from the solution's [gpu.]Dockerfile
    # (base image auto-built from a source checkout when missing).
    from .build import autobuild, docker_gpus_available, image_exists, run_in_image
    gpus = docker_gpus_available()
    image = args.image
    if image is None and not args.no_build:
        try:
            image = autobuild(graph_dir, needs_gpu = gpus, context_override = args.build_context)
        except RuntimeError as e:
            raise SystemExit(str(e)) from e

    # 2. Prepare hook: runs inside the image, before compiling (its outputs get
    # baked into the compiled specs).
    from .solution import find_prepare, prepare_command, run_prepare_local
    if find_prepare(graph_dir) is not None and not args.no_prepare:
        try:
            if image is not None:
                run_in_image(image, prepare_command(config_path), mounts = container_mounts,
                             workdir = graph_dir, gpus = gpus, interactive = interactive)
            else:
                run_prepare_local(graph_dir, config_path)
        except (RuntimeError, subprocess.CalledProcessError) as e:
            raise SystemExit(str(e)) from e

    # 3. Compile: locally when the graph's deps import on the host, else inside
    # the image (specs round-trip as JSON — same format as the specs ConfigMap).
    flow_id, flow_type, specs = _compile_graph(args, graph_target, graph_dir, image,
                                               container_mounts, gpus)
    if args.flow_id:
        flow_id = args.flow_id
    run_id = args.run_id or uuid.uuid4().hex[:12]

    # --dry-run / --render-only never touch the cluster (they include the dev-infra
    # manifests whenever the broker would have been auto-provisioned).
    if args.dry_run or args.render_only:
        _render_manifests_to_disk(args, flow_id, flow_type, specs, run_id, overrides, mounts)
        return

    # 4. Cluster mechanics: detect the flavor, load locally built images into it,
    # and warn (with copy-pasteable fixes) on hostPath/GPU mismatches.
    from .cluster import detect_cluster, gpu_preflight, hostpath_warning, load_images
    flavor = detect_cluster(args.kubectl)
    try:
        images = sorted({resolve_image(s.name, s.image, image, overrides) for s in specs})
    except ValueError as e:
        raise SystemExit(str(e)) from e
    local_images = [ref for ref in images if image_exists(ref)]
    if local_images:
        try:
            load_images(flavor, local_images, kubectl = args.kubectl)
        except RuntimeError as e:
            raise SystemExit(str(e)) from e
    if mounts:
        warning = hostpath_warning(flavor)
        if warning:
            print(f'WARNING: {warning}', file = sys.stderr)
    gpu_specs = [s for s in specs if s.device_type == 'gpu']
    if gpu_specs:
        from .manifests import _is_partitioned, gpu_demand
        # Whole-flow demand per extended resource: every replica of a GPU node claims
        # its own gpu_count devices, so a partially-schedulable flow deadlocks.
        demand = gpu_demand(specs, default_resource = args.gpu_resource_name)
        problems = gpu_preflight(args.kubectl, gpu_runtime_class = args.gpu_runtime_class,
                                 demand = demand, gpu_mode = args.gpu_mode)
        if args.gpu_mode == 'shared':
            from .cluster import SHARED_NEEDS_RUNTIME_CLASS, allocatable_gpus
            pods = sum(s.nb_tasks for s in gpu_specs)
            physical = allocatable_gpus(args.kubectl)
            shared_with = (f'{physical} schedulable GPU unit(s)' if physical
                           else 'the GPU pool')
            print(f'GPU mode shared: {pods} pod(s) will share {shared_with} '
                  f'with no memory isolation — VRAM is the only limit.', file = sys.stderr)
            # Without a runtime class, shared pods have no path to the device at all
            # (they carry no resource limit for the device plugin to act on) — this
            # is fatal regardless of --strict-preflight.
            fatal = [p for p in problems if p.startswith(SHARED_NEEDS_RUNTIME_CLASS)]
            if fatal:
                raise SystemExit('ERROR: ' + fatal[0])
        elif args.autoscaling and args.gpu_autoscaling:
            # Partitioned nodes never autoscale (fixed scale), so they contribute
            # their fixed replica count to the ceiling, not max_replicas.
            ceiling = sum((s.nb_tasks if _is_partitioned(s) else max(s.nb_tasks, args.max_replicas))
                          * s.gpu_count for s in gpu_specs)
            print(f'NOTE: --gpu-autoscaling can scale GPU demand up to {ceiling} device(s); '
                  f'replicas beyond allocatable capacity will wait Pending.', file = sys.stderr)
        elif args.autoscaling:
            print(f'NOTE: {len(gpu_specs)} GPU node(s) excluded from --autoscaling (each '
                  f'extra replica claims whole GPUs); pass --gpu-autoscaling to include them.',
                  file = sys.stderr)
        for problem in problems:
            print(f'WARNING: {problem}', file = sys.stderr)
        if problems and args.strict_preflight:
            raise SystemExit('ERROR: --strict-preflight set and the GPU preflight found '
                             'problems (above); nothing was applied.')

    # 5. Broker infra: bring-your-own via --nats, else auto-provision dev NATS
    # (+ Redis for the blob store) in the namespace, owning only what we created.
    from .infra import ensure_infra, ensure_namespace, teardown_infra, wait_infra_ready
    nats_url = args.nats
    blob_redis_url = args.blob_redis_url
    created = []
    if nats_url is None:
        try:
            ensure_namespace(args.kubectl, args.namespace)
            urls, created = ensure_infra(args.kubectl, args.namespace,
                                         need_redis = blob_redis_url is None)
            wait_infra_ready(args.kubectl, args.namespace, created)
        except RuntimeError as e:
            raise SystemExit(str(e)) from e
        nats_url = urls['nats']
        blob_redis_url = blob_redis_url or urls['redis']
        if created:
            print(f'Provisioned dev {" + ".join(created)} in namespace {args.namespace}.')

    keep_infra = args.keep or args.keep_infra
    teardown_cmd = (f'  videoflow teardown --flow-id {flow_id} --run-id {run_id} '
                    f'--nats {nats_url} --namespace {args.namespace}'
                    + (' --infra' if created and not keep_infra else ''))

    from .engines.kubernetes import KubernetesExecutionEngine
    engine = KubernetesExecutionEngine(
        nats_url = nats_url, namespace = args.namespace, default_image = image,
        image_overrides = overrides, blob_redis_url = blob_redis_url, specs = specs,
        kubectl = args.kubectl, envelope_version = args.envelope_version,
        allow_pickle = args.allow_pickle, provision_image = args.provision_image,
        autoscaling = args.autoscaling, max_replicas = args.max_replicas,
        mounts = mounts, gpu_runtime_class = args.gpu_runtime_class,
        gpu_mode = args.gpu_mode, gpu_resource_name = args.gpu_resource_name,
        gpu_autoscaling = args.gpu_autoscaling,
    )
    try:
        engine.allocate_and_run_tasks(None, flow_id, flow_type, run_id)
    except (RuntimeError, ValueError) as e:
        raise SystemExit(str(e)) from e
    print(f'Flow {flow_id} run {run_id} applied to namespace {args.namespace}.')

    if flow_type != BATCH:
        # A REALTIME deploy returns immediately, so an unschedulable node would fail
        # silently (producers keep publishing, frames evicted, downstream output never
        # appears). One bounded check turns that into a visible warning.
        for problem in engine.schedulability_report():
            print(f'WARNING: {problem}', file = sys.stderr)
        print('REALTIME flow is running. Tear it down with:')
        print(teardown_cmd)
        return

    # BATCH: run to completion, then clean everything up — on success, failure,
    # stall, or Ctrl-C (the finally block always runs unless --keep is set).
    failed = []
    stall = None
    try:
        failed = engine.wait_for_completion()
    except KeyboardInterrupt:
        print('\nInterrupted; cleaning up...', file = sys.stderr)
    except RuntimeError as e:
        # The unschedulable-pod watchdog: the flow can never finish, so clean up
        # instead of hanging (the historical behavior) — see wait_for_completion.
        stall = str(e)
    finally:
        if failed:
            engine.dump_failed_logs(failed)
        if args.keep:
            print('--keep set: leaving resources up. Tear them down with:')
            print(teardown_cmd)
        else:
            engine.teardown()
            if created and not keep_infra:
                teardown_infra(args.kubectl, args.namespace, created)
            print('Cleaned up all resources.')
    if stall:
        raise SystemExit(f'Flow aborted: {stall}')
    if failed:
        raise SystemExit(f'Flow failed: {", ".join(failed)}')
    print(f'Flow {flow_id} completed.')

def _compile_graph(args, graph_target, graph_dir, image, container_mounts, gpus) -> tuple:
    '''
    ``(flow_id, flow_type, specs)`` — via a local import when the graph's deps are
    installed on the host (cheap), else compiled inside the solution image (the
    graph dir is mounted at the same absolute path, so config paths resolve
    identically).
    '''
    from .core.compiler import compile_flow

    try:
        flow = _load_flow(graph_target)
    except ImportError as e:
        if image is None:
            raise SystemExit(
                f'Cannot import the graph on this machine ({e}) and there is no '
                f'solution image to compile it in — pass --image or drop --no-build.') from e
        from .build import run_in_image
        from .compile import specs_from_document
        compile_cmd = ['python', '-m', 'videoflow.compile', graph_target]
        if args.envelope_version is not None:
            compile_cmd += ['--envelope-version', str(args.envelope_version)]
        if args.allow_pickle:
            compile_cmd.append('--allow-pickle')
        try:
            out = run_in_image(image, compile_cmd, mounts = container_mounts,
                               workdir = graph_dir, gpus = gpus, capture = True)
        except RuntimeError as e2:
            raise SystemExit(str(e2)) from e2
        return specs_from_document(out)

    # Building the Flow already ran GraphEngine's cycle/uniqueness validation.
    try:
        specs = compile_flow(flow, envelope_version = args.envelope_version,
                             allow_pickle = args.allow_pickle)
    except ValueError as e:
        raise SystemExit(str(e)) from e
    return flow.flow_id, flow.flow_type, specs

def _render_manifests_to_disk(args, flow_id, flow_type, specs, run_id, overrides, mounts) -> None:
    '''--dry-run (stdout) / --render-only (files) — the manifest-generation escape hatch.'''
    from .infra import infra_urls, nats_manifests, redis_manifests
    from .manifests import dump_manifests, render_manifests

    # When the broker would be auto-provisioned, emit its manifests too so the
    # rendered output is a complete, appliable deployment.
    infra_manifests = []
    nats_url = args.nats
    blob_redis_url = args.blob_redis_url
    if nats_url is None:
        urls = infra_urls(args.namespace)
        nats_url = urls['nats']
        infra_manifests += nats_manifests(args.namespace)
        if blob_redis_url is None:
            blob_redis_url = urls['redis']
            infra_manifests += redis_manifests(args.namespace)

    try:
        manifests = render_manifests(
            specs, flow_id, flow_type, nats_url, run_id,
            namespace = args.namespace, default_image = args.image,
            image_overrides = overrides, blob_redis_url = blob_redis_url,
            autoscaling = args.autoscaling, max_replicas = args.max_replicas,
            envelope_version = args.envelope_version, allow_pickle = args.allow_pickle,
            provision_image = args.provision_image, mounts = mounts,
            gpu_runtime_class = args.gpu_runtime_class, gpu_mode = args.gpu_mode,
            gpu_resource_name = args.gpu_resource_name,
            gpu_autoscaling = args.gpu_autoscaling,
        )
    except ValueError as e:
        raise SystemExit(str(e)) from e
    manifests = infra_manifests + manifests

    if args.dry_run:
        sys.stdout.write(dump_manifests(manifests))
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
    print(f'Flow id: {flow_id}   Run id: {run_id}')

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
    import subprocess

    graph_path = args.graph.rsplit(':', 1)[0] if ':' in args.graph else args.graph
    if not os.path.isfile(graph_path):
        raise SystemExit(f'Graph module not found: {graph_path}')
    graph_dir = os.path.dirname(os.path.abspath(graph_path))
    graph_target = os.path.abspath(graph_path) + \
        (':' + args.graph.rsplit(':', 1)[1] if ':' in args.graph else '')

    # 0. Solution conventions: ensure a config (interactive Q&A over the solution's
    # config.template.yaml when none exists).
    from .solution import ensure_config
    interactive = not args.non_interactive and sys.stdin.isatty()
    config_path = ensure_config(graph_dir, args.config, interactive)

    # 1. Prepare hook, on this host (the workers are local processes too). Runs
    # before the graph is loaded, because the factory reads the hook's outputs.
    if not args.no_prepare:
        from .solution import run_prepare_local
        try:
            run_prepare_local(graph_dir, config_path)
        except subprocess.CalledProcessError as e:
            raise SystemExit(f'prepare.py failed: {e}') from e

    # 2. Warn about solution inputs that don't exist yet (nothing is mounted
    # locally, but a bad path is worth catching before spawning N processes).
    _warn_missing_solution_inputs(graph_dir, config_path)

    # 3. Broker: bring-your-own via --nats, else reuse whatever already listens on
    # localhost and start dev containers for whatever doesn't.
    from .localinfra import DEFAULT_NATS_URL, ensure_local_infra, teardown_local_infra, wait_local_infra_ready
    nats_url = args.nats
    blob_redis_url = args.blob_redis_url or os.environ.get('VIDEOFLOW_BLOB_REDIS_URL')
    created: list[str] = []
    if nats_url is None:
        if args.no_infra:
            nats_url = DEFAULT_NATS_URL
        else:
            try:
                urls, created = ensure_local_infra(
                    need_redis = blob_redis_url is None and not args.no_redis)
                wait_local_infra_ready(created, urls)
            except RuntimeError as e:
                raise SystemExit(str(e)) from e
            nats_url = urls['nats']
            blob_redis_url = blob_redis_url or urls['redis']
            if created:
                print(f'Started dev {" + ".join(created)} (docker).')

    # 4. Load + run. load_flow puts the graph dir on sys.path, which the engine
    # re-exports as PYTHONPATH so the workers can import its sibling modules.
    flow = _load_flow(graph_target)
    from .engines.local import LocalProcessEngine
    engine = LocalProcessEngine(nats_url = nats_url, blob_redis_url = blob_redis_url,
                                allow_pickle = args.allow_pickle,
                                local_docker_nats_url = args.local_docker_nats_url)
    try:
        try:
            flow.run(engine, run_id = args.run_id)
        except (RuntimeError, ValueError) as e:
            # Broker unreachable / incompatible wire settings: report the message,
            # not a traceback through the engine internals.
            raise SystemExit(str(e)) from e
        print(f'Flow {flow.flow_id} run {flow.run_id} running locally against {nats_url}. '
              f'Ctrl-C to stop.')
        try:
            flow.join()
        except KeyboardInterrupt:
            print('\nInterrupted; stopping...', file = sys.stderr)
            flow.stop()
    finally:
        if created and not args.keep_infra:
            teardown_local_infra(created)
        elif created:
            names = ' '.join(f'videoflow-{c}' for c in created)
            print(f'--keep-infra set: left {" + ".join(created)} running (reused next run; '
                  f'remove with `docker rm -f {names}`).')

    failed = sorted({name for name, _replica, _code in engine.failures()})
    if failed:
        engine.report_failures()
        raise SystemExit(f'Flow failed: {", ".join(failed)}')
    print(f'Flow {flow.flow_id} completed.')

def _warn_missing_solution_inputs(graph_dir, config_path) -> None:
    '''
    Warns when a path the solution declares in ``x-mounts`` doesn't exist. Locally
    nothing is mounted, so this is purely an early check — and only a warning,
    since an output directory legitimately may not exist yet.
    '''
    from .solution import find_template, load_template, resolve_mounts

    template_path = find_template(graph_dir)
    if not (template_path and config_path):
        return
    import yaml
    with open(config_path) as f:
        config = yaml.safe_load(f)
    for spec in resolve_mounts(load_template(template_path), config, graph_dir):
        read_only = spec.endswith(':ro')
        path = (spec[:-3] if read_only else spec).split(':', 1)[0]
        if read_only and not os.path.exists(path):
            print(f'WARNING: solution input {path} does not exist.', file = sys.stderr)

def _cmd_explain(args) -> None:
    from .core.compiler import specs_from_tasks_data
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
            if s.gpu_count != 1:
                bits.append(f'gpu_count={s.gpu_count}')
            if s.gpu_resource_name:
                bits.append(f'gpu_resource={s.gpu_resource_name}')
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
    # GPU demand summary: in the default exclusive mode every replica claims its own
    # whole devices, so this is the allocatable capacity the cluster must have.
    # Same helper as deploy's preflight (and the same --gpu-resource-name flag), so
    # what explain prints is exactly what deploy will request.
    gpu_specs = [s for s in specs if s.device_type == 'gpu']
    if gpu_specs:
        from .manifests import gpu_demand, resolve_gpu_resource
        default_resource = getattr(args, 'gpu_resource_name', None)
        demand = gpu_demand(specs, default_resource = default_resource)
        lines.append('GPU demand (exclusive mode — whole devices per replica):')
        for s in gpu_specs:
            resource = resolve_gpu_resource(s, default_resource)
            lines.append(f'  {s.name}: {s.nb_tasks} x {s.gpu_count} {resource}')
        for resource, units in sorted(demand.items()):
            lines.append(f'  total: {units} x {resource} — the cluster needs at least this '
                         f'allocatable (or deploy with --gpu-mode shared / time-slicing)')
    lines.append(f'DLQ stream: {dlq_stream_name(flow.flow_id, run_id)}')
    print('\n'.join(lines))

def _cmd_provision(args) -> None:
    import uuid

    from .core.compiler import specs_from_tasks_data
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

    async def _quiet(_e) -> None:
        pass  # swallow the client's connect-retry error logging (we handle failure)

    async def _go() -> None:
        nc = await nats.connect(args.nats, allow_reconnect = False, connect_timeout = 3,
                                max_reconnect_attempts = 0, error_cb = _quiet)
        try:
            await nc.publish(control_subject_for(args.flow_id, args.run_id), b'stop')
            await nc.flush()
            await delete_run_streams(nc, args.flow_id, args.run_id)
        finally:
            await nc.drain()

    async def _bounded() -> None:
        await asyncio.wait_for(_go(), timeout = 8)

    # Broker cleanup is best-effort: if the host can't reach --nats (e.g. an
    # in-cluster-only URL), still delete the k8s workloads below.
    try:
        asyncio.run(_bounded())
        print(f'Sent stop + deleted run streams for flow {args.flow_id} run {args.run_id}')
    except Exception as e:
        print(f'Broker teardown skipped (could not reach NATS at {args.nats}): {e}', file = sys.stderr)
    if args.namespace:
        from .manifests import delete_resources
        # Scope to this run (matches the run-id label the deploy stamps on every
        # resource); the broker phase above is already run-scoped.
        delete_resources(args.kubectl, args.namespace, args.flow_id, args.run_id)
        print(f'Deleted workloads in namespace {args.namespace} for flow {args.flow_id} run {args.run_id}')
    if args.infra:
        if not args.namespace:
            raise SystemExit('--infra requires --namespace.')
        from .infra import teardown_infra
        teardown_infra(args.kubectl, args.namespace, ['nats', 'redis'])
        print(f'Deleted auto-provisioned infra in namespace {args.namespace}.')

def _format_payload(message : Any) -> str:
    '''One-line human summary of a decoded payload for the debug inspector.'''
    if message is None:
        return 'None (EOS or empty)'
    import numpy as np
    if isinstance(message, np.ndarray):
        return f'ndarray shape={tuple(message.shape)} dtype={message.dtype}'
    from .wire.serialization import RawPayload
    if isinstance(message, RawPayload):
        return f'RawPayload type={message.payload_type} ({len(message.data)} bytes, opaque)'
    if hasattr(message, 'DESCRIPTOR'):
        return f'{message.DESCRIPTOR.full_name}: ' + str(message).replace('\n', ' ').strip()[:200]
    return repr(message)[:200]

def _print_decoded(buf : bytes, headers : dict = None) -> None:
    from .wire.serialization import decode_envelope
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

    deploy = sub.add_parser(
        'deploy',
        help = 'Deploy a graph to Kubernetes: config Q&A, prepare, image build+load, broker '
               'provisioning, apply — and for a BATCH flow run to completion and tear down.',
        description = 'One-stop deploy: generates the solution config (interactive Q&A over its '
                      'template), runs its prepare.py hook, builds the node image from its '
                      '[gpu.]Dockerfile and loads it into the detected cluster, provisions the '
                      'broker (dev NATS/Redis when --nats is omitted), applies the flow, and — '
                      'for a BATCH flow — waits for it to finish and then deletes every resource '
                      '(k8s + broker streams + owned infra). A REALTIME flow is left running '
                      '(tear it down with `videoflow teardown`). Every automatic step has an '
                      'explicit override flag.')
    deploy.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    deploy.add_argument('--nats', default = None,
                        help = 'NATS URL reachable from inside the cluster. Omit to auto-provision '
                               'a dev NATS (and Redis for the blob store) in --namespace; a BATCH '
                               'run tears them down again unless --keep-infra is set.')
    deploy.add_argument('--namespace', default = 'default')
    deploy.add_argument('--kubectl', default = 'kubectl', help = 'kubectl binary name/path.')
    deploy.add_argument('--flow-id', default = None,
                        help = 'Stable flow id for naming resources (overrides the graph module\'s). '
                               'Use the same value to redeploy/update an existing flow.')
    deploy.add_argument('--run-id', default = None,
                        help = 'Per-run id that scopes this run\'s broker streams (auto-generated if omitted). '
                               'A new run id gives fresh streams; reuse it to target the same run.')
    deploy.add_argument('--render-only', action = 'store_true',
                        help = 'Write manifests to --output (+ a kustomization.yaml) and print the '
                               'kubectl-apply command instead of applying. The old deploy behavior.')
    deploy.add_argument('--keep', '--no-cleanup', dest = 'keep', action = 'store_true',
                        help = 'For a BATCH flow, leave all resources up after the run finishes or '
                               'fails (for debugging) instead of tearing them down. Implies --keep-infra.')
    deploy.add_argument('--keep-infra', action = 'store_true',
                        help = 'Leave auto-provisioned NATS/Redis up after a BATCH run (faster '
                               'redeploys; they are reused when present).')
    deploy.add_argument('--config', default = None,
                        help = 'Solution config file (default: config.yaml next to the graph; when '
                               'absent and the solution ships config.template.yaml, deploy asks its '
                               'questions and writes config.yaml).')
    deploy.add_argument('--non-interactive', action = 'store_true',
                        help = 'Never prompt; fail with the list of missing config inputs instead.')
    deploy.add_argument('--no-prepare', action = 'store_true',
                        help = 'Skip the solution\'s prepare.py hook.')
    deploy.add_argument('--no-build', action = 'store_true',
                        help = 'Never auto-build images from the solution\'s Dockerfile.')
    deploy.add_argument('--build-context', default = None,
                        help = 'Docker build context for the auto-built solution image '
                               '(default: the git root enclosing the graph).')
    deploy.add_argument('--mount', action = 'append', metavar = 'HOST[:CONTAINER][:ro]',
                        help = 'hostPath volume mounted into every node workload (and prep/compile '
                               'containers), e.g. --mount /data/videos:ro. Absolute paths; the '
                               'single-path form mounts the same path on both sides. Repeatable. '
                               'Solution x-mounts are added automatically.')
    deploy.add_argument('--gpu-runtime-class', default = None, metavar = 'NAME',
                        help = 'runtimeClassName for GPU pods, e.g. --gpu-runtime-class nvidia. '
                               'Needed where the NVIDIA container runtime is an opt-in RuntimeClass '
                               'instead of the node default (k3s): without it a GPU pod schedules '
                               'but starts with no device visible.')
    deploy.add_argument('--gpu-mode', choices = ['exclusive', 'shared'], default = 'exclusive',
                        help = 'exclusive (default): each GPU replica claims whole devices via the '
                               'extended resource — a flow needs as many allocatable units as it has '
                               'GPU replicas. shared: emit no GPU resource limit; all GPU pods '
                               'co-schedule onto the gpu-pool and share the physical GPUs (local-run '
                               'semantics — dev clusters only, no memory isolation, requires '
                               '--gpu-runtime-class on clusters where the NVIDIA runtime is opt-in).')
    deploy.add_argument('--gpu-resource-name', default = None, metavar = 'RESOURCE',
                        help = 'Default extended-resource name GPU nodes request (default '
                               'nvidia.com/gpu). Use for MIG profiles (nvidia.com/mig-1g.10gb) or '
                               'clusters that rename time-sliced resources (nvidia.com/gpu.shared). '
                               'A node\'s own gpu_resource_name= wins.')
    deploy.add_argument('--gpu-autoscaling', action = 'store_true',
                        help = 'Include GPU nodes in --autoscaling (off by default: every autoscaled '
                               'replica claims its own GPUs, so lag-driven scaling can demand more '
                               'devices than the cluster has and strand pods Pending).')
    deploy.add_argument('--strict-preflight', action = 'store_true',
                        help = 'Exit non-zero (before applying anything) when the GPU preflight '
                               'finds problems, instead of proceeding with warnings.')
    deploy.add_argument('--output', default = './manifests',
                        help = 'Directory for --render-only manifest files (default ./manifests).')
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

    run = sub.add_parser(
        'run-local',
        help = 'Run a graph as local subprocesses: config Q&A, prepare, broker provisioning, '
               'run to completion — the local twin of `deploy`.',
        description = 'One-stop local run: generates the solution config (interactive Q&A over '
                      'its config.template.yaml when none exists), runs its prepare.py hook on '
                      'this host, starts a dev NATS (and Redis for the blob store) in docker when '
                      '--nats is omitted and nothing is already listening, spawns one worker '
                      'subprocess per node replica, waits for the flow to finish, reports any node '
                      'that exited non-zero, and stops only the containers it started. Every '
                      'automatic step has an explicit override flag.')
    run.add_argument('graph', help = 'path/to/graph.py[:build_flow]')
    run.add_argument('--nats', default = None,
                    help = 'NATS URL to use. Omit to reuse a broker already listening on '
                           'localhost:4222, or else start a dev NATS (and Redis) in docker and '
                           'stop it again when the flow ends.')
    run.add_argument('--blob-redis-url', default = None,
                    help = 'Redis URL for the large-payload blob store (default: '
                           '$VIDEOFLOW_BLOB_REDIS_URL, else auto-provisioned alongside NATS).')
    run.add_argument('--run-id', default = None,
                    help = 'Per-run id that scopes this run\'s broker streams (auto-generated if omitted).')
    run.add_argument('--config', default = None,
                    help = 'Solution config file (default: config.yaml next to the graph; when '
                           'absent and the solution ships config.template.yaml, run-local asks its '
                           'questions and writes config.yaml).')
    run.add_argument('--non-interactive', action = 'store_true',
                    help = 'Never prompt; fail with the list of missing config inputs instead.')
    run.add_argument('--no-prepare', action = 'store_true',
                    help = 'Skip the solution\'s prepare.py hook.')
    run.add_argument('--no-infra', action = 'store_true',
                    help = 'Never start broker containers; assume a broker is already running.')
    run.add_argument('--no-redis', action = 'store_true',
                    help = 'Do not auto-provision Redis for the large-payload blob store.')
    run.add_argument('--keep-infra', action = 'store_true',
                    help = 'Leave auto-started NATS/Redis containers running afterwards (faster '
                           'reruns; they are reused when present).')
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
    explain.add_argument('--gpu-resource-name', default = None, metavar = 'RESOURCE',
                         help = 'Default GPU extended-resource name, as on deploy — pass the same '
                                'value so the printed GPU demand matches what deploy will request.')
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
    teardown.add_argument('--namespace', default = None, help = 'If set, also kubectl-delete the run\'s workloads.')
    teardown.add_argument('--kubectl', default = 'kubectl', help = 'kubectl binary name/path.')
    teardown.add_argument('--infra', action = 'store_true',
                          help = 'Also delete auto-provisioned dev NATS/Redis in --namespace '
                                 '(only resources labeled videoflow.io/infra).')
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
