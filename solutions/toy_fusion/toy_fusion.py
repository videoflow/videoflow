'''
Toy fusion — the REALTIME videoflow example.

Simulated, independent live sources — N fixed-rate cameras plus a high-rate
IMU — fused by **event time**. The producers share no upstream lineage, so a
trace join could never group them; instead each stamps its messages with
``ctx.set_event_timestamp`` and the fusion node's ``JoinPolicy(mode='time')``
groups inputs whose timestamps fall within a tolerance:

    cam0 (10 fps) ──┐
    cam1 (10 fps) ──┼─> fuse (time join, quorum, collect) ──> live-report
    imu (100 Hz) ───┘        │
                             └── every fused moment atomically rewrites
                                 work_dir/latest.json

- **REALTIME flow type**: under pressure the broker drops rather than blocks —
  a straggler camera frame must not stall live output.
- **unbounded producers** (``is_finite=False`` when ``duration_s`` is 0): on
  Kubernetes they deploy as long-running Deployments; the run ends at teardown.
- **time-mode join knobs**: ``tolerance_ms`` (same-moment window), ``timeout_s``
  (lateness bound), ``quorum`` (emit with at least k cameras, missing ones
  passed as ``None``), and ``collect`` (the IMU never gates a group; its
  samples near the moment arrive as a list).
- **``ctx.input_info``**: the fusion node reads each input's exact event time
  to measure the spread, without any payload changes.

There is deliberately no ``prepare.py``: the prep hook is optional, and this
solution needs nothing prepared.

Deploy to Kubernetes (config Q&A, image build, broker — see README.md):

    videoflow deploy toy_fusion.py

Local smoke run (bounded by duration_s, all workers as subprocesses):

    python toy_fusion.py --config config.yaml

The glue nodes live in ``toy_fusion_nodes.py`` (a real importable module) so
distributed workers can reconstruct them by class path (the local engine puts
this directory on each worker's PYTHONPATH automatically).
'''
from __future__ import annotations

import argparse
import os

from common import load_config
from toy_fusion_nodes import LiveStateWriter, MomentFusion, SimCameraProducer, SimSensorProducer

from videoflow.core import Flow
from videoflow.core.policies import JoinPolicy


def build_flow(cfg=None):
    if cfg is None:
        # Module-dir-relative so `videoflow deploy` works from any cwd.
        cfg = load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml'))

    cameras = [
        SimCameraProducer(fps=cfg.camera_fps, phase_ms=i * cfg.phase_step_ms,
                          duration_s=cfg.duration_s, name=f'cam{i}')
        for i in range(cfg.cameras)
    ]
    sensor = SimSensorProducer(hz=cfg.sensor_hz, duration_s=cfg.duration_s, name='imu')
    fuse = MomentFusion(
        camera_names=[camera.name for camera in cameras],
        sensor_name=sensor.name,
        join_policy=JoinPolicy(
            mode='time',
            tolerance_ms=cfg.tolerance_ms,
            timeout_seconds=cfg.timeout_s,
            quorum=cfg.quorum,
            collect={sensor.name: cfg.sensor_window_ms},
        ),
        name='fuse')(*cameras, sensor)
    live = LiveStateWriter(cfg.latest_path(), cfg.summary_path(), name='live-report')(fuse)
    return Flow([live], flow_type=cfg.flow_type)


def main():
    ap = argparse.ArgumentParser(description='Run the toy-fusion flow locally (every node a subprocess).')
    ap.add_argument('--config', default='config.yaml')
    args = ap.parse_args()
    cfg = load_config(args.config)
    if cfg.duration_s is None:
        print('duration_s is 0: the sources never stop — Ctrl-C to end the run.')
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow(cfg)
    engine = LocalProcessEngine(blob_redis_url=os.environ.get('VIDEOFLOW_BLOB_REDIS_URL'))
    flow.run(engine)
    try:
        flow.join()
    except KeyboardInterrupt:
        flow.stop()
    if engine.failures():
        engine.report_failures()
        raise SystemExit(1)
    print(f'Live artifact: {cfg.latest_path()}; summary: {cfg.summary_path()}')


if __name__ == '__main__':
    main()
