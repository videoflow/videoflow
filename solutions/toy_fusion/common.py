'''
Shared config loading for the toy-fusion solution.

The config is a small YAML file (see config.example.yaml). ``load_config``
returns a Config with ``work_dir`` resolved to an absolute location **relative
to the config file's directory**, not the process cwd — so a local run and
``videoflow deploy`` (which compiles the graph from any cwd) bake the same
paths into the node parameters.

``duration_s`` of 0 (or null) means the simulated sources never stop — the
true REALTIME shape, where producers deploy as long-running Deployments and the
run ends only at teardown. A positive value bounds the run, which is how a
smoke test finishes on its own.
'''
from __future__ import annotations

import os
from dataclasses import dataclass

import yaml


@dataclass
class Config:
    path: str
    work_dir: str
    cameras: int
    camera_fps: float
    phase_step_ms: float
    sensor_hz: float
    duration_s: float | None
    tolerance_ms: float
    timeout_s: float
    quorum: int
    sensor_window_ms: float
    flow_type: str

    def latest_path(self) -> str:
        '''The continuously-rewritten live artifact; its advancing mtime is the REALTIME health check.'''
        return self.work_path('latest.json')

    def summary_path(self) -> str:
        '''Written from close() — only reachable on a bounded (duration_s > 0) or stopped run.'''
        return self.work_path('fusion_summary.json')

    def work_path(self, *parts: str) -> str:
        p = os.path.join(self.work_dir, *parts)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p


def load_config(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    cfg_dir = os.path.dirname(os.path.abspath(path))

    work_dir = os.path.abspath(os.path.join(cfg_dir, raw.get('work_dir', './out')))
    os.makedirs(work_dir, exist_ok=True)

    cameras = int(raw.get('cameras', 2))
    if cameras < 1:
        raise ValueError(f'cameras must be >= 1, got {cameras}')

    raw_duration = raw.get('duration_s', 0)
    duration_s = float(raw_duration) if raw_duration else None

    fusion = raw.get('fusion') or {}
    quorum = int(fusion.get('quorum', max(1, cameras - 1)))
    if not 1 <= quorum <= cameras:
        raise ValueError(f'fusion.quorum must be between 1 and cameras ({cameras}), got {quorum}')

    flow_type = str(raw.get('flow_type', 'realtime')).lower()
    if flow_type not in ('batch', 'realtime'):
        raise ValueError(f"flow_type must be 'batch' or 'realtime', got {flow_type!r}")

    return Config(
        path=os.path.abspath(path),
        work_dir=work_dir,
        cameras=cameras,
        camera_fps=float(raw.get('camera_fps', 10.0)),
        phase_step_ms=float(raw.get('phase_step_ms', 3.0)),
        sensor_hz=float(raw.get('sensor_hz', 100.0)),
        duration_s=duration_s,
        tolerance_ms=float(fusion.get('tolerance_ms', 15.0)),
        timeout_s=float(fusion.get('timeout_s', 0.25)),
        quorum=quorum,
        sensor_window_ms=float(fusion.get('sensor_window_ms', 40.0)),
        flow_type=flow_type,
    )
