'''
Shared config loading for the toy-calculator solution.

The config is a small YAML file (see config.example.yaml). ``load_config``
returns a Config with ``work_dir`` resolved to an absolute location **relative
to the config file's directory**, not the process cwd — so the prep script, a
local run, and ``videoflow deploy`` (which compiles the graph from any cwd) all
bake the same paths into the node parameters.
'''
from __future__ import annotations

import os
from dataclasses import dataclass

import yaml

MISSING_POLICIES = ('wait', 'drop', 'error')


@dataclass
class Config:
    path: str
    work_dir: str
    start_value: int
    end_value: int
    producer_fps: float
    delay_fps: float
    workers: int
    join_timeout_s: float | None
    join_missing: str
    join_max_pending: int
    flow_type: str

    def work_path(self, *parts: str) -> str:
        p = os.path.join(self.work_dir, *parts)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p

    def expected_path(self) -> str:
        '''Where prepare.py writes the closed-form ground truth.'''
        return self.work_path('expected.json')

    def report_path(self) -> str:
        '''The self-checking success artifact the report consumer writes in close().'''
        return self.work_path('report.json')


def load_config(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    cfg_dir = os.path.dirname(os.path.abspath(path))

    work_dir = os.path.abspath(os.path.join(cfg_dir, raw.get('work_dir', './out')))
    os.makedirs(work_dir, exist_ok=True)

    start_value = int(raw.get('start_value', 1))
    end_value = int(raw.get('end_value', 200))
    if end_value < start_value:
        raise ValueError(f'end_value ({end_value}) must be >= start_value ({start_value}); '
                         f'fix them in {os.path.abspath(path)}')

    square = raw.get('square') or {}
    workers = int(square.get('workers', 2))
    if workers < 1:
        raise ValueError(f'square.workers must be >= 1, got {workers}')

    join = raw.get('join') or {}
    join_missing = str(join.get('missing', 'wait')).lower()
    if join_missing not in MISSING_POLICIES:
        raise ValueError(f"join.missing must be one of {MISSING_POLICIES}, got {join_missing!r}")
    raw_timeout = join.get('timeout_s')
    join_timeout_s = float(raw_timeout) if raw_timeout is not None else None
    # The unpaced square branch runs ahead of the paced delay branch, so up to
    # (producer_fps - delay_fps) * runtime join groups sit incomplete at once.
    # The default policy cap (256) would evict — i.e. silently drop — beyond
    # that, so the toy raises it well past any configurable stream length.
    join_max_pending = int(join.get('max_pending', 100000))
    if join_max_pending < 1:
        raise ValueError(f'join.max_pending must be >= 1, got {join_max_pending}')

    flow_type = str(raw.get('flow_type', 'batch')).lower()
    if flow_type not in ('batch', 'realtime'):
        raise ValueError(f"flow_type must be 'batch' or 'realtime', got {flow_type!r}")

    return Config(
        path=os.path.abspath(path),
        work_dir=work_dir,
        start_value=start_value,
        end_value=end_value,
        producer_fps=float(raw.get('producer_fps', 50.0)),
        delay_fps=float(raw.get('delay_fps', 40.0)),
        workers=workers,
        join_timeout_s=join_timeout_s,
        join_missing=join_missing,
        join_max_pending=join_max_pending,
        flow_type=flow_type,
    )
