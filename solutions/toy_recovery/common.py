'''
Shared config loading for the toy-recovery solution.

``load_config`` resolves ``work_dir`` relative to the **config file's**
directory, not the process cwd, so the prep hook, a local run and
``videoflow deploy`` (which compiles the graph from any cwd) all bake the same
absolute paths into the node parameters.
'''
from __future__ import annotations

import os
from dataclasses import dataclass, field

import yaml


@dataclass
class Config:
    path: str
    work_dir: str
    events: int
    rate_fps: float
    poison_values: list = field(default_factory = list)
    crash_at: int | None = None
    max_retries: int = 3

    def work_path(self, *parts: str) -> str:
        p = os.path.join(self.work_dir, *parts)
        os.makedirs(os.path.dirname(p), exist_ok = True)
        return p

    def expected_path(self) -> str:
        '''Where prepare.py writes what a correct run must produce.'''
        return self.work_path('expected_recovery.json')

    def ledger_path(self) -> str:
        return self.work_path('ledger.jsonl')

    def report_path(self) -> str:
        '''The self-checking success artifact the ledger writes in close().'''
        return self.work_path('recovery_report.json')

    def marker_path(self) -> str:
        '''
        Records that the once-off crash already happened. It must live in the
        work dir rather than in the worker's memory: the whole point is that it
        survives the process that wrote it.
        '''
        return self.work_path('crashed_once.marker')


def load_config(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    cfg_dir = os.path.dirname(os.path.abspath(path))

    work_dir = os.path.abspath(os.path.join(cfg_dir, raw.get('work_dir', './out')))
    os.makedirs(work_dir, exist_ok = True)

    events = int(raw.get('events', 40))
    if events < 1:
        raise ValueError(f'events must be >= 1, got {events}')

    fragile = raw.get('fragile') or {}
    poison_values = [int(v) for v in (fragile.get('poison_values') or [])]
    crash_at = fragile.get('crash_at')
    crash_at = int(crash_at) if crash_at is not None else None

    for value in poison_values:
        if not 0 <= value < events:
            raise ValueError(f'poison value {value} is outside the stream 0..{events - 1}')
    if crash_at is not None:
        if not 0 <= crash_at < events:
            raise ValueError(f'crash_at {crash_at} is outside the stream 0..{events - 1}')
        if crash_at in poison_values:
            raise ValueError(
                f'crash_at {crash_at} is also a poison value; the two failures must be '
                f'distinct events or the run cannot show that they are handled differently')

    return Config(
        path = os.path.abspath(path),
        work_dir = work_dir,
        events = events,
        rate_fps = float(raw.get('rate_fps', 200)),
        poison_values = poison_values,
        crash_at = crash_at,
        max_retries = int(raw.get('max_retries', 3)),
    )
