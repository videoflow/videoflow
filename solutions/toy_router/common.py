'''
Shared config loading for the toy-router solution.

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

PARTITION_MODES = ('_partition_key', 'trace_id')


@dataclass
class Config:
    path: str
    work_dir: str
    seed: int
    events: int
    sensors: int
    rate_fps: float
    threshold: float
    lookup_ms: float
    partitions: int
    partition_by: str
    idempotent_sink: bool

    def work_path(self, *parts: str) -> str:
        p = os.path.join(self.work_dir, *parts)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p

    def expected_path(self) -> str:
        '''Where prepare.py writes the expected per-sensor totals.'''
        return self.work_path('expected_counts.json')

    def ledger_path(self) -> str:
        return self.work_path('ledger.jsonl')

    def counts_path(self) -> str:
        '''The self-checking success artifact the ledger consumer writes in close().'''
        return self.work_path('counts.json')


def load_config(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    cfg_dir = os.path.dirname(os.path.abspath(path))

    work_dir = os.path.abspath(os.path.join(cfg_dir, raw.get('work_dir', './out')))
    os.makedirs(work_dir, exist_ok=True)

    events = int(raw.get('events', 300))
    if events < 1:
        raise ValueError(f'events must be >= 1, got {events}')
    sensors = int(raw.get('sensors', 6))
    if sensors < 1:
        raise ValueError(f'sensors must be >= 1, got {sensors}')

    counter = raw.get('counter') or {}
    partitions = int(counter.get('partitions', 3))
    if partitions < 1:
        raise ValueError(f'counter.partitions must be >= 1, got {partitions}')
    partition_by = str(counter.get('partition_by', '_partition_key'))
    if partition_by not in PARTITION_MODES:
        raise ValueError(f'counter.partition_by must be one of {PARTITION_MODES}, '
                         f'got {partition_by!r}')

    enrich = raw.get('enrich') or {}

    return Config(
        path=os.path.abspath(path),
        work_dir=work_dir,
        seed=int(raw.get('seed', 7)),
        events=events,
        sensors=sensors,
        rate_fps=float(raw.get('rate_fps', 100.0)),
        threshold=float(enrich.get('threshold', 50.0)),
        lookup_ms=float(enrich.get('lookup_ms', 1.0)),
        partitions=partitions,
        partition_by=partition_by,
        idempotent_sink=bool(raw.get('idempotent_sink', True)),
    )
