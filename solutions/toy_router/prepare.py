'''
Prep for the toy-router solution: bake the ground truth.

Walks the same seeded PRNG as the flow's producer (``iter_events`` in
``toy_router_nodes.py``) and writes the expected per-sensor totals to
``expected_counts.json`` in ``work_dir`` — so the flow's ``ledger`` consumer
can record whether the distributed, partitioned run counted every event
exactly once (``counts.json`` → ``"matches_expected": true``).

`videoflow deploy toy_router.py` runs this automatically inside the solution
image before compiling; it can also be run by hand:

    python prepare.py --config config.yaml [--force]

Idempotent: skips the write when ``expected_counts.json`` already holds the
totals for the configured stream.
'''
from __future__ import annotations

import argparse
import json
import os

from common import Config, load_config
from toy_router_nodes import iter_events


def expected_counts(cfg: Config) -> dict:
    totals: dict = {}
    for event in iter_events(cfg.seed, cfg.events, cfg.sensors):
        totals[event['sensor']] = totals.get(event['sensor'], 0) + 1
    return dict(sorted(totals.items()))


def main():
    ap = argparse.ArgumentParser(description='Write the expected per-sensor totals into work_dir.')
    ap.add_argument('--config', default='config.yaml')
    ap.add_argument('--force', action='store_true', help='rewrite even if already up to date')
    args = ap.parse_args()
    cfg = load_config(args.config)

    expected = expected_counts(cfg)
    path = cfg.expected_path()
    if not args.force and os.path.exists(path):
        with open(path) as f:
            if json.load(f) == expected:
                print(f'==> {path} already matches the configured stream; skipping (--force to rewrite)')
                return
    with open(path, 'w') as f:
        json.dump(expected, f, indent=2)
    print(f'==> wrote {path} for {cfg.events} events over {cfg.sensors} sensors (seed {cfg.seed})')
    print('Prep complete.')


if __name__ == '__main__':
    main()
