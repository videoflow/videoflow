'''
Prep for the toy-calculator solution: bake the ground truth.

Writes ``expected.json`` — the closed-form final statistics for the configured
integer range — into ``work_dir``, so the flow's ``report`` consumer can record
whether the distributed run reproduced them. That turns ``report.json`` into a
self-checking artifact: ``"matches_expected": true`` means every message made
it through every edge exactly once.

`videoflow deploy toy_calculator.py` runs this automatically inside the
solution image before compiling; it can also be run by hand:

    python prepare.py --config config.yaml [--force]

Idempotent: skips the write when ``expected.json`` already holds the values
for the configured range.
'''
from __future__ import annotations

import argparse
import json
import os

from common import Config, load_config


def expected_stats(cfg: Config) -> dict:
    '''The final RunningStats snapshot for the range, in closed form (same key layout).'''
    a, b = cfg.start_value, cfg.end_value
    count = b - a + 1

    def sum_squares_to(m: int) -> int:
        return m * (m + 1) * (2 * m + 1) // 6 if m > 0 else 0

    return {
        'count': count,
        'sum': (a + b) * count // 2,
        'sum_squares': sum_squares_to(b) - sum_squares_to(a - 1),
        'min': a,
        'max': b,
    }


def main():
    ap = argparse.ArgumentParser(description='Write the expected final statistics into work_dir.')
    ap.add_argument('--config', default='config.yaml')
    ap.add_argument('--force', action='store_true', help='rewrite even if already up to date')
    args = ap.parse_args()
    cfg = load_config(args.config)

    expected = expected_stats(cfg)
    path = cfg.expected_path()
    if not args.force and os.path.exists(path):
        with open(path) as f:
            if json.load(f) == expected:
                print(f'==> {path} already matches the configured range; skipping (--force to rewrite)')
                return
    with open(path, 'w') as f:
        json.dump(expected, f, indent=2)
    print(f'==> wrote {path} for range {cfg.start_value}..{cfg.end_value}')
    print('Prep complete.')


if __name__ == '__main__':
    main()
