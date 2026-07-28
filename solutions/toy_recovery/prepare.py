'''
Prep for the toy-recovery solution: bake what a correct run must produce.

Walks the same stream as the flow's producer and splits it the way the framework
should: the poison events end up dead-lettered, everything else — *including*
the event whose worker died — ends up delivered. Writing that split before the
run is what turns ``recovery_report.json`` into a claim about correctness rather
than a description of whatever happened.

The distinction it encodes is the whole point: a message that fails on its own
content is the message's fault and is quarantined; a message that fails because
the worker was sick is not, and comes back.

`videoflow deploy toy_recovery.py` runs this automatically inside the solution
image before compiling; it can also be run by hand:

    python prepare.py --config config.yaml [--force]

Idempotent: skips the write when the file already matches the configured stream.
'''
from __future__ import annotations

import argparse
import json
import os

from common import Config, load_config
from toy_recovery_nodes import iter_events


def expected_outcome(cfg: Config) -> dict:
    produced = list(iter_events(cfg.events))
    dead_lettered = sorted(set(cfg.poison_values))
    delivered = [v for v in produced if v not in set(dead_lettered)]
    return {
        'produced': produced,
        'delivered': delivered,
        'dead_lettered': dead_lettered,
    }


def main():
    ap = argparse.ArgumentParser(description='Write the expected delivered/dead-lettered split.')
    ap.add_argument('--config', default='config.yaml')
    ap.add_argument('--force', action='store_true', help='rewrite even if already up to date')
    args = ap.parse_args()
    cfg = load_config(args.config)

    expected = expected_outcome(cfg)
    path = cfg.expected_path()
    if not args.force and os.path.exists(path):
        with open(path) as f:
            if json.load(f) == expected:
                print(f'==> {path} already matches the configured stream; skipping (--force to rewrite)')
                return
    # A stale marker would make the "crash once" worker skip its crash entirely,
    # so a fresh prep clears it: prepare defines what the run is supposed to do.
    marker = cfg.marker_path()
    if os.path.exists(marker):
        os.remove(marker)
    with open(path, 'w') as f:
        json.dump(expected, f, indent=2)
    print(f'==> wrote {path}: {len(expected["delivered"])} delivered, '
          f'{len(expected["dead_lettered"])} dead-lettered, of {cfg.events} events')
    print('Prep complete.')


if __name__ == '__main__':
    main()
