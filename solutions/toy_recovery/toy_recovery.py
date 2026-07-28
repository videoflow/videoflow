'''
Toy recovery — what videoflow does when things go wrong, end to end.

A deterministic stream runs through a processor that fails in the two ways the
framework distinguishes, into a ledger that checks the outcome:

    events ──> fragile ──> ledger

- **a bad message** (``fragile.poison_values``): raises a poison-classified
  error, so it is dead-lettered on its *first* failure rather than retried into
  the same queue four attempts later. The rest of the stream is unaffected.
- **a sick worker** (``fragile.crash_at``): raises a worker-fatal error once.
  The message is handed back rather than blamed, the worker exits, the
  supervisor restarts it, and the un-acked message is redelivered to the fresh
  worker — which succeeds. Locally that restart is the same policy Kubernetes
  applies via a Job ``backoffLimit``, which is why a crash that survives in the
  cluster now survives here too.

``recovery_report.json`` states the property both behaviours are meant to
produce, and it is the one worth remembering:

    every event either arrived exactly once or was dead-lettered —
    never both, never neither.

That is what ``matches_expected: true`` means. ``prepare.py`` bakes the expected
split before the run, so the claim is checked against ground truth rather than
against whatever happened.

After a run, the dead-lettered events are still inspectable — the dead-letter
queue is scoped to the *flow*, so tearing the run down does not delete them::

    videoflow dlq ls --flow-id toy-recovery
    videoflow dlq replay --flow-id toy-recovery --to-run <run>

Deploy to Kubernetes (config Q&A, image build, broker, run and teardown in one
command — see README.md):

    videoflow deploy toy_recovery.py

Local run, all workers as subprocesses on this machine:

    python toy_recovery.py --config config.yaml

The glue nodes live in ``toy_recovery_nodes.py`` (a real importable module) so
distributed workers can reconstruct them by class path.
'''
from __future__ import annotations

import argparse
import os

from common import load_config
from toy_recovery_nodes import EventProducer, FragileProcessor, RecoveryLedger

from videoflow.core import Flow


def build_flow(cfg=None):
    if cfg is None:
        # Module-dir-relative so `videoflow deploy` works from any cwd.
        cfg = load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml'))

    events = EventProducer(cfg.events, rate_fps=cfg.rate_fps, name='events')
    fragile = FragileProcessor(poison_values=cfg.poison_values, crash_at=cfg.crash_at,
                               marker_path=cfg.marker_path(), name='fragile')(events)
    ledger = RecoveryLedger(cfg.ledger_path(), cfg.report_path(), cfg.expected_path(),
                            name='ledger')(fragile)
    # BATCH on purpose: the report's claim is a *completeness* claim, and only
    # at-least-once retention can support one. Under realtime the broker may
    # legitimately drop a message, and "delivered or dead-lettered, never
    # neither" would stop being true through no fault of the error handling.
    return Flow([ledger], flow_type='batch', flow_id='toy-recovery')


def main():
    ap = argparse.ArgumentParser(description='Run the toy-recovery flow locally.')
    ap.add_argument('--config', default='config.yaml')
    ap.add_argument('--nats', default='nats://localhost:4222')
    args = ap.parse_args()

    from videoflow.engines.local import LocalProcessEngine

    cfg = load_config(args.config)
    flow = build_flow(cfg)
    flow.run(LocalProcessEngine(nats_url=args.nats))
    flow.join()
    print(f'==> report: {cfg.report_path()}')


if __name__ == '__main__':
    main()
