'''
Toy calculator — the smallest BATCH videoflow application.

Built only from core nodes plus five tiny glue nodes: no models, no video, no
dependencies beyond the videoflow base image. It exists so the whole deploy
pipeline (config Q&A, prep hook, in-image compile, manifests, broker, workers)
can be exercised in seconds, and as a readable reference for the graph
machinery itself:

    numbers ─┬─> square (N replicas) ─┬─> pair ─┬─> stats ─┬─> report
             └─> delay ───────────────┘         │          ├─> printer
                                                ├──────────┴─> report (2nd parent)
                                                └─> pairs-log
    square ──(metadata)──> proctimes

- **fan-out**: ``numbers`` feeds two branches; ``pair`` feeds three sinks.
- **a trace join**: ``pair`` re-aligns the branches by lineage even though the
  replicated ``square`` branch finishes out of order (its policy comes from
  ``join.missing`` / ``join.timeout_s`` in the config).
- **replication**: ``square`` runs ``square.workers`` competing replicas.
- **stateful aggregation**: ``stats`` is a ``OneTaskProcessorNode``.
- **consumer join + lifecycle**: ``report`` consumes (pair, stats) and writes
  the self-checking ``report.json`` from ``close()``.
- **a metadata consumer**: ``proctimes`` sees per-message timing, not payloads.

``prepare.py`` bakes the closed-form ground truth into ``work_dir`` so
``report.json`` can say whether the distributed run got the right answer.

Deploy to Kubernetes (config Q&A, image build, broker, run and teardown in one
command — see README.md):

    videoflow deploy toy_calculator.py

Local run, all workers as subprocesses on this machine:

    python toy_calculator.py --config config.yaml

The glue nodes live in ``toy_calculator_nodes.py`` (a real importable module)
so distributed workers can reconstruct them by class path (the local engine
puts this directory on each worker's PYTHONPATH automatically).
'''
from __future__ import annotations

import argparse
import os

from common import load_config
from toy_calculator_nodes import (
    PairJoiner,
    ProcTimeMonitor,
    ReportWriter,
    RunningStats,
    SquareProcessor,
)

from videoflow.consumers import CommandlineConsumer, FileAppenderConsumer
from videoflow.core import Flow
from videoflow.core.policies import JoinPolicy
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer


def build_flow(cfg=None):
    if cfg is None:
        # Module-dir-relative so `videoflow deploy` works from any cwd.
        cfg = load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml'))

    numbers = IntProducer(cfg.start_value, cfg.end_value, fps=cfg.producer_fps, name='numbers')
    square = SquareProcessor(nb_tasks=cfg.workers, name='square')(numbers)
    delay = IdentityProcessor(fps=cfg.delay_fps, name='delay')(numbers)
    pair = PairJoiner(
        join_policy=JoinPolicy(timeout_seconds=cfg.join_timeout_s, missing=cfg.join_missing,
                               max_pending=cfg.join_max_pending),
        name='pair')(delay, square)
    stats = RunningStats(name='stats')(pair)
    report = ReportWriter(cfg.report_path(), cfg.expected_path(), name='report')(pair, stats)
    printer = CommandlineConsumer(name='printer')(stats)
    pairs_log = FileAppenderConsumer(cfg.work_path('pairs.jsonl'), name='pairs-log')(pair)
    proctimes = ProcTimeMonitor(cfg.work_path('proctimes.jsonl'), name='proctimes')(square)
    return Flow([report, printer, pairs_log, proctimes], flow_type=cfg.flow_type)


def main():
    ap = argparse.ArgumentParser(description='Run the toy-calculator flow locally (every node a subprocess).')
    ap.add_argument('--config', default='config.yaml')
    args = ap.parse_args()
    cfg = load_config(args.config)
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow(cfg)
    engine = LocalProcessEngine(blob_redis_url=os.environ.get('VIDEOFLOW_BLOB_REDIS_URL'))
    flow.run(engine)
    flow.join()
    if engine.failures():
        engine.report_failures()
        raise SystemExit(1)
    print(f'Wrote {cfg.report_path()}')


if __name__ == '__main__':
    main()
