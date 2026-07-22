'''
Toy router — partitioned parallelism, end to end.

A deterministic stream of sensor events flows through an async enrichment
stage into a **replicated, stateful** per-sensor counter — the combination
that is only correct because the counter's input is partitioned by key:

    events ──> enrich (async, stamps the key) ──> count (N partitioned replicas) ──> ledger

- **partitioned routing**: ``count`` runs ``counter.partitions`` replicas with
  ``partition_by='_partition_key'``; every message with the same sensor id
  lands on the same replica (``hash(key) % replicas``), which is what makes
  per-key state safe to replicate. On Kubernetes a partitioned BATCH stage
  runs as an Indexed Job — each completion index is a replica id.
- **the business key on the wire**: ``enrich`` calls ``ctx.set_partition_key``
  so the sensor id rides the message metadata into the partitioned edge.
  Setting ``counter.partition_by: trace_id`` routes by lineage instead — the
  totals stay right (the ledger sums each replica's share) but ``counts.json``
  then shows several replicas per sensor: stickiness, made visible.
- **an async node**: ``enrich`` is ``async def process`` — videoflow awaits it
  on a task-owned event loop.
- **an idempotent sink**: ``ledger`` is constructed with
  ``idempotent=idempotent_sink``, so its side effects are deduplicated across
  broker redelivery when a Redis idempotency store is available.

``prepare.py`` walks the same seeded PRNG as the producer and bakes the
expected per-sensor totals, so ``counts.json`` can say whether the distributed
run counted correctly (``"matches_expected": true``).

Deploy to Kubernetes (config Q&A, image build, broker, run and teardown in one
command — see README.md):

    videoflow deploy toy_router.py

Local run, all workers as subprocesses on this machine:

    python toy_router.py --config config.yaml

The glue nodes live in ``toy_router_nodes.py`` (a real importable module) so
distributed workers can reconstruct them by class path (the local engine puts
this directory on each worker's PYTHONPATH automatically).
'''
from __future__ import annotations

import argparse
import os

from common import load_config
from toy_router_nodes import AsyncEnricher, EventProducer, KeyedCounter, LedgerWriter

from videoflow.core import Flow


def build_flow(cfg=None):
    if cfg is None:
        # Module-dir-relative so `videoflow deploy` works from any cwd.
        cfg = load_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml'))

    events = EventProducer(cfg.seed, cfg.events, cfg.sensors, rate_fps=cfg.rate_fps,
                           name='events')
    enrich = AsyncEnricher(threshold=cfg.threshold, lookup_ms=cfg.lookup_ms,
                           name='enrich')(events)
    count = KeyedCounter(nb_tasks=cfg.partitions, partition_by=cfg.partition_by,
                         name='count')(enrich)
    ledger = LedgerWriter(cfg.ledger_path(), cfg.counts_path(), cfg.expected_path(),
                          idempotent=cfg.idempotent_sink, name='ledger')(count)
    # BATCH on purpose: the count verification needs the loss-free retention
    # policy — under realtime the broker may legitimately drop.
    return Flow([ledger], flow_type='batch')


def main():
    ap = argparse.ArgumentParser(description='Run the toy-router flow locally (every node a subprocess).')
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
    print(f'Wrote {cfg.counts_path()}')


if __name__ == '__main__':
    main()
