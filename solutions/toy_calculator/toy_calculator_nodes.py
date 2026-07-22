'''
Glue nodes for the toy-calculator solution.

These live in their own importable module (not in the graph module) because
distributed workers reconstruct each node from its class path — recorded as
``toy_calculator_nodes.<Class>`` — and importing the graph module in a worker
would re-run graph-level code. Every constructor argument is stored as
``self._<name>`` so the node round-trips through ``get_params()`` into the
worker unchanged.

The nodes are deliberately trivial: this solution exists to exercise
videoflow's graph machinery (fan-out, trace joins, competing replicas,
stateful aggregation, the consumer lifecycle and metadata consumers), not to
compute anything interesting.
'''
from __future__ import annotations

import json
import os
from typing import Any, TextIO

from videoflow.core.node import ConsumerNode, OneTaskProcessorNode, ProcessorNode


class SquareProcessor(ProcessorNode):
    '''
    Squares each integer. Stateless, so it is safe to replicate: with
    ``nb_tasks > 1`` the replicas compete for messages and finish out of
    order — which is exactly the disorder the downstream trace join re-aligns.
    '''

    def process(self, n: int) -> int:
        return n * n


class PairJoiner(ProcessorNode):
    '''
    Two-parent join node: receives the original integer (via the paced delay
    branch) and its square (via the replicated branch) for the *same*
    originating message, grouped by trace id no matter how far the two branches
    drifted apart. Emits one dict per joined pair.
    '''

    # One positional argument per parent, so the signature can't match the
    # single-input base method.
    def process(self, n: int, square: int) -> dict:  # type: ignore[override]
        return {'n': n, 'square': square}


class RunningStats(OneTaskProcessorNode):
    '''
    Keeps running statistics over every pair seen. The state across messages is
    why this is a ``OneTaskProcessorNode``: replicating it would split the
    totals across workers and no replica would hold the true numbers.
    '''

    def __init__(self, **kwargs: Any) -> None:
        self._stats: dict = {'count': 0, 'sum': 0, 'sum_squares': 0, 'min': None, 'max': None}
        super().__init__(**kwargs)

    def process(self, pair: dict) -> dict:
        s = self._stats
        n = pair['n']
        s['count'] += 1
        s['sum'] += n
        s['sum_squares'] += pair['square']
        s['min'] = n if s['min'] is None else min(s['min'], n)
        s['max'] = n if s['max'] is None else max(s['max'], n)
        return dict(s)


class ReportWriter(ConsumerNode):
    '''
    Two-parent *consumer* join: consumes each pair together with the stats
    snapshot that very pair produced (they share a trace id, so the join groups
    them). Keeps the latest snapshot and writes ``report.json`` from
    ``close()`` — the lifecycle hook that runs in the worker after
    end-of-stream. If ``prepare.py`` left an ``expected.json`` next to it, the
    report also records whether the final stats match it, so a single file
    answers "did the whole distributed run compute the right thing".

    - Arguments:
        - report_path: where to write the final report (inside ``work_dir``, \
            which is mounted at the same absolute path in the worker pod).
        - expected_path: the ground-truth file ``prepare.py`` wrote, or None \
            to skip the comparison.
    '''

    def __init__(self, report_path: str, expected_path: str | None = None, **kwargs: Any) -> None:
        self._report_path = report_path
        self._expected_path = expected_path
        self._latest: dict | None = None
        self._pairs_seen = 0
        super().__init__(**kwargs)

    # One positional argument per parent, so the signature can't match the
    # single-input base method.
    def consume(self, pair: dict, stats: dict) -> None:  # type: ignore[override]
        self._pairs_seen += 1
        self._latest = stats

    def close(self) -> None:
        report: dict = {
            'pairs_seen': self._pairs_seen,
            'final_stats': self._latest,
            'matches_expected': None,
        }
        if self._expected_path and os.path.exists(self._expected_path):
            with open(self._expected_path) as f:
                report['matches_expected'] = self._latest == json.load(f)
        tmp = self._report_path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(report, f, indent=2)
        os.replace(tmp, self._report_path)


class ProcTimeMonitor(ConsumerNode):
    '''
    A ``metadata=True`` consumer: instead of its parent's payload it receives
    the per-message metadata the framework publishes alongside it (``proctime``
    and ``actual_proctime``). Appends one JSON line per message — a toy
    throughput monitor. The file handle follows the node lifecycle: opened in
    ``open()``, released in ``close()``.
    '''

    def __init__(self, out_path: str, **kwargs: Any) -> None:
        self._out_path = out_path
        # metadata is fixed to True below; pop it so reconstruction via
        # get_params() doesn't pass it twice.
        kwargs.pop('metadata', None)
        self._fh: TextIO | None = None
        super().__init__(metadata=True, **kwargs)

    def open(self) -> None:
        self._fh = open(self._out_path, 'a')

    def consume(self, meta: dict) -> None:
        assert self._fh is not None
        self._fh.write(json.dumps(meta) + '\n')
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
