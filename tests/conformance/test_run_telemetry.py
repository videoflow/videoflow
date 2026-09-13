'''
Conformance cases: RUN-045.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import json
import math
import re
from typing import Any, Dict, Sequence

import defects
import pytest

from videoflow.runtime import health
from videoflow.runtime.health import HealthState

METRIC = 'proctime_seconds'
#: Same count (100) and same sum (1.000 s); nearest-rank p95 of 10 ms versus 91 ms.
POPULATION_A = [0.010] * 100
POPULATION_B = [0.001] * 90 + [0.091] * 10
OBJECTIVE_SECONDS = 0.050

_COUNT = re.compile(r'^videoflow_proctime_seconds_count\{[^}]*\} (\d+)$', re.M)
_SUM = re.compile(r'^videoflow_proctime_seconds_sum\{[^}]*\} ([0-9.eE+-]+)$', re.M)


def _nearest_rank(values : Sequence[float], q : float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def _exposition(values : Sequence[float], node : str = 'w') -> str:
    state = HealthState(node)
    for value in values:
        state.observe(METRIC, value)
    return state.render_metrics()


def _count_sum_only(text : str) -> str:
    '''What a count/sum-only adapter exports: the same text without bucket lines.'''
    return '\n'.join(line for line in text.splitlines() if '_bucket{' not in line)


def _decide(text : str, q : float, threshold : float) -> Dict[str, Any]:
    '''The objective-evaluation path: met / violated / undecidable, or unavailable.'''
    bounds, cumulative = health.parse_histogram(text, METRIC)
    if not cumulative:
        return {'decision': 'unavailable', 'reason': 'no histogram exported; count and sum cannot place a percentile'}
    bracket = health.quantile_bounds(cumulative, q, bounds)
    if bracket is None:
        return {'decision': 'unavailable', 'reason': 'no observations'}
    lower, upper = bracket
    if upper <= threshold:
        decision = 'met'
    elif lower >= threshold:
        decision = 'violated'
    else:
        decision = 'undecidable'
    return {'decision': decision, 'bracket': [lower, upper]}


def _oracle_run_045(evidence : Dict[str, Any]) -> None:
    text_a, text_b = _exposition(POPULATION_A), _exposition(POPULATION_B)
    counts = (int(_COUNT.search(text_a).group(1)), int(_COUNT.search(text_b).group(1)))      # type: ignore[union-attr]
    sums = (float(_SUM.search(text_a).group(1)), float(_SUM.search(text_b).group(1)))        # type: ignore[union-attr]
    assert counts == (100, 100) and abs(sums[0] - sums[1]) < 1e-9, (counts, sums)
    reference = {'A': _nearest_rank(POPULATION_A, 0.95), 'B': _nearest_rank(POPULATION_B, 0.95)}
    assert (reference['A'], reference['B']) == (0.010, 0.091)
    decided = {'A': _decide(text_a, 0.95, OBJECTIVE_SECONDS), 'B': _decide(text_b, 0.95, OBJECTIVE_SECONDS)}
    evidence.update(count_sum = {'counts': counts, 'sums': sums}, reference_p95 = reference, decisions = decided)
    # The histogram path separates the tails, and each reference p95 lies inside its bracket.
    assert decided['A']['decision'] == 'met' and decided['B']['decision'] == 'violated', decided
    for name in ('A', 'B'):
        lower, upper = decided[name]['bracket']
        assert lower < reference[name] <= upper, (name, decided[name], reference[name])
    # A count/sum-only export cannot place a percentile and says so — never a mean labelled p95.
    for name, text in (('A', text_a), ('B', text_b)):
        verdict = _decide(_count_sum_only(text), 0.95, OBJECTIVE_SECONDS)
        assert verdict['decision'] == 'unavailable', (name, verdict)
    evidence['count_sum_only'] = _decide(_count_sum_only(text_b), 0.95, OBJECTIVE_SECONDS)
    # Merging two workers, one of which restarts its exporter mid-way, keeps the
    # bracket: cumulative buckets add, and a reset is bridged by the last snapshot.
    worker_1, worker_2 = HealthState('w1'), HealthState('w2')
    for value in POPULATION_B[:50]:
        worker_1.observe(METRIC, value)
    for value in POPULATION_B[50:75]:
        worker_2.observe(METRIC, value)
    snapshot_before_restart = worker_2.histogram(METRIC)
    worker_2 = HealthState('w2')                                    # the exporter restarts: counters reset
    for value in POPULATION_B[75:]:
        worker_2.observe(METRIC, value)
    parts = [worker_1.histogram(METRIC), snapshot_before_restart, worker_2.histogram(METRIC)]
    assert all(part is not None for part in parts)
    merged = tuple(sum(values) for values in zip(*parts))           # type: ignore[arg-type]
    full = HealthState('all')
    for value in POPULATION_B:
        full.observe(METRIC, value)
    assert merged == full.histogram(METRIC), (merged, full.histogram(METRIC))
    bracket = health.quantile_bounds(merged, 0.95)
    assert bracket is not None and bracket[0] < reference['B'] <= bracket[1]
    evidence['merged_after_restart'] = {'cumulative': merged, 'bracket': list(bracket)}


@pytest.mark.case('RUN-045')
@pytest.mark.level('model')
def test_run_045_tail_latency_objectives_cannot_be_inferred_from_count_and_sum(evidence_dir) -> None:
    '''
    RUN-045 (P1, runtime, model): Tail-latency objectives cannot be inferred from count and
    sum alone.

    Acceptance: The p95-capable path separates the two populations within its declared error
    bound; the count/sum-only path explicitly rejects a p95 decision rather than returning an
    invented estimate.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_run_045(evidence)
    (evidence_dir / 'percentile_decisions.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-045')
def test_run_045_detects_a_mean_labelled_as_p95(monkeypatch) -> None:
    defects.mean_as_p95(monkeypatch)
    assert defects.detects(_oracle_run_045, {})
