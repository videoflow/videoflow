'''
Summarise a conformance run: ``python -m tests.conformance.report [run_results.json]``.

Prints the by-family / by-status matrix, then every case that did not PASS with
its status and reason, so a partial run says so at a glance — a green pytest
line with 100 NOT_RUN rows is a partial run, and this is where that shows.
'''
from __future__ import absolute_import, division, print_function

import json
import pathlib
import sys
from typing import Any, Dict, List

STATUSES = ('PASS', 'FAIL', 'UNSUPPORTED', 'NOT_RUN', 'INVALID_TEST')
DEFAULT = pathlib.Path(__file__).parent / '_out' / 'run_results.json'


def render(results : Dict[str, Any]) -> str:
    lines : List[str] = []
    summary = results.get('summary', {})
    by_family = summary.get('by_family', {})
    lines.append(f"run {results.get('run_id')}  manifest {str(results.get('manifest_sha256', ''))[:12]}  "
                 f"executed {results.get('backend_scenarios_executed', 0)} case(s)")
    header = f"{'family':8}" + ''.join(f'{s:>13}' for s in STATUSES)
    lines.append(header)
    for family in ('MSG', 'PAY', 'ALLOC', 'RUN'):
        counts = by_family.get(family, {})
        lines.append(f'{family:8}' + ''.join(f'{counts.get(s, 0):>13}' for s in STATUSES))
    totals = summary.get('by_status', {})
    lines.append(f"{'total':8}" + ''.join(f'{totals.get(s, 0):>13}' for s in STATUSES))
    lines.append('')
    for row in results.get('results', []):
        status = row.get('status')
        if status == 'PASS':
            continue
        variants = row.get('variants') or []
        reason = next((v.get('reason') for v in variants if v.get('reason')), '')
        control = row.get('negative_control_result')
        extra = f'  [negative control: {control}]' if control else ''
        lines.append(f"{row['case_id']:10} {status:13} {reason[:110]}{extra}")
    invalid = [r['case_id'] for r in results.get('results', []) if r.get('status') == 'INVALID_TEST']
    if invalid:
        lines.append('')
        lines.append(f'INVALID_TEST: {", ".join(invalid)} — a fault never fired or a negative control missed its defect')
    return '\n'.join(lines)


def main(argv : List[str]) -> int:
    path = pathlib.Path(argv[1]) if len(argv) > 1 else DEFAULT
    if not path.exists():
        print(f'no results at {path}; run the conformance suite first', file = sys.stderr)
        return 2
    with open(path) as f:
        print(render(json.load(f)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
