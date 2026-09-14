#!/usr/bin/env python3
"""Lint the design package. This does NOT execute backend conformance scenarios."""
from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

FIELDS = {
    'id', 'title', 'priority', 'owner', 'profiles', 'level', 'given',
    'fault_schedule', 'assertions', 'evidence', 'failure_refs',
    'adapter_requirements', 'acceptance', 'execution_status',
}
LIST_FIELDS = {
    'profiles', 'given', 'fault_schedule', 'assertions', 'evidence',
    'failure_refs', 'adapter_requirements',
}
OWNERS = {'messaging', 'payload', 'runtime', 'allocation', 'deployment', 'component', 'integration'}
LEVELS = {'model', 'process', 'broker', 'kubernetes', 'gpu', 'benchmark'}


def validate(root: Path) -> dict:
    catalog = json.loads((root / 'test_catalog.json').read_text())
    trace = json.loads((root / 'traceability.json').read_text())
    cases = catalog['cases']
    errors = []
    identifiers = []
    allowed_profiles = set(catalog['profile_tags'])
    for case in cases:
        ident = case.get('id', '<missing>')
        identifiers.append(ident)
        if not FIELDS <= case.keys():
            errors.append(f'{ident}: missing {sorted(FIELDS - case.keys())}')
            continue
        if not re.fullmatch(r'(MSG|PAY|ALLOC|RUN)-\d{3}', ident):
            errors.append(f'{ident}: invalid ID')
        for field in LIST_FIELDS:
            value = case[field]
            if not isinstance(value, list) or not value or any(not isinstance(x, str) or not x.strip() for x in value):
                errors.append(f'{ident}: {field} must contain nonempty strings')
        for field in ('title', 'acceptance'):
            if not isinstance(case[field], str) or not case[field].strip():
                errors.append(f'{ident}: empty {field}')
        if case['owner'] not in OWNERS or case['level'] not in LEVELS or case['priority'] not in {'P0', 'P1', 'P2'}:
            errors.append(f'{ident}: invalid owner/level/priority')
        if case['execution_status'] != 'NOT_RUN':
            errors.append(f'{ident}: design catalog must not claim an execution result')
        for profile in case['profiles']:
            if profile not in allowed_profiles:
                errors.append(f'{ident}: unknown profile tag {profile}')
        for ref in case['failure_refs']:
            parsed = urlparse(ref)
            if parsed.scheme != 'https' or not parsed.netloc:
                errors.append(f'{ident}: invalid source URL {ref}')
            if parsed.netloc == 'github.com' and '/videoflow/' in parsed.path:
                if not re.search(r'/blob/[0-9a-f]{40}/', parsed.path):
                    errors.append(f'{ident}: unpinned Videoflow source {ref}')
    duplicates = [k for k, v in collections.Counter(identifiers).items() if v > 1]
    if duplicates:
        errors.append(f'duplicate IDs: {duplicates}')
    idset = set(identifiers)
    covered = set()
    finding_ids = []
    for finding in trace['findings']:
        finding_ids.append(finding['id'])
        if not finding.get('title') or not finding.get('cases'):
            errors.append(f'{finding["id"]}: empty finding or coverage')
        for ident in finding['cases']:
            if ident not in idset:
                errors.append(f'{finding["id"]}: unknown case {ident}')
            covered.add(ident)
    if len(finding_ids) != len(set(finding_ids)):
        errors.append('duplicate finding IDs')
    if idset - covered:
        errors.append(f'cases missing traceability: {sorted(idset - covered)}')
    results = json.loads((root / 'run_results.template.json').read_text())
    if {r['case_id'] for r in results['results']} != idset:
        errors.append('result template does not cover catalog IDs')
    if any(r['status'] != 'NOT_RUN' or r['evidence_paths'] for r in results['results']):
        errors.append('unexecuted result template contains an execution claim')
    return {
        'validation_kind': 'design-package structure and traceability only',
        'valid': not errors,
        'case_count': len(cases),
        'finding_groups': len(trace['findings']),
        'by_family': dict(sorted(collections.Counter(c['id'].split('-')[0] for c in cases).items())),
        'by_priority': dict(sorted(collections.Counter(c['priority'] for c in cases).items())),
        'by_primary_level': dict(sorted(collections.Counter(c['level'] for c in cases).items())),
        'backend_scenarios_executed': 0,
        'errors': errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', nargs='?', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path, help='Optional JSON validation report')
    args = parser.parse_args()
    try:
        report = validate(args.directory)
    except (OSError, KeyError, TypeError, ValueError) as exc:
        report = {'valid': False, 'backend_scenarios_executed': 0, 'errors': [str(exc)]}
    output = json.dumps(report, indent=2) + '\n'
    print(output, end='')
    if args.output:
        args.output.write_text(output)
    return 0 if report['valid'] else 1


if __name__ == '__main__':
    sys.exit(main())
