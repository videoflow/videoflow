'''
The catalog is the spec; this lint keeps the suite honest about it.

- The checked-in kit still validates with its own linter (structure, IDs,
  traceability, result template).
- Every catalog ID has exactly one ``@pytest.mark.case(ID)`` test and every such
  marker names a real ID — so a case cannot be quietly dropped from the release
  gate, and a typo cannot create a phantom one.
- Every non-pending case whose sources cite the reviewed videoflow code (a
  regression against a real defect) has a negative control, per the design
  package's rule that a test whose oracle cannot fail proves nothing.
'''
from __future__ import absolute_import, division, print_function

import ast
import collections
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).parent
if str(HERE / 'catalog') not in sys.path:
    sys.path.insert(0, str(HERE / 'catalog'))

import validate_catalog  # noqa: E402
from conftest import cases_by_id  # noqa: E402

CASE_RE = re.compile(r"@pytest\.mark\.case\(\s*'([A-Z]+-\d{3})'\s*\)")
NEGATIVE_RE = re.compile(r"@pytest\.mark\.negative_control\(\s*(?:of\s*=\s*)?'([A-Z]+-\d{3})'\s*\)")
PENDING_RE = re.compile(r"@pytest\.mark\.pending\(")

def _test_sources() -> dict:
    return {path: path.read_text() for path in sorted(HERE.rglob('test_*.py'))
            if path.name != 'test_catalog_lint.py'}

def test_kit_validates() -> None:
    report = validate_catalog.validate(HERE / 'catalog')
    assert report['valid'], report['errors']
    assert report['case_count'] == 130, report

def _marker(decorator : ast.expr, name : str) -> ast.Call | None:
    if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr == name and isinstance(decorator.func.value, ast.Attribute)
            and decorator.func.value.attr == 'mark'):
        return decorator
    return None

def _case_tests(text : str) -> list:
    '''``(function, case_id, has_variant, has_level)`` for every test carrying a case marker.'''
    found = []
    for node in ast.walk(ast.parse(text)):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        case_id, has_variant, has_level = None, False, False
        for decorator in node.decorator_list:
            call = _marker(decorator, 'case')
            if call is not None and call.args and isinstance(call.args[0], ast.Constant):
                case_id = str(call.args[0].value)
            has_variant = has_variant or _marker(decorator, 'variant') is not None
            has_level = has_level or _marker(decorator, 'level') is not None
        if case_id is not None:
            found.append((node.name, case_id, has_variant, has_level))
    return found

def test_every_case_has_exactly_one_primary_test_and_every_test_a_real_case() -> None:
    '''
    One test per case is the *primary* (no ``variant`` marker: the test at the
    catalog's primary level whose status is the case's); any number of extra
    tests may implement the case at other levels or fixtures, each marked
    ``@pytest.mark.variant('name')``. Every case test declares its level.
    '''
    ids = cases_by_id()
    seen : collections.Counter = collections.Counter()
    primaries : collections.Counter = collections.Counter()
    for path, text in _test_sources().items():
        for function, case_id, has_variant, has_level in _case_tests(text):
            assert case_id in ids, f'{path.name}::{function} marks unknown case {case_id}'
            assert has_level, f'{path.name}::{function} has no @pytest.mark.level'
            seen[case_id] += 1
            if not has_variant:
                primaries[case_id] += 1
    missing = sorted(set(ids) - set(seen))
    assert not missing, f'catalog cases without a test: {missing}'
    no_primary = sorted(cid for cid in seen if primaries[cid] == 0)
    assert not no_primary, f'cases with only variant tests (one test must be the primary): {no_primary}'
    duplicated = sorted(cid for cid, n in primaries.items() if n > 1)
    assert not duplicated, (f'cases with more than one primary test (mark extra variants with '
                            f'@pytest.mark.variant): {duplicated}')

def test_negative_controls_name_real_cases() -> None:
    ids = cases_by_id()
    for path, text in _test_sources().items():
        for case_id in NEGATIVE_RE.findall(text):
            assert case_id in ids, f'{path.name}: negative control for unknown case {case_id}'

def test_regression_cases_have_a_negative_control_once_implemented() -> None:
    '''
    A case is a regression when its sources pin reviewed videoflow code. Once its
    test is no longer a pending skeleton it must carry a negative control that
    fails the oracle against the original defect (``tests/conformance/defects.py``).
    '''
    ids = cases_by_id()
    negatives = set()
    implemented = set()
    for _path, text in _test_sources().items():
        negatives.update(NEGATIVE_RE.findall(text))
        # A module-level pending marker makes every case in the file pending.
        for block in _function_blocks(text):
            case_ids = CASE_RE.findall(block)
            if case_ids and not PENDING_RE.search(block):
                implemented.update(case_ids)
    regressions = {cid for cid, case in ids.items()
                   if any('github.com/videoflow/' in ref for ref in case['failure_refs'])}
    lacking = sorted((implemented & regressions) - negatives)
    assert not lacking, f'implemented regression cases without a negative control: {lacking}'

def _function_blocks(text : str) -> list:
    '''Split a module into per-test-function chunks (decorators travel with their function).'''
    chunks, current = [], []
    for line in text.splitlines():
        if line.startswith('def test_') and current:
            chunks.append('\n'.join(current))
            current = []
        current.append(line)
    if current:
        chunks.append('\n'.join(current))
    # Decorators precede `def`, so re-attach each chunk's trailing decorator lines
    # to the following function.
    merged, carry = [], []
    for chunk in chunks:
        lines = chunk.splitlines()
        body, trailing = [], []
        for line in reversed(lines):
            if line.startswith('@') and not body:
                trailing.insert(0, line)
            else:
                body.insert(0, line)
        merged.append('\n'.join(carry + body))
        carry = trailing
    if carry:
        merged.append('\n'.join(carry))
    return merged
