'''
The result vocabulary of the backend conformance suite.

The design package (the validation kit's ARCHITECTURE.md §5; only its catalog is
checked in under ``catalog/``) requires
five outcomes rather than pytest's three, and insists that two of them never be
confused with a passing run:

- ``PASS`` / ``FAIL``: the oracle ran and decided.
- ``NOT_RUN``: the fixture the case needs is absent here (no broker, no cluster,
  no GPU, no thresholds). It is *evidence of nothing*; a green run with NOT_RUN
  rows is a partial run and the results file says so.
- ``UNSUPPORTED``: the backend truthfully declares it cannot provide the requested
  guarantee, and the case's expected-rejection assertion passed. Useful
  information, not a qualification of the rejected feature.
- ``INVALID_TEST``: a scheduled fault never fired, or a negative control failed to
  detect the defect it exists for. Neither the oracle nor the fixture can be
  trusted, so the case is not PASS and not FAIL.

pytest carries the first three distinctions through ``skip`` reasons: a fixture
that cannot provide its resource calls ``not_run(reason)``; a capability check
that rejects the request calls ``unsupported(reason)``. The prefixes below are the
contract between those helpers and the results writer in ``_results.py`` — they
are what lets ``-rs`` output be read as evidence.
'''
from __future__ import absolute_import, division, print_function

from typing import NoReturn

import pytest

PASS = 'PASS'
FAIL = 'FAIL'
UNSUPPORTED = 'UNSUPPORTED'
NOT_RUN = 'NOT_RUN'
INVALID_TEST = 'INVALID_TEST'
STATUSES = (PASS, FAIL, UNSUPPORTED, NOT_RUN, INVALID_TEST)

NOT_RUN_PREFIX = 'not_run: '
UNSUPPORTED_PREFIX = 'unsupported: '

class InvalidTest(Exception):
    '''
    Raised by the harness when a test's evidence is unusable: a barrier the fault
    schedule expected was never hit, or a negative control passed against the
    defect it should have caught. Reported as ``INVALID_TEST``, never as PASS.
    '''

def not_run(reason : str) -> NoReturn:
    '''Skip the current test as ``NOT_RUN`` because ``reason`` names a missing fixture.'''
    pytest.skip(NOT_RUN_PREFIX + reason)

def unsupported(reason : str) -> NoReturn:
    '''Skip the current test as ``UNSUPPORTED``: the backend declared it cannot provide the guarantee.'''
    pytest.skip(UNSUPPORTED_PREFIX + reason)

def classify_skip(reason : str) -> tuple[str, str]:
    '''
    Map a pytest skip reason to a conformance status.

    - Returns:
        - ``(status, reason)`` where ``status`` is ``NOT_RUN`` or ``UNSUPPORTED`` and \
            ``reason`` has the prefix removed. A skip without a known prefix is \
            treated as NOT_RUN — the conservative reading of "nothing ran".
    '''
    text = reason.strip()
    if text.startswith('Skipped: '):
        text = text[len('Skipped: '):]
    if text.startswith(UNSUPPORTED_PREFIX):
        return UNSUPPORTED, text[len(UNSUPPORTED_PREFIX):]
    if text.startswith(NOT_RUN_PREFIX):
        return NOT_RUN, text[len(NOT_RUN_PREFIX):]
    return NOT_RUN, text
