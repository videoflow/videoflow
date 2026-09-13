'''
Conformance cases: ALLOC-002.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.
'''
from __future__ import absolute_import, division, print_function

import pytest


@pytest.mark.case('ALLOC-002')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 5')
def test_alloc_002_validate_mig_placement_constraints_independently_of_slice() -> None:
    '''
    ALLOC-002 (P0, allocation, gpu): Validate MIG placement constraints independently of slice
    totals.

    Acceptance: Every accepted sampled layout can be instantiated exactly; all independently
    known impossible layouts are rejected without changing the card.
    '''
