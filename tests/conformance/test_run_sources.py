'''
Conformance cases: RUN-014, RUN-015.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.
'''
from __future__ import absolute_import, division, print_function

import pytest


@pytest.mark.case('RUN-014')
@pytest.mark.level('process')
@pytest.mark.pending('phase 3')
def test_run_014_live_source_restarts_create_new_epochs_without_colliding() -> None:
    '''
    RUN-014 (P0, runtime, process): Live-source restarts create new epochs without colliding
    with earlier frames.

    Acceptance: No genuinely new post-restart frame is suppressed because its local counter
    matches a pre-restart frame.
    '''


@pytest.mark.case('RUN-015')
@pytest.mark.level('process')
@pytest.mark.pending('phase 3')
def test_run_015_replayable_sources_preserve_stable_offset_identities() -> None:
    '''
    RUN-015 (P1, runtime, process): Replayable sources preserve stable offset identities across
    attempts.

    Acceptance: Recovered offsets map one-to-one to original logical identities; new-analysis
    identities follow the declared namespace and no offset is silently lost.
    '''
