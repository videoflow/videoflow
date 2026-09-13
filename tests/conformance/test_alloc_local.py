'''
Conformance cases: ALLOC-014, ALLOC-015, ALLOC-016.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.
'''
from __future__ import absolute_import, division, print_function

import pytest


@pytest.mark.case('ALLOC-014')
@pytest.mark.level('process')
@pytest.mark.pending('phase 4')
def test_alloc_014_make_strict_local_grants_truthful_under_insufficient_gpu() -> None:
    '''
    ALLOC-014 (P1, allocation, process): Make strict local grants truthful under insufficient
    GPU count.

    Acceptance: Strict mode launches zero under-provisioned GPU workers; degraded mode, when
    selected, reports exactly the grant observed by each child.
    '''


@pytest.mark.case('ALLOC-015')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 4')
def test_alloc_015_enumerate_uuid_and_mig_grants_through_the_actual_cuda() -> None:
    '''
    ALLOC-015 (P1, allocation, gpu): Enumerate UUID and MIG grants through the actual CUDA-
    visible namespace.

    Acceptance: For every supported mask the reported grant exactly matches usable CUDA devices;
    unsupported masks produce an explicit capability result without claiming successful
    isolation.
    '''


@pytest.mark.case('ALLOC-016')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 4')
def test_alloc_016_admit_shared_local_workers_using_peak_memory_and_declared() -> None:
    '''
    ALLOC-016 (P1, allocation, gpu): Admit shared local workers using peak memory and declared
    guarantees.

    Acceptance: The over-budget set is rejected in strict mode before concurrent launch; the
    fitting set runs through measured peak phases within the admitted budget.
    '''
