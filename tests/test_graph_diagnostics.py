'''
Graph validation reports *every* problem in one pass.

The old behaviour raised on the first one, so a graph with three mistakes took
three edit-run cycles to fix — and each cycle costs whatever the build and deploy
loop costs. A compiler tells you all of them at once; so does this.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core.errors import SEVERITY_ERROR, SEVERITY_WARNING, GraphError
from videoflow.core.graph import GraphEngine, validate
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.producers import IntProducer


def _codes(diagnostics, severity = SEVERITY_ERROR):
    return sorted(d.code for d in diagnostics if d.severity == severity)


def test_a_sound_graph_has_no_diagnostics():
    a = IntProducer(name = 'a')
    b = IdentityProcessor(name = 'b')(a)
    out = CommandlineConsumer(name = 'out')(b)
    assert validate([a], [out]) == []


def test_three_problems_are_reported_in_one_pass():
    '''The whole point: one run, three findings, one edit cycle.'''
    a = IntProducer(name = 'dup')
    b = IdentityProcessor(name = 'dup')(a)              # 1. duplicate name
    joined = JoinerProcessor(name = 'j', nb_tasks = 3)(a, b)   # 2. unpartitioned join
    out = CommandlineConsumer(name = 'out')(joined)
    orphan = CommandlineConsumer(name = 'orphan')       # 3. unreachable consumer

    diagnostics = validate([a], [out, orphan])
    assert _codes(diagnostics) == ['VF_GRAPH_DUPLICATE_NAME',
                                'VF_GRAPH_UNPARTITIONED_JOIN',
                                'VF_GRAPH_UNREACHABLE_CONSUMER']


def test_every_diagnostic_names_a_fix():
    a = IntProducer(name = 'a')
    b = IdentityProcessor(name = 'b')(a)
    joined = JoinerProcessor(name = 'j', nb_tasks = 2)(a, b)
    out = CommandlineConsumer(name = 'out')(joined)
    for diagnostic in validate([a], [out]):
        assert diagnostic.remedy, diagnostic.code
    # The join diagnostic names the two ways out, not just the problem.
    join = [d for d in validate([a], [out]) if d.code == 'VF_GRAPH_UNPARTITIONED_JOIN'][0]
    assert 'partition_by' in join.remedy and 'nb_tasks=1' in join.remedy


def test_a_cycle_short_circuits_the_rest():
    '''
    A topological sort of a cyclic graph is meaningless, so every later check
    would be reporting nonsense. One honest finding beats four unreliable ones.
    '''
    a = IntProducer(name = 'a')
    b = IdentityProcessor(name = 'b')(a)
    c = IdentityProcessor(name = 'c')(b)
    # The cycle detector walks producers → children, so the loop has to be closed
    # in that direction to exist as far as validation is concerned.
    c._children.add(b)
    diagnostics = validate([a], [])
    assert _codes(diagnostics) == ['VF_GRAPH_CYCLE']


def test_a_non_producer_root_short_circuits_too():
    orphan = IdentityProcessor(name = 'not-a-producer')
    diagnostics = validate([orphan], [])                # type: ignore[list-item]
    assert _codes(diagnostics) == ['VF_GRAPH_NOT_A_PRODUCER']


def test_an_unread_output_is_a_warning_not_an_error():
    '''
    Computing a result and discarding it is nearly always a wiring mistake, but a
    processor kept purely for its side effects is legitimate — so it is said, not
    enforced.
    '''
    a = IntProducer(name = 'a')
    b = IdentityProcessor(name = 'b')(a)
    dead_end = IdentityProcessor(name = 'dead-end')(a)  # nothing consumes it
    out = CommandlineConsumer(name = 'out')(b)

    diagnostics = validate([a], [out])
    assert _codes(diagnostics) == []                    # no errors
    assert _codes(diagnostics, SEVERITY_WARNING) == ['VF_GRAPH_OUTPUT_UNREAD']
    assert dead_end.name in [d.node for d in diagnostics]
    GraphEngine([a], [out])                             # and it still builds


def test_the_raised_error_lists_every_problem():
    a = IntProducer(name = 'dup')
    b = IdentityProcessor(name = 'dup')(a)
    joined = JoinerProcessor(name = 'j', nb_tasks = 3)(a, b)
    out = CommandlineConsumer(name = 'out')(joined)

    with pytest.raises(GraphError) as exc:
        GraphEngine([a], [out])
    message = str(exc.value)
    assert '2 problems' in message
    assert 'VF_GRAPH_DUPLICATE_NAME' in message
    assert 'VF_GRAPH_UNPARTITIONED_JOIN' in message
    assert len([d for d in exc.value.diagnostics if d.severity == SEVERITY_ERROR]) == 2


def test_a_single_problem_reads_like_a_single_problem():
    # No "1 problems in the flow graph:" preamble for the common case.
    a = IntProducer(name = 'dup')
    b = IdentityProcessor(name = 'dup')(a)
    out = CommandlineConsumer(name = 'out')(b)
    with pytest.raises(GraphError) as exc:
        GraphEngine([a], [out])
    assert 'problems in the flow graph' not in str(exc.value)
    assert 'dup' in str(exc.value)


def test_diagnostics_render_one_readable_line():
    a = IntProducer(name = 'dup')
    b = IdentityProcessor(name = 'dup')(a)
    out = CommandlineConsumer(name = 'out')(b)
    line = validate([a], [out])[0].render()
    assert line.startswith('error: VF_GRAPH_DUPLICATE_NAME [dup]:')


if __name__ == '__main__':
    pytest.main([__file__])
