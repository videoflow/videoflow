import pytest

from videoflow.utils.graph import has_cycle, topological_sort
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor

def test_topological_sort():
    a = IntProducer()
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = IdentityProcessor()(c)
    e = IdentityProcessor()(d)

    expected_tsort = [a, b, c, d, e]
    tsort = topological_sort([a])
    assert len(tsort) == len(expected_tsort), "topological sort returned different number of nodes"
    assert all([tsort[i] is expected_tsort[i] for i in range(len(tsort))]), "wrong topological sort"

def test_setting_parents_twice():
    b = IdentityProcessor()
    c = IdentityProcessor()(b)

    with pytest.raises(RuntimeError):
        c(b)
    
    # Testing once more, just to check that is not a fluke
    with pytest.raises(RuntimeError):
        c(b)

def test_cycle_detection():
    #1. simple linear graph with cycle
    b = IdentityProcessor()
    c = IdentityProcessor()(b)
    d = IdentityProcessor()(c)
    b(c)
    assert has_cycle([b]), '#1 Cycle not detected'

    #2. More complex non linear graph
    a1 = IntProducer()
    b1 = IdentityProcessor()(a1)
    c1 = IdentityProcessor()(b1)
    d1 = IdentityProcessor()(a1)
    e1 = IdentityProcessor()
    f1 = JoinerProcessor()(e1, d1)
    g1 = JoinerProcessor()(c1, b1, d1)
    e1(g1)
    
    assert not has_cycle([e1]), "#2 Cycle detected"
    assert not has_cycle([a1]), "#3 Cycle not detected"

    a2 = IntProducer()
    b2 = IdentityProcessor()(a2)
    c2 = IdentityProcessor()(b2)
    d2 = IdentityProcessor()(a2)
    e2 = IdentityProcessor()
    f2 = JoinerProcessor()(e2, d2)
    g2 = JoinerProcessor()(c2, b2, f2)
    e2(g2)
    
    assert has_cycle([e2]), '#4 Cycle not detected'
    assert has_cycle([a2]), "#5 Cycle not detected"

if __name__ == "__main__":
    pytest.main([__file__])