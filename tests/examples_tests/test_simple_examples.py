'''
Does not test for correctness. 
It simply tests that this examples run well
without an exception being thrown.
'''
import pytest

from videoflow.core import Flow
from videoflow.core.node import TaskModuleNode
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.processors.aggregators import SumAggregator
from videoflow.consumers import CommandlineConsumer

@pytest.mark.timeout(30)
def test_simple_example1():
    producer = IntProducer(0, 40, 0.1)
    identity = IdentityProcessor()(producer)
    identity1 = IdentityProcessor()(identity)
    joined = JoinerProcessor()(identity, identity1)
    printer = CommandlineConsumer()(joined)
    flow = Flow([producer], [printer])
    flow.run()
    flow.join()

@pytest.mark.timeout(30)
def test_simple_example2():
    producer = IntProducer(0, 40, 0.01)
    sum_agg = SumAggregator()(producer)
    printer = CommandlineConsumer()(sum_agg)
    flow = Flow([producer], [printer])
    flow.run()
    flow.join()

@pytest.mark.timeout(30)
def test_mp_example1():
    producer = IntProducer(0, 40, 0.1)
    identity = IdentityProcessor(nb_tasks = 5)(producer)
    identity1 = IdentityProcessor(nb_tasks = 5)(identity)
    joined = JoinerProcessor(nb_tasks = 5)(identity, identity1)
    printer = CommandlineConsumer()(joined)
    flow = Flow([producer], [printer])
    flow.run()
    flow.join()

@pytest.mark.timeout(30)
def test_taskmodulenode_example1():
    producer = IntProducer(0, 40, 0.05)
    identity = IdentityProcessor(nb_tasks = 1)(producer)
    identity1 = IdentityProcessor(nb_tasks = 1)(identity)
    joined = JoinerProcessor(nb_tasks = 1)(identity, identity1)
    task_module = TaskModuleNode(identity, joined)
    printer = CommandlineConsumer()(task_module)
    flow = Flow([producer], [printer])
    flow.run()
    flow.join()

@pytest.mark.timeout(30)
def test_graph_with_deadend_processor():
    # Graph with no consumer should run.
    producer = IntProducer(0, 40, 0.05)
    identity = IdentityProcessor(nb_tasks = 1)(producer)
    identity1 = IdentityProcessor(nb_tasks = 1)(identity)
    joined = JoinerProcessor(nb_tasks = 1)(identity, identity1)
    task_module = TaskModuleNode(identity, joined)
    dead_end = IdentityProcessor()(task_module)
    printer = CommandlineConsumer()(task_module)
    flow = Flow([producer], [printer])
    flow.run()
    flow.join()

@pytest.mark.timeout(30)
def test_graph_with_no_consumer():
    # Graph with no consumer should run.
    producer = IntProducer(0, 40, 0.05)
    identity = IdentityProcessor(nb_tasks = 1)(producer)
    identity1 = IdentityProcessor(nb_tasks = 1)(identity)
    joined = JoinerProcessor(nb_tasks = 1)(identity, identity1)
    task_module = TaskModuleNode(identity, joined)
    flow = Flow([producer], [])
    flow.run()
    flow.join()


if __name__ == "__main__":
    pytest.main([__file__])

