'''
The running-aggregator processors. Each one keeps internal state across the whole
stream and emits the cumulative result so far. They subclass OneTaskProcessorNode,
so they are always a single replica -- the running state can't be split across
competing workers.

Here one integer stream fans out to five aggregators, and a joiner recombines
their latest values into a single (count, sum, product, min, max) tuple per input.
This is the same single-producer diamond as simple_example1, just wider: in a
BATCH flow the join waits for all five branches of each input before emitting.

    python examples/aggregators.py
'''
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.processors import JoinerProcessor
from videoflow.processors.aggregators import (
    CountAggregator,
    MaxAggregator,
    MinAggregator,
    MultiplicationAggregator,
    SumAggregator,
)
from videoflow.producers import IntProducer


def build_flow():
    # Start at 1 (not 0) so the running product isn't pinned to zero.
    numbers = IntProducer(1, 8, 0.05, name = 'numbers')

    count = CountAggregator(name = 'count')(numbers)
    total = SumAggregator(name = 'sum')(numbers)
    product = MultiplicationAggregator(name = 'product')(numbers)
    smallest = MinAggregator(name = 'min')(numbers)
    largest = MaxAggregator(name = 'max')(numbers)

    stats = JoinerProcessor(name = 'stats')(count, total, product, smallest, largest)
    printer = CommandlineConsumer(name = 'printer')(stats)
    return Flow([printer], flow_type = BATCH)


if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
