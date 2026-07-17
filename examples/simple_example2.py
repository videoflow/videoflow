'''
Running sum over a stream of integers. Run locally against a NATS server
(``nats-server -js`` or ``docker compose up -d``):

    python examples/simple_example2.py
'''
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer
from videoflow.processors.aggregators import SumAggregator
from videoflow.consumers import CommandlineConsumer

def build_flow():
    producer = IntProducer(0, 40, 0.01, name = 'producer')
    sum_agg = SumAggregator(name = 'sum')(producer)
    printer = CommandlineConsumer(name = 'printer')(sum_agg)
    return Flow([printer], flow_type = BATCH)

if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
