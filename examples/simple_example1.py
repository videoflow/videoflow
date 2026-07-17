'''
Minimal distributed videoflow example.

Run locally (needs a NATS JetStream server, e.g. ``nats-server -js``):

    python examples/simple_example1.py

Or deploy the same graph to Kubernetes without changing it:

    videoflow deploy examples/simple_example1.py:build_flow --nats nats://nats:4222 --namespace videoflow
'''
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

def build_flow():
    producer = IntProducer(0, 40, 0.1, name = 'producer')
    identity = IdentityProcessor(name = 'identity')(producer)
    identity1 = IdentityProcessor(name = 'identity1')(identity)
    joined = JoinerProcessor(name = 'joined')(identity, identity1)
    printer = CommandlineConsumer(name = 'printer')(joined)
    return Flow([printer], flow_type = BATCH)

if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
