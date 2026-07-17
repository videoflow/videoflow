'''
Starting a flow and stopping it after a fixed time. flow.stop() publishes on the
control channel, which every worker subscribes to, then waits for them to exit.

    python examples/stopping_flow.py
'''
import time

from videoflow.core import Flow
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

def build_flow():
    producer = IntProducer(0, 40, 0.1, name = 'producer')
    identity = IdentityProcessor(nb_tasks = 5, name = 'identity')(producer)
    identity1 = IdentityProcessor(nb_tasks = 5, name = 'identity1')(identity)
    joined = JoinerProcessor(nb_tasks = 1, name = 'joined')(identity, identity1)
    printer = CommandlineConsumer(name = 'printer')(joined)
    return Flow([printer])

if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    time.sleep(2)
    flow.stop()
