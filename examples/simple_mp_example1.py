'''
Parallel processors (nb_tasks > 1) become competing-consumer replicas. Locally
each replica is its own subprocess; on Kubernetes each is a Deployment replica.
Note the joiner runs with nb_tasks=1: a multi-parent join cannot be replicated
(the two halves of one event would land on different replicas).

    python examples/simple_mp_example1.py
'''
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

def build_flow():
    producer = IntProducer(0, 40, 0.1, name = 'producer')
    identity = IdentityProcessor(fps = 4, nb_tasks = 5, name = 'i1')(producer)
    identity1 = IdentityProcessor(fps = 2, nb_tasks = 10, name = 'i2')(identity)
    joined = JoinerProcessor(nb_tasks = 1, name = 'joined')(identity, identity1)
    printer = CommandlineConsumer(name = 'printer')(joined)
    return Flow([printer], flow_type = BATCH)

if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
