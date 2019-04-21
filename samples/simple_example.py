from videoflow.core import Flow
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor
from videoflow.consumers import CommandlineConsumer

producer = IntProducer(0, 40, 0.1)
identity = IdentityProcessor()(producer)
identity1 = IdentityProcessor()(identity)
printer = CommandlineConsumer()(identity1)
#printer = CommandlineConsumer()(identity, identity1)
flow = Flow([producer], [printer])
flow.run()
flow.join()
