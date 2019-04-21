from videoflow.core import Flow
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor
from videoflow.consumers import CommandlineConsumer

producer = IntProducer(0, 40)
identity = IdentityProcessor()(producer)
printer = CommandlineConsumer()(identity)

flow = Flow([producer], [printer])
flow.run()
flow.join()
