from videoflow.core import Flow
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

producer = IntProducer(0, 40, 0.1)
identity = IdentityProcessor()(producer)
identity1 = IdentityProcessor()(identity)
joined = JoinerProcessor()(identity, identity1)
printer = CommandlineConsumer()(joined)
flow = Flow([producer], [printer],maintain_states=True,state_config={"flow_name":"simple_example","save_interval":25})
flow.run()
flow.join()
