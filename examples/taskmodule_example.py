'''
This example tests wrapping the task module node
around other nodes and running a flow with it.
'''

from videoflow.core import Flow
from videoflow.core.node import TaskModuleNode
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

producer = IntProducer(0, 40, 0.05)
identity = IdentityProcessor(nb_tasks = 1)(producer)
identity1 = IdentityProcessor(nb_tasks = 1)(identity)
joined = JoinerProcessor(nb_tasks = 1)(identity, identity1)
task_module = TaskModuleNode(identity, joined)
printer = CommandlineConsumer()(task_module)
flow = Flow([producer], [printer])
flow.run()
flow.join()