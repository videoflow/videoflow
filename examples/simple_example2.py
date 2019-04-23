from videoflow.core import Flow
from videoflow.producers import IntProducer
from videoflow.processors.aggregators import SumAggregator
from videoflow.consumers import CommandlineConsumer

producer = IntProducer(0, 40, 0.01)
sum_agg = SumAggregator()(producer)
printer = CommandlineConsumer()(sum_agg)
flow = Flow([producer], [printer])
flow.run()
flow.join()