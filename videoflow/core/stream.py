from ..consumers import Consumer

class Stream:
    def __init__(self, *consumers):
        for consumer in consumers:
            assert isinstance(consumer, Consumer), '%s is not instance of Consumer' % str(consumer)
        self._consumers = consumers

    def _compile(self):
        pass

    def run(self):
        #1. Build graph from consumers all the way to producers
        for consumer in self._consumers:
            consumer.

        #2. Create pub/sub plumbing and task wrappers around producers, 
        #consumers and processors.  The task wrapper publishes and subscribes
        #from pub/sub channels, and passes specific data from those channels
        #to the processors and consumers.

        #3. Build a topological sort of the graph from producers to consumers

        #4. Build task registry.

        #4. For each of the tasks, put them to run in the topological order.

        #5. Return a stream handler that lets you stop the flow into the stream
        #by turning off the producers, and turning off everyone else after
        #that in the topological sort.
        a = 5