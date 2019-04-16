from .node import Node, ProducerNode, ProcessorNode, ConsumerNode

class Task:
    def run(self):
        raise NotImplemented('Sublcasses need to implement it')

class ProducerTask(Task):
    def __init__(self, producer : ProducerNode):
        self._producer = producer
        self._output_channel = self._producer.id
    
    @property
    def output_channel(self):
        return self._output_channel

    def run(self):
        for a in self._producer:
            broker.publish(self._output_channel, a)
        #TODO: Add code to add publishing stoppage condition
        #once the producer finishes iterating.

class ProcessorTask(Task):
    def __init__(self, processor : ProcessorNode, input_channel : str,
                inputs_needed : list):
        self._processor = processor
        self._input_channel = input_channel
        self._output_channel = self._processor.id
        self._inputs_needed = inputs_needed
    
    @property
    def output_channel(self):
        return self._output_channel

    def run(self):
        while True:
            #TODO: Add code to stop while loop on stoppage entry

            #1. Wait for input from input channel
            input = get_from_channel(self._input_channel)

            #2. Filter only what is needed by this processor
            inputs_needed

            #3. Pass inputs needed to processor
            self._processor.process(input)
        
class ConsumerTask(Task):
    def __init__(self, consumer : ConsumerNode, input_channel : str,
                inputs_needed : list):
        self._consumer = consumer
        self._input_channel = input_channel
        self._inputs_needed = inputs_needed
    
    def run(self):
        while True:
            #TODO: Add code to stop while loop on stoppage entry

            #1. Wait for input from input channel

            #2. Select only the inputs needed

            #3. Pass inputs needed to consumer
            self._consumer.consume(input)
    