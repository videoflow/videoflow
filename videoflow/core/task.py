from .node import Node, ProducerNode, ProcessorNode, ConsumerNode
from .messenger import Messenger

STOP_SIGNAL = "alalsl;'sdlfj2389jdalskmghsaiaw98y8asdf;askljoa8y;dsf;lkasdb"

class Task:
    def run(self):
        raise NotImplemented('Sublcasses need to implement it')

class ProducerTask(Task):
    def __init__(self, producer : ProducerNode, task_id : int):
        self._producer = producer
        self._task_id = task_id
        self._messenger = Messenger(self._producer, task_id, None)
    
    def run(self):
        for a in self._producer:
            self._messenger.publish_message(a)
        self._messenger.publish_message(STOP_SIGNAL)
    
    def send_stop_signal(self):
        # TODO: Figure out the blocking mechanism here to do thins kind of thing.
        self._messenger.publish_message(STOP_SIGNAL)

class ProcessorTask(Task):
    def __init__(self, processor : ProcessorNode, task_id : int, parent_task_id : int):
        self._processor = processor
        self._task_id = task_id
        self._parent_task_id = parent_task_id
        self._messenger = Messenger(self._processor, task_id, parent_task_id)
    
    def run(self):
        while True:
            inputs = self._messenger.receive_message()
            stop_signal_received = any([a == STOP_SIGNAL for a in inputs])
            if stop_signal_received:
                self._messenger.publish_message(STOP_SIGNAL)
                break

            #3. Pass inputs needed to processor
            output = self._processor.process(*inputs)
            messenger.publish_message(output)    
        
class ConsumerTask(Task):
    def __init__(self, consumer : ConsumerNode, task_id : int, parent_task_id : int):
        self._consumer = consumer
        self._task_id = task_id
        self._parent_task_id = parent_task_id
        self._messenger = Messenger(self._consumer, task_id, parent_task_id)
    
    def run(self):
        while True:
            inputs = self._messenger.receive_message()
            stop_signal_received = any([a == STOP_SIGNAL for a in inputs])
            if stop_signal_received:
                self._messenger.publish_message(STOP_SIGNAL)
                break
            
            #4. Publish same message to someone down the line that might need it.
            # TODO: Seat down to think about this here.
            self._messenger.publish_message(inputs)
            #3. Pass inputs needed to consumer
            self._consumer.consume(*inputs)
    