from .node import Node, ProducerNode, ProcessorNode, ConsumerNode

STOP_SIGNAL = "alalsl;'sdlfj2389jdalskmghsaiaw98y8asdf;askljoa8y;dsf;lkasdb"

class Task:
    def __init__(self, computation_node, task_id, parent_task_id = None):
        self._messenger = None
        self._computation_node = computation_node
        self._task_id = task_id
        self._parent_task_id = parent_task_id
    
    @property
    def id(self):
        return self._task_id
    
    @property
    def parent_id(self):
        return self._parent_task_id
    
    @property
    def computation_node(self):
        return self._computation_node

    def set_messenger(self, messenger):
        self._messenger = messenger

    def _assert_messenger(self):
        assert self._messenger is not None, 'Task cannot run if messenger has not been set.'

    def _run(self):
        raise NotImplemented('Sublcasses need to implement it')

    def run(self):
        self._assert_messenger()
        self._run()

class ProducerTask(Task):
    def __init__(self, producer : ProducerNode, task_id : int):
        self._producer = producer
        super(ProducerTask, self).__init__(producer, task_id)

    def _run(self):
        for a in self._producer:
            self._messenger.publish_message(a)
            if self._messenger.check_for_termination():
                break
        self._messenger.publish_termination_message(STOP_SIGNAL)

class ProcessorTask(Task):
    def __init__(self, processor : ProcessorNode, task_id : int, parent_task_id : int):
        self._processor = processor
        super(ProcessorTask, self).__init__(processor, task_id, parent_task_id)    
    
    def _run(self):
        while True:
            inputs = self._messenger.receive_message()
            stop_signal_received = any([a == STOP_SIGNAL for a in inputs])
            if stop_signal_received:
                self._messenger.publish_termination_message(STOP_SIGNAL)
                break

            #3. Pass inputs needed to processor
            output = self._processor.process(*inputs)
            messenger.publish_message(output)   
        
class ConsumerTask(Task):
    def __init__(self, consumer : ConsumerNode, task_id : int, parent_task_id : int):
        self._consumer = consumer
        super(ConsumerTask, self).__init__(consumer, task_id, parent_task_id)

    def _run(self):
        while True:
            inputs = self._messenger.receive_message()
            stop_signal_received = any([a == STOP_SIGNAL for a in inputs])
            if stop_signal_received:
                # No need to pass through stop signal to children.
                # If children need to stop, they will receive it from
                # someone else, so the message that I am passing through
                # might be the one carrying it.
                self._messenger.passthrough_termination_message()
                break

            self._messenger.passthrough_message()
            self._consumer.consume(*inputs)
    