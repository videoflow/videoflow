from multiprocessing import Process, Queue, Event
from ..core.node import Node
from ..core.task import Task, ProducerTask, ProcessorTask, ConsumerTask
from ..core.environment import ExecutionEnvironment, Messenger

def task_executor_fn(task : Task):
    task.run()

def allocate_process_task(task):
    proc = Process(target = task_executor_fn, args = (task))
    return proc

class RealtimeQueueMessenger(Messenger):
    def __init__(self, computation_node : Node, task_queue : Queue, parent_task_queue : Queue,
                termination_event : Event):
        self._computation_node = computation_node
        self._parent_task_queue = parent_task_queue
        self._task_queue = task_queue
        self._parent_nodes_ids = [a.id for a in self._computation_node.parents]
        self._termination_event = termination_event
        self._last_message_received = None

    def publish_message(self, message):
        if self._last_message_received is None:
            try:
                msg = {
                    self._computation_node.id : message
                }
                self._task_queue.put(msg, block = False)
            except:
                pass
        else:
            self._last_message_received[self._computation_node.id] = message
            try:
                self._task_queue.put(self._last_message_received, block = False)
            except:
                pass
    
    def check_for_termination(self) -> bool:
        return self._termination_event.is_set()

    def publish_termination_message(self, message):
        '''
        This method is identical to publish message, but is blocking
        Because, the termination message cannot be dropped.
        '''
        if self._last_message_received is None:
            try:
                msg = {
                    self._computation_node.id : message
                }
                self._task_queue.put(msg, block = True)
            except:
                pass
        else:
            self._last_message_received[self._computation_node.id] = message
            try:
                self._task_queue.put(self._last_message_received, block = True)
            except:
                pass

    def passthrough_message(self):
        try:
            self._task_queue.put(self._last_message_received, block = False)
        except:
            pass
    
    def passthrough_termination_message(self):
        try:
            self._task_queue.put(self._last_message_received, block = True)
        except:
            pass

    def receive_message(self):
        '''
        Blocking method
        '''
        input_message_dict = self._parent_task_queue.get()
        self._last_message_received = input_message_dict
        inputs = [input_message[a] for a in self._parent_nodes_ids]
        return inputs

def RealtimeQueueExecutionEnvironment(ExecutionEnvironment):
    '''
    The Realtime Queue Execution Environment will drop frames
    if the processors speed is not fast enough.
    '''
    def __init__(self):
        self._procs = []
        self._tasks = []
        self._task_output_queues = {}
        self._task_termination_notification_queues = {}
        self._termination_event = None

    def _al_create_communication_channels(self, tasks):
        #1. Create output queues
        for task in tasks:
            queue = Queue(10)
            self._task_output_queues[task.id] = queue
        
        self._termination_event = Event()
        
    def _al_create_and_set_messengers(self, tasks):
        for task in tasks:
            task_queue = self._task_output_queues.get(task.id)
            parent_task_queue = self._task_output_queues.get(task.parent_id, None)
            computation_node = task.computation_node
            messenger = RealtimeQueueMessenger(computation_node, task_queue, parent_task_queue, self._termination_event)
            task.set_messenger(messenger)
    
    def _al_create_and_start_processes(self, tasks):
        #2. Create processes
        for task in tasks:
            proc = allocate_process_task(task)
            self._procs.append(proc)
        
        #3. Start processes
        for proc in self._procs:
            proc.start()
    
    def signal_flow_termination(self):
        self._termination_event.set()
    
    def join_task_processes(self):
        for proc in self._procs:
            proc.join()
        