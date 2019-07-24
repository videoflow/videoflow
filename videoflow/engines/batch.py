from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
import os
from multiprocessing import Process, Queue, Event, Lock

from .task_functions import create_process_task, create_process_task_gpu, task_executor_fn, task_executor_gpu_fn
from ..core.constants import BATCH, REALTIME, GPU, CPU, LOGGING_LEVEL
from ..core.node import Node, ProducerNode, ConsumerNode, ProcessorNode
from ..core.task import Task, ProducerTask, ProcessorTask, ConsumerTask, MultiprocessingReceiveTask, MultiprocessingProcessorTask, MultiprocessingOutputTask
from ..core.engine import ExecutionEngine, Messenger
from ..utils.system import get_gpus_available_to_process
from ..core.constants import STOP_SIGNAL

class BatchprocessingQueueMessenger(Messenger):
    '''
    BatchprocessingQueueMessenger is a messenger that communicates
    through queues of type ``multiprocessing.Queue``.  It is not real
    time, which means that if a queue is full when publishing a 
    message to it, it will block until the queue can process it.
    '''
    def __init__(self, computation_node : Node, task_queue : Queue, parent_task_queue : Queue,
        termination_event : Event):
        self._computation_node = computation_node
        self._parent_task_queue = parent_task_queue
        self._task_queue = task_queue
        self._parent_nodes_ids = []
        if self._computation_node.parents is not None:
            self._parent_nodes_ids = [a.id for a in self._computation_node.parents]
        self._termination_event = termination_event
        self._last_message_received = None
        self._logger = self._configure_logger()
    
    def _configure_logger(self):
        logger = logging.getLogger(f'{self._computation_node.id}')
        logger.setLevel(LOGGING_LEVEL)
        ch = logging.StreamHandler()
        ch.setLevel(LOGGING_LEVEL)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def publish_message(self, message, metadata = None):
        '''
        Publishes output message to a place where the child task will receive it. \
        Will drop the message is the receiving queue is full.
        '''
        if self._last_message_received is None:
            msg = {
                self._computation_node.id : {
                    'message': message,
                    'metadata': metadata
                }
            }
            self._task_queue.put(msg, block = True)
            self._logger.debug(f'Published message {msg}')
        else:
            self._last_message_received[self._computation_node.id] = {
                'message': message,
                'metadata': metadata
            }
            self._task_queue.put(self._last_message_received, block = True)
            self._logger.debug(f'Published message {self._last_message_received}')
    
    def check_for_termination(self) -> bool:
        '''
        Checks if someone has set a termination event.
        '''
        return self._termination_event.is_set()

    def publish_termination_message(self, message, metadata = None):
        '''
        This method is identical to publish message
        '''
        return self.publish_message(message, metadata)

    def passthrough_message(self):
        self._task_queue.put(self._last_message_received, block = True)
    
    def passthrough_termination_message(self):
        return self.passthrough_message()
    
    def receive_raw_message(self):
        input_message_dict = self._parent_task_queue.get()
        self._last_message_received = input_message_dict
        self._logger.debug(f'Received message: {input_message_dict}')
        
        #1. Check for STOP_SIGNAL before returning
        inputs = [input_message_dict[a] for a in self._parent_nodes_ids]
        stop_signal_received = any([isinstance(a, str) and a == STOP_SIGNAL for a in inputs])
        
        #2. Returns one or the other.
        if stop_signal_received:
            return STOP_SIGNAL
        else:
            return dict(input_message_dict)

    def receive_message(self):
        input_message_dict = self._parent_task_queue.get()
        self._logger.debug(f'Received message: {input_message_dict}')
        self._last_message_received = input_message_dict
        inputs = [input_message_dict[a] for a in self._parent_nodes_ids]
        return inputs

class BatchExecutionEngine(ExecutionEngine):
    def __init__(self):
        self._procs = []
        self._tasks = []
        self._task_output_queues = {}
        self._task_termination_notification_queues = {}
        self._termination_event = None
        self._gpu_ids = get_gpus_available_to_process()
        self._nb_available_gpus = len(self._gpu_ids)
        self._next_gpu_index = -1
        super(BatchExecutionEngine, self).__init__()

    def _al_create_and_start_processes(self, tasks_data):
        #0. Create output queues
        for data in tasks_data:
            task_id = data[1]
            queue = Queue(1)
            self._task_output_queues[task_id] = queue
        
        self._termination_event = Event()

        #1. Initialize tasks
        tasks = []
        for data in tasks_data:
            node = data[0]
            node_id = data[1]
            parent_node_id = data[2]
            is_last = data[3]

            #1.1 Creating messenger for task
            task_queue = self._task_output_queues.get(node_id)
            if parent_node_id is not None:
                parent_task_queue = self._task_output_queues.get(parent_node_id)
            else:
                parent_task_queue = None
        
            messenger = BatchprocessingQueueMessenger(node, task_queue, parent_task_queue, self._termination_event)

            if isinstance(node, ProducerNode):
                task = ProducerTask(node, messenger, node_id, is_last)
                tasks.append(task)

            elif isinstance(node, ProcessorNode):
                if node.nb_tasks > 1:
                    receiveQueue = Queue(1)
                    accountingQueue = Queue()
                    output_queues = [Queue() for _ in range(node.nb_tasks)]

                    # Create receive task
                    receive_task = MultiprocessingReceiveTask(
                        node,
                        parent_task_queue,
                        receiveQueue,
                        BATCH
                    )
                    tasks.append(receive_task)

                    # Create processor tasks
                    mp_tasks_lock = Lock()
                    for idx in range(node.nb_tasks):
                        mp_task = MultiprocessingProcessorTask(
                            idx,
                            node,
                            mp_tasks_lock,
                            receiveQueue,
                            accountingQueue,
                            output_queues[idx]
                        )
                        tasks.append(mp_task)
                    
                    # Create output task
                    output_task = MultiprocessingOutputTask(
                        node,
                        task_queue,
                        accountingQueue,
                        output_queues,
                        BATCH,
                        is_last
                    )
                    tasks.append(output_task)
                else:
                    task = ProcessorTask(
                        node,
                        messenger,
                        node_id,
                        is_last,
                        parent_node_id
                    )
                    tasks.append(task)

            elif isinstance(node, ConsumerNode):
                task = ConsumerTask(
                    node,
                    messenger,
                    node_id,
                    is_last,
                    parent_node_id
                )
                tasks.append(task)
        
        #2. Create processes
        for task in tasks:
            if isinstance(task, ProcessorTask) or isinstance(task, MultiprocessingProcessorTask):
                if task.device_type == GPU:
                    self._next_gpu_index += 1
                    if self._next_gpu_index < self._nb_available_gpus:
                        proc = create_process_task_gpu(task, self._gpu_ids[self._next_gpu_index])
                    else:
                        try:
                            task.change_device(CPU)
                            proc = create_process_task(task)
                        except:
                            raise RuntimeError('No GPU available to allocate {}'.format(str(task._processor)))
                else:
                    proc = create_process_task(task)
            else:
                proc = create_process_task(task)
            self._procs.append(proc)
        
        #3. Start processes.
        for proc in self._procs:
            proc.start()
    
    def signal_flow_termination(self):
        self._termination_event.set()
    
    def join_task_processes(self):
        for proc in self._procs:
            try:
                proc.join()
            except KeyboardInterrupt:
                proc.join()
                continue
