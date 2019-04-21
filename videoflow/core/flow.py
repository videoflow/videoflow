from .node import Node, ProducerNode, ConsumerNode, ProcessorNode
from .task import Task, ProducerTask, ProcessorTask, ConsumerTask, STOP_SIGNAL
from ..environments.queues import RealtimeQueueExecutionEnvironment

def has_cycle_util(v : Node, visited, rec):
    visited[v] = True
    rec[v] = True
    
    for child in v.children:
        if not child in visited:
            visited[child] = False
        if visited[child] == False:
            if has_cycle_util(child, visited, rec):
                return True
        elif rec[child] == True:
            return True
    
    rec[v] = False
    return False

def has_cycle(producers):
    visited = {}
    rec = {}
    for v in producers:
        visited[v] = False
        rec[v] = False
    
    for v in producers:
        if visited[v] == False:
            if has_cycle_util(v, visited, rec):
                return True
    return False
    
def topological_sort_util(v : Node, visited, stack):
    if not v in visited:
        visited[v] = False
    visited[v] = True
    for child in v.children:
        topological_sort_util(child, visited, stack)
    stack.insert(0, v)

def topological_sort(producers):
    
    visited = {}
    for v in producers:
        visited[v] = False
    stack = []

    for v in producers:
        if visited[v] == False:
            topological_sort_util(v, visited, stack)
    
    return stack

class Flow:
    def __init__(self, producers, consumers):
        if len(producers) != 1:
            raise AttributeError('Only support flows with 1 producer for now.')
        self._producers = producers
        self._consumers = consumers
        self._tasks = None
        self._producer_tasks = []
        self._execution_environment = RealtimeQueueExecutionEnvironment()

    def run(self):
        '''
        Starts the flow. It is a blocking method.
        '''

        #1. Build a topological sort of the graph.
        if has_cycle(self._producers):
            raise ValueError('Cycle found in graph')

        tsort = topological_sort(self._producers)
        [print(a.id) for a in tsort]

        #2. TODO: OPtimize graph in the following ways:   
        # a) Tasks do not need to pass down to children
        # all of the outputs of parents.  Hence, at a given
        # level of the topological sort, have the list of 
        # inputs from parents that are not needed below that 
        # level

        # b) Not all the processors have to write to a pub/sub channel
        # If their output is only needed by the next preprocessor and non one
        # else below in the graph, then I can string subsequent preprocessors together
        # a big preprocessor
        
        #3. Create the tasks and the input/outputs
        # for them
        tasks = []
        for i in range(len(tsort)):
            node = tsort[i]
            
            if isinstance(node, ProducerNode):
                task = ProducerTask(node, i)
                self._producer_tasks.append(task)
            elif isinstance(node, ProcessorNode):
                task = ProcessorTask(
                    node, 
                    i,
                    i - 1
                )
            elif isinstance(node, ConsumerNode):
                task = ConsumerTask(
                    node,
                    i,
                    i - 1
                )
            else:
                raise ValueError('node is not of one of the valid types')
            tasks.append(task)
        
        # 4. Put each task to run in the place where the processor it
        # contains inside runs.
        self._execution_environment.allocate_and_run_tasks(tasks)
    
    def join(self):
        '''
        Will make the process that calls this method block until the flow finishes
        running naturally
        '''
        self._execution_environment.join_task_processes()


    def stop(self):
        self._execution_environment.signal_flow_termination()
