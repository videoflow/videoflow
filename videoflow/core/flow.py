def create_vertex_set(producers):
    V = set()
    for node in producers:
        V.add(node)
        for child in node.children:
            V.add(child)
    return V

def topological_sort_util(v, visited, stack):
    visited[v] = True
    for child in v.children:
        topological_sort_util(child, visited, stack)
    stack.insert(0, v)

def topological_sort(producers):
    V = create_vertex_set(producers)
    visited = {}
    for v in V:
        visited[v] = False
    stack = []

    for v in V:
        if visited[v] == False:
            topological_sort_util(v, visited, stack)
    
    return stack

class Flow:
    def __init__(self, producers, consumers):
        if len(producers) != 1:
            raise AttributeError('Only support flows with 1 producer for now.')
        self._producers = producers
        self._consumers = consumers

    def _compile(self):
        pass

    def start(self):
        '''
        Starts the flow
        '''

        #1. Build a topological sort of the graph.
        tsort = topological_sort(self._producers)

        #3. Create tasks from the topological sort that indicate what 
        # channel to read from, and what channel to write to.
        # Remember that I will always add into the channel I write to
        # the things that I read from.  The task will also know which
        # specific entries from the queue are actually needed for the process

        # 4. Put each task to run in the place where the processor it
        # contains inside runs.
        
    def stop(self):
        '''
        It should deallocate all the resources and stop the flow in an organic way.
        '''
        pass
