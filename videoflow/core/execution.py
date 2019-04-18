from .task import Task
from multiprocessing import Process

def task_executor_fn(task : Task):
    task.run()

def allocate_tasks(tasks):
    '''
    First version allocates all tasks as a processes
    '''
    procs = allocate_process_tasks(tasks)
    return procs

def allocate_thread_tasks(tasks):
    pass

def allocate_thread_task(task):
    pass

def allocate_process_tasks(tasks):
    procs = []
    
    for task in tasks:
        proc = allocate_process_task(task)
        procs.append(proc)
    
    for proc in procs:
        proc.start()
    
    return procs

def allocate_process_task(task):
    proc = Process(target = task_executor_fn, args = (task))
    return proc
    
