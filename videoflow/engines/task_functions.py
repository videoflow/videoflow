from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging

import os
from multiprocessing import Process, Queue, Event, Lock

from ..core.task import Task

def task_executor_fn(task : Task):
    task.run()

def task_executor_gpu_fn(task : Task, gpu_id : int):
    os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
    os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)
    task.run()

def create_process_task(task):
    proc = Process(target = task_executor_fn, args = (task,))
    return proc

def create_process_task_gpu(task, gpu_id):
    proc = Process(target = task_executor_gpu_fn, args = (task, gpu_id))
    return proc

