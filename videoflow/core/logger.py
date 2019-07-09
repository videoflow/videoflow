from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os

class Metric:
    def __init__(self, name = ''):
        self._name = name
        self._count = 0
        self._m2 = 0
        self._mean = 0
        self._last_n_entries = []

    def update_stats(self, new_value : float):
        '''
        Computes the mean and std of the series in an online manner
        using Welford's online algorithm:
        See: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

        - Arguments:
            - new_value: (float)
        '''
        self._count += 1
        delta = new_value - self._mean
        self._mean += delta / self._count
        delta2 = new_value - self._mean
        self._m2 += (delta * delta2)

    @property
    def name(self):
        return self._name

    @property
    def mean(self):
        return self._mean
    
    @property
    def variance(self):
        return self._m2 / float(self._count)

class Accountant:
    def __init__(self):
        #1. Keeps task speeds moving averages
        pass
    
    def update_stats(node_id : int, log_type : str, value : float):
        pass

class LoggerTask:
    def __init__(self, logging_queue : Queue, log_folder = './'):
        self._logging_queue = logging_queue
        self._log_folder = log_folder

    def run(self):
        if not os.path.exists(self._log_folder):
            os.makedirs(self._log_folder)
        pass
