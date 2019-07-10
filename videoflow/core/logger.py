from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
from multiprocessing import Queue
from collections import namedtuple

from .constants import STOP_SIGNAL

MetricMessage = namedtuple('MetricMessage', 'nodeid logtype value')

def detect_bottleneck(stats):
    pass

class Metric:
    '''
    Computes the mean and average of a series in an online manner.
    - Arguments:
        - name (str): metric name
    '''
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
        '''
        Returns the mean of the series up to this point
        '''
        return self._mean
    
    @property
    def variance(self):
        '''
        Returns the variance of the series up to this point
        '''
        return self._m2 / float(self._count)

class Accountant:
    '''
    Keeps track of the speed and actual speed of the nodes in the 
    topological sort.
    '''
    # logtype_proctime: The processing time of the processor.
    # It does not take into account the time waiting because
    # a bottleneck upstream in the flow
    logtype_proctime = 'proctime'  
    # logtype_actualproctime: The actual processing time of the processor.
    # It takes into account the waiting time because of a bottleneck
    # upstream in the flow.
    logtype_actualproctime = 'actualproctime'   # The actual 

    stat_mean = 'mean'
    stat_variance = 'variance'

    def __init__(self, nb_nodes):
        self._nodes_metrics = [dict() for _ in range(nb_nodes)]
    
    def update_actualproctime(self, node_id : int, value : float):
        self._update_stat(node_id, self.logtype_actualproctime, value)

    def update_proctime(self, node_id : int, value : float):
        self.update_stat(node_id, self.logtype_proctime, value)
    
    def update_stat(self, node_id : int, log_type : str, value : float):
        if not log_type in self._nodes_metrics[node_id]:
            self._nodes_metrics[node_id][log_type] = Metric(log_type)
        self._nodes_metrics[node_id][log_type].update_stats(value)

    def _get_stat(self, stat_name):
        to_return = []

        for node_acc in self._nodes_metrics:
            if stat_name in node_acc:
                metric = node_acc[stat_name]
                value = metric.mean()
                to_return.append(value)
            else:
                raise ValueError('stat_name is not in node accountant')
        return to_return

    def get_actual_proctime(self):
        '''
        Returns mean actual processing time of the nodes in the t-sort

        - Returns:
            - to_return: [float]
        '''
        return self._get_stat(self.logtype_actualproctime)
    
    def get_proctime(self):
        '''
        Returns mean processing time of the nodes in the t-sort

        - Returns:
            - to_return: [float]
        '''
        return self._get_stat(self.logtype_proctime)

class MetricsLoggerTask:
    def __init__(self, logging_queue : Queue, nb_nodes : int, 
                log_folder = './'):
        self._logging_queue = logging_queue
        self._accountant = Accountant(nb_nodes)
        self._log_folder = log_folder

    def run(self):
        if not os.path.exists(self._log_folder):
            os.makedirs(self._log_folder)
        
        while True:
            m_log_message = self._logging_queue.get(block = True)
            if isinstance(m_log_message, str) and m_log_message == STOP_SIGNAL:
                break
            node_id = m_log_message[0]
            log_type = m_log_message[1]
            value = m_log_message[2]
            self._accountant.update_stat(node_id, log_type, value)

