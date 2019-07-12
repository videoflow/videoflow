from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
import os
from multiprocessing import Queue
from collections import namedtuple
from .node import Node, ConsumerNode, ProcessorNode, ProducerNode

from .constants import STOP_SIGNAL

package_logger = logging.getLogger(__package__)
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

    - Arguments:
        - nb_nodes (int): nb of nodes in flow
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
    '''
    - Arguments:
        - logging_queue: Queue that receives data about processing time \
            from the running tasks of the flow
        - sorted_nodes: list of nodes of type ``node.Node`` in topological sort
        - log_folder: (str) Folder where to save the logs.
    '''
    def __init__(self, logging_queue : Queue, sorted_nodes, 
                log_folder = './'):
        self._logging_queue = logging_queue
        self._accountant = Accountant(len(sorted_nodes))
        self._sorted_nodes = sorted_nodes
        self._log_folder = log_folder
        self._bottlenecks_reported = False
        self._logger = self._get_metric_logger()

    def _get_metric_logger(self):
        logger = logging.getLogger(self.__class__)
        logger.setLevel(logging.DEBUG)

        #1. Stream Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        #2. File Handler
        filename = 'nodes_proctime.log'
        filename = os.path.join(self._log_folder, filename)
        fl = logging.handlers.RotatingFileHandler(filename, maxBytes = 10000000, backupCount = 4)

        logger.addHandler(ch)
        logger.addHandler(fl)
        return logger

    def get_bottlenecks(self):
        '''
        A bottleneck is any node that process at a speed that is 
        lower to the speed of any of the producers.

        There is also what we call ``effective bottleneck``. An ``effective
        bottleneck`` is a node that reduces the throughput of the 
        flow.  

        - Returns:
            - is_bottleneck: list of booleans of the same size as \
                self._sorted_nodes. Contains ``None`` entries if data
                is not statistically significant to be able to judge if a
                node is a bottleneck.
            - is_effective_bottleneck: list of booleans of the same size as \
                self._sorted_nodes. Contains ``None`` entries if data
                is not statistically significant to be able to judge if a
                node is a bottleneck.
        '''
        actual_proctime = self._accountant.get_actual_proctime()
        proctime = self._accountant.get_proctime()
        
        #1. Find bottlenecks
        is_producer_node = [isinstance(a, ProducerNode) for a in self._sorted_nodes]
        min_producer_time = min([proctime[i] for i in range(len(is_producer_node)) if is_producer_node[i]])
        is_bottleneck = [proctime[i] > min_producer_time and not is_producer_node[i] for i in range(len(self._sorted_nodes))]

        #2. Find effective bottlenecks
        is_effective_bottleneck = [proctime[i] > proctime[i - 1] and is_bottleneck[i] for i in range(len(self._sorted_nodes))]
        
        return is_bottleneck, is_effective_bottleneck

    def run(self):
        if not os.path.exists(self._log_folder):
            os.makedirs(self._log_folder)
        
        message_count = 0

        while True:
            #1. Get message
            m_log_message = self._logging_queue.get(block = True)
            message_count += 1
            if isinstance(m_log_message, str) and m_log_message == STOP_SIGNAL:
                break
            node_id = m_log_message[0]
            log_type = m_log_message[1]
            value = m_log_message[2]
            
            #2. Update in-memory accounting
            self._accountant.update_stat(node_id, log_type, value)

            #3. Write logs into filesytem
            self._logger.debug(f'{node_id},{log_type},{value}')

            #4. Report bottlenecks
            if not self._bottlenecks_reported and message_count > (len(self._sorted_nodes) * 10):
                is_bottleneck, is_effective_bottleneck = self.get_bottlenecks()
                bottleneck_node_names = [str(self._sorted_nodes[i]) for i in range(len(self._sorted_nodes)) if is_bottleneck[i]]
                effective_bottleneck_node_names = [str(self._sorted_nodes[i]) for i in range(len(self._sorted_nodes)) if is_effective_bottleneck[i]]
                package_logger.info('Bottleneck nodes: \n{}'.format('\n'.join(bottleneck_node_names)))
                package_logger.info('Effective bottleneck nodes: \n{}'.format('\n'.join(effective_bottleneck_nodes)))
                self._bottlenecks_reported = True
