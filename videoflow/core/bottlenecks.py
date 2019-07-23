from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import time
import logging
import logging.handlers
import os
from multiprocessing import Queue
from collections import namedtuple
from .node import Node, ConsumerNode, ProducerNode

package_logger = logging.getLogger(__package__)
MetricMessage = namedtuple('MetricMessage', 'nodeid logtype value')

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
    logtype_actualproctime = 'actual_proctime'   # The actual 

    stat_mean = 'mean'
    stat_variance = 'variance'

    def __init__(self, nb_nodes):
        self._nodes_metrics = [dict() for _ in range(nb_nodes)]
    
    def update_actualproctime(self, node_id : int, value : float):
        self._update_stat(node_id, self.logtype_actualproctime, value)

    def update_proctime(self, node_index : int, value : float):
        self.update_stat(node_index, self.logtype_proctime, value)
    
    def update_stat(self, node_index : int, log_type : str, value : float):
        if not log_type in self._nodes_metrics[node_index]:
            self._nodes_metrics[node_index][log_type] = Metric(log_type)
        self._nodes_metrics[node_index][log_type].update_stats(value)

    def _get_stat(self, stat_name : str):
        to_return = []

        for node_acc in self._nodes_metrics:
            if stat_name in node_acc:
                metric = node_acc[stat_name]
                value = metric.mean
                to_return.append(value)
            else:
                raise ValueError(f'stat_name {stat_name} is not in node accountant')
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

class MetricsLogger:
    def __init__(self, log_folder = './'):
        self._log_folder = log_folder
        self._logger = self._get_metric_logger()
        self._time_in_seconds = 1
        self._last_log_time = {}

    def _get_metric_logger(self):
        logger = logging.getLogger(str(self.__class__))
        logger.setLevel(logging.DEBUG)

        #1. Stream Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        ch.setFormatter(formatter)

        #2. File Handler
        filename = 'nodes_proctime.log'
        filename = os.path.join(self._log_folder, filename)
        fl = logging.handlers.RotatingFileHandler(filename, maxBytes = 10000000, backupCount = 4)

        logger.addHandler(ch)
        logger.addHandler(fl)
        return logger
    
    def log(self, node_id : str, log_type : str, value : float):
        now = time.time()
        
        if not (node_id, log_type) in self._last_log_time:
            self._logger.debug(f'{node_id},{log_type},{value}')
            self._last_log_time[(node_id, log_type)] = now
        
        diff_in_seconds = self._last_log_time[(node_id, log_type)] - now
        
        if diff_in_seconds > self._time_in_seconds:
            self._logger.debug(f'{node_id},{log_type},{value}')
            self._last_log_time[(node_id, log_type)] = now
            

class MetadataConsumer(ConsumerNode):
    '''
    - Arguments:
        - log_folder: (str) Folder where to save the logs.
    '''
    def __init__(self, log_folder = './'):
        self._accountant = None
        self._log_folder = log_folder
        self._mlogger = MetricsLogger(log_folder)
        self._bottlenecks_reported = False
        self._message_count = 0
        super(MetadataConsumer, self).__init__(metadata = True)
    
    def open(self):
        self._accountant = Accountant(len(self._parents))
    
    def close(self):
        if not self._bottlenecks_reported:
            self.report_bottlenecks()
    
    def get_bottlenecks(self):
        '''
        A bottleneck is any node that process at a speed that is 
        lower to the speed of any of the producers.

        There is also what we call ``effective bottleneck``. An ``effective
        bottleneck`` is a node that reduces the throughput of the 
        flow.  

        - Returns:
            - is_bottleneck: list of booleans of the same size as \
                self._parents. Contains ``None`` entries if data
                is not statistically significant to be able to judge if a
                node is a bottleneck.
            - is_effective_bottleneck: list of booleans of the same size as \
                self._parents. Contains ``None`` entries if data
                is not statistically significant to be able to judge if a
                node is a bottleneck.
        '''
        actual_proctime = self._accountant.get_actual_proctime()
        proctime = self._accountant.get_proctime()
        
        #1. Find bottlenecks
        is_producer_node = [isinstance(a, ProducerNode) for a in self._parents]
        min_producer_time = min([proctime[i] for i in range(len(is_producer_node)) if is_producer_node[i]])
        is_bottleneck = [proctime[i] > min_producer_time and not is_producer_node[i] for i in range(len(self._parents))]

        #2. Find effective bottlenecks
        is_effective_bottleneck = [proctime[i] > proctime[i - 1] and is_bottleneck[i] for i in range(len(self._parents))]
        
        return is_bottleneck, is_effective_bottleneck

    def report_bottlenecks(self):
        b, eb = self.get_bottlenecks()
        proctime = self._accountant.get_proctime()
        actual_proctime = self._accountant.get_actual_proctime()
        fps = [1.0 / a if a != 0 else float('nan') for a in proctime]
        actual_fps = [1.0 / a if a !=0 else float('nan') for a in actual_proctime]

        fps_lines = ["\n%-28s%-14.1f%-14.1f%-14s%-14s" % (str(self._parents[i]), fps[i], actual_fps[i], str(b[i]), str(eb[i])) for i in range(len(self._parents))]
        fps_lines = ['\n%-28s%-14s%-14s%-14s%-14s' % ("Node name", "Possible fps", "Actual fps", "Bottleneck", "Eff. bottleneck")] + fps_lines
        package_logger.info('Flow fps per nodes: {}'.format(''.join(fps_lines)))
        self._bottlenecks_reported = True

    def consume(self, *metadata):
        '''
        - Arguments:
            - metadata: list of metadata for all the parent \
                nodes for which we gather the data 
        '''
        if not os.path.exists(self._log_folder):
            os.makedirs(self._log_folder)
        
        self._message_count += 1

        for idx, entry in enumerate(metadata):
            for log_type in [Accountant.logtype_proctime, Accountant.logtype_actualproctime]:
                node_id = str(self._parents[idx])
                value = entry.get(log_type, None)
            
                #2. Update in-memory accounting
                self._accountant.update_stat(idx, log_type, value)

                #3. Write logs into filesytem
                # TODO: Figure out how to not write this in command line,
                # but only write it on file system.
                #self._mlogger.log(node_id, log_type, value)

        #4. Report bottlenecks
        if (not self._bottlenecks_reported) and (self._message_count > (len(self._parents) * 40)):
            self.report_bottlenecks()
