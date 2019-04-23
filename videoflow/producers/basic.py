from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import time

from ..core.node import ProducerNode

class IntProducer(ProducerNode):
    def __init__(self, start_value : int = 0, end_value : int = None, 
                wait_time_in_seconds : float = 0):
        self._start_value = start_value
        self._end_value = end_value
        self._wait_time_in_seconds = wait_time_in_seconds
        self._current_value = self._start_value
        super(IntProducer, self).__init__()
        
    def next(self):
        if self._end_value is not None and self._current_value > self._end_value:
            raise StopIteration()
        to_return = self._current_value
        self._current_value += 1
        time.sleep(self._wait_time_in_seconds)
        return to_return
