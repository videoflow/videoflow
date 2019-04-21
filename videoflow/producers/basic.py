import time

from ..core.node import ProducerNode

class IntProducer(ProducerNode):
    def __init__(self, start_value : int = 0, end_value : int = None, 
                wait_time_in_seconds : float = 0):
        self._start_value = start_value
        self._end_value = end_value
        self._wait_time_in_seconds = wait_time_in_seconds
        super(IntProducer, self).__init__()
    
    def __iter__(self):
        return self

    def __next__(self):
        start = self._start_value
        while True:
            yield start
            if self._end_value is not None and start >= self._end_value:
                break
            start += 1
            time.sleep(self._wait_time_in_seconds)
        
        raise StopIteration()
