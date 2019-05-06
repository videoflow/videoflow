from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from ..core.node import OneTaskProcessorNode

class SumAggregator(OneTaskProcessorNode):
    '''
    Keeps a running sum of all the inputs processed
    '''
    def __init__(self):
        self._sum = 0
        super(SumAggregator, self).__init__()
    
    def process(self, inp):
        '''
        - Arguments:
            - inp: a number
        
        - Returns:
            - sum: the cumulative sum up to this point, including ``inp`` in it.
        '''
        self._sum += inp
        return self._sum

class MultiplicationAggregator(OneTaskProcessorNode):
    '''
    Keeps a running multiplication of all the inputs processed
    '''
    def __init__(self):
        self._mult = 1
        super(MultiplicationAggregator, self).__init__()

    def process(self, inp):
        '''
        - Arguments:
            - inp: a number
        
        - Returns:
            - mult: the cumulative multiplication up to this point, including ``inp`` in it.
        '''
        self._mult *= inp
        return self._mult  

class CountAggregator(OneTaskProcessorNode):
    '''
    Keeps count of all the items processed
    '''
    def __init__(self):
        self._count = 0
        super(CountAggregator, self).__init__()
    
    def process(self, inp):
        '''
        - Arguments:
            - inp: a number
        
        - Returns:
            - count: the cumulative count up to this point
        '''
        self._count += 1
        return self._count

class MaxAggregator(OneTaskProcessorNode):
    def __init__(self):
        self._max = float("-inf")
        super(MaxAggregator, self).__init__()
    
    def process(self, inp):
        '''
        - Arguments:
            - inp: a number
        
        - Returns:
            - max: the maximum seen value up to this point
        '''
        if inp > self._max:
            self._max = inp
        return self._max

class MinAggregator(OneTaskProcessorNode):
    def __init__(self):
        self._min = float("inf")
        super(MinAggregator, self).__init__()
    
    def process(self, inp):
        '''
        - Arguments:
            - inp: a number
        
        - Returns:
            - min: the the minimum seen value up to this point
        '''
        if inp < self._min:
            self._min = inp
        return self._min
