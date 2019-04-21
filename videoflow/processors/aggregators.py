from ..core.node import ProcessorNode

class SumAggregator(ProcessorNode):
    def __init__(self):
        self._sum = 0
        super(SumAggregator, self).__init__()
    
    def process(self, inp):
        self._sum += inp
        return self._sum

class MultiplicationAggregator(ProcessorNode):
    def __init__(self):
        self._mult = 1
        super(MultiplicationAggregator, self).__init__()

    def process(self, inp):
        self._mult *= inp
        return self._mult  

class CountAggregator(ProcessorNode):
    def __init__(self):
        self._count = 0
        super(CountAggregator, self).__init__()
    
    def process(self, inp):
        self._count += 1
        return self._count

class MaxAggregator(ProcessorNode):
    def __init__(self):
        self._max = float("-inf")
        super(MaxAggregator, self).__init__()
    
    def process(self, inp):
        if inp > self._max:
            self._max = inp
        return self._max

class MinAggregator(ProcessorNode):
    def __init__(self):
        self._min = float("inf")
        super(MinAggregator, self).__init__()
    
    def process(self, inp):
        if inp < self._min:
            self._min = inp
        return self._min
