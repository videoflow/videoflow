from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from ..core.node import ConsumerNode

class CommandlineConsumer(ConsumerNode):
    '''
    Writes the input received to the command line.
    
    - Arguments:
        - sep: separator to use between tokens.
        - end: end of line character
    '''
    def __init__(self, sep = ' ', end = '\n'):

        self._end = end
        self._sep = sep
        super(CommandlineConsumer, self).__init__()
    
    def consume(self, item):
        '''
        Prints `item` to the command line, adding an end of line character after it.
        - Arguments:
            - item: It can be anything that can be printed with the ``print()`` function
        '''
        print(item, sep = self._sep, end = self._end)
