from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import time

from ..core.node import ProcessorNode

class IdentityProcessor(ProcessorNode):
    '''
    IdentityProcessor implements the identity
    function: it returns the same value that it received
    as input. You can introduce some delay by setting fps 
    to a value greater than 0.

    - Arguments:
        - fps (int): frames per second. If value is less \
            than or equal to zero, it is ignored, and no delay \
            is introduced.
    '''
    def __init__(self, fps = -1, *args, **kargs):
        super(IdentityProcessor, self).__init__(*args, **kargs)
        if fps > 0:
            self._wts = 1.0 / fps # wait time in seconds
        else:
            self._wts = 0

    def process(self, inp):
        if self._wts > 0:
            time.sleep(self._wts)
        return inp

class JoinerProcessor(ProcessorNode):
    '''
    Takes all the parameters received in the ``process`` method
    and makes them a tuple of items.

    - Arguments:
        - fps (int): frames per second. If value is less \
            than or equal to zero, it is ignored, and no delay \
            is introduced.
    '''
    def __init__(self, fps = -1, *args, **kargs):
        super(JoinerProcessor, self).__init__(*args, **kargs)
        if fps > 0:
            self._wts = 1.0 / fps
        else:
            self._wts = 0
    
    def process(self, *inp):
        if self._wts > 0:
            time.sleep(self._wts)
        return tuple(inp)
