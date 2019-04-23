from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from ..core.node import ProcessorNode

class IdentityProcessor(ProcessorNode):
    '''
    IdentityProcessor implements the identity
    function: it returns the same value that it received
    as input.
    '''
    def process(self, inp):
        return inp

class JoinerProcessor(ProcessorNode):
    '''
    Takes all the parameters received in the ``process`` method
    and makes them a tuple of items.
    '''
    def process(self, *inp):
        return tuple(inp)
