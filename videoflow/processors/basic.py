from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from ..core.node import ProcessorNode

class IdentityProcessor(ProcessorNode):
    def process(self, inp):
        return inp

class JoinerProcessor(ProcessorNode):
    def process(self, *inp):
        return tuple(inp)
