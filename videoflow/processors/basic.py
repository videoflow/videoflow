from ..core.node import ProcessorNode

class IdentityProcessor(ProcessorNode):
    def process(self, inp):
        return inp
