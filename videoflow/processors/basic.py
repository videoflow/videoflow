from ..core.node import ProcessorNode

class IdentityProcessor(ProcessorNode):
    def process(self, inp):
        return inp

class JoinerProcessor(ProcessorNode):
    def process(self, *inp):
        return tuple(inp)
