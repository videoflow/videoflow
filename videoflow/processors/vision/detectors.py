from ...core.node import ProcessorNode
from ..tensorflow import TensorflowModel

class ObjectDetector(ProcessorNode):
    def __init__(self, ):
        pass
    
    def _detect():
        raise NotImplemented('Subclass must implement it')
    
    def process(self, inp):
        return self._detect(inp)

class TensorflowObjectDetector(ObjectDetector):
    def __init__(self, path_to_pb_file):
        self._tensorflow_model = TensorflowModel(path_to_pb_file)

    def _detect():
        pass