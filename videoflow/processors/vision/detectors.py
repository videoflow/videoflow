from ...core.node import ProcessorNode
from ...utils.tensorflow import TensorflowModel

class ObjectDetector(ProcessorNode):
    def __init__(self, ):
        pass
    
    def _detect():
        raise NotImplemented('Subclass must implement it')
    
    def process(self, im):
        return self._detect(im)

class TensorflowObjectDetector(ObjectDetector):
    def __init__(self, path_to_pb_file):
        self._tensorflow_model = TensorflowModel(
            path_to_pb_file
        )

    def _detect():
        pass