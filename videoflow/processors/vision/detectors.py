import numpy as np
import tensorflow as tf

from ...core.node import ProcessorNode
from ...utils.tensorflow import TensorflowModel

class ObjectDetector(ProcessorNode):
    def __init__(self, ):
        pass
    
    def _detect(self, im):
        raise NotImplemented('Subclass must implement it')
    
    def process(self, im : np.array):
        return self._detect(im)

class TensorflowObjectDetector(ObjectDetector):
    def __init__(self, path_to_pb_file,
                num_classes, path_to_labels,
                min_score_threshold = 0.5):
        self._tensorflow_model = TensorflowModel(
            path_to_pb_file,
            ["image_tensor:0"],
            ["detection_boxes:0", "detection_scores:0", "detection_classes:0", "num_detections:0"]
        )
        self._num_classes = num_classes
        self._path_to_labels = path_to_labels
        self._min_score_threshold = min_score_threshold
        label_map = label_map_util.load_labelmap(self._path_to_labels)
        categories = label_map_util.convert_map_to_categories(label_map, max_num_classes = num_classes, use_display_name = True)
        self._category_index = label_map_util.create_category_index(categories)

    def _detect(self, im : np.array) -> np.array:
        boxes, scores, classes, num = self._tensorflow_model.run_on_input(im)
        boxes = np.squeeze(boxes)
        scores = np.squeeze(scores)
        classes = np.squeeze(classes)

        #TODO: continue working here

