'''
Collection of object detection processors
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np
import tensorflow as tf

from ...core.node import ProcessorNode
from ...utils.tensorflow import TensorflowModel

class ObjectDetector(ProcessorNode):
    '''
    Abstract class that defines the interface of object detectors
    '''
    def _detect(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - dets: np.array of shape (nb_boxes, 6) \
              Specifically (nb_boxes, [xmin, ymin, xmax, ymax, class_index, score])
        '''
        raise NotImplementedError('Subclass must implement it')
    
    def process(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [xmin, ymin, xmax, ymax, class_index, score])
        '''
        return self._detect(im)

class TensorflowObjectDetector(ObjectDetector):
    '''
    Finds object detections by running a Tensorflow model
    on an image.
    '''
    def __init__(self, path_to_pb_file,
                num_classes, 
                min_score_threshold = 0.5):
        '''
        Initializes the tensorflow model.  

        - Arguments:
            - path_to_pb_file (str): Path where model pb file is \
            It expects the model to have the following input tensors: `image_tensor:0`, and \
            the following output tensors: `detection_boxes:0`, `detection_scores:0`, \
            `detection_classes:0`, and `num_detections:0`
            - num_classes (int): number of classes that the detector can recognize
            - min_score_threshold (float): detection will filter out entries with score below threshold score
        '''
        self._tensorflow_model = None
        self._num_classes = num_classes
        self._min_score_threshold = min_score_threshold
    
    def open(self):
        '''
        Creates session with tensorflow model
        '''
        self._tensorflow_model = TensorflowModel(
            path_to_pb_file,
            ["image_tensor:0"],
            ["detection_boxes:0", "detection_scores:0", "detection_classes:0", "num_detections:0"]
        )
    
    def close(self):
        '''
        Closes tensorflow model session.
        '''
        self._tensorflow_model._close_session()

    def _detect(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [xmin, ymin, xmax, ymax, class_index, score])
        '''
        im_expanded = np.expand_dims(im, axis = 0)
        boxes, scores, classes, num = self._tensorflow_model.run_on_input(im_expanded)
        boxes, scores, classes = np.squeeze(boxes), np.squeeze(scores), np.squeeze(classes)
        indexes = np.where(scores > self._min_score_threshold)[0]
        boxes, scores, classes = boxes[indexes], scores[indexes], classes[indexes]
        return np.concatenate((boxes, classes, scores), axis = 1)
