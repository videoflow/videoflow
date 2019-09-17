'''
Collection of object detection processors
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np

from ...core.node import ProcessorNode

BASE_URL_DETECTION = 'https://github.com/videoflow/videoflow-contrib/releases/download/detector_tf/'

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
              Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
        '''
        raise NotImplementedError('Subclass must implement it')
    
    def process(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
                The box coordinates are returned unnormalized (values NOT between 0 and 1, \
                but using the original dimension of the image)
        '''
        return self._detect(im)
