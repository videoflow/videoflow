from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np

from ...core.node import OneTaskProcessorNode

class BoundingBoxTracker(OneTaskProcessorNode):
    '''
    Tracks bounding boxes from one frame to another.
    It keeps an internal state representation that allows
    it to track across frames.
    '''
    def _track(self, dets : np.array) -> np.array:
        '''
        - Arguments: 
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
        '''
        raise NotImplementedError("Subclass must implement _track method")
    
    def process(self, dets : np.array) -> np.array:
        '''
        - Arguments: 
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
        - Returns:
            - tracks: np.array of shape (nb_boxes, 5) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, track_id])
        '''
        return self._track(dets)
