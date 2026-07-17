from __future__ import absolute_import, division, print_function

import numpy as np

from ...core.node import OneTaskProcessorNode


class BoundingBoxTracker(OneTaskProcessorNode):
    '''
    Tracks bounding boxes from one frame to another.
    It keeps an internal state representation that allows
    it to track across frames.
    '''
    def _track(self, dets : np.ndarray) -> np.ndarray:
        '''
        - Arguments:
            - dets: np.ndarray of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
        '''
        raise NotImplementedError("Subclass must implement _track method")

    def process(self, dets : np.ndarray) -> np.ndarray:
        '''
        - Arguments:
            - dets: np.ndarray of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
        - Returns:
            - tracks: np.ndarray of shape (nb_boxes, 5) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, track_id])
        '''
        return self._track(dets)
