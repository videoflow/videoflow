from __future__ import absolute_import, division, print_function

import numpy as np

from ...core.node import ProcessorNode


class Segmenter(ProcessorNode):
    '''
    Abstract class that defines the interface to do image
    segmentation in images
    '''
    def _segment(self, im : np.ndarray) -> np.ndarray:
        '''
        - Arguments:
            - im (np.ndarray): (h, w, 3)

        - Returns:
            - mask: np.ndarray of shape (h, w, num_classes)
        '''
        raise NotImplementedError('Subclass must implement it')

    def process(self, im : np.ndarray) -> np.ndarray:
        '''
        - Arguments:
            - im (np.ndarray): (h, w, 3)

        - Returns:
            - masks: np.ndarray of shape (nb_masks, h, w)
            - classes: np.ndarray of shape (nb_masks,)
            - scores: np.ndarray of shape (nb_masks,)
        '''
        return self._segment(im)
