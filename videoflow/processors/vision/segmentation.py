from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np

from ...core.node import ProcessorNode

class Segmenter(ProcessorNode):
    '''
    Abstract class that defines the interface to do image
    segmentation in images
    '''
    def _segment(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - mask: np.array of shape (h, w, num_classes)
        '''
        raise NotImplementedError('Subclass must implement it')

    def process(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - masks: np.array of shape (nb_masks, h, w)
            - classes: np.array of shape (nb_masks,)
            - scores: np.array of shape (nb_masks,)
        '''
        return self._segment(im)