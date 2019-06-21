from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np
import cv2

from ...core.node import ProcessorNode


class CropImageTransformer(ProcessorNode):
    def __init__(self):
        super(CropImageTransformer, self).__init__()

    def _crop(self, im : np.array, c) -> np.array:
        '''
        - Arguments:
            - im (np.array): shape of (h, w, 3)
            - c (list): Crop dimensions: [ymin, xmin, ymax, xmax]
        
        - Raises:
            - ValueError:
                - If any of crop_dimensions less than 0
                - If any of crop_dimensions out of bounds
                - If ymin > ymax or xmin > xmax
        '''
        if any([a < 0 for a in c]):
            raise ValueError('One of the crop values is less than 0')
        ymin, xmin, ymax, xmax = c
        if ymin > ymax or xmin > xmax:
            raise ValueError('ymin > ymax or xmin > xmax')
        if ymax > im.shape[0] or xmax > im.shape[1]:
            raise ValueError('One of the crop indexes is out of bounds')
        im = im[ymin : ymax, xmin : xmax, :]
        return im
    
    def process(self, im : np.array, crop_dimensions) -> np.array:
        '''
        Crops image according to the coordinates in crop_dimensions.
        If those coordinates are out of bounds, it will raise errors

        - Arguments:
            - im (np.array): shape of (h, w, 3)
            - crop_dimensions (tuple): (ymin, xmin, ymax, xmax)
        
        - Raises:
            - ValueError:
                - If any of crop_dimensions less than 0
                - If any of crop_dimensions out of bounds
                - If ymin > ymax or xmin > xmax
        '''
        to_transform = np.array(im)
        return self._crop(im, crop_dimensions)


class MaskImageTransformer(ProcessorNode):
    def _mask(self, im : np.array, mask : np.array) -> np.array:
        raise NotImplementedError()
    
    def process(self, im : np.array, mask : np.array) -> np.array:
        to_transform = np.array(im)
        return self._crop(im, mask)


class ResizeImageTransformer(ProcessorNode):
    def _resize(self, im : np.array, new_size) -> np.array:
        raise NotImplementedError()
    
    def process(self, im : np.array, new_size) -> np.array:
        to_transform = np.array(im)
        return self._resize(im, new_size)