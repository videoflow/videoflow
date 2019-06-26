from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np
import cv2

from ...core.node import ProcessorNode
from ...utils.transforms import resize_add_padding

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
    def __init__(self):
        super(MaskImageTransformer, self).__init__()
    
    def _mask(self, im : np.array, mask : np.array) -> np.array:
        if mask.shape[:2] != im.shape[:2]:
            raise ValueError("`mask` does not have same dimensions as `im`")
        im = im.astype(float)
        alpha = cv2.merge((mask, mask, mask))
        masked = cv2.multiply(im, alpha)
        return masked.astype(np.uint8)
    
    def process(self, im : np.array, mask : np.array) -> np.array:
        '''
        Masks an image according to given masks

        - Arguments:
            - im (np.array): shape of (h, w, 3)
            - mask (np.array): (h, w) of type np.float32, with \
                values between zero and one
        
        - Raises:
            - ValueError:
                - If ``mask`` does not have same height and width as \
                    ``im``

        '''
        to_transform = np.array(im)
        return self._mask(im, mask)


class ResizeImageTransformer(ProcessorNode):
    def __init__(self, maintain_ratio = False):
        self._maintain_ratio = maintain_ratio
        super(ResizeImageTransformer, self).__init__()

    def _resize(self, im : np.array, new_size) -> np.array:
        height, width = new_size
        if height < 0 or width < 0:
            raise ValueError("One of `width` or `height` is a negative value")
        if self._maintain_ratio:
            im = resize_add_padding(im, height, width)
        else:
            im = cv2.resize(im, (width, height))
        return im
    
    def process(self, im : np.array, new_size) -> np.array:
        '''
        Resizes image according to coordinates in new_size

        - Arguments:
            - im (np.array): shape of (h, w, 3)
            - new_size (tuple): (new_height, new_width)
        
        - Raises:
            - ValueError:
                - If ``new_height`` or ``new_width`` are negative
        '''
        to_transform = np.array(im)
        return self._resize(im, new_size)