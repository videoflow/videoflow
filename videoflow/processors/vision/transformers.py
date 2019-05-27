from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np
import cv2

from ...core.node import ProcessorNode


class CropImageTransformer(ProcessorNode):
    def _crop(self, im : np.array) -> np.array:
        raise NotImplementedError()
    
    def process(self, im : np.array, crop_dimensions) -> np.array:
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