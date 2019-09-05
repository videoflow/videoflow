from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from typing import Optional, List

import numpy as np
import cv2

from ...core.node import ProcessorNode
from ...utils.transforms import resize_add_padding


class CropImageTransformer(ProcessorNode):
    '''
    - Arguments:
        - crop_dimensions: np.array of shape (nb_boxes, 4) \
                second dimension entries are [ymin, xmin, ymax, xmax] \
                or None

    - Raises:
        - ValueError:
            - If any of crop_dimensions less than 0
            - If ymin > ymax or xmin > xmax
    '''
    def __init__(self, crop_dimensions: Optional[np.array] = None):
        if crop_dimensions:
            self._check_crop_dimensions(crop_dimensions)
            self.crop_dimensions = crop_dimensions
        super(CropImageTransformer, self).__init__()

    @staticmethod
    def _check_crop_dimensions(crop_dimensions: np.array):
        '''
        - Arguments:
            - crop_dimensions: np.array of shape (nb_boxes, 4) \
                    second dimension entries are [ymin, xmin, ymax, xmax]

    - Raises:
        - ValueError:
            - If any of crop_dimensions less than 0
            - If ymin > ymax or xmin > xmax
        '''
        if (crop_dimensions < 0).any():
            raise ValueError('One of the crop values is less than 0')
        if ((crop_dimensions[:, 0] > crop_dimensions[:, 2]).any()
                or (crop_dimensions[:, 1] > crop_dimensions[:, 3]).any()):
            raise ValueError('ymin > ymax or xmin > xmax')

    def _crop(self, im: np.array, crop_dimensions: Optional[np.array] = None) -> List[np.array]:
        '''
        - Arguments:
            - im (np.array): shape of (h, w, 3)
            - crop_dimensions: np.array of shape (nb_boxes, 4) \
                    second dimension entries are [ymin, xmin, ymax, xmax] \
                    or None

        - Raises:
            - ValueError:
                - If any of crop_dimensions less than 0
                - If any of crop_dimensions out of bounds
                - If ymin > ymax or xmin > xmax
        
        - Returns:
            - list of np.arrays: Returns a list of cropped images of the same size as crop_dimensions
        '''
        if crop_dimensions is None:
            if self.crop_dimensions is None:
                raise RuntimeError("Crop dimensions were not specified")
            crop_dimensions = self.crop_dimensions
        self._check_crop_dimensions(crop_dimensions)

        if ((crop_dimensions[:, 0] > im.shape[0]).any()
                or (crop_dimensions[:, 2] > im.shape[1]).any()):
            raise ValueError('One of the crop indexes is out of bounds')
        result = []
        for crop_dimensions_x in crop_dimensions:
            ymin, ymax = int(crop_dimensions_x[0]), int(crop_dimensions_x[2])
            xmin, xmax = int(crop_dimensions_x[1]), int(crop_dimensions_x[3])
            im_cropped = im[ymin:ymax, xmin:xmax, :]
            result.append(im_cropped)
        return result
    
    def process(self, im: np.array, crop_dimensions: Optional[np.array]) -> List[np.array]:
        '''
        Crops image according to the coordinates in crop_dimensions.
        If those coordinates are out of bounds, it will raise errors

        - Arguments:
            - im (np.array): shape of (h, w, 3)
            - crop_dimensions: np.array of shape (nb_boxes, 4) \
                    second dimension entries are [ymin, xmin, ymax, xmax] \
                    or None

        - Raises:
            - ValueError:
                - If any of crop_dimensions less than 0
                - If any of crop_dimensions out of bounds
                - If ymin > ymax or xmin > xmax
        
        - Returns:
            - list of np.arrays: Returns a list of cropped images of the same size as crop_dimensions
        '''
        to_transform = np.array(im)
        return self._crop(to_transform, crop_dimensions)


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