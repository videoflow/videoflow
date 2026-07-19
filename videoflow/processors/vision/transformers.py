from __future__ import absolute_import, division, print_function

from typing import List, Optional

import cv2
import numpy as np

from ...core.node import ProcessorNode
from ...utils.transforms import resize_add_padding


class CropImageTransformer(ProcessorNode):
    '''
    - Arguments:
        - crop_dimensions: np.ndarray of shape (nb_boxes, 4) \
                second dimension entries are [ymin, xmin, ymax, xmax] \
                or None

    - Raises:
        - ValueError:
            - If any of crop_dimensions less than 0
            - If ymin > ymax or xmin > xmax
    '''
    def __init__(self, crop_dimensions: Optional[np.ndarray] = None, **kwargs) -> None:
        self.crop_dimensions = crop_dimensions
        if crop_dimensions:
            self._check_crop_dimensions(crop_dimensions)
        super(CropImageTransformer, self).__init__(**kwargs)

    @staticmethod
    def _check_crop_dimensions(crop_dimensions: np.ndarray) -> None:
        '''
        - Arguments:
            - crop_dimensions: np.ndarray of shape (nb_boxes, 4) \
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

    def _crop(self, im: np.ndarray, crop_dimensions: Optional[np.ndarray] = None) -> List[np.ndarray]:
        '''
        - Arguments:
            - im (np.ndarray): shape of (h, w, 3)
            - crop_dimensions: np.ndarray of shape (nb_boxes, 4) \
                    second dimension entries are [ymin, xmin, ymax, xmax] \
                    or None

        - Raises:
            - ValueError:
                - If any of crop_dimensions less than 0
                - If any of crop_dimensions out of bounds
                - If ymin > ymax or xmin > xmax

        - Returns:
            - list of np.ndarrays: Returns a list of cropped images of the same size as crop_dimensions
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

    # override: one positional arg per parent — the by-parent input contract,
    # not LSP substitutability. See [tool.mypy] disable/enable notes in pyproject.
    def process(self, im: np.ndarray, crop_dimensions: Optional[np.ndarray]) -> List[np.ndarray]:   # type: ignore[override]
        '''
        Crops image according to the coordinates in crop_dimensions.
        If those coordinates are out of bounds, it will raise errors

        - Arguments:
            - im (np.ndarray): shape of (h, w, 3)
            - crop_dimensions: np.ndarray of shape (nb_boxes, 4) \
                    second dimension entries are [ymin, xmin, ymax, xmax] \
                    or None

        - Raises:
            - ValueError:
                - If any of crop_dimensions less than 0
                - If any of crop_dimensions out of bounds
                - If ymin > ymax or xmin > xmax

        - Returns:
            - list of np.ndarrays: Returns a list of cropped images of the same size as crop_dimensions
        '''
        to_transform = np.array(im)
        return self._crop(to_transform, crop_dimensions)


class MaskImageTransformer(ProcessorNode):
    def __init__(self, **kwargs) -> None:
        super(MaskImageTransformer, self).__init__(**kwargs)

    def _mask(self, im : np.ndarray, mask : np.ndarray) -> np.ndarray:
        if mask.shape[:2] != im.shape[:2]:
            raise ValueError("`mask` does not have same dimensions as `im`")
        im = im.astype(float)
        alpha = cv2.merge((mask, mask, mask))
        masked = cv2.multiply(im, alpha)
        return masked.astype(np.uint8)

    # override: one positional arg per parent — the by-parent input contract,
    # not LSP substitutability. See [tool.mypy] disable/enable notes in pyproject.
    def process(self, im : np.ndarray, mask : np.ndarray) -> np.ndarray:   # type: ignore[override]
        '''
        Masks an image according to given masks

        - Arguments:
            - im (np.ndarray): shape of (h, w, 3)
            - mask (np.ndarray): (h, w) of type np.float32, with \
                values between zero and one

        - Raises:
            - ValueError:
                - If ``mask`` does not have same height and width as \
                    ``im``

        '''
        np.array(im)
        return self._mask(im, mask)


class ResizeImageTransformer(ProcessorNode):
    def __init__(self, maintain_ratio : bool = False, **kwargs) -> None:
        self._maintain_ratio = maintain_ratio
        super(ResizeImageTransformer, self).__init__(**kwargs)

    def _resize(self, im : np.ndarray, new_size : tuple[int, int]) -> np.ndarray:
        height, width = new_size
        if height < 0 or width < 0:
            raise ValueError("One of `width` or `height` is a negative value")
        if self._maintain_ratio:
            im = resize_add_padding(im, height, width)
        else:
            im = cv2.resize(im, (width, height))
        return im

    # override: one positional arg per parent — the by-parent input contract,
    # not LSP substitutability. See [tool.mypy] disable/enable notes in pyproject.
    def process(self, im : np.ndarray, new_size : tuple[int, int]) -> np.ndarray:   # type: ignore[override]
        '''
        Resizes image according to coordinates in new_size

        - Arguments:
            - im (np.ndarray): shape of (h, w, 3)
            - new_size (tuple): (new_height, new_width)

        - Raises:
            - ValueError:
                - If ``new_height`` or ``new_width`` are negative
        '''
        np.array(im)
        return self._resize(im, new_size)
