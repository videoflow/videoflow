from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import cv2
import numpy as np

def resize_add_padding(im, t_h, t_w):
    '''
    Resizes an image to a target size, adding padding if necessary to maintain
    the aspect ratio
    - Arguments:
        - im (np.array): shape (h, w, 3)
        - t_h (int): target height
        - t_w (int): target width
    '''
    min_idx = [t_h, t_w].index(min(t_h, t_w))
    ratio = [t_h, t_w][min_idx] / im.shape[min_idx]
    new_im = np.zeros((t_h, t_w, 3), dtype = im.dtype)
    res_h, res_w = int(im.shape[0] * ratio), int(im.shape[1] * ratio)
    res_im = cv2.resize(im, (res_w, res_h))
    new_im[:res_h, :res_w, :] = res_im
    return new_im