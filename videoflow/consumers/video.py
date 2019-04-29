from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import cv2

from ..core.node import ConsumerNode

class VideofileWriter(ConsumerNode):
    '''
    Opens a video file writer object and writes subsequent
    frames received into the object.
    '''