from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import cv2

from ..core.node import ConsumerNode, ContextNode

class VideofileWriter(ConsumerNode, ContextNode):
    '''
    Opens a video file writer object and writes subsequent
    frames received into the object.
    '''
    pass

    def __enter__(self):
        pass
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def consume(self, item):
        pass