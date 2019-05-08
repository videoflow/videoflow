from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import cv2
import numpy as np

from ..core.node import ConsumerNode

class VideofileWriter(ConsumerNode):
    '''
    Opens a video file writer object and writes subsequent \
    frames received into the object.  If video file exists \
    it overwrites it.
    
    The video writer will open when it receives the first frame.

    - Arguments:
        - video_file: path to video.  Folder where video lives must exist. Extension must be .avi
        - fps: frames per second
    '''
    def __init__(self, video_file : str, fps : int = 30):
        if video_file[-4:] != '.avi':
            raise ValueError("Video extension must be .avi")
        self._video_file = video_file
        self._fps = fps
        self._out = None
        super(VideofileWriter, self).__init__()

    def open(self):
        '''
        We don't open the video writer here.  We need to wait for the first frame \
        to arrive in order to determine the width and height of the video
        '''
        pass
    
    def close(self):
        '''
        Closes the video stream
        '''
        if self._out is not None and self._out.isOpened():
            self._out.release()

    def consume(self, item : np.array):
        '''
        Receives the picture frame to append to the video and appends it to the video.
        
        If it is the first frame received, it opens the video file and determines \
            the height and width of the video from the dimensions of that first frame. \
            Every subsequent frame is expected to have the same height and width. If it \
            does not has it, it gets resized to it.
        
        
        - Arguments:
            - item: np.array of dimension (height, width, 3)
        '''
        if self._out is None:
            self._height = item.shape[0]
            self._width = item.shape[1]
            self._out = cv2.VideoWriter(self._video_file, cv2.VideoWriter_fourcc('M', 'J', 'P', 'G'), self._fps, (self._width, self._height))
        
        resized = cv2.resize(item, (self._width, self._height), interpolation = cv2.INTER_AREA)
        self._out.write(resized)