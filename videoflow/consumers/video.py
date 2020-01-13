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
    avi_h264 = 'h264'
    avi_x264 = 'x264'
    mp4_mp4v = 'mp4v'
    mp4_avc1 = 'avc1'
    mp4_avc3 = 'avc3'

    def __init__(self, video_file : str, swap_channels : bool = True, fps : int = 30,
                codec = 'x264'):
        if video_file[-4:] == '.avi' and codec not in [self.avi_h264, self.avi_x264]:
            raise ValueError(f".avi video extension must use one of the following codecs: {[self.avi_h264, self.avi_x264]}")
        elif video_file[-4:] == '.mp4' and codec not in [self.mp4_avc1, self.mp4_avc3, self.mp4_mp4v]:
            raise ValueError(f'.mp4 video extension must use one of the following codecs: {[self.mp4_avc1, self.mp4_avc3, self.mp4_mp4v]}')
        self._video_file = video_file
        self._swap_channels = swap_channels
        self._fps = fps
        self._codec = codec
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
            self._out = cv2.VideoWriter(self._video_file, cv2.VideoWriter_fourcc(*self._codec), self._fps, (self._width, self._height))
        
        resized = cv2.resize(item, (self._width, self._height), interpolation = cv2.INTER_AREA)
        if self._swap_channels:
            resized = resized[...,::-1]
        self._out.write(resized)