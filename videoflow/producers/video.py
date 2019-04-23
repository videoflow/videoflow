from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import cv2

from ..core.node import ProducerNode

class VideofileReader(ProducerNode):
    def __init__(self, video_file : str):
        '''
        Arguments:
        - video_file: path to video file
        - retry_attempts: number of retries to read video
          stream before raising StopIteration exception
        '''
        self._video_file = video_file
        self._video = None
        super(VideofileReader, self).__init__()

    def next(self):
        if self._video is None:
            self._video = cv2.VideoCapture(self._video_file)

        if self._video.isOpened():
            success, frame = self._video.read()
            if not success:
                self._video.release()
                raise StopIteration()
            else:
                return frame
        else:
            self._video.release()
            raise StopIteration()
