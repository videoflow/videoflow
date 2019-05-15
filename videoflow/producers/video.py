from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import cv2

from ..core.node import ProducerNode

class VideofileReader(ProducerNode):
    '''
    Opens a video capture object and returns subsequent frames
    from the video each time ``next`` is called
    '''
    def __init__(self, video_file : str, nb_frames = -1):
        '''
        - Arguments:
            - video_file: path to video file
            - nb_frames: number of frames to process. -1 means all of them
        '''
        self._video_file = video_file
        self._video = None
        self._nb_frames = nb_frames
        self._frame_count = 0
        super(VideofileReader, self).__init__()
    
    def open(self):
        '''
        Opens the video stream
        '''
        if self._video is None:
            self._video = cv2.VideoCapture(self._video_file)

    def close(self):
        '''
        Releases the video stream object
        '''
        self._video.release()

    def next(self):
        '''
        - Returns:
            - frame: np.array of shape (h, w, 3)
        
        - Raises:
            - StopIteration: after it finishes reading the videofile \
                or when it reaches the specified number of frames to \
                process.
        '''
        
        if self._video.isOpened():
            success, frame = self._video.read()
            self._frame_count += 1
            if not success or self._frame_count == self._nb_frames:
                raise StopIteration()
            else:
                return frame
        else:
            raise StopIteration()
