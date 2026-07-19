from __future__ import absolute_import, division, print_function

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

    def __init__(self, video_file : str, swap_channels : bool = True, fps : int = 30, **kwargs) -> None:
        if video_file[-4:] != '.avi':
            raise ValueError('Only .avi format is supported')
        self._video_file = video_file
        self._swap_channels = swap_channels
        self._fps = fps
        self._out: cv2.VideoWriter | None = None  # created lazily on the first frame
        super(VideofileWriter, self).__init__(**kwargs)

    def open(self) -> None:
        '''
        We don't open the video writer here.  We need to wait for the first frame \
        to arrive in order to determine the width and height of the video
        '''
        pass

    def close(self) -> None:
        '''
        Closes the video stream
        '''
        if self._out is not None and self._out.isOpened():
            self._out.release()

    def consume(self, item : np.ndarray) -> None:
        '''
        Receives the picture frame to append to the video and appends it to the video.

        If it is the first frame received, it opens the video file and determines \
            the height and width of the video from the dimensions of that first frame. \
            Every subsequent frame is expected to have the same height and width. If it \
            does not has it, it gets resized to it.

        - Arguments:
            - item: np.ndarray of dimension (height, width, 3)
        '''
        if self._out is None:
            self._height = item.shape[0]
            self._width = item.shape[1]
            fourcc = cv2.VideoWriter_fourcc('M', 'J', 'P', 'G')  # type: ignore[attr-defined]
            self._out = cv2.VideoWriter(self._video_file, fourcc, self._fps, (self._width, self._height))

        resized = cv2.resize(item, (self._width, self._height), interpolation = cv2.INTER_AREA)
        if self._swap_channels:
            resized = resized[...,::-1]
        self._out.write(resized)
