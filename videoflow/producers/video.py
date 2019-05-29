from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import cv2
import numpy as np

from ..core.node import ProducerNode

class ImageFolderReader(ProducerNode):
    '''
    Reads from a folder of images and returns them one by one.
    Passes through images in alphabetical order.
    '''
    def __init__(self):
        pass
    
    def open(self):
        raise NotImplementedError()
    
    def close(self):
        raise NotImplementedError()
    
    def next(self) -> np.array:
        raise NotImplementedError()

class VideoFolderReader(ProducerNode):
    '''
    Reads videos from a folder of videos and returns the frames of 
    the videos one by one.
    Passes through videos in alphabetical order.
    '''
    def __init__(self):
        pass

    def open(self):
        raise NotImplementedError()
    
    def close(self):
        raise NotImplementedError()
    
    def next(self) -> np.array:
        raise NotImplementedError()

class VideostreamReader(ProducerNode):
    '''
    Reader of video streams, using ``cv2``
    
    - Arguments:
        - url_or_deviceid: (int or str) The url, filesystem path or id of the \
            video stream.
        - nb_frames: (int) The number of frames when to stop. -1 never stops
        - nb_retries: (int) If there are errors reading the stream, how \
            many times to retry.
    '''
    def __init__(self, url_or_deviceid, nb_frames = -1, nb_retries = 0):
        self._url_or_deviceid = url_or_deviceid
        self._video = None
        self._nb_frames = nb_frames
        self._frame_count = 0
        self._nb_retries = nb_retries
        self._retries_count = 0
        super(VideostreamReader, self).__init__()

    def open(self):
        '''
        Opens the video stream
        '''
        if self._video is None:
            self._video = cv2.VideoCapture(self._url_or_deviceid)

    def close(self):
        '''
        Releases the video stream object
        '''
        if self._video and self._video.isOpened():
            self._video.release()

    def next(self):
        '''
        - Returns:
            - frame no / index  : integer value of the frame read
            - frame: np.array of shape (h, w, 3)
        
        - Raises:
            - StopIteration: after it finishes reading the videofile \
                or when it reaches the specified number of frames to \
                process, or if it reaches the number of retries wihout \
                success.
        '''
        if self._frame_count == self._nb_frames:
            raise StopIteration()

        while self._retries_count <= self._nb_retries:
            if self._video.isOpened():
                success, frame = self._video.read()
                self._frame_count += 1
                if not success:
                    if self._video.isOpened():
                        self._video.release()
                    self._video = cv2.VideoCapture(self._url_or_deviceid)
                else:
                    return (self._frame_count, frame)
            else:
                self._video = cv2.VideoCapture(self._url_or_deviceid)
            self._retries_count += 1
        raise StopIteration()
    
class VideoUrlReader(VideostreamReader):
    '''
    Opens a video capture object and returns subsequent frames
    from the video url each time ``next`` is called.

    - Arguments:
        - device_id: id of the video device connected to the computer
        - nb_frames: number of frames to process. -1 means all of them
    '''
    def __init__(self, url : str, nb_frames : int = -1, nb_retries = 0):
        super(VideoUrlReader, self).__init__(url, nb_frames = nb_frames, nb_retries = nb_retries)

class VideoDeviceReader(VideostreamReader):
    '''
    Opens a video capture object and returns subsequent frames
    from the video device each time ``next`` is called.

    - Arguments:
        - device_id: id of the video device connected to the computer
        - nb_frames: number of frames to process. -1 means all of them
    '''
    def __init__(self, device_id : int, nb_frames : int = -1, nb_retries = 0):
        super(VideoDeviceReader, self).__init__(device_id, nb_frames = nb_frames, nb_retries = nb_retries)    

class VideoFileReader(VideostreamReader):
    '''
    Opens a video capture object and returns subsequent frames
    from the video file each time ``next`` is called.

    - Arguments:
        - video_file: path to video file
        - nb_frames: number of frames to process. -1 means all of them
    '''
    def __init__(self, video_file : str, nb_frames = -1):
        super(VideoFileReader, self).__init__(video_file, nb_frames = nb_frames, nb_retries = 0)

# Here for the sake of not breaking
# old code
VideofileReader = VideoFileReader