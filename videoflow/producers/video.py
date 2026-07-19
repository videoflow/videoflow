from __future__ import absolute_import, division, print_function

import logging
import time

import cv2
import numpy as np

from ..core.context import RuntimeContext
from ..core.node import ProducerNode

logger = logging.getLogger(__name__)

#: Timestamp sources for ``VideostreamReader``: ``clock`` stamps each frame with
#: the wall-clock time it was read (live cameras/streams on NTP/PTP-disciplined
#: hosts); ``position`` stamps it with the frame's position on the video's own
#: timeline (``CAP_PROP_POS_MSEC`` — synchronized recordings played back together).
TIMESTAMP_CLOCK = 'clock'
TIMESTAMP_POSITION = 'position'
TIMESTAMP_SOURCES = (TIMESTAMP_CLOCK, TIMESTAMP_POSITION)

class ImageProducer(ProducerNode):
    '''
    Reads a single image and produces it
    '''

    def __init__(self, image_path : str, **kwargs) -> None:
        self._image_path = image_path
        self._image_returned = False
        super(ImageProducer, self).__init__(**kwargs)

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    def next(self) -> np.ndarray:
        '''
        Returns image in RGB format.
        '''
        if not self._image_returned:
            im = cv2.imread(self._image_path)
            if im is None:
                raise StopIteration()
            im = im[...,::-1]
            self._image_returned = True
            return im
        else:
            raise StopIteration()

class ImageFolderReader(ProducerNode):
    '''
    Reads from a folder of images and returns them one by one.
    Passes through images in alphabetical order.
    '''
    def __init__(self, **kwargs) -> None:
        super(ImageFolderReader, self).__init__(**kwargs)

    def open(self) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def next(self) -> np.ndarray:
        raise NotImplementedError()

class VideoFolderReader(ProducerNode):
    '''
    Reads videos from a folder of videos and returns the frames of
    the videos one by one.
    Passes through videos in alphabetical order.
    '''
    def __init__(self, **kwargs) -> None:
        super(VideoFolderReader, self).__init__(**kwargs)

    def open(self) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def next(self) -> np.ndarray:
        raise NotImplementedError()

class VideostreamReader(ProducerNode):
    '''
    Reader of video streams, using ``cv2``

    - Arguments:
        - url_or_deviceid: (int or str) The url, filesystem path or id of the \
            video stream.
        - swap_channels: if True, it will change channels from BGR to RGB
        - nb_frames: (int) The number of frames when to stop. -1 never stops
        - nb_retries: (int) If there are errors reading the stream, how \
            many times to retry.
        - timestamp_source: (str) how each frame's event time is stamped (see \
            ``TIMESTAMP_SOURCES``): ``clock`` (default — wall clock at read) or \
            ``position`` (the video's own timeline). The stamp is attached to the \
            published message via ``ctx.set_event_timestamp`` so time-aligned \
            joins downstream can synchronize this stream with others.
    '''
    def __init__(self, url_or_deviceid : int | str, swap_channels : bool = True, nb_frames : int = -1,
                nb_retries : int = 0, is_finite : bool = True,
                timestamp_source : str = TIMESTAMP_CLOCK, **kwargs) -> None:
        if timestamp_source not in TIMESTAMP_SOURCES:
            raise ValueError(f'timestamp_source must be one of {TIMESTAMP_SOURCES}, '
                            f'got {timestamp_source!r}')
        self._url_or_deviceid = url_or_deviceid
        self._video : cv2.VideoCapture | None = None   # opened lazily in open()
        self._swap_channels = swap_channels
        self._nb_frames = nb_frames
        self._frame_count = 0
        self._nb_retries = nb_retries
        self._retries_count = 0
        self._timestamp_source = timestamp_source
        super(VideostreamReader, self).__init__(is_finite = is_finite, **kwargs)

    def open(self) -> None:
        '''
        Opens the video stream
        '''
        if self._video is None:
            self._video = cv2.VideoCapture(self._url_or_deviceid)

    def close(self) -> None:
        '''
        Releases the video stream object
        '''
        if self._video and self._video.isOpened():
            self._video.release()

    def next(self, ctx : RuntimeContext | None = None) -> tuple[int, np.ndarray]:
        '''
        - Returns:
            - frame no / index  : integer value of the frame read
            - frame: np.ndarray of shape (h, w, 3)

        When run by a task (which passes the runtime ``ctx``), each frame's event
        time is stamped on the published message per ``timestamp_source``.

        - Raises:
            - StopIteration: after it finishes reading the videofile \
                or when it reaches the specified number of frames to \
                process, or if it reaches the number of retries wihout \
                success.
        '''
        if self._video is None:
            raise RuntimeError(
                f'{type(self).__name__}.next() called before open(). The capture object is '
                'created in open(), which the task runs in the worker — call open() first.')

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
                    if ctx is not None:
                        if self._timestamp_source == TIMESTAMP_POSITION:
                            ctx.set_event_timestamp(self._video.get(cv2.CAP_PROP_POS_MSEC) / 1000.0)
                        else:
                            ctx.set_event_timestamp(time.time())
                    if self._swap_channels:
                        frame = frame[...,::-1]
                    return (self._frame_count, frame)
            else:
                self._video = cv2.VideoCapture(self._url_or_deviceid)
            self._retries_count += 1
            logger.error(f'Error reading video, increasing retries count to {self._retries_count}')
        raise StopIteration()

class VideoUrlReader(VideostreamReader):
    '''
    Opens a video capture object and returns subsequent frames
    from the video url each time ``next`` is called.

    - Arguments:
        - device_id: id of the video device connected to the computer
        - nb_frames: number of frames to process. -1 means all of them
    '''
    def __init__(self, url : str, nb_frames : int = -1, nb_retries : int = 0, is_finite : bool = False, **kwargs) -> None:
        super(VideoUrlReader, self).__init__(url, nb_frames = nb_frames, nb_retries = nb_retries,
                                            is_finite = is_finite, **kwargs)

    def get_params(self) -> dict:
        # Overridden because this class's own __init__ names its first argument
        # `url`, while the base class stores it as `self._url_or_deviceid` — the
        # default MRO-walking get_params() can't bridge that rename automatically.
        return {
            'url': self._url_or_deviceid,
            'nb_frames': self._nb_frames,
            'nb_retries': self._nb_retries,
            'is_finite': self._is_finite,
            'swap_channels': self._swap_channels,
            'timestamp_source': self._timestamp_source,
            'name': self._name,
        }

class VideoDeviceReader(VideostreamReader):
    '''
    Opens a video capture object and returns subsequent frames
    from the video device each time ``next`` is called.

    - Arguments:
        - device_id: id of the video device connected to the computer
        - nb_frames: number of frames to process. -1 means all of them
    '''
    def __init__(self, device_id : int, nb_frames : int = -1, nb_retries : int = 0, is_finite : bool = False, **kwargs) -> None:
        super(VideoDeviceReader, self).__init__(device_id, nb_frames = nb_frames, nb_retries = nb_retries,
                                                is_finite = is_finite, **kwargs)

    def get_params(self) -> dict:
        return {
            'device_id': self._url_or_deviceid,
            'nb_frames': self._nb_frames,
            'nb_retries': self._nb_retries,
            'is_finite': self._is_finite,
            'swap_channels': self._swap_channels,
            'timestamp_source': self._timestamp_source,
            'name': self._name,
        }


class VideoFileReader(VideostreamReader):
    '''
    Opens a video capture object and returns subsequent frames
    from the video file each time ``next`` is called.
    - Arguments:
        - video_file: path to video file
        - swap_channels: If true, swaps from BGR to RGB
        - nb_frames: number of frames to process. -1 means all of them
        - timestamp_source: defaults to ``position`` (the file's own timeline), \
            which is what synchronized recordings replayed together should align on.
    '''
    def __init__(self, video_file : str, swap_channels : bool = False, nb_frames : int = -1,
                timestamp_source : str = TIMESTAMP_POSITION, **kwargs) -> None:
        super(VideoFileReader, self).__init__(video_file, swap_channels = swap_channels, nb_frames = nb_frames,
                                            nb_retries = 0, is_finite = True,
                                            timestamp_source = timestamp_source, **kwargs)

    def get_params(self) -> dict:
        return {
            'video_file': self._url_or_deviceid,
            'swap_channels': self._swap_channels,
            'nb_frames': self._nb_frames,
            'timestamp_source': self._timestamp_source,
            'name': self._name,
        }

# Here for the sake of not breaking
# old code
VideofileReader = VideoFileReader
