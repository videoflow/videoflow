import cv2

from ..core.node import ProducerNode

class VideoProducer(ProducerNode):
    def __init__(self, video_file : str):
        '''
        Arguments:
        - video_file: path to video file
        '''
        self._video_file = video_file
        self._video = None
        super(VideoProducer, self).__init__()

    def __next__(self):
        while (self._video.isOpened()):
            success, frame = video.read()
            if not success:
                break
            yield frame
        self._video.release()
        raise StopIteration()

    def __iter__(self):
        self._video = cv2.VideoCapture(self._video_file)
        return self
        