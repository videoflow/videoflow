import cv2

from ..core.node import ProducerNode

class VideoProducer(ProducerNode):
    def __init__(self, video_file : str):
        '''
        Arguments:
        - video_file: path to video file
        '''
        self._video_file = video_file

    def __iter__(self):
        video = cv2.VideoCapture(self._video_file)
        while (video.isOpened()):
            success, frame = video.read()
            if not success:
                break
            yield frame
        video.release()


