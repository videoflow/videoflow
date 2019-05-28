import sys

import cv2
import videoflow
import videoflow.core.flow as flow
from videoflow.core.constants import REALTIME
from videoflow.consumers import VideofileWriter

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    def __init__(self):
        super(FrameIndexSplitter, self).__init__()
    
    def process(self, data):
        index, frame = data
        return frame

class RTSPStreamReader(videoflow.core.node.ProducerNode):
    def __init__(self, address : str, username : str, password : str, nb_frames = -1):
        self._url = f'rtsp://{username}:{password}@{address}'
        self._video = None
        self._nb_frames = nb_frames
        self._frame_count = 0
        super(RTSPStreamReader, self).__init__()

    def open(self):
        '''
        Opens the video stream
        '''
        if self._video is None:
            self._video = cv2.VideoCapture(self._url)

    def close(self):
        '''
        Releases the video stream object
        '''
        self._video.release()

    def next(self):
        '''
        - Returns:
            - frame no / index  : integer value of the frame read
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
                return (self._frame_count, frame)
        else:
            raise StopIteration()

def main():
    stream_address = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    output_file = "output.avi"

    reader = RTSPStreamReader(stream_address, username, password)
    frame = FrameIndexSplitter()(reader)
    writer = VideofileWriter(output_file, fps = 30)(frame)
    fl = flow.Flow([reader], [writer], flow_type = REALTIME)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()    
    