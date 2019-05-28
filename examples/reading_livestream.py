'''
Example of how to read from an rtsp stream
'''

import sys

import cv2
import videoflow
import videoflow.core.flow as flow
from videoflow.core.constants import REALTIME
from videoflow.producers import VideoUrlReader
from videoflow.consumers import VideofileWriter

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    def __init__(self):
        super(FrameIndexSplitter, self).__init__()
    
    def process(self, data):
        index, frame = data
        return frame

def main():
    # i.e: 10.23.232.43
    stream_address = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    output_file = "output.avi"

    stream_url = f'rtsp://{username}:{password}@{stream_address}'
    reader = VideoUrlReader(stream_url)
    frame = FrameIndexSplitter()(reader)
    writer = VideofileWriter(output_file, fps = 30)(frame)
    fl = flow.Flow([reader], [writer], flow_type = REALTIME)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()    
    