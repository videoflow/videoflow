'''
Will download a sample video file of an 
intersection, and will run the detector on
it.  Will output annotated video to output.avi
'''

import videoflow
import videoflow.core.flow as flow
from videoflow.core.constants import BATCH
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow_contrib.detector_tf import TensorflowObjectDetector
from videoflow.processors.vision.annotators import BoundingBoxAnnotator
from videoflow.utils.downloader import get_file

BASE_URL_EXAMPLES = "https://github.com/videoflow/videoflow/releases/download/examples/"
VIDEO_NAME = 'intersection.mp4'
URL_VIDEO = BASE_URL_EXAMPLES + VIDEO_NAME

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    def __init__(self):
        super(FrameIndexSplitter, self).__init__()
    
    def process(self, data):
        index, frame = data
        return frame

def main():
    input_file = get_file(
        VIDEO_NAME, 
        URL_VIDEO)
    output_file = "output.avi"
    reader = VideofileReader(input_file)
    frame = FrameIndexSplitter()(reader)
    detector = TensorflowObjectDetector()(frame)
    annotator = BoundingBoxAnnotator()(frame, detector)
    writer = VideofileWriter(output_file, fps = 30)(annotator)
    fl = flow.Flow([reader], [writer], flow_type = BATCH)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()