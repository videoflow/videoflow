'''
Will download a sample video file of an 
intersection, and will run the detector on
it.  Will output annotated video to output.avi
'''

import numpy as np
import videoflow
import videoflow.core.flow as flow
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow.processors.vision import TensorflowObjectDetector, BoundingBoxAnnotator
from videoflow.utils.downloader import get_file


BASE_URL_EXAMPLES = "https://github.com/jadielam/videoflow/releases/download/examples/"
VIDEO_NAME = 'intersection.mp4'
URL_VIDEO = BASE_URL_EXAMPLES + VIDEO_NAME

def main():
    input_file = get_file(
        VIDEO_NAME, 
        URL_VIDEO)
    output_file = "output.avi"
    reader = VideofileReader(input_file, 15)
    detector = TensorflowObjectDetector()(reader)
    annotator = BoundingBoxAnnotator()(reader, detector)
    writer = VideofileWriter(output_file, fps = 30)(annotator)
    fl = flow.Flow([reader], [writer], flow_type = flow.BATCH)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()