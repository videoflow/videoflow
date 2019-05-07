'''
Car tracking sample here.
'''
import sys

import videoflow.core.flow
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow.processors.vision import TensorflowObjectDetector, KalmanFilterBoundingBoxTracker, TrackerAnnotator

def main():
    input_file = "cars_in.mp4"
    output_file = "cars_out.avi"

    reader = VideofileReader(input_file)
    detector = TensorflowObjectDetector()(reader)
    tracker = KalmanFilterBoundingBoxTracker()(detector)
    annotator = TrackerAnnotator()(reader, tracker)
    writer = VideofileWriter(output_file, fps = 30)
    flow = videoflow.core.flow.Flow([reader], [writer], flow_type = videoflow.core.flow.REALTIME)
    flow.run()
    flow.join()

if __name__ == "__main__":
    main()