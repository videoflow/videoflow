'''
Car tracking sample here.
'''
import argparse

import numpy as np
import videoflow
import videoflow.core.flow as flow
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow.processors.vision import TensorflowObjectDetector, BoundingBoxAnnotator

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type = str, required = True)
    parser.add_argument('--output_file', type = str, required = True)
    args = parser.parse_args()

    reader = VideofileReader(args.input_file, 15)
    detector = TensorflowObjectDetector()(reader)
    annotator = BoundingBoxAnnotator()(reader, detector)
    writer = VideofileWriter(args.output_file, fps = 30)(annotator)
    fl = flow.Flow([reader], [writer], flow_type = flow.BATCH)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()