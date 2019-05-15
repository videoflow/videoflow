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
    parser.add_argument('--tensorflow_model_path', type = str, default = '/Users/dearj019/Downloads/ssd_mobilenet_v2_coco_2018_03_29/frozen_inference_graph.pb')
    parser.add_argument('--class_labels_path', type = str, default = 'mscoco_labels.pbtxt')
    parser.add_argument('--tensorflow_model_classes', type = int, default = 90)
    args = parser.parse_args()

    reader = VideofileReader(args.input_file, 15)
    detector = TensorflowObjectDetector(args.tensorflow_model_path, args.tensorflow_model_classes)(reader)
    annotator = BoundingBoxAnnotator(args.class_labels_path)(reader, detector)
    writer = VideofileWriter(args.output_file, fps = 30)(annotator)
    fl = flow.Flow([reader], [writer], flow_type = flow.BATCH)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()