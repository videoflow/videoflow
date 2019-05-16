'''
Car tracking sample here.
'''
import argparse

import numpy as np
import videoflow
import videoflow.core.flow as flow
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow.processors.vision import TensorflowObjectDetector, KalmanFilterBoundingBoxTracker, TrackerAnnotator

class BoundingBoxesFilter(videoflow.core.node.ProcessorNode):
    def __init__(self, class_indexes_to_keep):
        self._class_indexes_to_keep = class_indexes_to_keep
        super(BoundingBoxesFilter, self).__init__()
    
    def process(self, dets):
        '''
        Keeps only the boxes with the class indexes
        specified in self._class_indexes_to_keep

        - Arguments:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [xmin, ymin, xmax, ymax, class_index, score])
        '''
        f = np.array([dets[:, 4] == a for a in self._class_indexes_to_keep])
        f = np.any(f, axis = 0)
        filtered = dets[f]
        return filtered

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type = str, required = True)
    parser.add_argument('--output_file', type = str, required = True)
    parser.add_argument('--tensorflow_model_path', type = str, default = '/Users/dearj019/Downloads/faster_rcnn_resnet101_coco_2018_01_28/frozen_inference_graph.pb')
    parser.add_argument('--class_labels_path', type = str, default = 'mscoco_labels.pbtxt')
    parser.add_argument('--tensorflow_model_classes', type = int, default = 90)
    args = parser.parse_args()

    reader = VideofileReader(args.input_file)
    detector = TensorflowObjectDetector(args.tensorflow_model_path, args.tensorflow_model_classes)(reader)
    filter_ = BoundingBoxesFilter([3])(detector)
    tracker = KalmanFilterBoundingBoxTracker()(filter_)
    annotator = TrackerAnnotator()(reader, tracker)
    writer = VideofileWriter(args.output_file, fps = 30)(annotator)
    fl = flow.Flow([reader], [writer], flow_type = flow.BATCH)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()