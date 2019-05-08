'''
Car tracking sample here.
'''
import sys

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
    
    def filter_boxes(self, dets):
        '''
        Keeps only the boxes with the class indexes
        specified in self._class_indexes_to_keep

        - Arguments:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [xmin, ymin, xmax, ymax, class_index, score])
        '''
        f = np.array([dets[:, 4] == a for a in self._class_indexes_to_keeep])
        f = np.any(f, axis = 0)
        filtered = dets[f]
        return filtered

def main():
    input_file = "cars_in.mp4"
    output_file = "cars_out.avi"

    reader = VideofileReader(input_file)
    detector = TensorflowObjectDetector("/Users/dearj019/Downloads/ssd_mobilenet_v2_coco_2018_03_29/frozen_inference_graph.pb", num_classes = 90)(reader)
    filter_ = BoundingBoxesFilter([2])(detector)
    tracker = KalmanFilterBoundingBoxTracker()(filter_)
    annotator = TrackerAnnotator()(reader, tracker)
    writer = VideofileWriter(output_file, fps = 30)
    fl = flow.Flow([reader], [writer], flow_type = flow.REALTIME)
    fl.run()
    fl.join()

if __name__ == "__main__":
    main()