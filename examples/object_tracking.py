'''
Runs a detector + tracker over a sample intersection video and writes an
annotated copy to output.avi. Requires the external ``videoflow_contrib`` package.

    python examples/object_tracking.py
'''
import numpy as np
import videoflow
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow.processors.vision.annotators import TrackerAnnotator
from videoflow.utils.downloader import get_file

BASE_URL_EXAMPLES = "https://github.com/videoflow/videoflow/releases/download/examples/"
VIDEO_NAME = "intersection.mp4"
URL_VIDEO = BASE_URL_EXAMPLES + VIDEO_NAME

class BoundingBoxesFilter(videoflow.core.node.ProcessorNode):
    def __init__(self, class_indexes_to_keep, **kwargs):
        self._class_indexes_to_keep = class_indexes_to_keep
        super(BoundingBoxesFilter, self).__init__(**kwargs)

    def process(self, dets):
        '''
        Keeps only the boxes with the class indexes specified in
        self._class_indexes_to_keep.

        - Arguments:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [xmin, ymin, xmax, ymax, class_index, score])
        '''
        f = np.array([dets[:, 4] == a for a in self._class_indexes_to_keep])
        f = np.any(f, axis = 0)
        filtered = dets[f]
        return filtered

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    def __init__(self, **kwargs):
        super(FrameIndexSplitter, self).__init__(**kwargs)

    def process(self, data):
        index, frame = data
        return frame

def build_flow():
    from videoflow_contrib.detector_tf import TensorflowObjectDetector
    from videoflow_contrib.tracker_sort import KalmanFilterBoundingBoxTracker
    input_file = get_file(VIDEO_NAME, URL_VIDEO)
    output_file = "output.avi"

    reader = VideofileReader(input_file, name = 'reader')
    frame = FrameIndexSplitter(name = 'frame')(reader)
    detector = TensorflowObjectDetector(
        num_classes = 2, architecture = 'fasterrcnn-resnet101', dataset = 'kitti',
        nb_tasks = 1, name = 'detector')(frame)
    tracker = KalmanFilterBoundingBoxTracker(name = 'tracker')(detector)
    annotator = TrackerAnnotator(name = 'annotator')(frame, tracker)
    writer = VideofileWriter(output_file, fps = 30, name = 'writer')(annotator)
    return Flow([writer], flow_type = BATCH)

if __name__ == "__main__":
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
