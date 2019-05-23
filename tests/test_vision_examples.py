import pytest

import numpy as np
import videoflow
import videoflow.core.flow as flow
from videoflow.core.constants import BATCH
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow.processors.vision import TensorflowObjectDetector, BoundingBoxAnnotator, \
    KalmanFilterBoundingBoxTracker, TrackerAnnotator
from videoflow.utils.downloader import get_file


BASE_URL_EXAMPLES = "https://github.com/videoflow/videoflow/releases/download/examples/"
VIDEO_NAME = 'intersection.mp4'
URL_VIDEO = BASE_URL_EXAMPLES + VIDEO_NAME

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

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    def __init__(self):
        super(FrameIndexSplitter, self).__init__()
    
    def process(self, data):
        index, frame = data
        return frame

@pytest.mark.timeout(120)
def test_object_detector():
    input_file = get_file(
        VIDEO_NAME, 
        URL_VIDEO)
    output_file = "output.avi"
    reader = VideofileReader(input_file, 2)
    frame = FrameIndexSplitter()(reader)
    detector = TensorflowObjectDetector()(frame)
    annotator = BoundingBoxAnnotator()(frame, detector)
    writer = VideofileWriter(output_file, fps = 30)(annotator)
    fl = flow.Flow([reader], [writer], flow_type = BATCH)
    fl.run()
    fl.join()

@pytest.mark.timeout(120)
def test_object_tracker():
    input_file = get_file(
        VIDEO_NAME, 
        URL_VIDEO
        )
    output_file = "output.avi"

    reader = VideofileReader(input_file, 2)
    frame = FrameIndexSplitter()(reader)
    detector = TensorflowObjectDetector()(frame)
    filter_ = BoundingBoxesFilter([1, 2, 3, 4, 6, 8, 10, 13])(detector)
    tracker = KalmanFilterBoundingBoxTracker()(filter_)
    annotator = TrackerAnnotator()(frame, tracker)
    writer = VideofileWriter(output_file, fps = 30)(annotator)
    fl = flow.Flow([reader], [writer], flow_type = BATCH)
    fl.run()
    fl.join()

if __name__ == "__main__":
    pytest.main([__file__])

