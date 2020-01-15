Object Tracking Sample Application
==================================

See below an example of an object tracking sample application::

    import numpy as np
    import videoflow
    import videoflow.core.flow as flow
    from videoflow.core.constants import BATCH
    from videoflow.consumers import VideofileWriter
    from videoflow.producers import VideofileReader
    from videoflow_contrib.detector_tf import TensorflowObjectDetector
    from videoflow_contrib.tracker_sort import KalmanFilterBoundingBoxTracker
    from videoflow.processors.vision.annotators import TrackerAnnotator
    from videoflow.utils.downloader import get_file

    BASE_URL_EXAMPLES = "https://github.com/videoflow/videoflow/releases/download/examples/"
    VIDEO_NAME = "intersection.mp4"
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

    def main():
        input_file = get_file(VIDEO_NAME, URL_VIDEO)
        output_file = "output.avi"

        reader = VideofileReader(input_file)
        frame = FrameIndexSplitter()(reader)
        detector = TensorflowObjectDetector()(frame)
        # keeps only automobile classes: autos, buses, cycles, etc.
        filter_ = BoundingBoxesFilter([1, 2, 3, 4, 6, 8, 10, 13])(detector)
        tracker = KalmanFilterBoundingBoxTracker()(filter_)
        annotator = TrackerAnnotator()(frame, tracker)
        writer = VideofileWriter(output_file, fps = 30)(annotator)
        fl = flow.Flow([reader], [writer], flow_type = BATCH)
        fl.run()
        fl.join()

    if __name__ == "__main__":
        main()

The source code of the flow is very simple.  It first downloads from the internet a sample fake video of cars 
crossing an interception.  The consumer in this case is the ``reader`` that will read the file, frame
by frame from the filesystem. 

Each time a frame is read, it is passed to the ``detector``, which
in this case is a ``TensorflowObjectDetector``.  If no path to a local file is given,
the ``TensorflowObjectDetector`` object will download a default pretrained model on the COCO dataset
from the internet and use it.  

The ``detector`` passes its bounding boxes output to the ``filter``.  Notice that the filter is a
user defined processor node. It only keeps objects that are either persons, bicycles, cars, motorcycles,
buses, trucks, traffic lights or stop signs.  It passes the filtered bounding boxes to the ``tracker``. 
The ``tracker``'s job is to keep 'track' of objects across a sequence frames.  
The output of the tracker is the bounding boxes with an id assigned to each of them. 

Notice how the ``annotator`` receives as input both the picture from the ``reader``, and the bounding 
boxes from the ``tracker``. Its job is to draw those bounding boxes in the picture, which in terms 
it passes to the ``writer`` that creates a new video in the filesystem with such annotations.

.. warning:: ``VideofileReader`` should not be used in a ``REALTIME`` setting.
    The reason is that is likely that subsequent processors (such as object detectors) 
    in the flow will not be able to keep up with the pace of the VideofileReader.
    The default behaviour of the execution engine in ``REALTIME`` mode is to 
    drop the excess frames.  Use ``REALTIME`` for when you need to process 
    something in real time, such as when reading video from a realtime video stream.
    For a more complete explanation, see the **Batch versus realtime mode** section.
