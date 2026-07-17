Object tracking sample application
==================================

This recipe detects and tracks vehicles in a video and writes an annotated copy to
``output.avi``. It uses the external
`videoflow-contrib <https://github.com/videoflow/videoflow-contrib>`_ package for the
TensorFlow detector and the SORT tracker. See the full script in
`examples/object_tracking.py <https://github.com/videoflow/videoflow/blob/master/examples/object_tracking.py>`_.

::

    import numpy as np
    import videoflow
    from videoflow.core import Flow
    from videoflow.core.constants import BATCH
    from videoflow.consumers import VideofileWriter
    from videoflow.producers import VideofileReader
    from videoflow.processors.vision.annotators import TrackerAnnotator
    from videoflow.utils.downloader import get_file

    URL_VIDEO = "https://github.com/videoflow/videoflow/releases/download/examples/intersection.mp4"

    class BoundingBoxesFilter(videoflow.core.node.ProcessorNode):
        def __init__(self, class_indexes_to_keep, **kwargs):
            self._class_indexes_to_keep = class_indexes_to_keep
            super().__init__(**kwargs)

        def process(self, dets):
            f = np.array([dets[:, 4] == a for a in self._class_indexes_to_keep])
            f = np.any(f, axis=0)
            return dets[f]

    class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

        def process(self, data):
            index, frame = data
            return frame

    def build_flow():
        from videoflow_contrib.detector_tf import TensorflowObjectDetector
        from videoflow_contrib.tracker_sort import KalmanFilterBoundingBoxTracker

        input_file = get_file("intersection.mp4", URL_VIDEO)
        reader    = VideofileReader(input_file, name='reader')
        frame     = FrameIndexSplitter(name='frame')(reader)
        detector  = TensorflowObjectDetector(name='detector')(frame)
        # keep only vehicle classes
        filtered  = BoundingBoxesFilter([1, 2, 3, 4, 6, 8, 10, 13], name='filter')(detector)
        tracker   = KalmanFilterBoundingBoxTracker(name='tracker')(filtered)
        annotator = TrackerAnnotator(name='annotator')(frame, tracker)
        writer    = VideofileWriter("output.avi", fps=30, name='writer')(annotator)
        return Flow([writer], flow_type=BATCH)

    if __name__ == "__main__":
        from videoflow.engines.local import LocalProcessEngine
        flow = build_flow()
        flow.run(LocalProcessEngine())
        flow.join()

Walking through the graph:

- ``reader`` reads the video file frame by frame (as ``(index, frame)`` tuples), and
  ``frame`` strips the index off. Note the reader **fans out**: both the detector
  branch and the final annotator consume ``frame``.
- ``detector`` runs a model and emits bounding boxes; ``filter`` (a custom node)
  keeps only vehicle classes; ``tracker`` assigns a stable id to each box across
  frames.
- ``annotator`` is a **join**: it receives both the original ``frame`` and the
  ``tracker`` boxes, and draws the boxes onto the frame. ``writer`` encodes the
  annotated frames into a new video.

Two things to note for a real deployment:

- The whole flow uses ``BATCH`` mode. A video **file** must never run in
  ``REALTIME`` mode — the reader emits frames far faster than the detector can
  process them, and realtime's drop policy would discard most of the video. See
  :doc:`../user-documentation/batch-versus-realtime-mode`.
- The ``tracker`` is stateful (it correlates boxes across frames), so it subclasses
  ``OneTaskProcessorNode`` and always runs as a single worker. The ``detector``,
  being stateless, can be scaled with ``nb_tasks`` to keep up with the reader.

To run this on Kubernetes instead, build an image with your detector/tracker
dependencies (``FROM videoflow-base``) and deploy the same factory, pointing every
node at it::

    videoflow deploy examples/object_tracking.py:build_flow \
        --nats nats://nats.videoflow.svc:4222 --namespace videoflow \
        --image ghcr.io/acme/tracking:v1

See :doc:`../distributed/deploying-to-kubernetes`.
