'''
Runs an object detector over a sample intersection video and writes an annotated
copy to output.avi. Requires the external ``videoflow_contrib`` package for the
TensorFlow detector backend.

Local run (needs a NATS server):

    python examples/object_detector.py

Deploy to Kubernetes (detector runs in the vision image, reader/writer in video-io):

    videoflow deploy examples/object_detector.py:build_flow --nats nats://nats:4222 --namespace videoflow
'''
import videoflow
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.consumers import VideofileWriter
from videoflow.producers import VideofileReader
from videoflow.processors.vision.annotators import BoundingBoxAnnotator
from videoflow.utils.downloader import get_file

BASE_URL_EXAMPLES = "https://github.com/videoflow/videoflow/releases/download/examples/"
VIDEO_NAME = 'intersection.mp4'
URL_VIDEO = BASE_URL_EXAMPLES + VIDEO_NAME

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    '''Drops the frame index from a (index, frame) producer tuple, keeping the frame.'''
    def __init__(self, **kwargs):
        super(FrameIndexSplitter, self).__init__(**kwargs)

    def process(self, data):
        index, frame = data
        return frame

def build_flow():
    from videoflow_contrib.detector_tf import TensorflowObjectDetector
    input_file = get_file(VIDEO_NAME, URL_VIDEO)
    output_file = "output.avi"
    reader = VideofileReader(input_file, name = 'reader')
    frame = FrameIndexSplitter(name = 'frame')(reader)
    detector = TensorflowObjectDetector(name = 'detector')(frame)
    annotator = BoundingBoxAnnotator(name = 'annotator')(frame, detector)
    writer = VideofileWriter(output_file, fps = 30, name = 'writer')(annotator)
    return Flow([writer], flow_type = BATCH)

if __name__ == "__main__":
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
