'''
Reads from an RTSP stream (an infinite producer) and writes frames to output.avi
in REALTIME mode (stale frames are dropped rather than queued). Configure the
stream via env vars so the graph module can be imported by ``videoflow deploy``
without command-line args:

    VF_STREAM_ADDRESS=10.23.232.43 VF_STREAM_USER=admin VF_STREAM_PASS=secret \\
        python examples/reading_livestream.py
'''
import os

import videoflow
from videoflow.core import Flow
from videoflow.core.constants import REALTIME
from videoflow.producers import VideoUrlReader
from videoflow.consumers import VideofileWriter

class FrameIndexSplitter(videoflow.core.node.ProcessorNode):
    def __init__(self, **kwargs):
        super(FrameIndexSplitter, self).__init__(**kwargs)

    def process(self, data):
        index, frame = data
        return frame

def build_flow():
    stream_address = os.environ['VF_STREAM_ADDRESS']
    username = os.environ['VF_STREAM_USER']
    password = os.environ['VF_STREAM_PASS']
    output_file = os.environ.get('VF_OUTPUT_FILE', 'output.avi')

    stream_url = f'rtsp://{username}:{password}@{stream_address}'
    reader = VideoUrlReader(stream_url, name = 'reader')
    frame = FrameIndexSplitter(name = 'frame')(reader)
    writer = VideofileWriter(output_file, fps = 30, name = 'writer')(frame)
    return Flow([writer], flow_type = REALTIME)

if __name__ == "__main__":
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
