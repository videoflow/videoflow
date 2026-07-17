'''
Time-synchronized fusion of several independent camera streams plus a high-rate
sensor — the shape of a multi-view 3D-reconstruction / offside pipeline.

Unlike a diamond join (which groups by shared upstream lineage), the cameras and
the sensor here are *independent* producers with no common ancestor, so the fusion
node joins them by **event time**: frames whose capture timestamps fall within a
few milliseconds of each other are one synchronized moment. The ball sensor runs
much faster than the cameras, so it is a ``collect`` parent — every sample near the
frame time is delivered as a list for interpolation.

Run against N camera video files (they should be recordings of the same moment):

    python examples/multicamera_time_sync.py cam0.mp4 cam1.mp4 cam2.mp4 cam3.mp4

Each per-camera branch would normally run pose/keypoint detection before fusion;
here that stage is stubbed so the example needs no models — the point is the
time-aligned join.
'''
import sys
import time

import numpy as np

import videoflow
from videoflow.core import Flow
from videoflow.core.constants import REALTIME
from videoflow.core.node import ProducerNode, ProcessorNode
from videoflow.core.policies import JoinPolicy
from videoflow.consumers import CommandlineConsumer
from videoflow.producers import VideofileReader


class KeypointStub(ProcessorNode):
    '''
    Stands in for a per-camera pose/keypoint detector. Emits only compact features
    (not the frame), so only small payloads cross the broker into the fusion node —
    the pattern you want with many high-resolution cameras.
    '''
    def process(self, frame_tuple):
        frame_idx, frame = frame_tuple
        h, w = frame.shape[:2]
        # A real node would return detected 2D keypoints; stub with the centroid.
        return {'frame_idx': frame_idx, 'centroid': (w / 2.0, h / 2.0)}


class BallSensorProducer(ProducerNode):
    '''
    A high-rate sensor (e.g. an in-ball IMU at ~500Hz) as an independent producer.
    Stamps each sample with its own event time so the fusion node can align it with
    the camera frames.
    '''
    def __init__(self, hz = 500.0, **kwargs):
        self._hz = hz
        self._t0 = None
        self._n = 0
        super().__init__(is_finite = False, **kwargs)

    def open(self):
        self._t0 = time.time()

    def next(self, ctx = None):
        t = self._t0 + self._n / self._hz
        # Pace to roughly real time so the stream interleaves with the cameras.
        now = time.time()
        if t > now:
            time.sleep(t - now)
        self._n += 1
        if ctx is not None:
            ctx.set_event_timestamp(t)
        return {'sample': self._n, 'accel': float(np.sin(self._n * 0.1))}


class OffsideFusion(ProcessorNode):
    '''
    Fuses the synchronized per-camera features and the collected ball samples for
    one moment. Reads each input's exact event time from ``ctx.input_info`` to know
    how to interpolate the ball position onto the frame time.
    '''
    def __init__(self, camera_names, sensor_name, **kwargs):
        self._camera_names = list(camera_names)
        self._sensor_name = sensor_name
        super().__init__(**kwargs)

    def process(self, *inputs, ctx = None):
        info = ctx.input_info if ctx is not None else {}
        views = sum(1 for name in self._camera_names
                    if info.get(name) is not None)
        sensor = info.get(self._sensor_name)
        nb_samples = len(sensor['event_ts']) if sensor else 0
        times = [info[n]['event_ts'] for n in self._camera_names
                if info.get(n) is not None and info[n]['event_ts'] is not None]
        spread_ms = (max(times) - min(times)) * 1000 if len(times) > 1 else 0.0
        return (f'moment: {views}/{len(self._camera_names)} camera views '
                f'(spread {spread_ms:.1f}ms) + {nb_samples} ball samples')


def build_flow():
    camera_files = sys.argv[1:] or ['cam0.mp4', 'cam1.mp4']
    camera_names = [f'cam{i}' for i in range(len(camera_files))]

    keypoint_nodes = []
    for name, path in zip(camera_names, camera_files):
        # timestamp_source='position' aligns on the recordings' shared timeline.
        reader = VideofileReader(path, timestamp_source = 'position', name = f'reader-{name}')
        keypoint_nodes.append(KeypointStub(name = name)(reader))

    sensor = BallSensorProducer(name = 'ball')

    fusion = OffsideFusion(
        camera_names = camera_names,
        sensor_name = 'ball',
        name = 'fusion',
        join_policy = JoinPolicy(
            mode = 'time',
            tolerance_ms = 8,          # < one frame period; same "moment"
            timeout_seconds = 0.1,     # lateness bound before emitting a quorum group
            quorum = max(1, len(camera_names) - 1),   # tolerate one missing view
            collect = {'ball': 25},    # gather ~ a few IMU samples per frame set
        ),
    )(*keypoint_nodes, sensor)

    printer = CommandlineConsumer(name = 'printer')(fusion)
    # REALTIME: a straggler frame must not stall live offside decisions.
    return Flow([printer], flow_type = REALTIME)


if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
