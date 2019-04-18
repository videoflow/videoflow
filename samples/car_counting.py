'''
An idea of how a programmer should write a streaming program
'''

from videoflow.core import Flow
from videoflow.producers import VideoReader
from videoflow.processors.vision import Detector, Tracker, Counter, ImageAnnotator
from videoflow.consumers import StreamingServer, EndpointPublisher

video_reader = VideoReader()
detector = Detector()
tracker = Tracker()(detector)
counter = Counter()(tracker)
video_annotator = ImageAnnotator()(video_reader, detector, tracker, counter)
stream_server = StreamingServer()(video_annotator)
results_publisher = EndpointPublisher()(counter)

flow = flow(consumers = [video_annotator, results_publisher])
flow.start()  # So that the run method is non-blocking

# If you want to stop it later on, you can:
flow.stop()

detecto