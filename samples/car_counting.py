'''
An idea of how a programmer should write a streaming program
'''

from flujo.core import Stream
from flujo.producers import VideoReader
from flujo.processors.vision import Detector, Tracker, Counter, ImageAnnotator
from flujo.consumers import StreamingServer, EndpointPublisher

video_reader = VideoReader()
detector = Detector()(video_reader)
tracker = Tracker()(detector)
counter = Counter()(tracker)
video_annotator = ImageAnnotator()(video_reader, detector, tracker, counter)
stream_server = StreamingServer()(video_annotator)
results_publisher = EndpointPublisher()(counter)

stream = Stream(consumers = [video_annotator, results_publisher])
stream_handler = stream.run()  # So that the run method is non-blocking