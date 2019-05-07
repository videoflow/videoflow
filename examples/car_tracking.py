'''
Car tracking sample here.
'''

import videoflow.core.flow
from videoflow.consumers import VideoWriter
from videoflow.producers import VideoReader
from videoflow.processors.vision import 

flow = videoflow.core.flow.Flow([reader], [writer], flow_type = videoflow.core.flow.REALTIME)
flow.run()
flow.join()