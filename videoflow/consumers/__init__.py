'''
This package contains a collection of consumers that implement the ``videoflow.core.node.ConsumerNode`` \
    interface
'''
from .basic import CommandlineConsumer, VoidConsumer
from .video import VideofileWriter