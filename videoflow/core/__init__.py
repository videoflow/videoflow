from .task import Task, ConsumerTask, ProcessorTask, ProducerTask
from .node import Node, ConsumerNode, ProcessorNode, ProducerNode, FunctionProcessorNode
from .flow import Flow
from .constants import LOGGING_LEVEL

import logging
logger = logging.getLogger(__package__)
logger.setLevel(LOGGING_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
