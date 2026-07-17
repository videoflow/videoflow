import logging

from .constants import LOGGING_LEVEL
from .flow import Flow
from .node import ConsumerNode, FunctionProcessorNode, Node, ProcessorNode, ProducerNode
from .task import ConsumerTask, ProcessorTask, ProducerTask, Task

logger = logging.getLogger(__package__)
logger.setLevel(LOGGING_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
