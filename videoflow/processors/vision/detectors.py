'''
Collection of object detection processors
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np
import tensorflow as tf

from ...core.node import ProcessorNode, CPU, GPU
from ...utils.tensorflow import TensorflowModel
from ...utils.downloader import get_file

BASE_URL_DETECTION = 'https://github.com/jadielam/videoflow/releases/download/detection/'

class ObjectDetector(ProcessorNode):
    '''
    Abstract class that defines the interface of object detectors
    '''
    def _detect(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - dets: np.array of shape (nb_boxes, 6) \
              Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
        '''
        raise NotImplementedError('Subclass must implement it')
    
    def process(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
                The box coordinates are returned unnormalized (values NOT between 0 and 1, \
                but using the original dimension of the image)
        '''
        return self._detect(im)

class TensorflowObjectDetector(ObjectDetector):
    '''
    Finds object detections by running a Tensorflow model on an image.

    Initializes the tensorflow model.  If ``path_to_pb_file`` is provided, it uses a local
    model. If not, it uses ``architecture`` and ``dataset`` parameters to download tensorflow
    pretrained models.  
    
    .. csv-table:: Models supported
        
        "Model","Speed (ms)","COCO mAP"
        "ssd-mobilenetv2_coco","30","21"
        "ssd-resnet50-fpn_coco","76","35"
        "fasterrcnn-resnet101_coco","106","32"

    - Arguments:
        - num_classes (int): number of classes that the detector can recognize.
        - path_to_pb_file (str): Path where model pb file is \
            It expects the model to have the following input tensors: ``image_tensor:0``, and \
            the following output tensors: ``detection_boxes:0``, ``detection_scores:0``, \
            ``detection_classes:0``, and ``num_detections:0``.  If no path is provided, then \
            it will download the model from the internet using the values provided for ``architecture``\
            and ``dataset``.
        - architecture (str): One of `fasterrcnn-resnet101` or `ssd-mobilenetv2`
        - dataset (str): For now, only `coco` is accepted.
        - min_score_threshold (float): detection will filter out entries with score below threshold score
    '''
    def __init__(self, 
                num_classes = 90,
                path_to_pb_file = None,
                architecture = 'ssd-resnet50-fpn',
                dataset = 'coco',
                min_score_threshold = 0.5,
                nb_tasks = 1,
                device_type = CPU):
        self._tensorflow_model = None
        self._num_classes = num_classes
        self._path_to_pb_file = path_to_pb_file
        
        if path_to_pb_file is None and (architecture is None or dataset is None):
            raise ValueError('If path_to_pb_file is None, then architecture and dataset cannot be None')

        supported_architectures = ['fasterrcnn-resnet101', 'ssd-mobilenetv2', 'ssd-resnet50-fpn']
        if architecture not in supported_architectures:
            raise ValueError('architecture is not one of {}'.format(', '.join(supported_architectures)))
        self._architecture = architecture

        supported_datasets = ['coco']
        if dataset not in supported_datasets:
            raise ValueError('dataset is not one of {}'.format(', '.join(supported_datasets)))
        self._dataset = dataset

        self._min_score_threshold = min_score_threshold
        super(TensorflowObjectDetector, self).__init__(nb_tasks = nb_tasks, device_type = device_type)
    
    def open(self):
        '''
        Creates session with tensorflow model
        '''
        if self.device_type == CPU:
            device_id = 'cpu'
        elif self.device_type == GPU:
            device_id = 'gpu'
        else:
            device_id = 'cpu'
        
        if self._path_to_pb_file is None:
            model_file_name = f'{self._architecture}_{self._dataset}.pb'
            remote_url = BASE_URL_DETECTION + model_file_name
            self._path_to_pb_file = get_file(model_file_name, remote_url)

        self._tensorflow_model = TensorflowModel(
            self._path_to_pb_file,
            ["image_tensor:0"],
            ["detection_boxes:0", "detection_scores:0", "detection_classes:0", "num_detections:0"],
            device_id = device_id
        )
    
    def close(self):
        '''
        Closes tensorflow model session.
        '''
        self._tensorflow_model._close_session()

    def _detect(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - dets: np.array of shape (nb_boxes, 6) \
                Specifically (nb_boxes, [ymin, xmin, ymax, xmax, class_index, score])
        '''
        h, w, _ = im.shape
        im_expanded = np.expand_dims(im, axis = 0)
        boxes, scores, classes, num = self._tensorflow_model.run_on_input(im_expanded)
        boxes, scores, classes = np.squeeze(boxes, axis = 0), np.squeeze(scores, axis = 0), np.squeeze(classes, axis = 0)
        
        # boxes denormalization
        boxes[:,[0, 2]] = boxes[:,[0, 2]] * h
        boxes[:,[1, 3]] = boxes[:,[1, 3]] * w

        indexes = np.where(scores > self._min_score_threshold)[0]
        boxes, scores, classes = boxes[indexes], scores[indexes], classes[indexes]
        scores, classes = np.expand_dims(scores, axis = 1), np.expand_dims(classes, axis = 1)
        return np.concatenate((boxes, classes, scores), axis = 1)
