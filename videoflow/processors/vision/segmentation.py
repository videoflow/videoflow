from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np

from ...core.node import ProcessorNode
from ...core.constants import CPU, GPU
from ...utils.tensorflow import TensorflowModel
from ...utils.downloader import get_file

import tensorflow as tf

BASE_URL_SEGMENTATION = 'https://github.com/videoflow/videoflow/releases/download/segmentation/'

class Segmenter(ProcessorNode):
    '''
    Abstract class that defines the interface to do image
    segmentation in images
    '''
    def _segment(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - mask: np.array of shape (h, w, num_classes)
        '''
        raise NotImplementedError('Subclass must implement it')

    def process(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - mask: np.array of shape (h, w, num_classes)
        '''
        return self._segment(im)

class TensorflowSegmenter(Segmenter):
    '''
    Finds masks by running a Tensorflow model on an image.

    Initializes the tensorflow model.  If ``path_to_pb_file`` is provided, it uses a local
    model. If not, it uses ``architecture`` and ``dataset`` parameters to download tensorflow
    pretrained models.  

    .. csv-table:: Models supported COCO dataset

        "Model","Speed (ms)","COCO mAP"
        "maskrcnn-resnet101_coco","470","33"
        "maskrcnn-inceptionv2_coco","79","25"


    - Arguments:
        - num_classes (int): The number of classes that the segmenter can recognize.
        - path_to_pb_file (str): Path where model pb file is \
            It expects the model to have the following input tensors: ``image_tensor:0``, and \
            the following output tensors: ``detection_boxes:0``, ``detection_scores:0``, \
            ``detection_classes:0``, ``num_detections:0`` and ``detection_masks:0``.  If no path is provided, then \
            it will download the model from the internet using the values provided for ``architecture``\
            and ``dataset``.
        - architecture (str): One of the architectures mentioned in the tables above is accepted.
        - dataset (str): For now, only `coco` is accepted.
        - min_score_threshold (float): detection will filter out entries with score below threshold score
    '''
    supported_models = [
        "maskrcnn-resnet101_coco",
        "maskrcnn-inceptionv2_coco"
    ]

    def __init__(self, 
                num_classes = 90,
                path_to_pb_file = None,
                architecture = 'maskrcnn-inceptionv2',
                dataset = 'coco',
                min_score_threshold = 0.5,
                nb_tasks = 1,
                device_type = GPU):
        self._tensorflow_model = None
        self._num_classes = num_classes
        self._path_to_pb_file = path_to_pb_file
        
        if path_to_pb_file is None and (architecture is None or dataset is None):
            raise ValueError('If path_to_pb_file is None, then architecture and dataset cannot be None')

        if path_to_pb_file is None:
            remote_model_id = f'{architecture}_{dataset}'
            if remote_model_id not in self.supported_models:
                raise ValueError('model is not one of supported models: {}'.format(', '.join(self.supported_models)))        
            self._remote_model_file_name = f'{architecture}_{dataset}.pb'

        self._min_score_threshold = min_score_threshold
        super(TensorflowSegmenter, self).__init__(nb_tasks = nb_tasks, device_type = device_type)
    
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
            remote_url = BASE_URL_SEGMENTATION + self._remote_model_file_name
            self._path_to_pb_file = get_file(self._remote_model_file_name, remote_url)
        
        self._tensorflow_model = TensorflowModel(
            self._path_to_pb_file,
            ["image_tensor:0"],
            ["detection_boxes:0", "detection_scores:0", "detection_classes:0", 
                "num_detections:0", "detection_masks:0"],
            device_id = device_id
        )
    
    def close(self):
        '''
        Closes tensorflow model session.
        '''
        self._tensorflow_model._close_session()
    
    def _segment(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - masks: np.array of shape (h, w, num_classes)
            - classes:
            - scores:
        '''
        h, w, _ = im.shape
        im_expanded = np.expand_dims(im, axis = 0)
        boxes, scores, classes, nb_detections, masks = self._tensorflow_model.run_on_input(im_expanded)
        boxes, scores, classes, masks = np.squeeze(boxes, axis = 0), np.squeeze(scores, axis = 0), np.squeeze(classes, axis = 0), np.squeeze(masks, axis = 0)
        
        indexes = np.where(scores > self._min_score_threshold)[0]
        boxes, scores, classes, masks = boxes[indexes], scores[indexes], classes[indexes], masks[indexes]
        print(boxes)
        print(scores)
        print(classes)
        print(masks)
        scores, classes = np.expand_dims(scores, axis = 1), np.expand_dims(classes, axis = 1)
        return np.array(masks), np.array(classes), np.array(scores)



        
