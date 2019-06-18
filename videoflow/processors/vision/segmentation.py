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

def reframe_box_masks_to_image_masks(box_masks, boxes, h, w):
    '''
    Transforms the box masks back to full image masks

    - Arguments:
        - box_masks: tf.float32 tensor of size [nb_masks, height, width],
        - boxes: tf.float32 tensor of size [nb_masks, 4] containing box \
            coordinates [ymin, xmin, ymax, xmax].  Note that the coordinates \
            are normalized
        - h: Image height. The output masks will have height h.
        - w: Image width. The output masks will have widht w.
    
    - Returns:
        - image_masks: tf.float32 tnesor of size [nb_masks, h, w]
    '''
    def reframe_box_masks_to_image_masks_default():
        '''
        The default function when there are more than 0 box masks
        '''
        def transform_boxes_relative_to_boxes(boxes, reference_boxes):
            boxes = tf.reshape(boxes, [-1, 2, 2])
            min_corner = tf.expand_dims(reference_boxes[:, 0:2], 1)
            max_corner = tf.expand_dims(reference_boxes[:, 2:4], 1)
            transformed_boxes = (boxes - min_corner) / (max_corner - min_corner)
            return tf.reshape(transformed_boxes, [-1, 4])
        
        box_masks_expanded = tf.expand_dims(box_masks, axis = 3)
        num_boxes = tf.shape(box_masks_expanded)[0]
        unit_boxes = tf.concat(
            [
                tf.zeros([num_boxes, 2]),
                tf.ones([num_boxes, 2])
            ],
            axis = 1
        )
        reverse_boxes = transform_boxes_relative_to_boxes(unit_boxes, boxes)
        return tf.image.crop_and_resize(
            image = box_masks_expanded,
            boxes = reverse_boxes,
            box_ind = tf.range(num_boxes),
            crop_size = [h, w],
            extrapolation_value = 0.0
        )
    
    image_masks = tf.cond(
        tf.shape(box_masks)[0] > 0,
        reframe_box_masks_to_image_masks_default,
        lambda: tf.zeros([0, h, w, 1], dtype = tf.float32)
    )
    return tf.squeeze(image_masks, axis = 3)

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
            - masks: np.array of shape (nb_masks, h, w)
            - classes: np.array of shape (nb_masks,)
            - scores: np.array of shape (nb_masks,)
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
        
        '''
        self._tensorflow_model = TensorflowModel(
            self._path_to_pb_file,
            ["image_tensor:0"],
            ["detection_boxes:0", "detection_scores:0", "detection_classes:0", 
                "num_detections:0", "detection_masks:0"],
            device_id = device_id
        )
        '''
        # TODO: Will work on this later
    
    def close(self):
        '''
        Closes tensorflow model session.
        '''
        # TODO: Write code to close model session
        #self._tensorflow_model._close_session()
    
    def _segment(self, im : np.array) -> np.array:
        '''
        - Arguments:
            - im (np.array): (h, w, 3)
        
        - Returns:
            - masks: np.array of shape (nb_masks, h, w)
            - classes: np.array of shape (nb_masks,)
            - scores: np.array of shape (nb_masks,)
        '''
        # TODO: Work on this code later on.
        # Will be modified very much.
        h, w, _ = im.shape
        im_expanded = np.expand_dims(im, axis = 0)
        boxes, scores, classes, nb_detections, masks = self._tensorflow_model.run_on_input(im_expanded)
        boxes, scores, classes, masks = np.squeeze(boxes, axis = 0), np.squeeze(scores, axis = 0), np.squeeze(classes, axis = 0), np.squeeze(masks, axis = 0)
        
        indexes = np.where(scores > self._min_score_threshold)[0]
        boxes, scores, classes, masks = boxes[indexes], scores[indexes], classes[indexes], masks[indexes]
        
        # 1. Create new masks' array to return
        new_masks = np.zeros((h, w, boxes.shape[0]), dtype = masks.dtype)

        # 2. Resize each mask to the value that it needs to have

        # 3. Place the mask on its location on the array.

        # 4. Convert masks into 0-1 arrays
        
        scores, classes = np.expand_dims(scores, axis = 1), np.expand_dims(classes, axis = 1)
        return new_masks, np.array(classes), np.array(scores)
