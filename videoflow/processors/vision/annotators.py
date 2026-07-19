from __future__ import absolute_import, division, print_function

from typing import Any

import cv2
import numpy as np

from ...core.node import ProcessorNode
from ...utils.downloader import get_file
from ...utils.parsers import parse_label_map
from .detectors import BASE_URL_DETECTION


class ImageAnnotator(ProcessorNode):
    '''
    Interface for all image annotators.
    All image annotators receive as input an image and annotation
    metadata, and return as output a copy of the image with
    the drawings representing the metadata.
    '''
    def __init__(self, nb_tasks : int = 1, **kwargs) -> None:
        super(ImageAnnotator, self).__init__(nb_tasks = nb_tasks, **kwargs)

    def _annotate(self, im : np.ndarray, annotations : Any) -> np.ndarray:
        raise NotImplementedError('Subclass must implement this method')

    # override: one positional arg per parent — the by-parent input contract,
    # not LSP substitutability. See [tool.mypy] disable/enable notes in pyproject.
    def process(self, im : np.ndarray, annotations : Any) -> np.ndarray:   # type: ignore[override]
        '''
        Returns a copy of ``im`` visually annotated with the annotations defined in `annotations`
        '''
        to_annotate = np.array(im)
        return self._annotate(to_annotate, annotations)

class BoundingBoxAnnotator(ImageAnnotator):
    '''
    Draws bounding boxes on images.

    - Arguments:
        - class_labels_path: path to pbtxt file that defines the labels indexes
        - class_labels_dataset: If class_labels_path is None, then we use this attribute to \
            download the file from the releases folder.  Currently supported datasets \
            are: ``coco``, ``oidv4``, ``pascal`` and ``kitti``.
        - box_color: color to use to draw the boxes
        - box_thickness: thickness of boxes to draw
        - text_color: color of text to draw
    '''
    supported_datasets = [
        'coco',
        'oidv4',
        'pascal',
        'kitti',
        'faces'
    ]

    def __init__(self, class_labels_path : str | None = None, class_labels_dataset : str | None = 'coco',
                box_color : tuple = (255, 225, 0), box_thickness : int = 2,
                text_color : tuple = (255, 255, 0), nb_tasks : int = 1,
                **kwargs) -> None:
        self._box_color = box_color
        self._text_color = text_color
        self._box_thickness = box_thickness
        self._class_labels_dataset = class_labels_dataset

        if class_labels_path is None and class_labels_dataset is None:
            raise ValueError('If class_labels_path is None, then class_labels_dataset cannot be None')

        if class_labels_path is None:
            if class_labels_dataset not in self.supported_datasets:
                raise ValueError('dataset is not one of supported datasets: {}'.format(', '.join(self.supported_datasets)))
            labels_file_name = f'labels_{class_labels_dataset}.pbtxt'
            remote_url = BASE_URL_DETECTION + labels_file_name
            class_labels_path = get_file(labels_file_name, remote_url)

        self._class_labels_path = class_labels_path
        self._index_label_d = parse_label_map(class_labels_path)
        super(BoundingBoxAnnotator, self).__init__(nb_tasks = nb_tasks, **kwargs)

    def _annotate(self, im : np.ndarray, boxes : np.ndarray) -> np.ndarray:
        '''
        - Arguments:
            - im: np.ndarray
            - boxes: np.ndarray of shape (nb_boxes, 6) \
                second dimension entries are [ymin, xmin, ymax, xmax, class_index, score]

        - Returns:
            - annotated_im: image with the visual annotations embedded in it.
        '''

        for i in range(len(boxes)):
            bbox = boxes[i]
            ymin, xmin, ymax, xmax = int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3])
            y_label = ymin - 15 if ymin - 15 > 15 else min(ymin + 15, ymax)
            klass_id = int(bbox[4])
            klass_text = self._index_label_d[klass_id]
            confidence = bbox[5]
            label = "{}: {:.2f}%".format(klass_text, confidence * 100)
            cv2.rectangle(im, (xmin, ymin), (xmax, ymax), self._box_color, self._box_thickness)
            cv2.putText(im, label, (xmin, y_label), cv2.FONT_HERSHEY_SIMPLEX, 0.5, self._text_color, lineType = cv2.LINE_AA)
        return im

class TrackerAnnotator(ImageAnnotator):
    '''
    Draws bounding boxes on images with track id.
    '''
    def __init__(self, box_color : tuple = (255, 225, 0), box_thickness : int = 2,
                text_color : tuple = (255, 255, 255), nb_tasks : int = 1,
                **kwargs) -> None:
        self._box_color = box_color
        self._text_color = text_color
        self._box_thickness = box_thickness
        super(TrackerAnnotator, self).__init__(nb_tasks = nb_tasks, **kwargs)

    def _annotate(self, im : np.ndarray, boxes : np.ndarray) -> np.ndarray:
        '''
        - Arguments:
            - im: np.ndarray
            - boxes: np.ndarray of shape (nb_boxes, 5) \
                second dimension entries are [ymin, xmin, ymax, xmax, track_id]

        - Returns:
            - annotated_im: image with the visual annotations embedded in it.
        '''
        for i in range(len(boxes)):
            bbox = boxes[i]
            ymin, xmin, ymax, xmax = int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3])
            y_label = ymin - 15 if ymin - 15 > 15 else min(ymin + 15, ymax)
            track_id = int(bbox[4])
            label = "{}".format(track_id)
            cv2.rectangle(im, (xmin, ymin), (xmax, ymax), self._box_color, self._box_thickness)
            cv2.putText(im, label, (xmin, y_label), cv2.FONT_HERSHEY_SIMPLEX, 0.5, self._text_color, lineType = cv2.LINE_AA)
        return im

class SegmenterAnnotator(ImageAnnotator):
    '''
    Draws bounding boxes on images.
    - Arguments:
        - class_labels_path: path to pbtxt file that defines the labels indexes
        - class_labels_dataset: If class_labels_path is None, then we use this attribute to \
            download the file from the releases folder.  Currently supported datasets \
            are: ``coco``, ``oidv4``, ``pascal`` and ``kitti``.
        - transparency: A value between 0 and 1 for the transparency of the mask
    '''

    supported_datasets = [
        'coco',
        'oidv4',
        'pascal',
        'kitti'
    ]

    colors = [
        (255, 0, 0),
        (0, 255, 0),
        (0, 0, 255),
        (255, 255, 0),
        (255, 0, 255),
        (0, 255, 255),
        (255, 127.5, 0),
        (255, 0, 127.5),
        (127.5, 255, 0),
        (0, 255, 127.5),
        (127.5, 0, 255),
        (0, 127.5, 255)
    ]

    def __init__(self, class_labels_path : str | None = None, class_labels_dataset : str | None = 'coco',
                transparency : float = 0.5, nb_tasks : int = 1, **kwargs) -> None:

        if class_labels_path is None and class_labels_dataset is None:
            raise ValueError('If class_labels_path is None, then class_labels_dataset cannot be None')

        if class_labels_path is None:
            if class_labels_dataset not in self.supported_datasets:
                raise ValueError('dataset is not one of supported datasets: {}'.format(', '.join(self.supported_datasets)))
            labels_file_name = f'labels_{class_labels_dataset}.pbtxt'
            remote_url = BASE_URL_DETECTION + labels_file_name
            class_labels_path = get_file(labels_file_name, remote_url)

        self._class_labels_path = class_labels_path
        self._index_label_d = parse_label_map(class_labels_path)
        self._transparency = transparency
        super(SegmenterAnnotator, self).__init__(nb_tasks = nb_tasks, **kwargs)

    def _annotate(self, im : np.ndarray, annotations : list) -> np.ndarray:
        '''
        - Arguments:
            - im: np.ndarray of shape (h, w, 3)
            - annotations: a list with 3 entries:
                - masks: np.ndarray of shape (nb_masks, h, w)
                - classes: np.ndarray of shape (nb_masks, )
                - scores: np.ndarray of shape (nb_masks, )

        - Returns:
            - annotated_im: image with the visual annotations embedded in it.
        '''
        masks = annotations[0]
        classes = annotations[1]

        to_return = im.copy().astype(float)

        # TODO: Add border to masks
        # TODO: Think about how to solve issue of overlaping masks
        # TODO: Add class names to masks

        for idx, mask in enumerate(masks):
            alpha = cv2.merge((mask, mask, mask))
            alpha = alpha.astype(float)
            alpha[alpha == 1.0] = self._transparency

            #1. Mask foreground
            foreground = np.zeros_like(to_return, dtype = float)
            foreground[:] = self.colors[int(classes[idx]) % len(self.colors)]
            foreground = cv2.multiply(alpha, foreground)

            #2. Image background
            background = cv2.multiply(1.0 - alpha, to_return)
            to_return = cv2.add(foreground, background)

        return to_return.astype(np.uint8)
