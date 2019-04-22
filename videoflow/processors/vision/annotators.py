import numpy as np
import cv2

from ...core.node import ProcessorNode

class ImageAnnotator(ProcessorNode):
    def _annotate(self, im : np.array, annotations : any) -> np.array:
        raise NotImplemented('Subclass must implement this method')

    def process(self, im : np.array, annotations : any) -> np.array:
        to_annotate = np.array(im)
        return self._annotate(to_annotate, annotations)
        
class BoundingBoxAnnotator(ImageAnnotator):
    def __init__(self, box_color = (255, 225, 0), box_thickness = 2, text_color = (255, 255, 255)):
        self._box_color = box_color
        self._text_color = text_color
        self._box_thickness = box_thickness

    def _annotate(self, im : np.array, annotations : any) -> np.array:
        '''
        Arguments:
        - im: np.array
        - annotations: dict with keys: 'boxes', 'classes'
           - 'boxes': np.array of dimensions (nb_boxes, 5)
           - 'classes': list with len(list)==nb_boxes
        '''
        boxes = annotations['boxes']
        classes = annotations['classes']

        for i in range(len(boxes)):
            bbox = boxes[i]
            xmin, ymin, xmax, ymax = int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3])
            confidence = bbox[4]
            label = "{}: {:.2f}%".format(classes[i], confidence * 100)
            cv2.rectangle(im, (xmin, ymin), (xmax, ymax), self._box_color, self._box_thickness)
            y_label = ymin - 15 if ymin - 15 > 15 else min(ymin + 15, ymax)
            cv2.putText(im, label, (xmin, y_label), cv2.FONT_HERSHEY_SIMPLEX, 0.5, self._text_color, lineType = cv2.LINE_AA)
        return im

