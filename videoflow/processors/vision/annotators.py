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
            klass_text = classes[i]
            cv2.rectangle(im, (bbox[0], bbox[1]), (bbox[2], bbox[3]), (255, 255, 0), 2)
            cv2.putText(im, klass_text, (bbox[0], bbox[1]), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (255, 255, 255), lineType = cv2.LINE_AA)
        
        return im

