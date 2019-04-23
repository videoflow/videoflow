from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np
from filterpy.kalman import KalmanFilter
from sklearn.utils.linear_assignment_ import linear_assignment
import math

from ...core.node import ProcessorNode

class BoundingBoxTracker(ProcessorNode):
    def _track(self, dets : np.array) -> np.array:
        raise NotImplemented("Subclass must implement _track method")
    
    def process(self, dets : np.array) -> np.array:
        return self._track(dets)

def eucl(bb_test, bb_gt):
    '''
    Computes the euclidean distance between two boxes
    in the form [x1, y1, x2, y2]
    '''
    center_1 = [(bb_test[0] + bb_test[2]) / 2.0, (bb_test[1] + bb_test[3]) / 2.0]
    center_2 = [(bb_gt[0] + bb_gt[2]) / 2.0, (bb_gt[1] + bb_gt[3]) / 2.0]
    eucl = math.sqrt((center_1[0] - center_2[0])*(center_1[0] - center_2[0]) + (center_1[1] - center_2[1])*(center_1[1] - center_2[1]))
    return -eucl

def iou(bb_test, bb_gt):
    """
      Computes IUO between two bboxes in the form [x1, y1, x2, y2]
      IOU is the intersection of areas.
    """
    xx1 = np.maximum(bb_test[0], bb_gt[0])
    yy1 = np.maximum(bb_test[1], bb_gt[1])
    xx2 = np.minimum(bb_test[2], bb_gt[2])
    yy2 = np.minimum(bb_test[3], bb_gt[3])
    w = np.maximum(0., xx2 - xx1)
    h = np.maximum(0., yy2 - yy1)
    wh = w * h
    o = wh / ((bb_test[2] - bb_test[0]) * (bb_test[3] - bb_test[1])
              + (bb_gt[2] - bb_gt[0]) * (bb_gt[3] - bb_gt[1]) - wh)
    return(o)

def convert_bbox_to_z(bbox):
    """
      Takes a bounding box in the form [x1, y1, x2, y2] and returns z in the form
        [x, y, s, r] where x, y is the centre of the box and s is the scale/area and r is
        the aspect ratio
    """
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    x = bbox[0] + w/2.
    y = bbox[1] + h/2.
    s = w * h    #scale is just area
    r = w / float(h)
    return np.array([x, y, s, r]).reshape((4, 1))

def convert_x_to_bbox(x, score=None):
    """
      Takes a bounding box in the form [x, y, s, r] and returns it in the form
    [x1, y1, x2, x2] where x1, y1 is the top left and x2, y2 is the bottom right
    """
    w = np.sqrt(x[2]*x[3])
    h = x[2]/w
    if(score==None):
        return np.array([x[0] - w/2., x[1] - h/2., x[0] + w/2., x[1] + h/2.]).reshape((1, 4))
    else:
        return np.array([x[0] - w/2., x[1] - h/2.,x[0] + w/2., x[1] + h/2., score]).reshape((1, 5))

def associate_detections_to_trackers(detections, trackers, metric_function, iou_threshold = 0.1):
    """
      Assigns detections to tracked object (both represented as bounding boxes)
      Returns 3 lists of matches, unmatched_detections and unmatched_trackers
    """
    distance_threshold = 500

    if(len(trackers) == 0):
        return np.empty((0, 2), dtype = int), np.arange(len(detections)), np.empty((0, 5), dtype = int)
    iou_matrix = np.zeros((len(detections), len(trackers)), dtype=np.float32)

    for d, det in enumerate(detections):
        for t, trk in enumerate(trackers):
            iou_matrix[d, t] = metric_function(det, trk)
    matched_indices = linear_assignment(-iou_matrix)

    unmatched_detections = []
    for d,det in enumerate(detections):
        if(d not in matched_indices[:,0]):
            unmatched_detections.append(d)
    unmatched_trackers = []
    for t, trk in enumerate(trackers):
        if(t not in matched_indices[:,1]):
            unmatched_trackers.append(t)

    #filter out matched with low IOU
    matches = []
    for m in matched_indices:
        if(iou_matrix[m[0], m[1]] < iou_threshold):
        #if(iou_matrix[m[0], m[1]] > distance_threshold):
            unmatched_detections.append(m[0])
            unmatched_trackers.append(m[1])
        else:
            matches.append(m.reshape(1, 2))
    if(len(matches)==0):
        matches = np.empty((0, 2), dtype = int)
    else:
        matches = np.concatenate(matches, axis = 0)

    return matches, np.array(unmatched_detections), np.array(unmatched_trackers)

class KalmanBoxTracker(object):
    """
      This class represents the internel state of individual tracked objects observed as bbox.
    """
    count = 0
    def __init__(self,bbox):
        """
        Initialises a tracker using initial bounding box.
        """
        #define constant velocity model
        self.kf = KalmanFilter(dim_x=7, dim_z=4)
        self.kf.F = np.array([[1,0,0,0,1,0,0],[0,1,0,0,0,1,0],[0,0,1,0,0,0,1],[0,0,0,1,0,0,0],[0,0,0,0,1,0,0],[0,0,0,0,0,1,0],[0,0,0,0,0,0,1]])
        self.kf.H = np.array([[1,0,0,0,0,0,0],[0,1,0,0,0,0,0],[0,0,1,0,0,0,0],[0,0,0,1,0,0,0]])

        self.kf.R[2:,2:] *= 10.
        self.kf.P[4:,4:] *= 1000. #give high uncertainty to the unobservable initial velocities
        self.kf.P *= 10.
        self.kf.Q[-1,-1] *= 0.01
        self.kf.Q[4:,4:] *= 0.01

        self.kf.x[:4] = convert_bbox_to_z(bbox)
        self.time_since_update = 0
        self.id = KalmanBoxTracker.count
        KalmanBoxTracker.count += 1
        self.history = []
        self.hits = 0
        self.hit_streak = 0
        self.age = 0

    def update(self, bbox):
        """
        Updates the state vector with observed bbox.
        """
        self.time_since_update = 0
        self.history = []
        self.hits += 1
        self.hit_streak += 1
        self.kf.update(convert_bbox_to_z(bbox))

    def predict(self):
        """
        Advances the state vector and returns the predicted bounding box estimate.
        """
        if((self.kf.x[6] + self.kf.x[2]) <= 0):
            self.kf.x[6] *= 0.0
        self.kf.predict()
        self.age += 1
        if(self.time_since_update > 0):
            self.hit_streak = 0
        self.time_since_update += 1
        self.history.append(convert_x_to_bbox(self.kf.x))
        
        return self.history[-1]
    
    def get_state(self):
        """
        Returns the current bounding box estimate.
        """
        return convert_x_to_bbox(self.kf.x)

        