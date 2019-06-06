'''
Tests that resources are present in repo releases and
can be downloaded
'''

import pytest

from videoflow.utils.downloader import get_file
from videoflow.processors.vision.detectors import TensorflowObjectDetector, BASE_URL_DETECTION
from videoflow.processors.vision.annotators import BoundingBoxAnnotator
from videoflow.processors.vision.segmentation import TensorflowSegmenter, BASE_URL_SEGMENTATION

def test_detector_resources():
    for modelid in TensorflowObjectDetector.supported_models:
        filename = f'{modelid}.pb'
        url_path = BASE_URL_DETECTION + filename
        get_file(filename, url_path)

def test_bboxannotator_resources():
    for datasetid in BoundingBoxAnnotator.supported_datasets:
        filename = f'labels_{datasetid}.pbtxt'
        url_path = BASE_URL_DETECTION + filename
        get_file(filename, url_path)

def test_segmenter_resources():
    for modelid in TensorflowSegmenter.supported_models:
        filename = f'{modelid}.pb'
        url_path = BASE_URL_SEGMENTATION + filename
        get_file(filename, url_path)

if __name__ == "__main__":
    pytest.main([__file__])