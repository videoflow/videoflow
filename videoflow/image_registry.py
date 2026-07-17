'''
Maps a node's fully-qualified class path to the Docker image family that has the
right runtime dependencies installed to run it. Used by the compiler (to stamp
each NodeSpec) and by the Kubernetes engine / CLI (to pick each pod's image).
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

DEFAULT_IMAGE_FAMILY = 'basic'

#: Longest-prefix-wins mapping from node module path to image family. The family
#: names correspond to the directories under ``docker/`` (base, basic, vision,
#: video-io) — each with its own Dockerfile and dependency set.
IMAGE_FAMILY_BY_MODULE_PREFIX = {
    'videoflow.processors.vision': 'vision',
    'videoflow.producers.video': 'video-io',
    'videoflow.consumers.video': 'video-io',
    'videoflow.producers.basic': 'basic',
    'videoflow.processors.basic': 'basic',
    'videoflow.processors.aggregators': 'basic',
    'videoflow.consumers.basic': 'basic',
}

#: Runtime overrides, keyed by node name, populated from the CLI
#: (``--image-override name=family``) for user-defined nodes the static table
#: can't classify. This module-level dict is consulted by ``image_family_for``.
_OVERRIDES_BY_NAME = {}

def set_override(node_name : str, image_family : str):
    _OVERRIDES_BY_NAME[node_name] = image_family

def image_family_for(node_class : str, node_name : str = None) -> str:
    '''
    - Arguments:
        - node_class: fully-qualified class path, e.g. ``videoflow.processors.vision.detectors.ObjectDetector``.
        - node_name: optional node name, checked against CLI-supplied overrides first.

    - Returns:
        - the image family key (str). Falls back to ``DEFAULT_IMAGE_FAMILY`` for \
            classes that don't match any known prefix (e.g. user-defined nodes in \
            an external package) — the user is expected to override those explicitly.
    '''
    if node_name is not None and node_name in _OVERRIDES_BY_NAME:
        return _OVERRIDES_BY_NAME[node_name]
    best_prefix = None
    for prefix in IMAGE_FAMILY_BY_MODULE_PREFIX:
        if node_class.startswith(prefix + '.') or node_class == prefix:
            if best_prefix is None or len(prefix) > len(best_prefix):
                best_prefix = prefix
    if best_prefix is not None:
        return IMAGE_FAMILY_BY_MODULE_PREFIX[best_prefix]
    return DEFAULT_IMAGE_FAMILY
