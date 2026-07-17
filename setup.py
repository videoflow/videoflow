"""Setup script for videoflow"""

import os.path
import setuptools
from setuptools import setup

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
    README = fid.read()

__version__ = None  # set __version__ in this exec() call
exec(open('videoflow/version.py').read())

# Core is deliberately lean: the graph model, serialization, and worker runtime.
# Heavy or environment-specific dependencies live in extras so each per-component
# Docker image installs only what its node family needs.
CORE_REQUIRES = [
    'numpy>=1.9.1',
    'six>=1.9.0',
]

# Broker client + wire format: needed by any node that actually runs in a
# distributed flow (i.e. every worker), but not to merely import/inspect a graph.
DISTRIBUTED_REQUIRES = [
    'nats-py>=2.0.0',
    'msgpack>=1.0.0',
]

VISION_REQUIRES = [
    'opencv-python-headless>=4.0.0',
]

VIDEO_REQUIRES = [
    'opencv-python-headless>=4.0.0',
]

# Used by the deploy CLI to render Kubernetes manifests.
DEPLOY_REQUIRES = [
    'PyYAML>=5.1',
]

# Optional external blob store for payloads over the inline size threshold.
BLOB_REQUIRES = [
    'redis>=4.0.0',
]

setup(
    name = "videoflow",
    version = __version__,
    description="Python video stream processing library",
    long_description = README,
    long_description_content_type = "text/markdown",
    url = "https://github.com/videoflow/videoflow",
    author = "Jadiel de Armas",
    author_email = "jadielam@gmail.com",
    license = "MIT",
    packages = setuptools.find_packages(),
    include_package_data = True,
    install_requires = CORE_REQUIRES,
    extras_require = {
        'distributed': DISTRIBUTED_REQUIRES,
        'vision': VISION_REQUIRES + DISTRIBUTED_REQUIRES,
        'video': VIDEO_REQUIRES + DISTRIBUTED_REQUIRES,
        'deploy': DEPLOY_REQUIRES + DISTRIBUTED_REQUIRES,
        'blob': BLOB_REQUIRES,
        'all': (DISTRIBUTED_REQUIRES + VISION_REQUIRES + VIDEO_REQUIRES
                + DEPLOY_REQUIRES + BLOB_REQUIRES + ['requests>=2.22.0']),
    },
    entry_points = {
        'console_scripts': [
            'videoflow=videoflow.cli:main',
        ],
    },
    classifiers = [
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
