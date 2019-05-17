"""Setup script for realpython-reader"""

import os.path
import setuptools
from setuptools import setup

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
    README = fid.read()

# This call to setup() does all the work
exec(open('videoflow/version.py').read())
setup(
    name = "videoflow",
    version = __version__,
    description="Python video streams processing library",
    long_description = README,
    long_description_content_type = "text/markdown",
    url = "https://github.com/jadielam/videoflow",
    author = "Jadiel de Armas",
    author_email = "jadielam@gmail.com",
    license = "MIT",
    packages = setuptools.find_packages(),
    include_package_data = True,
    install_requires = [
        'numpy>=1.9.1',
        'opencv-python',
        'filterpy',
        'six>=1.9.0',
        'scikit-learn'
    ],
    classifiers = [
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)