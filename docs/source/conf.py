# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath('../..'))

# -- Project information ------------------------------------------------------

project = 'videoflow'
author = 'Jadiel de Armas'
copyright = '2019-2026, Jadiel de Armas'

# Single source of truth: read __version__ from videoflow/version.py without
# importing the whole package (which would require all runtime dependencies).
_version_ns = {}
with open(os.path.join(os.path.dirname(__file__), '..', '..', 'videoflow', 'version.py')) as _f:
    exec(_f.read(), _version_ns)
release = _version_ns['__version__']
version = release

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinxcontrib.apidoc',
]

templates_path = ['_templates']
exclude_patterns = []

# Some modules import optional third-party libraries only present in specific
# per-component images; mock them so autodoc never fails to import a module.
autodoc_mock_imports = ['cv2', 'nats', 'msgpack', 'redis', 'yaml', 'requests', 'six']

# -- Options for HTML output -------------------------------------------------

html_theme = 'sphinx_rtd_theme'
master_doc = 'index'
html_static_path = []

# -- API doc generation (sphinxcontrib-apidoc) -------------------------------

apidoc_module_dir = '../../videoflow'
apidoc_output_dir = 'apidocs'
apidoc_excluded_paths = ['tests']
apidoc_module_first = True
apidoc_separate_modules = False
