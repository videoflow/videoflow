'''
Third-party registration via ``importlib.metadata`` entry points.

Videoflow's extension points are plain dict/list registries with an explicit
``register_*()`` function, pre-seeded with the built-ins. That covers the common
case, where the code doing the registering is imported anyway (a component
module the worker loads via ``VF_NODE_CLASS`` registers its own payload codecs
on import).

It does not cover the case where nothing imports the extension first — a blob
store selected by URL scheme in a config file, say, or a host-side
``videoflow debug decode`` that must understand a vendor payload type without
knowing which package defines it. For those, a package declares an entry point
and videoflow imports it on demand.

Stdlib only, by design: an extension mechanism that needs its own dependency is
a worse trade than the ``if/elif`` it replaces.
'''
from __future__ import absolute_import, division, print_function

import logging
from importlib.metadata import entry_points

logger = logging.getLogger(__package__)

# Entry-point groups already loaded, so a miss on a repeatedly-consulted registry
# does not re-scan installed distributions every time.
_loaded : set[str] = set()

def load_plugin_group(group : str) -> None:
    '''
    Imports every extension registered in the ``importlib.metadata`` entry-point
    group ``group``, so its ``register_*()`` side effects take hold. Idempotent:
    a group is scanned at most once per process.

    Each entry point may resolve to either a module (imported for its top-level
    ``register_*()`` calls) or a callable (imported and then called). A plugin
    that raises is logged and skipped rather than propagated — one broken
    third-party package must not stop a worker from starting, since the
    registration it was providing may not even be needed by this flow.

    - Arguments:
        - group: entry-point group name, e.g. ``'videoflow.blob_stores'``.
    '''
    if group in _loaded:
        return
    _loaded.add(group)

    for ep in entry_points(group = group):
        try:
            loaded = ep.load()
            if callable(loaded):
                loaded()
        except Exception as e:                        # noqa: BLE001 — see docstring
            logger.warning(f'Failed to load videoflow plugin {ep.name!r} '
                           f'from group {group!r}: {type(e).__name__}: {e}')
