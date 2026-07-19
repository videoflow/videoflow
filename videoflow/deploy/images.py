'''
Resolve the container image a node's worker runs in on Kubernetes.

There is no module-path "family" inference: a user defines their processors in their
own package and builds their own image (their code + deps on top of
``videoflow-base``), so the image must be stated explicitly. Resolution order, first
match wins:

1. a deploy-time override for the node (``--image-override <name>=<ref>``)
2. the node's own ``image=`` kwarg (declared in graph code)
3. the deploy-time default (``--image <ref>``)

If none apply, resolution raises with an actionable message instead of guessing.
'''
from __future__ import absolute_import, division, print_function

from typing import Optional


def parse_override(spec : str) -> tuple:
    '''Parses a ``name=ref`` CLI override into a ``(name, ref)`` tuple.'''
    if '=' not in spec:
        raise ValueError(f'--image-override must be name=image-ref, got: {spec!r}')
    name, ref = spec.split('=', 1)
    if not name or not ref:
        raise ValueError(f'--image-override must be name=image-ref, got: {spec!r}')
    return name, ref

def resolve_image(node_name : str, node_image : Optional[str],
                default_image : Optional[str] = None,
                overrides : Optional[dict] = None) -> str:
    '''
    - Arguments:
        - node_name: the node's stable name (matched against ``overrides``).
        - node_image: the image declared on the node (``Node.image``), or None.
        - default_image: the deploy-time flow default (``--image``), or None.
        - overrides: mapping of node name to image ref (``--image-override``).

    - Returns:
        - the resolved image ref (str).

    - Raises:
        - ``ValueError`` if no image can be determined for the node.
    '''
    if overrides and node_name in overrides:
        return overrides[node_name]
    if node_image:
        return node_image
    if default_image:
        return default_image
    raise ValueError(
        f"node '{node_name}' has no container image. Declare image=... on the node, "
        "pass a default with --image <ref>, or override it with "
        f"--image-override {node_name}=<ref>."
    )
