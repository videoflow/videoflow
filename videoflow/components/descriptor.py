'''
Loading and validation of component descriptors (``component.yaml``).

A descriptor is how a language-agnostic component describes itself so videoflow can
wire it into a graph, pick its image, and validate its use — without importing any
of the component's code (which may not even be Python). See
``spec/descriptor/component-schema.json`` for the full schema and Phase 2 of the
migration plan.

Descriptors are validated against that schema, and a component's params against the
JSON Schema the descriptor declares, with ``jsonschema`` (a core dependency). The
few cross-field rules JSON Schema cannot express (``_validate_gpu_resources``,
``_validate_host_resources``) are checked by hand on top.
'''
from __future__ import absolute_import, division, print_function

import functools
import json
import os
from typing import Dict, List, Optional

import jsonschema
import yaml

#: Payload-type token meaning "any payload" in a descriptor's io section.
IO_ANY = 'any'

class ComponentDescriptor:
    '''
    A parsed, validated component descriptor. Construct via ``load_descriptor`` (from
    a path) or ``ComponentDescriptor.from_dict`` (from an already-parsed mapping).
    '''
    def __init__(self, raw : dict, source : str | None = None) -> None:
        self._raw = raw
        self.source = source
        meta = raw.get('metadata', {})
        spec = raw.get('spec', {})
        runtime = spec.get('runtime', {})
        self.name : str = meta.get('name')
        self.version : str = meta.get('version')
        self.license : Optional[str] = meta.get('license')
        self.description : Optional[str] = meta.get('description')
        self.role : str = spec.get('role')
        self.protocol : int = spec.get('protocol')
        self.finite : bool = spec.get('finite', True)
        self.images : Dict[str, str] = dict(runtime.get('images', {}))
        # A Python component names the class the worker imports; a native component
        # leaves this None and runs its own image entrypoint/command.
        self.python_class : Optional[str] = runtime.get('pythonClass')
        self.command : Optional[List[str]] = runtime.get('command')
        self.local_command : Optional[List[str]] = runtime.get('localCommand')
        self.device : List[str] = list(spec.get('device', []))
        self.params_schema : dict = (spec.get('params', {}) or {}).get('schema', {}) or {}
        io = spec.get('io', {}) or {}
        self.inputs : List[dict] = list(io.get('inputs', []) or [])
        self.output : Optional[dict] = io.get('output')
        constraints = spec.get('constraints', {}) or {}
        self.partitionable : bool = constraints.get('partitionable', False)
        self.singleton : bool = constraints.get('singleton', False)
        # spec.resources.gpu (RFC 0003/0004): the component's default GPU request.
        # Graph-side gpu_count=/gpu_memory_gib= override these — defaults, not floors.
        resources = spec.get('resources') or {}
        gpu = resources.get('gpu') or {}
        self.gpu_count : int = gpu.get('count', 1)
        # ``spec.resources.gpu.count`` exactly as written, or None when the
        # descriptor is silent. ``gpu_count`` folds the silence into 1, which is
        # right for consumers of the value and wrong for provenance: the resolver
        # in ``core.provenance`` must not record a default the file never made.
        self.gpu_count_declared : Optional[int] = gpu.get('count')
        self.gpu_memory_gib : int | float | None = gpu.get('memoryGiB')
        # spec.resources.cpu / memory: optional host-resource requests per
        # replica, as Kubernetes quantities ('500m', '2Gi'). Accepted and parsed
        # so a renderer can pick them up; nothing renders them yet.
        self.cpu_request : Optional[str] = _quantity(resources.get('cpu'))
        self.memory_request : Optional[str] = _quantity(resources.get('memory'))

    @classmethod
    def from_dict(cls, raw : dict, source : str | None = None) -> 'ComponentDescriptor':
        _validate_descriptor_shape(raw, source)
        return cls(raw, source = source)

    @property
    def is_native(self) -> bool:
        '''A native (non-Python) component: runs its own image entrypoint and must speak the protobuf wire.'''
        return self.python_class is None

    def image_for(self, device_type : str) -> Optional[str]:
        '''The image ref for a device type ('cpu'/'gpu'), or None if not declared.'''
        return self.images.get(device_type)

    def input_accepts(self, index : int, capability : str) -> bool:
        '''Whether input ``index`` declares it accepts ``'missing'`` or ``'collected'`` inputs (default False).'''
        if index < 0 or index >= len(self.inputs):
            return False
        return bool((self.inputs[index].get('accepts', {}) or {}).get(capability, False))

    def all_inputs_accept(self, capability : str) -> bool:
        if not self.inputs:
            return False
        return all((i.get('accepts', {}) or {}).get(capability, False) for i in self.inputs)

    def validate_params(self, params : Optional[dict]) -> dict:
        '''
        Validate ``params`` against the descriptor's JSON Schema and return a copy
        with defaults filled in. Raises ``ValueError`` on a violation, naming the
        component so the error is actionable at graph-build time.
        '''
        params = dict(params or {})
        try:
            return _validate_params(self.params_schema, params)
        except ValueError as e:
            raise ValueError(f"component '{self.name}': invalid params: {e}") from e

    def __repr__(self) -> str:
        return f'ComponentDescriptor(name={self.name!r}, version={self.version!r}, role={self.role!r})'

def _quantity(value : object) -> Optional[str]:
    '''A ``spec.resources.cpu``/``memory`` value as the quantity string Kubernetes takes (``2`` -> ``'2'``), or None.'''
    return None if value is None else str(value)

def load_descriptor(ref : str) -> ComponentDescriptor:
    '''
    Load a descriptor from a reference:

    - a path to a ``component.yaml`` file, or
    - a path to a directory containing ``component.yaml``.

    ``oci://...`` refs (a descriptor published as an OCI artifact) are resolved in
    Phase 6; for now they raise a clear error.
    '''
    if ref.startswith('oci://'):
        # Resolve (pull + cache) the descriptor artifact, then load the cached file.
        # Deferred to break the descriptor <-> oci circular import (oci imports
        # load_descriptor at module scope to validate what it pushes).
        from .oci import pull_component
        ref = pull_component(ref)
    path = ref
    if os.path.isdir(path):
        path = os.path.join(path, 'component.yaml')
    if not os.path.isfile(path):
        raise FileNotFoundError(f'Component descriptor not found: {path}')
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f'Component descriptor {path} is not a mapping')
    return ComponentDescriptor.from_dict(raw, source = path)

# -- descriptor shape validation -------------------------------------------

def _schema_path() -> str:
    '''
    ``spec/descriptor/component-schema.json``: the copy hatch places inside the
    package for a wheel install, else the checkout's ``spec/`` next to the package
    (an editable install). A build that ships neither is a packaging bug, so it
    fails here rather than silently validating nothing.
    '''
    package = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates = [os.path.join(package, 'spec', 'descriptor', 'component-schema.json'),
                  os.path.join(os.path.dirname(package), 'spec', 'descriptor', 'component-schema.json')]
    for path in candidates:
        if os.path.isfile(path):
            return path
    raise RuntimeError('videoflow is packaged without spec/descriptor/component-schema.json '
                       f'(looked in {candidates}); reinstall it from a wheel or a checkout.')

@functools.lru_cache(maxsize = 1)
def _descriptor_validator() -> jsonschema.Draft202012Validator:
    with open(_schema_path()) as f:
        return jsonschema.Draft202012Validator(json.load(f))

def _violation(error : jsonschema.ValidationError) -> str:
    '''``spec.role: 'pilot' is not one of [...]`` — the offending path, then jsonschema's own sentence.'''
    path = '.'.join(str(p) for p in error.absolute_path)
    return f'{path}: {error.message}' if path else error.message

def _validate_descriptor_shape(raw : dict, source : str | None = None) -> None:
    where = f' ({source})' if source else ''
    # Cross-field checks first: JSON Schema cannot express "count > 1 requires the
    # gpu device type", and their messages say what to change.
    _validate_gpu_resources(raw.get('spec') or {}, where)
    _validate_host_resources(raw.get('spec') or {}, where)
    error = jsonschema.exceptions.best_match(_descriptor_validator().iter_errors(raw))
    if error is not None:
        raise ValueError(f'Invalid component descriptor{where}: {_violation(error)}') from error

def _validate_gpu_resources(spec : dict, where : str) -> None:
    '''
    Validate ``spec.resources.gpu`` (RFC 0003): the block belongs to processors,
    the count is an integer >= 1, and a multi-GPU component supports the gpu
    device. Cross-field rules JSON Schema cannot express.
    '''
    resources = spec.get('resources') or {}
    if 'gpu' not in resources:
        return
    if spec.get('role') != 'processor':
        # A silently ignored GPU need would schedule an underprovisioned pod.
        raise ValueError(f"component descriptor{where}: spec.resources.gpu applies to processor "
                        f"components only, but spec.role is {spec.get('role')!r} — drop the "
                        f"resources.gpu block")
    gpu = resources.get('gpu') or {}
    count = gpu.get('count', 1)
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ValueError(f'component descriptor{where}: spec.resources.gpu.count must be an integer >= 1, got {count!r}')
    if count > 1 and 'gpu' not in (spec.get('device') or []):
        raise ValueError(f"component descriptor{where}: spec.resources.gpu.count > 1 requires 'gpu' in spec.device "
                        f"— add gpu to the device list or drop the resources.gpu block")
    memory = gpu.get('memoryGiB')
    if memory is not None:
        if isinstance(memory, bool) or not isinstance(memory, (int, float)) or memory <= 0:
            raise ValueError(f'component descriptor{where}: spec.resources.gpu.memoryGiB must be a '
                            f'positive number, got {memory!r}')
        if 'gpu' not in (spec.get('device') or []):
            raise ValueError(f"component descriptor{where}: spec.resources.gpu.memoryGiB requires 'gpu' "
                            f"in spec.device — add gpu to the device list or drop the field")
        if count > 1:
            raise ValueError(f'component descriptor{where}: spec.resources.gpu.memoryGiB and count > 1 '
                            f'are mutually exclusive — a model cannot span MIG slices, so a component '
                            f'declares either a memory demand (a fraction of one device) or a '
                            f'whole-device count, never both (RFC 0004)')

def _validate_host_resources(spec : dict, where : str) -> None:
    '''
    Validate ``spec.resources.cpu`` / ``spec.resources.memory``: optional
    host-resource requests, each a Kubernetes quantity — a non-empty string
    (``'500m'``, ``'2Gi'``) or a positive number. The JSON Schema only says
    ``string | number``; the non-empty / positive rules live here.
    '''
    resources = spec.get('resources') or {}
    for key, example in (('cpu', '500m'), ('memory', '2Gi')):
        if key not in resources:
            continue
        value = resources[key]
        acceptable = ((isinstance(value, str) and value.strip() != '')
                      or (isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0))
        if not acceptable:
            raise ValueError(f'component descriptor{where}: spec.resources.{key} must be a Kubernetes '
                            f'quantity (a non-empty string such as {example!r}, or a positive number), '
                            f'got {value!r}')

# -- params validation -----------------------------------------------------

def _validate_params(schema : dict, params : dict) -> dict:
    '''Validate ``params`` (an object) against the descriptor's JSON Schema; returns them with defaults filled.'''
    if not schema:
        return params
    try:
        jsonschema.Draft202012Validator.check_schema(schema)
    except jsonschema.SchemaError as e:
        raise ValueError(f'the descriptor declares an invalid params schema: {_violation(e)}') from e
    error = jsonschema.exceptions.best_match(jsonschema.Draft202012Validator(schema).iter_errors(params))
    if error is not None:
        raise ValueError(_violation(error)) from error
    return _fill_defaults(schema, params)

def _fill_defaults(schema : dict, params : dict) -> dict:
    out = dict(params)
    for key, prop in (schema.get('properties', {}) or {}).items():
        if key not in out and isinstance(prop, dict) and 'default' in prop:
            out[key] = prop['default']
    return out
