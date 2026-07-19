'''
Loading and validation of component descriptors (``component.yaml``).

A descriptor is how a language-agnostic component describes itself so videoflow can
wire it into a graph, pick its image, and validate its use — without importing any
of the component's code (which may not even be Python). See
``spec/descriptor/component-schema.json`` for the full schema and Phase 2 of the
migration plan.

This module deliberately avoids a hard dependency on ``jsonschema``: it validates
component params against the descriptor's declared JSON Schema with a small built-in
checker covering the common subset (type/required/enum/default), and uses
``jsonschema`` for full validation only if it happens to be installed.
'''
from __future__ import absolute_import, division, print_function

import json
import os
from typing import Dict, List, Optional

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
    import yaml  # optional dependency (extra): the core imports without PyYAML
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f'Component descriptor {path} is not a mapping')
    return ComponentDescriptor.from_dict(raw, source = path)

# -- descriptor shape validation -------------------------------------------

_VALID_ROLES = ('producer', 'processor', 'consumer')
_VALID_DEVICES = ('cpu', 'gpu')

def _validate_descriptor_shape(raw : dict, source : str | None = None) -> None:
    where = f' ({source})' if source else ''
    # Prefer full JSON Schema validation when jsonschema is available.
    try:
        import jsonschema  # optional dependency (extra): full validation only when installed
        schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                'spec', 'descriptor', 'component-schema.json')
        if os.path.isfile(schema_path):
            with open(schema_path) as f:
                jsonschema.validate(raw, json.load(f))
            return
    except ImportError:
        pass
    except Exception as e:  # jsonschema.ValidationError and friends
        raise ValueError(f'Invalid component descriptor{where}: {e}') from e

    # Minimal built-in checks (subset of the JSON Schema) for when jsonschema is absent.
    if raw.get('apiVersion') != 'videoflow.io/v1':
        raise ValueError(f'component descriptor{where}: apiVersion must be videoflow.io/v1')
    if raw.get('kind') != 'Component':
        raise ValueError(f'component descriptor{where}: kind must be Component')
    meta = raw.get('metadata') or {}
    if not meta.get('name') or not meta.get('version'):
        raise ValueError(f'component descriptor{where}: metadata.name and metadata.version are required')
    spec = raw.get('spec') or {}
    if spec.get('role') not in _VALID_ROLES:
        raise ValueError(f'component descriptor{where}: spec.role must be one of {_VALID_ROLES}')
    if not isinstance(spec.get('protocol'), int) or spec['protocol'] < 1:
        raise ValueError(f'component descriptor{where}: spec.protocol must be an integer >= 1')
    images = ((spec.get('runtime') or {}).get('images')) or {}
    if not images:
        raise ValueError(f'component descriptor{where}: spec.runtime.images must declare at least one of cpu/gpu')
    devices = spec.get('device') or []
    if not devices or any(d not in _VALID_DEVICES for d in devices):
        raise ValueError(f'component descriptor{where}: spec.device must be a non-empty subset of {_VALID_DEVICES}')

# -- minimal params validation (JSON Schema subset) ------------------------

_JSON_TYPE_CHECKS = {
    'object': lambda v: isinstance(v, dict),
    'array': lambda v: isinstance(v, (list, tuple)),
    'string': lambda v: isinstance(v, str),
    'integer': lambda v: isinstance(v, int) and not isinstance(v, bool),
    'number': lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    'boolean': lambda v: isinstance(v, bool),
    'null': lambda v: v is None,
}

def _validate_params(schema : dict, params : dict) -> dict:
    '''Validate ``params`` (an object) against a JSON-Schema-subset ``schema``.'''
    if not schema:
        return params
    # Full validation if jsonschema is installed.
    try:
        import jsonschema  # optional dependency (extra): full validation only when installed
        validator = jsonschema.Draft202012Validator(schema)
        validator.validate(params)
        return _fill_defaults(schema, params)
    except ImportError:
        pass
    except Exception as e:
        raise ValueError(str(e)) from e

    if schema.get('type', 'object') != 'object':
        raise ValueError('top-level params schema must be an object')
    props = schema.get('properties', {}) or {}
    for req in schema.get('required', []) or []:
        if req not in params:
            raise ValueError(f"missing required param '{req}'")
    for key, value in params.items():
        if key not in props:
            if schema.get('additionalProperties', True) is False:
                raise ValueError(f"unknown param '{key}'")
            continue
        _validate_value(props[key], value, key)
    return _fill_defaults(schema, params)

def _validate_value(prop_schema : dict, value : object, name : str) -> None:
    jtype = prop_schema.get('type')
    if jtype is not None:
        types = jtype if isinstance(jtype, list) else [jtype]
        if not any(_JSON_TYPE_CHECKS.get(t, lambda _v: True)(value) for t in types):
            raise ValueError(f"param '{name}' must be of type {jtype}, got {type(value).__name__}")
    if 'enum' in prop_schema and value not in prop_schema['enum']:
        raise ValueError(f"param '{name}' must be one of {prop_schema['enum']}, got {value!r}")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if 'minimum' in prop_schema and value < prop_schema['minimum']:
            raise ValueError(f"param '{name}' must be >= {prop_schema['minimum']}")
        if 'maximum' in prop_schema and value > prop_schema['maximum']:
            raise ValueError(f"param '{name}' must be <= {prop_schema['maximum']}")

def _fill_defaults(schema : dict, params : dict) -> dict:
    out = dict(params)
    for key, prop in (schema.get('properties', {}) or {}).items():
        if key not in out and isinstance(prop, dict) and 'default' in prop:
            out[key] = prop['default']
    return out
