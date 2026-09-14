'''
Provenance-aware GPU requirement resolution (videoflow.core.provenance) and its
wiring into the node constructors, ``component()``, the compiler's companion
document and the deploy-time resource-name merge (ALLOC-034).

Every row below is one declared combination with one deterministic outcome: the
resolved values, where each came from, or the contradiction that rejects it
naming both sources. Two properties the rows pin: a default never silently
defeats an explicit requirement (an explicit ``gpu_count=1`` and the default
resolve alike but are recorded differently, and a descriptor default that
collides with an explicit request is rejected naming the descriptor), and
today's precedence is unchanged — the serialized ``NodeSpec`` carries nothing
new (decision D8), so a compiled document is byte-identical.
'''
from __future__ import absolute_import, division, print_function

import dataclasses

import pytest

from videoflow.components.descriptor import ComponentDescriptor
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow, component
from videoflow.core.compiler import NodeSpec, compile_flow, gpu_provenance
from videoflow.core.constants import CPU, GPU, REALTIME
from videoflow.core.errors import CapabilityError
from videoflow.core.provenance import (
    FIELD_DEVICE_TYPE,
    FIELD_GPU_COUNT,
    FIELD_GPU_MEMORY_GIB,
    FIELD_GPU_RESOURCE_NAME,
    SOURCE_CLI_DEFAULT,
    SOURCE_DEFAULT,
    SOURCE_DESCRIPTOR,
    SOURCE_NODE,
    SOURCE_STRATEGY,
    Declaration,
    GpuRequirementConflict,
    builtin_defaults,
    descriptor_declarations,
    node_declarations,
    resolve_gpu_requirements,
)
from videoflow.deploy.gpu import gpu_resource_provenance, resolve_gpu_resource
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

BOTH = ['cpu', 'gpu']

# -- the resolver ------------------------------------------------------------------

@pytest.mark.parametrize('title,declarations,values,provenance,dropped', [
    ('no GPU declaration',
     node_declarations(CPU) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: CPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {}),
    ('default count=1 on a GPU node',
     node_declarations(GPU) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {}),
    ('explicit count=1',
     node_declarations(GPU, gpu_count = 1) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {}),
    ('explicit count=2',
     node_declarations(GPU, gpu_count = 2) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 2, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {}),
    ('explicit memory demand',
     node_declarations(GPU, gpu_memory_gib = 10) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: 10},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_NODE},
     {}),
    ('descriptor memory default',
     node_declarations(GPU) + descriptor_declarations(BOTH, gpu_memory_gib = 20) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: 20},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DESCRIPTOR},
     {}),
    ('descriptor count default',
     node_declarations(GPU) + descriptor_declarations(BOTH, gpu_count = 2) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 2, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DESCRIPTOR, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {}),
    ('explicit count overrides the descriptor default',
     node_declarations(GPU, gpu_count = 1) + descriptor_declarations(BOTH, gpu_count = 4) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {}),
    ('explicit memory overrides the descriptor default',
     node_declarations(GPU, gpu_memory_gib = 10) + descriptor_declarations(BOTH, gpu_memory_gib = 20)
     + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: 10},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_NODE},
     {}),
    # Default count=1 is not a whole-device demand that cancels a slice request,
    # and neither is an explicit 1: both keep the descriptor's memory default.
    ('explicit count=1 with a descriptor memory default',
     node_declarations(GPU, gpu_count = 1) + descriptor_declarations(BOTH, gpu_memory_gib = 20) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: 20},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DESCRIPTOR},
     {}),
    # RFC 0004: a descriptor memory default describes the gpu flavor; a cpu run
    # sets it aside and records that it did.
    ('descriptor memory default on a cpu run',
     node_declarations(CPU) + descriptor_declarations(BOTH, gpu_memory_gib = 20) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: CPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {FIELD_GPU_MEMORY_GIB: SOURCE_DESCRIPTOR}),
    ('a GPU-only component on a GPU node: two agreeing hard declarations',
     node_declarations(GPU) + descriptor_declarations(['gpu']) + builtin_defaults(),
     {FIELD_DEVICE_TYPE: GPU, FIELD_GPU_COUNT: 1, FIELD_GPU_MEMORY_GIB: None},
     {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
     {}),
    ('resource name: strategy beats the CLI default beats the built-in',
     [Declaration(FIELD_GPU_RESOURCE_NAME, 'nvidia.com/mig-1g.10gb', SOURCE_STRATEGY),
      Declaration(FIELD_GPU_RESOURCE_NAME, 'amd.com/gpu', SOURCE_CLI_DEFAULT, hard = False),
      Declaration(FIELD_GPU_RESOURCE_NAME, 'nvidia.com/gpu', SOURCE_DEFAULT, hard = False)],
     {FIELD_GPU_RESOURCE_NAME: 'nvidia.com/mig-1g.10gb'}, {FIELD_GPU_RESOURCE_NAME: SOURCE_STRATEGY}, {}),
    ('resource name: CLI default beats the built-in',
     [Declaration(FIELD_GPU_RESOURCE_NAME, 'nvidia.com/gpu', SOURCE_DEFAULT, hard = False),
      Declaration(FIELD_GPU_RESOURCE_NAME, 'amd.com/gpu', SOURCE_CLI_DEFAULT, hard = False)],
     {FIELD_GPU_RESOURCE_NAME: 'amd.com/gpu'}, {FIELD_GPU_RESOURCE_NAME: SOURCE_CLI_DEFAULT}, {}),
], ids = lambda value: value if isinstance(value, str) else None)
def test_resolver_table(title, declarations, values, provenance, dropped):
    resolution = resolve_gpu_requirements(declarations, subject = title)
    assert resolution.values == values
    assert resolution.provenance == provenance
    assert resolution.dropped == dropped
    # The record's field order is fixed, whatever order the declarations came in.
    assert list(resolution.provenance) == [f for f in (FIELD_DEVICE_TYPE, FIELD_GPU_COUNT, FIELD_GPU_MEMORY_GIB,
                                                      FIELD_GPU_RESOURCE_NAME) if f in provenance]
    assert resolve_gpu_requirements(list(reversed(declarations))).provenance == provenance


@pytest.mark.parametrize('title,declarations,sources,in_message,in_remedy', [
    ('two hard counts that differ',
     [Declaration(FIELD_GPU_COUNT, 2, SOURCE_NODE), Declaration(FIELD_GPU_COUNT, 1, SOURCE_STRATEGY)],
     [SOURCE_NODE, SOURCE_STRATEGY], ('gpu_count', 'node says 2', 'strategy says 1'),
     ('gpu_count= argument on the node', "strategy's resolved gpu_count")),
    ('a GPU-only component asked to run on cpu',
     node_declarations(CPU) + descriptor_declarations(['gpu']) + builtin_defaults(),
     [SOURCE_NODE, SOURCE_DESCRIPTOR], ("device_type='cpu'", 'spec.device', "'gpu'"),
     ('device_type= argument on the node', 'spec.device in the component descriptor')),
    ('a descriptor count default on a cpu run',
     node_declarations(CPU) + descriptor_declarations(BOTH, gpu_count = 2) + builtin_defaults(),
     [SOURCE_NODE, SOURCE_DESCRIPTOR], ('gpu_count=2 (descriptor)', "requires device_type='gpu'"),
     ('spec.resources.gpu.count in the component descriptor', 'gpu_count=1')),
    ('an explicit count against a descriptor memory default',
     node_declarations(GPU, gpu_count = 2) + descriptor_declarations(BOTH, gpu_memory_gib = 20) + builtin_defaults(),
     [SOURCE_NODE, SOURCE_DESCRIPTOR], ('gpu_count=2 (node)', 'gpu_memory_gib=20 (descriptor)', 'mutually exclusive'),
     ('gpu_count= argument on the node', 'spec.resources.gpu.memoryGiB in the component descriptor')),
    ('an explicit memory demand against a descriptor count default',
     node_declarations(GPU, gpu_memory_gib = 10) + descriptor_declarations(BOTH, gpu_count = 2) + builtin_defaults(),
     [SOURCE_DESCRIPTOR, SOURCE_NODE], ('gpu_count=2 (descriptor)', 'gpu_memory_gib=10 (node)', 'mutually exclusive'),
     ('spec.resources.gpu.count in the component descriptor', 'gpu_memory_gib= argument on the node')),
], ids = lambda value: value if isinstance(value, str) else None)
def test_resolver_rejects_contradictions_naming_both_sources(title, declarations, sources, in_message, in_remedy):
    with pytest.raises(GpuRequirementConflict) as excinfo:
        resolve_gpu_requirements(declarations, subject = title)
    err = excinfo.value
    # The taxonomy class the CLI renders, and the builtin class graph authors catch.
    assert isinstance(err, CapabilityError) and isinstance(err, ValueError)
    assert err.code == 'VF_CAPABILITY' and err.exit_code == 2
    assert err.message.startswith(title)
    for needle in in_message:
        assert needle in err.message, (needle, err.message)
    for needle in in_remedy:
        assert needle in err.remedy, (needle, err.remedy)
    assert err.context['sources'] == sources
    # to_dict keeps the remedy structured (the DLQ inspector / termination log render it).
    assert err.to_dict()['remedy'] == err.remedy


def test_same_source_contradictions_are_left_to_the_constructor():
    '''A node contradicting itself is not a two-source conflict: the resolver
    resolves, and the constructor rejects with the message the API documents.'''
    resolution = resolve_gpu_requirements(node_declarations(CPU, gpu_count = 2) + builtin_defaults())
    assert resolution.values[FIELD_GPU_COUNT] == 2 and resolution.provenance[FIELD_GPU_COUNT] == SOURCE_NODE
    with pytest.raises(ValueError, match = 'device_type=GPU') as excinfo:
        IdentityProcessor(name = 'g', gpu_count = 2)
    assert not isinstance(excinfo.value, GpuRequirementConflict)
    with pytest.raises(ValueError, match = 'mutually exclusive'):
        IdentityProcessor(name = 'g', device_type = GPU, gpu_count = 2, gpu_memory_gib = 10)


def test_two_hard_declarations_that_agree_are_not_a_conflict():
    resolution = resolve_gpu_requirements(
        [Declaration(FIELD_GPU_COUNT, 2, SOURCE_STRATEGY), Declaration(FIELD_GPU_COUNT, 2, SOURCE_NODE)])
    assert resolution.values == {FIELD_GPU_COUNT: 2}
    assert resolution.provenance == {FIELD_GPU_COUNT: SOURCE_NODE}      # node ranks first among equals


def test_unknown_fields_and_sources_are_rejected():
    with pytest.raises(ValueError, match = 'unknown GPU requirement field'):
        resolve_gpu_requirements([Declaration('gpu_colour', 'red', SOURCE_NODE)])
    with pytest.raises(ValueError, match = 'unknown declaration source'):
        resolve_gpu_requirements([Declaration(FIELD_GPU_COUNT, 1, 'rumour')])


def test_descriptor_declarations_treat_a_single_device_as_a_requirement():
    assert descriptor_declarations(['gpu']) == [Declaration(FIELD_DEVICE_TYPE, 'gpu', SOURCE_DESCRIPTOR, hard = True)]
    assert descriptor_declarations(BOTH) == []
    soft = descriptor_declarations(BOTH, gpu_count = 2, gpu_memory_gib = None)
    assert soft == [Declaration(FIELD_GPU_COUNT, 2, SOURCE_DESCRIPTOR, hard = False)]

# -- native nodes ------------------------------------------------------------------

@pytest.mark.parametrize('kwargs,expected', [
    ({}, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ({'device_type': GPU}, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT,
                            FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ({'device_type': GPU, 'gpu_count': 1}, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE,
                                            FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ({'device_type': GPU, 'gpu_count': 2}, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE,
                                            FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ({'device_type': GPU, 'gpu_memory_gib': 10}, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT,
                                                  FIELD_GPU_MEMORY_GIB: SOURCE_NODE}),
])
def test_processor_node_records_provenance(kwargs, expected):
    node = IdentityProcessor(name = 'n', **kwargs)
    assert node.gpu_provenance == expected
    assert node.gpu_count == kwargs.get('gpu_count', 1)
    assert node.gpu_memory_gib == kwargs.get('gpu_memory_gib')


def test_explicit_and_default_count_reconstruct_identically():
    '''The record distinguishes them; the worker-side round trip does not need to.'''
    explicit = IdentityProcessor(name = 'n', device_type = GPU, gpu_count = 1)
    defaulted = IdentityProcessor(name = 'n', device_type = GPU)
    assert explicit.get_params() == defaulted.get_params()
    assert explicit.get_params()['gpu_count'] == 1
    assert 'gpu_provenance' not in explicit.get_params()
    rebuilt = IdentityProcessor(**defaulted.get_params())
    assert rebuilt.gpu_count == 1


def test_processor_node_still_validates_gpu_count():
    with pytest.raises(ValueError, match = 'positive integer'):
        IdentityProcessor(name = 'n', device_type = GPU, gpu_count = 0)
    with pytest.raises(ValueError, match = 'positive integer'):
        IdentityProcessor(name = 'n', device_type = GPU, gpu_count = True)

# -- component() -------------------------------------------------------------------

def _descriptor(**spec_overrides):
    spec = {
        'role': 'processor', 'protocol': 1,
        'runtime': {'images': {'cpu': 'x:cpu', 'gpu': 'x:gpu'}}, 'device': BOTH,
        'io': {'inputs': [{'name': 'in', 'type': 'any'}]},
    }
    spec.update(spec_overrides)
    return ComponentDescriptor.from_dict({
        'apiVersion': 'videoflow.io/v1', 'kind': 'Component',
        'metadata': {'name': 'acme/thing', 'version': '1.0.0'}, 'spec': spec})


@pytest.mark.parametrize('title,resources,kwargs,count,memory,provenance', [
    ('no GPU declaration', {}, {},
     1, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('default count on gpu', {}, {'device_type': 'gpu'},
     1, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('explicit count=1', {}, {'device_type': 'gpu', 'gpu_count': 1},
     1, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('explicit count=2', {}, {'device_type': 'gpu', 'gpu_count': 2},
     2, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('explicit memory', {}, {'device_type': 'gpu', 'gpu_memory_gib': 10},
     1, 10, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_NODE}),
    ('descriptor memory default', {'gpu': {'memoryGiB': 20}}, {'device_type': 'gpu'},
     1, 20, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DESCRIPTOR}),
    ('descriptor count default', {'gpu': {'count': 2}}, {'device_type': 'gpu'},
     2, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DESCRIPTOR, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('descriptor count=1 is recorded as the descriptor, not the default', {'gpu': {'count': 1}}, {'device_type': 'gpu'},
     1, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DESCRIPTOR, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('explicit count overrides the descriptor', {'gpu': {'count': 4}}, {'device_type': 'gpu', 'gpu_count': 1},
     1, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('explicit count=1 keeps the descriptor memory default', {'gpu': {'memoryGiB': 20}},
     {'device_type': 'gpu', 'gpu_count': 1},
     1, 20, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DESCRIPTOR}),
    ('explicit memory overrides the descriptor', {'gpu': {'memoryGiB': 20}}, {'device_type': 'gpu', 'gpu_memory_gib': 10},
     1, 10, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_NODE}),
    ('descriptor memory default dropped on a cpu run', {'gpu': {'memoryGiB': 20}}, {'device_type': 'cpu'},
     1, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
    ('descriptor count=2 overridden to run on cpu', {'gpu': {'count': 2}}, {'device_type': 'cpu', 'gpu_count': 1},
     1, None, {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT}),
], ids = lambda value: value if isinstance(value, str) else None)
def test_component_resolves_with_todays_precedence_and_records_provenance(title, resources, kwargs,
                                                                         count, memory, provenance):
    node = component(_descriptor(resources = resources), **kwargs)
    assert (node.gpu_count, node.gpu_memory_gib) == (count, memory), title
    assert node.gpu_provenance == provenance, title
    assert node.device_type == kwargs.get('device_type', 'cpu')


@pytest.mark.parametrize('title,spec,kwargs,expect', [
    ('GPU-only component on cpu', {'device': ['gpu'], 'runtime': {'images': {'gpu': 'x:gpu'}}},
     {'device_type': 'cpu'}, ("device_type='cpu'", 'spec.device')),
    ('descriptor count=2 on cpu', {'resources': {'gpu': {'count': 2}}}, {'device_type': 'cpu'},
     ("requires device_type='gpu'", 'descriptor')),
    ('explicit count=2 against a descriptor memory default', {'resources': {'gpu': {'memoryGiB': 20}}},
     {'device_type': 'gpu', 'gpu_count': 2}, ('mutually exclusive', 'gpu_memory_gib=20 (descriptor)')),
    ('explicit memory against a descriptor count default', {'resources': {'gpu': {'count': 2}}},
     {'device_type': 'gpu', 'gpu_memory_gib': 10}, ('mutually exclusive', 'gpu_count=2 (descriptor)')),
], ids = lambda value: value if isinstance(value, str) else None)
def test_component_rejects_cross_source_contradictions_before_any_node_exists(title, spec, kwargs, expect):
    with pytest.raises(GpuRequirementConflict) as excinfo:
        component(_descriptor(**spec), **kwargs)
    err = excinfo.value
    assert err.message.startswith("component 'acme/thing'")
    for needle in expect:
        assert needle in err.message
    assert SOURCE_DESCRIPTOR in err.context['sources'] and SOURCE_NODE in err.context['sources']
    assert 'descriptor' in err.remedy
    # Graph authors catching the documented build-time class keep working.
    with pytest.raises(ValueError):
        component(_descriptor(**spec), **kwargs)
    with pytest.raises(CapabilityError):
        component(_descriptor(**spec), **kwargs)


def test_component_keeps_the_documented_messages_for_its_own_contradictions():
    with pytest.raises(ValueError, match = "gpu_count=2 requires device_type='gpu'") as excinfo:
        component(_descriptor(), device_type = 'cpu', gpu_count = 2)
    assert not isinstance(excinfo.value, GpuRequirementConflict)
    with pytest.raises(ValueError, match = 'device_type=GPU'):
        component(_descriptor(resources = {'gpu': {'memoryGiB': 20}}), device_type = 'cpu', gpu_memory_gib = 10)
    # A device outside a two-entry list is a plain unsupported device, as before.
    with pytest.raises(ValueError, match = 'not supported'):
        component(_descriptor(), device_type = 'tpu')

# -- the compiler's companion document, and nothing on NodeSpec (D8) -------------

def _flow():
    producer = IntProducer(0, 10, name = 'numbers')
    native = IdentityProcessor(name = 'native', device_type = GPU, gpu_count = 2)(producer)
    remote = component(_descriptor(resources = {'gpu': {'memoryGiB': 20}}), name = 'remote',
                       device_type = 'gpu')(native)
    plain = IdentityProcessor(name = 'plain')(remote)
    printer = CommandlineConsumer(name = 'printer')(plain)
    return Flow([printer], flow_type = REALTIME, flow_id = 'prov')


def test_compiler_returns_provenance_alongside_and_specs_carry_nothing_new():
    flow = _flow()
    document = gpu_provenance(flow)
    assert document == {
        'native': {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_NODE, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
        'remote': {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT,
                   FIELD_GPU_MEMORY_GIB: SOURCE_DESCRIPTOR},
        'plain': {FIELD_DEVICE_TYPE: SOURCE_NODE, FIELD_GPU_COUNT: SOURCE_DEFAULT, FIELD_GPU_MEMORY_GIB: SOURCE_DEFAULT},
    }
    specs = compile_flow(flow, envelope_version = 4)
    fields = {f.name for f in dataclasses.fields(NodeSpec)}
    assert 'gpu_provenance' not in fields
    for spec in specs:
        assert set(spec.to_dict()) == fields
        assert 'gpu_provenance' not in spec.params
    by_name = {s.name: s for s in specs}
    assert (by_name['native'].gpu_count, by_name['remote'].gpu_memory_gib) == (2, 20)
    assert by_name['native'].params['gpu_count'] == 2

# -- deploy time: the resource-name merge -----------------------------------------

def _spec(gpu_resource_name = None):
    return NodeSpec('n', 'x.Y', {}, [], 'processor', False, 1, 'gpu', True,
                    gpu_resource_name = gpu_resource_name)


@pytest.mark.parametrize('resolved,default,expected', [
    ('nvidia.com/mig-1g.10gb', 'amd.com/gpu', ('nvidia.com/mig-1g.10gb', SOURCE_STRATEGY)),
    ('nvidia.com/mig-1g.10gb', None, ('nvidia.com/mig-1g.10gb', SOURCE_STRATEGY)),
    (None, 'amd.com/gpu', ('amd.com/gpu', SOURCE_CLI_DEFAULT)),
    (None, None, ('nvidia.com/gpu', SOURCE_DEFAULT)),
    ('', '', ('nvidia.com/gpu', SOURCE_DEFAULT)),           # empty strings are "unset", as before
])
def test_resource_name_provenance(resolved, default, expected):
    assert gpu_resource_provenance(_spec(resolved), default) == expected
    assert resolve_gpu_resource(_spec(resolved), default) == expected[0]


if __name__ == '__main__':
    pytest.main([__file__])
