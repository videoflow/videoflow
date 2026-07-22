~~4. resources.gpu on a producer/consumer descriptor is silently ignored.~~ **Done**
(two-mode GPU redesign): descriptor validation now rejects `spec.resources.gpu` on
non-processor roles at load, and `component()` rejects explicit `gpu_count=` /
`gpu_memory_gib=` arguments for producer/consumer components
(`tests/test_component_descriptor.py::test_descriptor_rejects_gpu_resources_on_non_processor_roles`,
`tests/test_remote_component.py::test_gpu_count_on_a_non_processor_component_is_rejected`).


Integration tests:
A broker-backed test running a device_type=GPU pure-Python node under the local engine (e.g. IdentityProcessor(device_type=GPU) in a small flow). CI has no GPUs, so it proves the critical compatibility property: a GPU-typed flow on a GPU-less host launches, completes, and sets no CUDA_VISIBLE_DEVICES — i.e. the new probe can't break the long-standing CPU-fallback behavior. On a GPU dev box the same test exercises real masking for free. This is the one genuine gap.
