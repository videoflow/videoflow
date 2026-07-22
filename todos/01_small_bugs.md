4. resources.gpu on a producer/consumer descriptor is silently ignored. The schema allows spec.resources.gpu on any role, but only the processor branch of component() (remote.py:205-213) reads it. A producer descriptor declaring count: 2 validates cleanly and does nothing — no error, no warning. Descriptor validation should reject (or the docs should state) that resources.gpu is processor-only.


Integration tests:
A broker-backed test running a device_type=GPU pure-Python node under the local engine (e.g. IdentityProcessor(device_type=GPU) in a small flow). CI has no GPUs, so it proves the critical compatibility property: a GPU-typed flow on a GPU-less host launches, completes, and sets no CUDA_VISIBLE_DEVICES — i.e. the new probe can't break the long-standing CPU-fallback behavior. On a GPU dev box the same test exercises real masking for free. This is the one genuine gap.
