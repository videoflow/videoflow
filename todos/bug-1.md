Latent bug (not fixed — behaviour change is out of scope)
The lead is real and worse than "latent": compile_flow() raises AttributeError on any flow containing a remote producer component. Confirmed by repro:


File "videoflow/core/compiler.py", line 191, in _validate_remote_node
    policy = node._join_policy
AttributeError: 'RemoteProducer' object has no attribute '_join_policy'
_join_policy is set by ProcessorNode.__init__ and ConsumerNode.__init__, but never by ProducerNode/Node. specs_from_tasks_data line 142 reads it defensively (if hasattr(node, '_join_policy')); line 191 does not, and _validate_remote_node is called for every RemoteNodeMixin including RemoteProducer. Any graph built with component(descriptor_with_role_producer) fails to compile, so a native producer component cannot be deployed at all.

Typing the parameter as RemoteNodeMixin is exactly what surfaces it — I confirmed mypy then reports videoflow/core/compiler.py:191: error: "RemoteNodeMixin" has no attribute "_join_policy" — so the annotation cannot land until the read is guarded. The one-line fix would be policy = getattr(node, '_join_policy', None) (matching line 142), after which node : RemoteNodeMixin type-checks cleanly. I left both the Any and the bug in place; the existing test suite has no coverage for compiling a remote producer, so a fix should add one.
