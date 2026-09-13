'''
Backend contracts: the responsibility boundaries behind a running flow.

Videoflow builds a graph on one machine and executes it on many. Between the graph
and the workers sit a handful of *backends* — a message transport, a payload store,
an accelerator allocator, an execution environment — and a runtime that decides what
correct processing means regardless of which concrete backend is underneath. This
package holds the contracts for those boundaries, the truthful outcome types they
share, and reference in-memory implementations that double as the executable
specification every real adapter is tested against.

- ``MessagingBackend`` (``messaging``): how does an envelope reach its required consumers?
- ``PayloadStore`` (``payload``): where are the image bytes, and how long must they survive?
- ``AcceleratorAllocationBackend`` (``allocation``): which accelerators may this workload use, \
  under what guarantees?
- ``FlowRuntime`` (``runtime``): what constitutes correct processing and recovery?
- the composition planner (``capabilities``): can this graph meet its requested contract?

Two rules run through all of it. Policy lives above adapters: an adapter reports
what it can guarantee, and the planner rejects a request it cannot meet instead
of quietly weakening it. And observations are truthful: a read that failed is
``Unknown``, never zero, never empty, never complete.

Nothing here imports a broker, a store or a cluster client at module scope, so the
contracts can be imported wherever the graph can.
'''
