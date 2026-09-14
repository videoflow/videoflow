'''
Reference in-memory backends: the executable specification.

Each module here implements one contract faithfully enough that the conformance
suite can run every model-level case against it — retention limits, leases and
redelivery, dedup windows, obligation ledgers, compare-and-swap ownership, MIG
geometry — under a fake clock and the fault barriers in ``videoflow.backends.faults``.
They are not test doubles that return canned answers: a real adapter must produce
the same outcomes for the same schedule, which is what makes the same conformance
runner usable for every provider.
'''
