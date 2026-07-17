Time-synchronized joins
=======================

By default a multi-parent join groups its inputs by **lineage**: the halves it
combines must descend from the same originating message of one producer — a diamond
that fans out and reconverges (``mode='trace'``). That is the right model when the
branches share an upstream ancestor.

To fuse streams from **independent** producers — several cameras plus sensors, none
sharing an upstream — group by **event time** instead::

    from videoflow.core.policies import JoinPolicy

    fused = FusionProcessor(name='fuse', join_policy=JoinPolicy(
        mode='time',            # group by event_ts, not trace lineage
        tolerance_ms=8,         # messages within 8ms are the same moment (< one 60fps frame)
        timeout_seconds=0.05,   # lateness bound: how long to wait for stragglers
        quorum=6,               # emit once >= 6 of N cameras are present (missing ones -> None)
        collect={'imu': 25},    # high-rate parent: deliver every sample within 25ms as a list
    ))(cam1, cam2, cam3, cam4, cam5, cam6, cam7, cam8, imu)

Event timestamps
----------------

Each input carries an **event timestamp** (epoch seconds) that a producer stamps and
that travels with the message through the whole flow — downstream nodes inherit it
automatically. A producer stamps it via ``ctx.set_event_timestamp(ts)``; the built-in
``VideostreamReader`` does this per frame (``timestamp_source='clock'`` for live
streams, ``'position'`` for synchronized recordings).

A fusion node reads each input's exact time from ``ctx.input_info`` (per-parent
``event_ts`` / ``metadata``) so it can interpolate between samples of a high-rate
parent. Cross-device time accuracy itself is an operations concern — genlocked cameras
and PTP/NTP-disciplined hosts — the framework aligns on whatever timestamps it is
given.

Scaling
-------

A time-aligned join runs with ``nb_tasks=1``: every parent's half must reach the same
worker to be grouped. Scale the per-stream work in the nodes **upstream** of the
fusion node instead.

Backward compatibility
----------------------

``mode='trace'`` is the default and never reads ``event_ts``, so existing flows —
including ones whose producers stamp no time at all — behave exactly as before. A
producer that never calls ``ctx.set_event_timestamp`` still gets an event time on the
wire: its publish wall-clock, which trace-mode joins ignore and which serves as a
sensible fallback if such a stream is later fed into a ``mode='time'`` join.

See ``examples/multicamera_time_sync.py`` for a runnable multi-camera fusion flow.
