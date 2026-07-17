Language-agnostic components
============================

A node does not have to be Python. Videoflow defines a **language-agnostic wire and
runtime contract** so a component can be written in any language, shipped as its own
container image, and dropped into a Python-authored graph by reference. This is the
foundation for a component **marketplace**.

The Python process only ever *builds and compiles* the graph. A remote component's
``next`` / ``process`` / ``consume`` run out-of-process in the vendor image, driven by
that image's own SDK speaking the protocol; the Python worker never imports the
component.

The ``component()`` factory
---------------------------

You wire a remote component in with the ``component()`` factory instead of importing a
class::

    from videoflow.core import Flow, component
    from videoflow.core.constants import BATCH

    def build_flow():
        reader  = component('oci://ghcr.io/acme/camera-reader:1.0.0',
                            params={'address': 'rtsp://...'}, name='reader')
        tracker = component('oci://ghcr.io/acme/sort-tracker:1.2.0',
                            params={'max_age': 30})(reader)     # a Rust/C++/... node
        sink    = component('./my-consumer')(tracker)           # a local descriptor dir
        return Flow([sink], flow_type=BATCH)

A remote node behaves like a normal producer / processor / consumer for wiring,
validation, scaling (``nb_tasks``, ``partition_by``) and manifest generation; the
compiler records a ``component_ref`` and descriptor instead of a Python class.

Component descriptors
---------------------

A component is described by a ``component.yaml`` (validated against
``spec/descriptor/component-schema.json``) declaring its params, inputs/outputs,
device support, protocol version, and the container image(s) to run. A descriptor with
a ``spec.runtime.pythonClass`` names a Python node the worker imports directly; without
one it is a **native** component that runs its own image entrypoint.

Validate any descriptor before shipping it::

    videoflow component validate ./sort-tracker/component.yaml

Publishing and consuming (OCI)
------------------------------

Descriptors are distributed as **OCI artifacts** (media type
``application/vnd.videoflow.component.v1+yaml``) alongside the images they reference,
so a consumer can inspect a component's contract without pulling multi-gigabyte ML
images. An ``oci://`` reference in ``component()`` is pulled and cached under
``~/.videoflow/components/`` automatically::

    videoflow component push    ./sort-tracker oci://ghcr.io/acme/sort-tracker:1.2.0
    videoflow component inspect oci://ghcr.io/acme/sort-tracker:1.2.0   # params/io, no images
    videoflow component pull    oci://ghcr.io/acme/sort-tracker:1.2.0 --verify   # cosign

See ``spec/DISTRIBUTION.md`` for the reference grammar and publishing model.

The wire protocol and spec
--------------------------

Cross-language interop runs over a **protobuf envelope (wire v4)** with well-known
payload types (``Tensor``, ``Frame``, ``Detections``, ``Tracks``, ``BlobRef``,
``Value``). Pure Python flows are unaffected — they keep using the msgpack wire by
default; a flow that contains any remote or native component automatically upgrades to
v4. A mixed flow that would need Python pickle on the wire is a hard compile error.

The normative contract lives in the ``spec/`` directory: ``spec/PROTOCOL.md`` (protocol
v1 — every requirement an SDK must implement, with stable IDs), the protobuf IDL under
``spec/proto/videoflow/v1/``, and golden test vectors in ``spec/vectors/`` replayed
against every SDK to enforce lockstep. A vendor can hand-write a conforming component
against the spec today; the Python worker is the executable reference implementation.
