**Superseded by the two-mode GPU redesign** (RFC 0004, `--gpu-mode mix`): nodes
declare `gpu_memory_gib`, the layout solver in `videoflow/deploy/mig.py` computes
per-card MIG geometry (whole cards reserved for spanners, sharers packed into the
smallest fitting profiles), and the `MixGpu` strategy's `prepare()`/`cleanup()`
apply and restore it for the run via the GPU Operator's `nvidia.com/mig.config`
label. That is exactly the "partition" idea sketched below, built on MIG (hard
isolation) rather than time-slicing values.

Not carried over, still open if ever wanted:
- Deploy-time *time-slicing* reconfiguration (set replicas for the run, revert
  after) — rejected for now: no memory isolation, so "an exclusive slice of
  ≥ N GiB" would be a promise with no hardware backing.
- A test run that *measures* a component's GPU memory need to derive
  `gpu_memory_gib` automatically (original item 3 below).

Original notes:
- Explore another GPU mode where the GPU shared time slicing values are set at deployment time depending on the number of GPU needs of the flow (in case there are less GPUs currently than what needed by the flow), and then reverted back to what it was before the run.
    - maybe, for gpu based tasks, allow the user to specify as an optional the amount of ram that task consumes in the gpu. Then let videoflow do calculations on how to distribute per physical gpu based on that task. remember that videoflow could be in the situation of not having good visibility on the number of physical gpus in the cluster, unless it takes control of the setup of the config mapping at runtime. This sohuld be a new way, added to the old way, of adding gpus (fail | share | partition)
- Explore alternatives on how to do a test run to measure the components GPU memory need and set up partitioning of GPUs based on those measurements.
