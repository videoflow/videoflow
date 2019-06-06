# Roadmap of Videoflow up to v1.0

## v0.3
- Add a Sequence processor.
- Add ability to compose subflows.
- Add processors for:
    - Top-down human pose estimation.
    - Semantic segmentation using tensorflow published models.
    - Pose annotation of images.
    - Mask annotation of images.
    - Basic image manipulation operations.
- Reimplement batch execution engine so that it uses microbadges.

## v0.4
- Logging of events, flow performance and errors.
- Allow user to determine what to do when in presence of errors:
    - Stop flow
    - Drop error frames and continue
    - Other?
- v0.4 is the ground work for v0.5 and v0.9

## v0.5
- Commandline tools to detect bottlenecks by reading logs.
- Commandline tools to debug errors in flow.

## v0.6 
- Automatic detection of bottlenecks

## v0.7
- Automatic scaling/decrease of resources in flow without stopping it.
- Will require some changes and augmentations to current execution engines.

## v0.8
- Processors:
    - SLAM
    - Advanced tracking techniques.
    - Others

## v0.9
- Flowboard: Something similar to tensorboard, but for flows.

## v1.0
- Nothing new to add here.

.
.
.

# v2.0
- Distributed version of **Videoflow**


