3. Figure out how to create a Kubernetes integration test for non gpu stuff (errors, etc.)
5. Do research on if we should use kubernetes python api vs what we are using now.
4. Ask the agent to threview the code and (1) find bugs in error handling, (2) find inconsistencies with the documentation in ideal.md file compared to the tests.
5. Ask from ideal_error_handling.md file, why is it that BATCH and REALTIME behave differently under node failure.
6. Confirm that DeviceErrors and other kind of fatal errors reintroduce the worker back.
8. Check why mypy errors is not detecting things that the editor detects in the ui.
9. Introduce github workflows that: (a) tag the repo with a new tag (b) compile a docker image
9.1. Update videoflow-contrib documentation to use published image instead compiling from scratch.
10. Check that Redis data is getting deleted properly as indicated by code.

A latent bug I did not fix, since it's outside this scope: cli.py:373 catches RuntimeError around wait_for_completion, but FlowStalled is a VideoflowError, not a RuntimeError. Cleanup still runs and the exit code is still 5, but the Flow aborted: message at line 395 is unreachable. Worth its own change.
