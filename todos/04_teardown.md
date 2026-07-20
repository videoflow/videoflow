`deploy`'s broker teardown can't reach the in-cluster NATS from the host
  (`nats://nats.default.svc:4222`), so it prints a "teardown skipped" hint with a manual
  `videoflow teardown` command each run. Pre-existing and unrelated to the image work, but it
  means run streams accumulate in a long-lived broker.
