# Solution-testing workflow audit

Reviewed **2026-09-20**, against core `73e3b84` and contrib `4573c4c`.

**Status: Partially resolved.** Image-build delegation is implemented; reducing preliminary work remains a workflow preference.

The [solution verifier](../../videoflow-contrib/.claude/agents/solution-verifier.md#L64) explicitly delegates image building to `videoflow deploy`, with `run-local` building and reusing the same image (lines 64–71). Its bootstrap leaves the first image build to deploy (lines 110–112), and its per-solution loop starts with `run-local` building and executing the solution (lines 118–125). The original concern about separate preliminary Docker builds can be closed.

The agent still requires every precondition probe before building (lines 43–47) and a render check between the local run and cluster deployment (lines 121–125). The [verification runbook](../../videoflow-contrib/.claude/docs/DEPLOY_VERIFY.md#L14) lists those broad preconditions (lines 14–45). This only partially satisfies the request to get to execution sooner; it is an efficiency follow-up, not an outstanding image-build bug.

**Remaining work:** if the shorter workflow is still desired, narrow the initial checks to prerequisites of the next command, defer cluster-only checks until deployment, and reserve additional diagnostics for failures or a specific validation need. Preserve the existing command-owned builds.

**Verification:** inspected the agent and runbook; no solution, Docker build, or live cluster deployment was run. These findings establish what the instructions require, not how much time a future agent will spend following them.

## Historical notes (preserved)

Explore a plan that modifies the solution testing agent so that it does not do so many preliminary things but instead goes straight into videoflow run-local and videoflow deploy instead, so that the building of dockerfiles happens right with that command and not in the preliminaries.
