# RFC 0000: <title>

- **Status:** draft | proposed | accepted | rejected | superseded
- **Author(s):**
- **Created:** YYYY-MM-DD
- **Protocol version affected:** e.g. 1 → 2, or "no version change"
- **Requirement IDs touched:** e.g. `JOIN-20`, `EOS-3` (from `spec/PROTOCOL.md`)

## Summary

One paragraph: what changes and why, in plain terms.

## Motivation

What problem does this solve? Who is blocked without it? Include the concrete
scenario (a flow topology, a language, a failure mode) that motivates it.

## Why an RFC

The videoflow component protocol is a public API implemented by multiple
independent SDKs (Python, Rust, C/C++, and vendor components in the wild). Any
change to observable **wire** or **routing** behavior can break a deployed vendor
image. This process exists so such changes are deliberate, reviewed, and versioned.

Purely additive, non-observable changes (a new optional metric, a clarified doc
sentence) MAY skip the RFC and go straight to a PR that edits `spec/PROTOCOL.md`.

## Proposal

The precise change. If it edits `spec/PROTOCOL.md`, quote the before/after of each
requirement. If it touches the wire, show the `.proto` diff and state whether it is
`buf breaking`-clean (field-number rules) or requires an envelope version bump.

## Compatibility

- **Wire compatibility:** can old and new envelopes coexist in one run? (Recall a
  run is version-homogeneous — `WIRE-1`.) What is the migration path?
- **Behavioral compatibility:** does an existing component that did nothing about
  this change still work? If not, what is the deprecation window?
- **Protocol version:** does this require a major bump? If so, existing vendor
  images pin `spec.protocol` in their descriptor and the compiler must gate on it.

## Conformance impact

Which scenarios in `conformance/` must be added or changed. New MUST statements
need new scenario IDs; the Python reference implementation must pass first, then
the other SDKs.

## Alternatives considered

What else was weighed, and why this was chosen.

## Open questions

Anything unresolved that reviewers should weigh in on.
