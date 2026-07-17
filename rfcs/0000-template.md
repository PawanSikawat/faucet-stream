# RFC 0000 — Template

*Copy this file to `rfcs/NNNN-short-title.md` and fill in every section. Delete this italic line.*

| | |
|---|---|
| **RFC** | 0000 |
| **Title** | *A short, descriptive title* |
| **Status** | Draft \| Discussion \| Accepted \| Rejected \| Implemented |
| **Authors** | *your name / handle* |
| **Related issues** | *#NNN, epic #38* |
| **Related ADRs** | *docs/adr/NNNN-*.md, if any* |

## Summary

One paragraph. What does this RFC propose, in plain terms? A reader should be
able to decide from this section alone whether the rest is relevant to them.

## Motivation

Why are we doing this? What problem does it solve, and for whom (framework
maintainers, connector authors, or end users)? Tie the motivation back to a
concrete pain in the current implementation — cite the code or behaviour that is
inadequate today, with file paths. Explain what happens if we do nothing.

## Guide-level explanation

Explain the proposal as if it were already implemented and you were teaching it
to a contributor. Use the project's [terminology](../docs/architecture/README.md)
(page, bookmark, checkpoint, commit token, connector, run). Show the new config,
API, or workflow with realistic examples. This section is the "what it feels
like to use"; the next is the "how it actually works".

## Reference-level explanation

The precise design. Trait signatures, data structures, config field names,
feature flags, module locations, and how the change interacts with the existing
execution flow. Be specific enough that the implementation PR is a transcription,
not a second design exercise. Call out how the change preserves the
[design invariants](../docs/architecture/invariants.md) — especially the
write → flush → checkpoint ordering — or argue why an invariant must change.

## Drawbacks

Why might we *not* do this? Every non-trivial change has real costs — added
surface area, a second code path to maintain, a harder story for connector
authors, performance regressions, or reduced object-safety. List them honestly.
An RFC with no drawbacks section is under-examined.

## Rationale and alternatives

- Why is this design the best of the ones considered?
- What other designs were seriously evaluated, and why were they rejected?
- What is the impact of not doing this at all?

At least one genuine alternative is required.

## Prior art

How do comparable systems solve this — other Rust data tooling, Arrow, dbt,
Airflow/Dagster, Kafka Connect, Singer/Meltano? What can we learn or borrow, and
where do our constraints differ?

## Unresolved questions

What is deliberately left open for the discussion or the implementation to
settle? Distinguish "must resolve before Accepted" from "can resolve during
implementation".

## Future possibilities

What does this unlock or make easier later? Note follow-on work without
committing to it.

## Related

- [RFC process](./README.md)
- [Documentation hub](../docs/README.md)
