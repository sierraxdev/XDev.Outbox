# Engineering Decisions

This directory contains durable engineering decisions.

Use decision records when future engineers or AI agents may reasonably ask:

"Why was this implemented this way?"

Suggested naming:

- `001-short-description.md`
- `002-short-description.md`
- `003-short-description.md`

Suggested document structure:

# Decision Title

## Status

Proposed | Accepted | Superseded | Rejected

## Context

What problem or constraint required a decision?

## Decision

What was decided?

## Rationale

Why was this approach selected?

## Constraints

What must future changes preserve?

## Alternatives Considered

What meaningful alternatives were rejected and why?

## Verification

How can future changes verify they still satisfy this decision?

Do not create decision records for trivial implementation choices.
