# Business Rules

This directory contains durable observable business behavior.

Create separate files by meaningful business capability.

Examples:

- `shipping-import.md`
- `settlements.md`
- `fulfillment.md`
- `shipping-export.md`

A useful document structure is:

- Purpose
- Inputs
- Preconditions
- Business Rules
- State Transitions
- Persistence Effects
- Duplicate / Repeated Execution Behavior
- Failure Behavior
- Edge Cases
- Legacy Behavior
- Known Intentional Differences
- Evidence

Business-rule documentation should answer:

"What behavior must future implementations preserve?"

Avoid documenting implementation details that are already obvious from
source code.
