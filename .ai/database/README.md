# Database

This directory contains persistence knowledge that materially affects
engineering behavior.

Document topics such as:

- important tables;
- stored procedures;
- views;
- triggers;
- constraints;
- transaction boundaries;
- concurrency behavior;
- duplicate handling;
- idempotency;
- database-generated values;
- environment-specific differences;
- legacy persistence behavior.

Prefer separate documents for meaningful persistence areas.

Examples:

- `shipping-import.md`
- `settlements.md`
- `fulfillment.md`

Do not store database credentials or secrets here.

When database behavior is part of a business rule, link to the relevant
business-rule document rather than unnecessarily duplicating it.
