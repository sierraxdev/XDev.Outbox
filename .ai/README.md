# Project AI Knowledge

This directory contains persistent engineering knowledge for this repository.

Its purpose is to preserve project-specific context across development
sessions, AI conversations, engineers, and specialized AI agents.

This directory describes how this specific system works.

Generic engineering standards belong in the shared AI Engineering Framework,
not here.

## Structure

- `project.md` - project purpose, technology, terminology, repository layout,
  constraints, and verification commands.
- `architecture.md` - architecture, boundaries, dependency direction, and
  architectural constraints.
- `business-rules/` - durable observable business behavior.
- `components/` - important application components and responsibilities.
- `database/` - persistence behavior and database constraints.
- `integrations/` - external-system and messaging behavior.
- `legacy/` - authoritative legacy behavior and parity information.
- `decisions/` - durable engineering decisions and rationale.
- `tasks/` - active engineering context that must survive sessions.

## Evidence

Project documentation provides engineering context but may become outdated.

When documentation conflicts with source code, executable tests, database
constraints, integration contracts, or observed runtime behavior, investigate
the discrepancy.

Do not silently assume documentation or implementation is correct.

## Durable vs Temporary Knowledge

Durable knowledge belongs in the appropriate project knowledge document.

Examples:

- confirmed business rules;
- architectural constraints;
- confirmed database behavior;
- integration contracts;
- intentional legacy differences;
- accepted engineering decisions.

Temporary task information belongs under `tasks/`.

Examples:

- hypotheses;
- debugging observations;
- failed approaches;
- intermediate investigation results;
- unresolved questions.

Do not promote unconfirmed hypotheses into durable project knowledge.

## Security

Do not store passwords, API keys, access tokens, private certificates,
connection-string secrets, production credentials, or other authentication
material in this directory.
