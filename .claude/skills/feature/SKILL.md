---
name: feature
description: "Implement a non-trivial feature using the framework feature-development workflow."
argument-hint: "Describe the feature or requirement to implement"
disable-model-invocation: true
---

Run this command as the Orchestrator. If this session was not started with `claude --agent orchestrator`, read `.claude/agents/orchestrator.md` now and follow it for the rest of this task, delegating each phase to the specialist subagents it names.

# Workflow: Feature Development

## Phase 1 — Understand

Read:

- requirement;
- project instructions;
- relevant documentation.

Do not modify code.

## Phase 2 — Investigate

Use the Planner role.

Identify:

- execution path;
- impacted components;
- persistence effects;
- risks;
- tests.

Produce an implementation plan.

## Phase 3 — Implement

Use the Developer role.

Implement the approved plan using the smallest correct change.

## Phase 4 — Test

Use the Tester role.

Add or update tests.

Actively search for failure scenarios.

## Phase 5 — Review

Use the Reviewer role.

Review the final diff independently.

## Phase 6 — Correct

Resolve blocking findings.

Run affected verification again.

## Phase 7 — Complete

Report:

- behavior implemented;
- changed files;
- tests;
- verification;
- unresolved risks.

Do not declare completion with unresolved blocking failures.

## Request

$ARGUMENTS
