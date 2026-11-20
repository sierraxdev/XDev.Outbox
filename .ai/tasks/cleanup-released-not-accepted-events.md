# Task: Remove `_releasedNotAcceptedEvents` and the `keepUnsavedEvents` parameter

## Status

Complete (2026-11-20). Workflow: refactor (behavior-preserving cleanup),
requested by the user.

## Background

Since never-saved events stay buffered at every transaction end
(`tasks/fix-rollback-event-loss-and-retry-identity-conflict.md`,
`tasks/fix-commit-drops-never-saved-events.md`), every caller of
`UpdateBuffer` passes `keepUnsavedEvents: true`, and the
`_releasedNotAcceptedEvents` list in `src/XDev.Outbox.EFCore/OutboxDbContext.cs`
reportedly no longer affects which events stay buffered (code-confirmed by the
developer of the commit fix; to be re-verified).

## Goal

Remove the dead state and parameter with no observable behavior change. All
existing tests must pass unchanged (tests that only inspect removed internals,
if any, may be adapted).

## Planning findings (planner, 2026-11-20, code-traced)

- Confirmed dead: `_releasedNotAcceptedEvents` (only read in the `UpdateBuffer`
  branch "matches list -> keep", which equals the fall-through keep when
  `keepUnsavedEvents` is true; all other uses only mutate the list),
  `keepUnsavedEvents` (all 4 call sites + pass-through pass true),
  `ForgetReleasedEventsNotBuffered` (list-only).
- Not referenced by tests (no InternalsVisibleTo/reflection);
  `IOutboxTransactionParticipant` is internal and unaffected; all edits private.
- Stays: `SavedOutboxMessage.AcceptedChanges` and `MarkPendingChangesAccepted`
  (B1: released accepted message -> event removed).
- Remaining-pending branch in `UpdateBuffer` is also outcome-redundant;
  decision (orchestrator, planner's recommendation): keep it (out of the
  requested scope; documents intent).
- Edits: `OutboxDbContext.cs` only. Guarding tests: Round2/ReleasedEventTrackingTests,
  NeverSavedEventsOnCommitTests, NeverSavedEventsOnTransactionEndTests,
  TesterRollbackKeepTests, TesterNeverSavedOnCommitTests,
  CallerTransactionRound2Tests (MixedFlags), pooled tests; no test changes expected.

## Progress

- [x] Planning (planner): confirm dead code, list every use
- [x] Implementation (developer, 2026-11-20): `OutboxDbContext.cs` only (+14/-75). Removed
  `_releasedNotAcceptedEvents` (field + all writes), `ForgetReleasedEventsNotBuffered` and its call,
  `keepUnsavedEvents` (UpdateBuffer, ReleasePendingOutboxMessages, all call sites); UpdateBuffer fall-through is
  now an unconditional keep; released-branch keep/remove by `AcceptedChanges` (B1) and remaining-pending branch
  kept; class header and UpdateBuffer XML doc updated. Grep of `*.cs` in src/ and tests/ for the removed names:
  0 hits. Build 0 errors, no new warnings. Tests (no test changes), net8/9/10/11: Core 26 each; EFCore
  1075/1074/1074/1074; SQL Server (LocalDB) 342 each, 0 failed, 0 skipped; 0 leftover XDevOutboxSmoke_ DBs.
- [x] Review (reviewer): APPROVE, no findings. Independently confirmed the
  removed branch was outcome-identical to the fall-through keep; B1 intact;
  comments accurate. Could not run git: the orchestrator confirmed the diff
  scope.
- [x] Verification (orchestrator): HEAD (20f57ee) call sites all passed
  `keepUnsavedEvents: true` (git show); non-comment diff contains only the
  planned removals; build 0 errors; net8–net11 Core 26, EFCore 1075/1074,
  SQL Server 342, 0 failed/skipped, no test changes; 0 leftover smoke DBs;
  `git diff --check` clean.
- [x] Knowledge updates: `components/efcore-outbox-store.md` (removed-as-dead
  note). Follow-ups in earlier task docs marked done.
