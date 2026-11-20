# Task: Commit drops never-saved events

## Status

Complete (2026-11-19). Workflow: Bug Fix. User-approved behavior change.

## Problem (confirmed by code and tests)

`IOutboxTransactionParticipant.TransactionCommitted` updates the buffer with
`keepUnsavedEvents: false`, so a commit removes buffered events that were not
saved in that transaction, while their business changes stay pending. A later
save commits the business changes without an outbox row (silent event loss).
Scenarios (pinned by
`NeverSavedEventsOnTransactionEndTests.NeverSavedEventThenCommit_...`):

1. Event added before `BeginTransaction`; the transaction commits without a
   save (e.g. raw SQL only).
2. Event added after the last save in the transaction; commit.
3. A save fails (undone by the library savepoint) and the caller commits.
4. Never-saved events kept by an earlier rollback/dispose; a later
   transaction commits without saving them.

## Approved fix

Keep never-saved events in the buffer on commit too, so only a successful
save-and-commit, `Outbox.ClearEvents()`, or the end of the context/lease
removes an event. Consequence (accepted, same as the rollback change): an app
that abandons its unit of work must call `Outbox.ClearEvents()`.

## Progress

- [x] Implementation with tests first (developer, 2026-11-19, uncommitted)
  - `TransactionCommitted` now calls `UpdateBuffer(..., keepUnsavedEvents: true)`; every caller now passes
    true. Committed events still removed/dispatched; B1 for stale/released saved events unchanged. XML docs
    updated (interceptor summary, participant, state-machine header, `UpdateBuffer`).
  - Consequence (code-confirmed): `_releasedNotAcceptedEvents` and the `keepUnsavedEvents` parameter no longer
    affect the buffer (candidates for a later cleanup; not removed here).
  - Tests: new `CallerTransactionPartialSave/NeverSavedEventsOnCommitTests.cs` (4 scenarios x sync/async x
    flag, kept across several commits, abandon + `ClearEvents()`, pooled lease) and SQL Server
    `TesterCallerSavepointTests.FailedSaveThenCommit_EventKept_...`; flipped/renamed the pin
    (`NeverSavedEventThenCommit_EventKept_...`), `CallerTransactionPartialSaveTests.FailedSaveAfterCallerSavepoint_...`
    final assertion, `ReleasedEventTrackingTests.ReleasedEvent_ClearedThenChangesAcceptedThenReAdded_...`.
    Fail-before confirmed (SQLite 34 cases, SQL Server 8). All suites green net8-net11 (SQL Server 1 expected skip).
  - Found, not changed: an owned save clears the whole buffer, so an event added during the save
    (e.g. `SavingChanges` handler/interceptor) is dropped without a row (probe-confirmed); the caller path keeps it.
- [x] Adversarial testing (tester, 2026-11-19): no product defect found in the change.
  - New tests: `tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterNeverSavedOnCommitTests.cs`
    (42 cases: two saves + event after last save; D-C via accepting save without events; SaveChanges(false) +
    direct AcceptAllChanges + commit; user savepoint rollback then re-save / then commit without re-save (B1);
    same instance added 3x (multiplicity); commit failing before the server; server committed then an app
    interceptor throws in TransactionCommitted; server committed then the queue throws during dispatch;
    EnsureCreated's internal transaction commit; ExecuteDelete + event + commit; retried unit of work adding an
    event without a save; retrying strategy + user transaction with a failed commit; SavingChanges handler adding
    an event during owned and caller saves; owned save after a kept event) and SQL Server
    `tests/XDev.Outbox.EFCore.SqlServer.Tests/TesterNeverSavedOnCommitTests.cs` (added-after-last-save and
    failed-save-after-successful-save with identity user+orders, x sync/async x flag; doomed transaction
    (XACT_ABORT) then Commit instead of Rollback).
  - Fail-before confirmed in a scratch worktree at HEAD: 20 SQLite and 8 SQL Server cases fail without the fix;
    the others (no-duplicate invariants, commit-unknown characterization, doomed commit) pass on both, i.e. unchanged.
  - Confirmed: no event committed in a transaction is saved again (multiple saves, savepoints, D-C,
    AcceptAllChanges, retries); no phantom dispatch (doomed commit throws `InvalidOperationException` "This
    SqlTransaction has completed", nothing dispatched). An EF-internal commit on the context (EnsureCreated DDL)
    used to drop buffered events; it now keeps them.
  - Commit-unknown (characterization, unchanged by the fix): when the server commits and an app interceptor
    registered before the library's throws in TransactionCommitted, EF reports a failed Commit and the library
    applies B1: flag=false saved events are kept and written again by the next save (duplicate row, like plain EF's
    re-run); flag=true dropped, not dispatched (poller). When the library's TransactionCommitted already ran
    (e.g. the queue throws), nothing is released or rewritten.
  - Consumer hazards (behavior of the approved fix, pinned): an event recorded for a change made without
    SaveChanges (ExecuteUpdate/Delete, raw SQL, another context) is no longer dropped at commit; it is written by
    the context's next, unrelated save in a different transaction (or lost if the context ends first). A retried
    unit of work that adds such an event per attempt gets one row per attempt.
  - Known gap pinned (not changed): an event added by a SavingChanges handler during an owned save is dropped
    without a row (`CompleteOwnedSave` clears the buffer); in a caller transaction it is now kept and written by the
    next save.
  - Results net8-net11: XDev.Outbox.Tests 26/26; EFCore (SQLite) 946 passed (947 on net8, which has one
    net8-only case); SQL Server 288 passed + 1 pre-existing skip; 0 leftover smoke DBs.
- [x] Review (reviewer): approve with comments, no blocking. Owned-save drop
  of events added during the save (open, separate decision); inaccurate
  comment in `MarkPendingChangesAccepted` fixed by the orchestrator (comment
  only); `ReleasedEventTrackingTests` class summary outdated (test doc only).
- [x] Verification (orchestrator): build 0 errors; net8–net11:
  `XDev.Outbox.Tests` 26/26, EFCore (SQLite) 947/946 all passed, SQL Server
  288 passed + 1 skipped (`itemsCount: null`); 0 leftover smoke DBs;
  `git diff --check` clean; only production code change is the one argument.
- [x] Knowledge updates: `components/efcore-outbox-store.md`,
  `business-rules/message-delivery.md`, `architecture.md`, `project.md`.

## Open follow-ups

- Owned save drops events added during the save (needs a decision).
- Cleanup of `_releasedNotAcceptedEvents` / `keepUnsavedEvents`: done
  (`tasks/cleanup-released-not-accepted-events.md`).
