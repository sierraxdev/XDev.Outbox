# Task: Event lost on explicit rollback after a failed save; identity conflict on retry after a partial EF save

## Status

Complete, uncommitted (2026-11-19). Workflow: Bug Fix. Both found by the D5 adversarial
tests (`tasks/fix-d5-caller-transaction-partial-save.md`).

## Bug 1 — explicit `Rollback()` after a failed save loses the event

After a failed outbox save in a caller transaction, the business entity stays
Added but the event was never successfully saved, so no message is pending. An
explicit `Rollback()` (what warning 41002 advises) drops never-saved events
from the buffer; a later save in a new transaction commits the entity without
an outbox row. Disposing the transaction without `Rollback()` keeps the event
(stale detection at the next save keeps never-saved events).
Pinned by `FailedSave_CallerRollsBackExplicitly_RetryInNewTransactionCommitsUserWithoutOutboxRow`.

**User-approved fix (2026-11-19):** on a transaction ending without commit,
keep never-saved events in the buffer (same as the dispose path), so events
follow their still-pending business changes. B1 is unchanged for saved
events (flag=false kept, flag=true dropped).

**Not approved, investigate and report only:** whether never-saved events
are also dropped on commit ("On commit, committed and never-saved events are
removed from the buffer", `business-rules/message-delivery.md`).

## Bug 2 — retry after EF's own save fails partway throws an identity conflict

If an earlier command of EF's save ran (outbox INSERT received a generated
key) and a later command failed, EF reverts the entry to a temporary key but
its identity map keeps the generated key. `RecoverFailedAttempt` detaches the
outbox entries through the cached `EntityEntry` (no DetectChanges), leaving a
stale identity-map entry. SQLite reuses the rolled-back rowid, so the retry
throws "The instance of entity type 'OutboxMessage<int>' cannot be tracked
because another instance with the same key value..." (owned, caller and
retrying-strategy paths). Latent on SQL Server (identity values not reused).
Fix direction (plain-EF experiment only): detach via a fresh
`context.Entry(entity)` / ensure the identity map is consistent before
detaching. Skipped tests: `tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/RetryAfterPartialEfSaveTests.cs`
and the item-8 test in `tests/XDev.Outbox.EFCore.SqlServer.Tests/TesterCallerSavepointTests.cs`.

## Progress

- [x] Implementation with tests first (developer, 2026-11-19, uncommitted)
  - Bug 1: `EndTransactionWithoutCommit` now releases with `keepUnsavedEvents: true`
    (rollback, failed commit/rollback = dispose). "Never-saved" = buffered event
    matched (by reference/multiplicity) to no pending message of the ended
    transaction and not in `_releasedNotAcceptedEvents`: failed-save events,
    events added after the last save or before the transaction. Commit path
    unchanged (still drops them; pinned by
    `NeverSavedEventsOnTransactionEndTests.NeverSavedEventThenCommit_...`).
  - Bug 2 root cause confirmed (IStateManager probe: after EF's failed save
    the outbox and user entries are still in the identity map under the
    discarded key; `DetectChanges` re-keys them). `RecoverFailedAttempt` now
    calls `EntityEntry.DetectChanges()` before detaching each outbox entry
    (works with AutoDetectChangesEnabled=false). Business entries: same state
    as plain EF; fixed by the retry's DetectChanges (auto on) or the app's
    `ChangeTracker.DetectChanges()` (auto off, as plain EF requires).
  - Tests: new `CallerTransactionPartialSave/NeverSavedEventsOnTransactionEndTests.cs`,
    un-skipped/extended `RetryAfterPartialEfSaveTests.cs`, flipped 8 tests
    asserting never-saved drop on rollback, item-8 SQL Server assertion flipped
    (it encoded the stale entry), new SQL Server end-mode test. Fail-before
    confirmed; all suites green net8–net11 (SQL Server: 1 expected skip).
- [x] Adversarial testing (tester, 2026-11-19): no product defect found in the change.
  - New tests: `tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterRollbackKeepTests.cs`
    (orphan-event hazard, re-added accepted event, event kept across 5 uncommitted transactions,
    rollback then owned save, savepoint rollback + full rollback, failing Rollback (throw in
    TransactionRollingBack / RolledBack), pooled lease reset, retrying strategy + user transaction),
    `.../TesterPartialEfSaveRetryTests.cs` (5 users/orders/events, failure at first/middle/last
    outbox INSERT, first/last Users INSERT, last Orders INSERT; owned/caller/retrying; auto-detect
    on/off; auto-detect off leaves an unrelated undetected modification Unchanged and unsaved;
    an app-tracked OutboxMessage is not detached), SQL Server
    `tests/XDev.Outbox.EFCore.SqlServer.Tests/TesterRollbackKeepAndPartialSaveTests.cs`.
  - Fail-before confirmed in a scratch worktree at HEAD (SQLite and SQL Server): 75 SQLite and
    19 SQL Server cases fail without the fix.
  - Hazard (behavior of the approved fix, pinned, not a defect): after a failed save and an
    explicit Rollback, an app that calls `ChangeTracker.Clear()` but not `Outbox.ClearEvents()`
    gets the orphan event persisted and dispatched by the next save (payload key 0). The dispose
    path already did this; Rollback used to drop it. Document for consumers.
  - Plain EF behavior (not caused by the library): with a retrying strategy around
    BeginTransaction/SaveChanges(false)/Commit, a commit failure followed by the retry of a
    principal plus dependent fails with an FK error on SQLite, with or without events (pinned by
    a baseline test); nothing is committed or dispatched, and the event stays buffered.
  - All suites green net8–net11 (SQL Server: 1 pre-existing skip); 0 leftover smoke DBs.
- [x] Review (reviewer): approve with comments, no blocking. NB-1 (needs
  user acknowledgment): rollback + `ChangeTracker.Clear()` without
  `Outbox.ClearEvents()` persists an orphan event; apps that re-added events
  after a rollback now get duplicates. NB-2: kept never-saved events are
  still dropped by a later commit (commit-path follow-up). NB-3 failed
  rollback test: covered by the tester. NB-4 interceptor XML doc wording:
  fixed by the orchestrator (comment only).
- [x] Verification (orchestrator): build 0 errors; net8–net11:
  `XDev.Outbox.Tests` 26/26, EFCore (SQLite) 883/882 all passed, SQL Server
  268 passed + 1 skipped (`itemsCount: null`); 0 leftover smoke DBs;
  `git diff --check` clean.
- [x] Knowledge updates: `components/efcore-outbox-store.md`,
  `business-rules/message-delivery.md`, `project.md`.

## Open follow-up (needs a user decision)

Commit drops never-saved events (four scenarios, see the component doc).
