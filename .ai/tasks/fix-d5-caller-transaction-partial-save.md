# Task: D5 — partial save inside a caller transaction duplicates rows on retry

## Status

Complete, uncommitted (2026-11-19). Workflow: Bug Fix. The user asked to fix D5
(previously deferred, see `decisions/001-deferred-acceptance-and-payload-projection.md`).

## Problem (confirmed on SQLite and SQL Server)

In a caller-owned transaction, an outbox save runs `base.SaveChanges(false)`
(business + outbox INSERTs; EF wraps it in its own savepoint and releases it
on success), then the payload UPDATE that writes store-generated keys into
payloads (Rule A). If the UPDATE (or anything after the base save) fails, the
library restores the tracker to "not saved" so the save can be retried, but
the INSERTed rows remain in the caller's open transaction. Retrying in the
same transaction and committing persists them twice. SQL Server evidence:
3 Users rows instead of 2, 3 outbox rows; the failed attempt's outbox row has
default keys and is later dispatched by the poller.

Not affected: library-owned transactions (whole transaction rolls back);
callers who roll back after a failure.

Regression tests (skipped today):
- SQLite: the D5 test in `tests/XDev.Outbox.EFCore.Tests/Adversarial/AdversarialSaveTests.cs`.
- SQL Server: `CallerTransactionTests.CallerTransaction_SecondSavePayloadUpdateFails_...`
  in `tests/XDev.Outbox.EFCore.SqlServer.Tests`.

## Confirmed root cause (planner, 2026-11-19)

EF's automatic savepoint (`__EFSavePoint`, created by `BatchExecutor` when
`SupportsSavepoints && AutoSavepointsEnabled`; same on EF 8 and main) covers
only `base.SaveChanges(false)`. Payload re-serialization, projection, restore,
the per-message payload UPDATEs and `CompleteOutboxEntries` run afterwards
with no database-side undo, while `RecoverFailedAttempt` restores only the
tracker. After such a failure the transaction still holds the business
INSERT/UPDATE/DELETE, outbox rows with pre-projection payloads, and any
UPDATEs already run. Expected (unverified) further symptoms on retry: PK
violation (client keys), `DbUpdateConcurrencyException` (Deleted /
concurrency-token rows). Out of scope (plain-EF parity): an `AcceptAllChanges`
failure in `CompleteAmbientSave`.

## Design

Library savepoint `__XDevOutboxSavePoint` (SQL Server limit 32 chars; distinct
from `__EFSavePoint`), only on the caller-transaction branch with events:
created inside the try before `RunAttempt`, released inside the try on
success; on failure `RollbackToSavepoint` (+ best-effort release; async uses
`CancellationToken.None`), then `RecoverFailedAttempt`, then rethrow the
original exception. Interceptor/participant unchanged: the `_saveDepth` skip
is correct because the savepoint postdates all pending rows. Owned path
unchanged. Cost: one `SAVE TRANSACTION` round trip per outbox save in a caller
transaction on SQL Server.

Open user decisions: Q1 unsupported savepoints (MARS), Q2
`AutoSavepointsEnabled=false`, Q3 EF's inner savepoint, Q4 rollback-failure
diagnostics. Full test plan in the planner report (failure at each
post-insert step, Deleted/rowversion/Guid, caller savepoints, two saves,
interceptor events, MARS, pooling, `UseTransaction`).

## Progress

- [x] Investigation and design (planner)
- [x] User decisions Q1–Q4 (2026-11-19), all as recommended:
  - Q1: savepoints unsupported (MARS / `SupportsSavepoints` false) → fall
    back to today's behavior, document "roll back after a failed save".
  - Q2: `AutoSavepointsEnabled == false` → honor it (no library savepoint).
  - Q3: keep EF's inner `__EFSavePoint` (+1 SQL Server round trip).
  - Q4: on rollback failure rethrow the original exception and log a library
    warning (EventId 41002: outbox save failed in a caller transaction and
    could not be undone; roll back the transaction). Also logged on the Q1/Q2
    fallback failure path.
- [x] Tests first, implementation (developer, 2026-11-19)
  - `OutboxDbContext`: caller-transaction branch with events (sync/async) wraps
    `RunAttempt` in `__XDevOutboxSavePoint` when `AutoSavepointsEnabled &&
    SupportsSavepoints` (read once, AutoSavepointsEnabled first); failure →
    rollback to it (async: `CancellationToken.None`) + best-effort release →
    `RecoverFailedAttempt` → `throw;`. Rollback failure swallowed + warning
    41002 `XDev.Outbox.EFCore.CallerTransactionSaveNotUndone` (with the
    rollback exception). No-savepoint fallback logs 41002 only when EF's save
    had completed (`SaveAttempt.BaseSaveCompleted`). Interceptor/participant
    logic unchanged (comments only).
  - Tests: SQLite `tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/`
    (58 cases; 46 failed before the fix, 12 caller-savepoint/no-warning guards
    passed before), D5 un-skipped in `AdversarialSaveTests`; SQL Server
    `CallerTransactionPartialSaveTests` (rowversion → DbUpdateConcurrencyException
    and client Guid key → PK violation before the fix; MARS success + fallback),
    D5 un-skipped in `CallerTransactionTests`, `SmokeHostOptions.MultipleActiveResultSets`.
  - Confirmed: event order of a failed save is Created(lib), Created(EF),
    Released(EF), RollingBack/RolledBack(lib), Released(lib); no existence SELECT.
  - Found (pre-existing, not D5, out of scope): on SQLite, when EF's own save
    fails after an earlier command of the same save ran (outbox INSERT, then
    Users INSERT fails), retrying the save throws "OutboxMessage<int> cannot be
    tracked ... same key" — owned and caller paths, also without this change.
- [x] Review (reviewer, 2026-11-19): approve with comments, no blocking.
  - N1 (medium): untested error paths — savepoint create fails, release after
    success fails, best-effort release after rollback fails; also pooled and
    `UseTransaction` cases.
  - N2: without savepoints, a failure inside EF's own save after some commands
    ran leaves rows with no 41002 (plain-EF parity) — document "roll back
    after any failed save".
  - N3: 41002 wording assumes changes were written; advice still correct.
  - N4: MARS logs EF's `SavepointsDisabledBecauseOfMARS` per save (noise).
  - The pre-existing identity-conflict defect is plausible and unrelated;
    hypothesis: stale identity-map key after EF discards the generated outbox
    Id, with SQLite reusing the rowid. Unverified.
- [x] Adversarial testing (tester, 2026-11-19). New tests only, no src change:
  SQLite `CallerTransactionPartialSave/TesterAdversarialSavepointTests.cs` (+`TesterSavepointHost.cs`) and
  `RetryAfterPartialEfSaveTests.cs` (item 8, skipped with reason); SQL Server `TesterCallerSavepointTests.cs`
  (+`TesterGuidKeyCallerSavepointTests`) and `TesterDoomedTransactionTests.cs`. Against the original code 40 of
  the first 54 new SQLite cases and 29 of 39 new SQL Server cases fail; with the fix all pass on net8–net11
  (EFCore 709/708 passed + 3 skipped, SqlServer 228 + 1 skipped, core 26; 0 leftover smoke DBs). No D5 defect.
  - Confirmed: caller savepoints named `__XDevOutboxSavePoint`/`__EFSavePoint` and accumulated SQL Server
    savepoints: the library rolls back to its own (most recent) one, earlier saves survive; caller rollbacks
    plus the existence check dispatch exactly the surviving rows; `UseTransaction` (SQLite, SqlTransaction),
    pooled second lease, Guid key; interceptor throwing in Creating/ReleasingSavepoint or in the best-effort
    release (covers reviewer N1), or suppressing creation (release fails, 41002, rollback guidance holds);
    cancellation before/at/after the savepoint and after the UPDATE: no partial state, no hang; doomed
    transaction (XACT_ABORT ON + CHECK 547): rollback fails with 3903, original error surfaces, 41002, caller
    Rollback does not throw, a retrying strategy retries on the original SqlException. SQL Server savepoints per
    save: owned 1 (EF), caller with events 2, caller without events 1.
  - Hazard (pinned): on SQL Server a caller savepoint named `__XDevOutboxSavePoint` resolves to the library's
    leftover savepoint on rollback (same class as `__EFSavePoint` with plain EF).
  - Pre-existing, needs a decision (pinned): after a failed save the business change stays Added but an explicit
    caller Rollback (what 41002 asks) drops the event as "never saved"; a retry in a new transaction commits the
    row without an outbox row. Disposing without Rollback keeps the event.
  - Item 8 root cause confirmed (pre-existing): on failure EF discards the outbox entry's store-generated key but
    its identity map keeps the entry under that key until a DetectChanges; `RecoverFailedAttempt` detaches through
    the pre-save `EntityEntry` (no DetectChanges), leaving a stale Detached entry; SQLite reuses the rowid on retry
    → "cannot be tracked". Also with the retrying strategy. Detaching via `context.Entry(entity)` leaves none.
    SQL Server identity: stale entry exists but no reuse, retry succeeds (latent).
- [x] Verification (orchestrator): build 0 errors; net8–net11:
  `XDev.Outbox.Tests` 26/26; EFCore (SQLite) 709/708 passed + 3 skipped
  (retry-after-partial-EF-save defect); SQL Server 228 passed + 1 skipped
  (`itemsCount: null`); 0 leftover smoke DBs; `git diff --check` clean.
- [x] Knowledge updates: new `decisions/003-library-savepoint-for-caller-transaction-saves.md`;
  `components/efcore-outbox-store.md`, `database/outbox-messages.md`,
  `project.md`, decision 001.

## Open follow-ups (need user decisions)

1. Explicit `Rollback()` after a failed caller-transaction save drops the
   never-saved event while the entity stays Added; a later save commits it
   without an outbox row (disposal without rollback keeps the event).
2. Retry after EF's own save fails partway: stale identity-map entry from
   detaching via the cached `EntityEntry` (SQLite throws; latent on SQL
   Server). Fix direction: detach via `context.Entry(entity)`.
