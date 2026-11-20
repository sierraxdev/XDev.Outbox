# Task: AcceptAllChanges throwing after an owned commit; claim without itemsCount on SQL Server

## Status

Complete (2026-11-20). Workflow: Bug Fix (two independent defects). User
asked to fix both.

## Bug A — `AcceptAllChanges` throwing after an owned commit (pre-existing, low)

`CompleteOwnedSave` (`src/XDev.Outbox.EFCore/OutboxDbContext.cs`) accepts
changes before updating the buffer. If acceptance throws (e.g. a
`ChangeTracker.StateChanged` handler throws), the committed event is not
dispatched in memory, stays buffered and is written again by the next save
(duplicate row). Pinned by
`PreExistingDefect_AcceptAllChangesThrowsAfterOwnedCommit_CommittedEventWrittenTwice`
(`tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterOwnedSaveEventsAddedDuringSaveTests.cs`).

## Bug B — `GetAllAsync(itemsCount: null)` fails on SQL Server (error 1033)

The claim CTE keeps `ORDER BY Id` but emits `TOP (@take)` only when
`itemsCount` has a value. The poller always passes a count; the interface
default is null. See `database/outbox-messages.md`. Skipped test
`ClaimWithoutItemsCountTests` (SQL Server suite).

## Planning findings (planner, 2026-11-19)

### Bug A — confirmed
- Root cause: `CompleteOwnedSave` calls `ChangeTracker.AcceptAllChanges()`
  before trimming the buffer; the enqueue loop runs after it. A throw skips
  the trim and the enqueue.
- Acceptance runs outside the execution strategy (after commit): no retry.
- Plain EF (8.0 `StateManager.SaveChanges`) commits, then accepts, and
  rethrows an acceptance exception. Parity: propagate.
- Caller-transaction path (`CompleteAmbientSave`) registers pending messages
  before accepting, so commit dispatches and trims normally: not affected.
- Hypothesis DISPROVED by the tester (`CallerTransaction_AcceptanceThrows_ThenRollback_EventReleasedAsAccepted_NothingWrittenLater`): partial acceptance + caller rollback
  keeps the event even if its business entries were accepted. Conservative.

### Bug A — plan
- Reorder `CompleteOwnedSave`: trim the buffer and clear
  `_releasedNotAcceptedEvents`, then accept.
- USER DECISION (2026-11-19): do NOT enqueue committed messages in memory
  when acceptance throws; leave them to the poller. Rationale (poller delivers after
  `LockExpiresAt`; avoids dispatching an original instance whose generated
  keys were not written back by acceptance).

### Bug B — confirmed
- `EntityFrameworkOutboxStore.GetAllAsync`: `TOP (@take)` only when
  `itemsCount` has a value, but `ORDER BY Id` always in the CTE -> error 1033.
- The store is SQL Server only; no SQLite claim path or tests.
- The poller always passes a count (`OutboxProcessorHostedService.cs`).

### Bug B — plan
- Emit `ORDER BY Id` in the CTE only together with `TOP (@take)`. SQL
  unchanged when a count is passed; no new parameter; result order without a
  count unspecified (delivery contract has no ordering guarantee).
- Tests (SQL Server): un-skip `ClaimWithoutItemsCountTests` (order-insensitive,
  check locks, ineligible rows excluded); add null to
  `ClaimParameterCombinations`.

## Progress

- [x] Planning (planner)
- [x] Implementation with tests first (developer)
- [x] Implementation, Bug A (developer, 2026-11-20)
  - Change: `CompleteOwnedSave` (`src/XDev.Outbox.EFCore/OutboxDbContext.cs`)
    now trims the buffer (saved events by reference and multiplicity) and
    clears `_releasedNotAcceptedEvents` first, then calls
    `ChangeTracker.AcceptAllChanges()` (flag = true). The acceptance exception
    propagates unchanged; the enqueue loop (still after `CompleteOwnedSave` in
    sync and async paths) is skipped, so the committed row is left to the
    poller (per user decision). XML doc of `CompleteOwnedSave` and the
    state-machine header comment updated. Caller-transaction path unchanged.
  - Tests (`tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterOwnedSaveEventsAddedDuringSaveTests.cs`,
    sync + async each): `PreExistingDefect_..._CommittedEventWrittenTwice`
    flipped and renamed to
    `AcceptAllChangesThrowsAfterOwnedCommit_CommittedEventNotRewritten_ExceptionPropagates`
    (original exception type, one business/outbox row, buffer empty, nothing
    enqueued, row LockExpiresAt in the future, still one row after recovery and
    an unrelated save); new `..._DuringSaveEventKept_EachWrittenOnce`,
    `..._TwoEvents_BothTrimmed_EachWrittenOnce`,
    `..._RetryingStrategy_NotReExecuted` (one SavingChanges call, one commit),
    and characterization `AcceptAllChangesThrowsInCallerTransaction_ThenCommit_WrittenAndDispatchedOnce`.
  - Fail-before (net8.0, production change absent): 8 failed (all four
    owned-save tests, sync and async: buffer not empty / during-save buffer
    had 2 items); characterization 2 passed.
  - Results: `XDev.Outbox.Tests` 26 on net8-11; `XDev.Outbox.EFCore.Tests`
    1049 (net8) / 1048 (net9-11) (+8: -2 old, +10 new); SQL Server suite 324
    passed / 0 skipped on net8-11. Build 0 errors; no new warnings in changed
    files (pre-existing xUnit2013 at line ~615 of the test file is not from
    this change). No leftover `XDevOutboxSmoke_` databases.
  - Knowledge candidate: `components/efcore-outbox-store.md` "After success"
    steps (order: trim, accept, enqueue) and the open-defect note (~L294) to be
    updated after review.
- [x] Implementation, Bug B (developer, 2026-11-19)
  - Change: `EntityFrameworkOutboxStore.GetAllAsync` emits `ORDER BY Id` in
    the CTE only when `itemsCount` has a value (same branch condition as
    `TOP (@take)`); comment explains error 1033. The count branch keeps the
    original raw literal, so the SQL with a count is unchanged; no new
    parameter. Without a count the claim has no ORDER BY (order unspecified).
  - Tests (`tests/XDev.Outbox.EFCore.SqlServer.Tests/AdversarialTableNameTests.cs`):
    `ClaimWithoutItemsCountTests.GetAllAsync_WithoutItemsCount_ClaimsEveryEligibleRow`
    un-skipped and extended (free, expired, future-locked and
    Attemps = 15 rows; order-insensitive ids; future lock never returned;
    Attemps above max excluded when maxAttemps given; with lock every claimed
    row's LockExpiresAt set in the DB, other rows unchanged; without lock all
    rows unchanged). `ClaimParameterCombinations` now includes
    `itemsCount: null` (exclusion comment removed); comparison is
    order-insensitive only for null; parameter-count check kept.
  - Fail-before (net8.0): 20 failures (4 + 16 null rows across the 4 table-name
    classes), all `SqlException` 1033; 32 count rows passed.
  - SQL identity: claim SQL text captured from test output before/after for
    all 8 count combinations (DefaultAndTableSchema class): identical.
  - Results: SQL Server suite 324 passed / 0 skipped on net8, net9, net10,
    net11 (baseline 304 + 1 skipped; +4 un-skipped rows, +16 null rows).
    `XDev.Outbox.EFCore.Tests` 1041 (net8) / 1040 (net9-11); `XDev.Outbox.Tests`
    26 on all four. Build 0 errors, no warnings in changed files. No leftover
    `XDevOutboxSmoke_` databases on LocalDB.
  - Knowledge candidate: `database/outbox-messages.md` (claim SQL ~L67 and the
    "Defect (open, pre-existing)" note ~L117) to be updated after review.
- [x] Adversarial testing (tester, 2026-11-20)
  - Files (tests only; src untouched, `git diff src` hash identical before/after):
    `tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterAcceptAfterOwnedCommitTests.cs` (26 cases),
    `tests/XDev.Outbox.EFCore.SqlServer.Tests/TesterAcceptAfterCommitAndClaimTests.cs` (18 cases, 3 classes).
  - Bug A cases (SQLite, sync + async): acceptance throwing on User / Order (dependent) / Customer / Address (owned)
    -> original exception, committed rows carry generated keys (user id, order ids/userId, customer id, owned
    city), buffer empty, nothing enqueued, nothing rewritten; buffer observed from StateChanged during acceptance is
    already trimmed (only the during-save event); handler adds an event then throws during acceptance -> kept,
    written/dispatched once later; same without throw (success path); flag = false unaffected (dispatched once,
    app's own AcceptAllChanges throws later, no rewrite); app retries without re-adding (outbox: no new row, no
    dispatch; business: plain EF re-inserts the still-Added user, 3 user rows, out of scope); app re-adds the same
    event (second row from the re-add only, dispatched once, first row left to the poller); throw then ClearEvents;
    pooled lease after the throw (next lease clean, row once); caller-transaction throw + rollback characterization.
    SQL Server (sync/async x retrying/non-retrying): row committed with keys (GraphAssert), not enqueued, locked ~5 min,
    not claimable with or without count while locked, claimed after the lock expires with correct payload keys.
  - Bug B cases (SQL Server): empty table; all rows ineligible (rows unchanged); maxAttemps boundary (null/0/max-1 in,
    max/max+1 out; same set as with a count); SQL shape (no TOP/ORDER BY without count; hints, lock UPDATE, OUTPUT kept;
    count SQL keeps TOP (@take)/ORDER BY Id); 500 rows all claimed once, lock persisted and returned; count claimer
    holding rows in a tx -> null claimer skips them without blocking (commit/rollback); null claimer holding all rows
    -> other claimer (null/5) gets nothing without blocking; 6 concurrent claimers (null and 7, 3 tx rounds) over 300
    rows never claim the same row, all claimed; Guid key table (own fixture).
  - Fail-before (net8.0, scratch worktree at HEAD with the new tests): SQLite 20/26 failed (the 6 passing are the
    success-path, flag = false and caller-transaction cases, expected unaffected); SQL Server 18/18 failed (4 Bug A:
    buffer not empty; 14 Bug B: SqlException 1033).
  - Findings: no product defect found. Task-doc hypothesis "partial acceptance + caller rollback keeps the event" is
    DISPROVED: the pending message is recorded as accepted (flag = true), so on rollback its event leaves the buffer
    and nothing is written later. Observable change (intended): during acceptance the buffer no longer contains the
    committed events. By-design hazards: after an acceptance failure, delivery waits for LockExpiresAt (default 5 min);
    re-adding the event creates a second row. Hazard (pre-existing in kind, unbounded with null): a claim of >~5000
    rows escalates to an OBJECT X lock (observed: 7001 rows claimed in an open tx -> `OBJECT:X`, a concurrent claimer
    with count 5 timed out after 3 s; READPAST cannot skip a table lock). Not kept as a test (environment-dependent).
  - Results: XDev.Outbox.Tests 26 (net8-11); XDev.Outbox.EFCore.Tests 1075 (net8) / 1074 (net9-11); SQL Server 342
    passed / 0 skipped (net8-11). No leftover `XDevOutboxSmoke_` databases.
- [x] Review (reviewer): approve with non-blocking comments. (1) no
  concurrent null-count claim test: covered by the tester
  (`TesterClaimWithoutItemsCountTests`, six mixed claimers, no overlap);
  (2) lock escalation on large backlogs: documented in
  `database/outbox-messages.md`; (3) Fix A tests do not check the payload
  keys: covered by the tester's SQL Server test (claimed row's payload keys
  match the database). `project.md` counts stale: updated.
- [x] Verification (orchestrator, 2026-11-20): build 0 errors; net8–net11:
  `XDev.Outbox.Tests` 26/26, EFCore (SQLite) 1075/1074 all passed, SQL Server
  342 passed, 0 skipped; 0 leftover smoke DBs; `git diff --check` clean;
  production changes limited to `CompleteOwnedSave` order and the
  conditional `ORDER BY` (plus comments); no debug output in test files.
- [x] Knowledge updates: `components/efcore-outbox-store.md` (owned-save
  steps, fixed-defect note), `database/outbox-messages.md` (claim SQL,
  null-count behavior, lock escalation), `business-rules/message-delivery.md`,
  `project.md` (counts).

## Notes

- Leftover tester shell processes (a stuck `cat > /tmp/x` followed by a
  script that would add debug output to `TesterAcceptAfterOwnedCommitTests.cs`)
  were stopped at the user's request (2026-11-20), shells first; the test file
  was confirmed clean afterwards.
