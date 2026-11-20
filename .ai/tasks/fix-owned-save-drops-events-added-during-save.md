# Task: Owned save drops events added during the save

## Status

Complete (2026-11-19). Workflow: Bug Fix. User approved fixing it "the same
way" as the commit fix (keep events that were not saved).

## Problem (probe-confirmed, pinned by tester tests)

When the library owns the transaction (no caller transaction),
`CompleteOwnedSave` clears the whole event buffer after the commit. An event
added during the save, e.g. by a `SavingChanges` event handler or a
`SaveChangesInterceptor` that collects domain events, is not in the snapshot
the save wrote, so it is dropped without an outbox row. In a caller transaction
the same event is kept and written by the next save.
Pinned in `tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterNeverSavedOnCommitTests.cs`
(SavingChanges handler during owned and caller saves).

## Approved direction

After an owned save commits, remove only the events that were saved (by
reference/multiplicity), keeping events added during the save.

## Confirmed evidence (planner, 2026-11-19)

- EF order (release/8.0 `DbContext.SaveChanges`): SavingChanges event ->
  SaveChangesStarting interceptors -> DetectChanges -> save ->
  SaveChangesCompleted -> SavedChanges, all inside `base.SaveChanges(false)`,
  i.e. after `GetEventsToSave`. An event added by a SavingChanges/SavedChanges
  handler or a save interceptor is never part of the same save on any path
  (owned, caller, no-events). EF 9–11 order: hypothesis, to be confirmed by
  tests on net8–net11.
- Root cause: `CompleteOwnedSave` calls `_outbox.Events.Clear()`. The caller
  path removes only committed events (`UpdateBuffer(committed: ...)`). The
  no-events pass-through never clears (already keeps them).
- Retrying strategy: the event list is captured once outside the strategy
  lambda; handlers fire per attempt.

## Plan

- `CompleteOwnedSave(saved.Messages, flag)`: AcceptAllChanges if flag; remove
  the saved events by reference/multiplicity (`UpdateBuffer(empty, saved,
  keepUnsavedEvents: true)` or an equivalent loop); keep
  `_releasedNotAcceptedEvents.Clear()`. Update comments/XML docs.
- Tests: flip `KnownGap_EventAddedBySavingChangesDuringOwnedSave_DroppedWithoutRow`;
  new SQLite tests (handler, ISaveChangesInterceptor, SavedChanges,
  multiplicity, retrying guarded/unguarded, characterization); SQL Server
  identity-graph and retry variants.

## Decisions

- Keep (user-approved) rather than include in the same save: including would
  need a second base save (business entries still Added/Modified under
  deferred acceptance) or a library save interceptor with uncontrolled
  ordering.

## Consumer consequences (to document, same class as earlier accepted hazards)

- Domain events collected in a SavingChanges interceptor are written by the
  next save, not atomically with the save that raised them (already true on
  the caller and no-events paths).
- An unguarded handler that adds an event per retry attempt leaves one copy
  per attempt.

## Progress

- [x] Planning (planner)
- [x] Implementation with tests first (developer, 2026-11-19)
  - Production (`src/XDev.Outbox.EFCore/OutboxDbContext.cs`): both owned call sites
    pass `saved.Messages` to `CompleteOwnedSave(saved, flag)`, which (after
    `AcceptAllChanges` when flag=true, unchanged) removes each saved message's
    `Event` from the buffer with `FindIndex(ReferenceEquals)` + `RemoveAt`
    (first occurrence = the snapshot copy, since events added during the save
    are appended). `_releasedNotAcceptedEvents.Clear()` kept. Chose the loop
    over `UpdateBuffer(empty, saved, keepUnsavedEvents: true)`: it does not
    depend on `_pendingOutboxMessages` being empty (it is on this path:
    `GetEventsToSave` releases anything pending when there is no current
    transaction, so the saved list is the whole buffer at snapshot time) and
    does not rebuild `_releasedNotAcceptedEvents`. Updated the state-machine
    header comment, the `Outbox` XML remarks (events added during a save are
    written by the next save; add events before calling SaveChanges for
    atomicity) and `CompleteOwnedSave` docs.
  - Tests (SQLite): flipped/renamed `KnownGap_EventAddedBySavingChangesDuringOwnedSave_DroppedWithoutRow`
    -> `EventAddedBySavingChangesDuringOwnedSave_Kept_WrittenOnceByNextSave`;
    `CommitTesterHost.Create` got an optional `extraInterceptors` parameter.
    New `CallerTransactionPartialSave/OwnedSaveEventsAddedDuringSaveTests.cs`
    (10 theories x sync/async x flag = 40 cases): SavingChanges handler,
    SavedChanges handler, `ISaveChangesInterceptor` domain-event collector
    (payload carries the generated user key), same instance twice (char.),
    captured instance re-added during save, retrying strategy commit failure
    with guarded handler (kept once) and unguarded handler (one copy per
    attempt, char.), failed owned save (char.), no-events save (char.), stale
    pending message then owned save (char.). Note: the interceptor test needs
    an explicit event in the same save, otherwise the no-events pass-through
    already kept the event before the fix.
  - Tests (SQL Server): new `OwnedSaveEventsAddedDuringSaveTests.cs` (8
    cases): SavingChanges handler collecting `OrderPlaced` for a user+orders
    identity graph (sync/async x flag) and the same under
    `SmokeRetryingExecutionStrategy` with one commit fault; checks order ids
    and FKs in payloads, single dispatch of committed rows, no explicit
    identity inserts.
  - Fail-before (production change stashed): SQLite 26 failed / 18 passed of
    the 44 filtered cases (2 flipped + 24 new; the 16 characterization cases
    and the 2 caller-path cases passed), identical on net8/9/10/11. SQL Server
    8/8 failed on net8 (kept count 0, expected 2).
  - Results after the fix: Core 26 x4; EFCore 987 (net8), 986 (net9/10/11),
    all passed; SQL Server 296 passed + 1 known skip on each of net8–net11.
    No `XDevOutboxSmoke_` databases left on LocalDB.
  - EF 9–11 order: same results on all four, consistent with SavingChanges,
    save interceptors and SavedChanges all firing after `GetEventsToSave`
    (they run inside `base.SaveChanges(false)` on every version tested).
- [x] Adversarial testing (tester, 2026-11-19)
  - SQLite: new `tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterOwnedSaveEventsAddedDuringSaveTests.cs`
    (54 cases): handler `ClearEvents()` during the save; clear + re-add the saved instance; clear + add new + re-add
    (reorder); record event value-equal but a different instance (kept: reference matching); snapshot with two copies,
    handler leaves one; queue throws while enqueuing after the owned commit (saved event not re-written, during-save
    event written once); retrying strategy with the business INSERT failing and with the payload UPDATE failing
    (guarded handler: kept once); `SaveChangesFailed` handler adding an event, retry succeeds (kept once); handler
    adding an event on every save (buffer one save behind, no duplicates); kept event then caller transaction commit
    (written and dispatched once) / rollback (flag=false: written once later; flag=true: dropped with the accepted
    changes, existing rule); pooled lease ends with a kept event (discarded, next lease writes nothing); pre-existing
    characterizations: nested `SaveChanges` from a SavingChanges handler, `AcceptAllChanges` throwing after commit.
  - SQL Server: new `tests/XDev.Outbox.EFCore.SqlServer.Tests/TesterOwnedSaveEventsAddedDuringSaveTests.cs` (8 cases):
    retried owned save with the first attempt failing after identity generation (dependent Orders INSERT after
    execution, payload UPDATE after execution) x sync/async x flag; guarded OrderPlaced collector; kept once, next save
    writes final order ids/FKs, every row dispatched once after commit.
  - Fail-before (production change stashed, restored; `git diff src` hash identical before/after): SQLite 34 failed /
    20 passed of 54 on net8/9/10/11 (passing: clear, clear+re-add, two copies, rollback flag=true, and the 6
    pre-existing characterizations); SQL Server 8/8 failed on net8.
  - Findings: no defect in the fix. Pre-existing (identical before the fix, not caused by it): (1) a nested
    `SaveChanges(true)` from a SavingChanges handler during an owned save takes the caller-transaction path in the
    library's own transaction and writes the outer event twice; nested `SaveChanges(false)` makes the outer save fail
    with DbUpdateException (re-entrant saves unsupported). (2) If `AcceptAllChanges` throws after an owned commit
    (e.g. a `ChangeTracker.StateChanged` handler), the buffer is not updated: the committed event is not dispatched and
    the next save writes it a second time. Hazards by design: unguarded per-save handler keeps the buffer one save
    behind and its last event is discarded with the context/lease; an event kept from an owned save and then saved
    with flag=true in a caller transaction that rolls back is dropped (existing rule).
  - Results: Core 26 x4; EFCore 1041 (net8), 1040 (net9/10/11) all passed; SQL Server 304 passed + 1 known skip on
    each of net8–net11. No `XDevOutboxSmoke_` databases left on LocalDB.
- [x] Review (reviewer): approve with non-blocking comments. `Outbox` XML
  remark said "in a later transaction" (not true for caller transactions):
  reworded by the orchestrator (comment only). Knowledge docs out of date:
  updated. Pre-existing questions (nested save, AcceptAllChanges throwing
  after commit): confirmed by the tester, recorded as open items.
- [x] Verification (orchestrator): build 0 errors; net8–net11:
  `XDev.Outbox.Tests` 26/26, EFCore (SQLite) 1041/1040 all passed, SQL
  Server 304 passed + 1 skipped (`itemsCount: null`); 0 leftover smoke DBs;
  `git diff --check` clean; production change limited to `CompleteOwnedSave`
  and its two call sites (plus comments).
- [x] Knowledge updates: `components/efcore-outbox-store.md` (owned-save
  step 2, events-added-during-save limitation, two pre-existing open items,
  reference matching), `business-rules/message-delivery.md`,
  `architecture.md`; `project.md` counts after verification.

## Open follow-ups

- Nested `SaveChanges` from a save handler (duplicate row or failed save).
- `AcceptAllChanges` throwing after an owned commit (committed event written
  again by the next save); fix by updating the buffer before/finally.
- Cleanup of `_releasedNotAcceptedEvents` / `keepUnsavedEvents`: done
  (`tasks/cleanup-released-not-accepted-events.md`).
