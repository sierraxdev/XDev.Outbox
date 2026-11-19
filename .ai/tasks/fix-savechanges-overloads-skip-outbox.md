# Task: SaveChanges overloads skip the outbox

## Status

Complete (uncommitted). Implementation verified; durable knowledge updated. Not committed: awaiting user.

## User Decision (2026-11-18)

The user was asked how `acceptAllChangesOnSuccess=false` should behave when
events are buffered. The options were: throw; ignore the flag; or fix #2 too.

The user chose **"Fix #2 too"**. Scope now includes finding #2:

- Change acceptance is deferred until after the library-owned transaction
  commits.
- The caller's `acceptAllChangesOnSuccess` flag is honored.
- The interceptor is adjusted as needed.

Finding #3 (`AutoTransactionBehavior`) is still out of scope unless the #2
design requires touching it.

## Requirement / Problem

Fix finding #1 from the 2026-11-18 bug check.

`OutboxDbContext<TKey>` overrides only `SaveChangesAsync(CancellationToken)`.
Calls to `SaveChanges()`, `SaveChanges(bool)` and
`SaveChangesAsync(bool, CancellationToken)` do not persist buffered `IOutbox`
events. The outcome depends on how many statements the save runs:

- **Multi-statement save:** EF opens its own transaction.
  `OutboxDispatchTransactionInterceptor.TransactionCommitted` clears the
  buffer on commit, so the event is lost silently.
- **Single-statement save:** no transaction is opened, so the event stays
  buffered. It is written by the next unrelated `SaveChangesAsync` in the
  same scope.

Expected behavior: every public save overload persists buffered events
atomically with the business changes, and dispatches them after commit,
exactly like `SaveChangesAsync(CancellationToken)` does today.

Scope: finding #1 only. Findings #2 (changes accepted before commit) and #3
(`AutoTransactionBehavior.Never` left set) are separate. Any change to them
needs justification as required for a correct #1 fix, and the user must be
told.

## Relevant Project Knowledge

- `.ai/components/efcore-outbox-store.md`: save path, interceptor, observed risks.
- `.ai/business-rules/message-delivery.md`: publishing rules.
- `.ai/database/outbox-messages.md`: transactions.

## Confirmed Evidence

These were reproduced on LocalDB with the scratch harness
`C:\Users\danni\AppData\Local\Temp\claude\253e6a6f156a4c6e\563cc541-0827-4aa0-a7ec-823572351ac6\scratchpad\bugrepro`
(scenario H2):

- One business row saved with sync `SaveChanges`: `orders=1`,
  `outbox rows=0`, `IOutbox pending=1`. A later `SaveChangesAsync` writes and
  dispatches the event.
- Two business rows in different tables saved with sync `SaveChanges`:
  `outbox rows=0`, `pending=0`. The event is never written; it is lost.

## Constraints

- The working tree already contains uncommitted user changes in
  `OutboxDbContext.cs`, `ServiceCollectionExtensions.cs`, and the untracked
  `OutboxDispatchTransactionInterceptor.cs`. Build on top of them; do not
  revert them.
- No test project exists. A regression test project is to be added under
  `tests/`.
- No commits or pushes.

### Confirmed by planner

- In EF Core (release/8.0 and main), `SaveChanges()` calls `SaveChanges(true)`
  and `SaveChangesAsync(ct)` calls `SaveChangesAsync(true, ct)`. Only the bool
  overloads do the work.
- Root cause: `OutboxDbContext.cs:48` is the only override. In a sync save that
  runs several statements, EF's auto-transaction commit fires
  `OutboxDispatchTransactionInterceptor.cs:33-46`. That enqueues nothing and
  clears the buffer (line 45).
- Microsoft.Data.Sqlite accepts `IsolationLevel.ReadCommitted` and upgrades it
  to Serializable without error, so SQLite in-memory can exercise the save path.
  `EntityFrameworkOutboxStore` raw SQL is not used by the save path.

## Hypotheses

(none)

## Plan

1. Override `SaveChanges(bool)` and `SaveChangesAsync(bool, CancellationToken)`,
   and delete the `SaveChangesAsync(CancellationToken)` override. Keeping it
   would cause double processing, because its base call routes virtually to the
   new override.
2. Make every internal save an explicit `base.SaveChanges(true)` or
   `base.SaveChangesAsync(true, ct)`. In the no-events branch, pass the caller's
   flag through.
3. Extract I/O-free helpers as verbatim moves, leaving retry-snapshot behavior
   unchanged: `PrepareOutboxMessages`, `CaptureEntityStateSnapshot`, and
   `RestoreStateForRetry`.
4. Make the sync path mirror the async flow: `CreateExecutionStrategy().Execute`,
   `BeginTransaction(ReadCommitted)`, `Commit`. Do not use sync-over-async.
5. Handle `acceptAllChangesOnSuccess=false` when events are buffered. **Pending
   the user's decision.**
6. Add a test project at `tests/XDev.Outbox.EFCore.Tests`, targeting
   net8–net11. It uses xUnit, SQLite in-memory, and a recording
   `IMessageQueue<int>` registered before `AddOutbox`. Add it to
   `XDev.Outbox.slnx`.

Scope notes:

- #2 is untouched. Fully honouring `false` would need the #2 fix plus an
  interceptor change, because the interceptor enqueues only `Unchanged`
  entries.
- #3 is untouched, but the sync path mirrors the
  `AutoTransactionBehavior = Never` line for parity.
- Observable change: a consumer override of the bool overloads now runs once
  around the library's logic, instead of once per internal save.

## Implementation

## Tests

## Review Findings

## Unresolved Questions

## Next Steps

1. Planner: investigate and plan.

---

## Re-plan for #1 + #2 (planner, 2026-11-18). Supersedes the Plan above.

### Confirmed by planner (EF Core source release/8.0 + main, not yet test-verified)

- **Generated values are held back until acceptance.** Store-generated values
  are kept in `InternalEntityEntry._storeGeneratedValues` and reported by
  `PropertyEntry.CurrentValue`. The entity objects receive them only in
  `AcceptChanges()`.
- **Failed saves discard them.** `StateManager.SaveChanges` discards
  store-generated values when a save fails, and `PrepareToSave()` discards
  leftovers. Deferred acceptance therefore makes retry safe without resetting
  keys.
- **Rule A needs its own mechanism.** Any save that serializes `Payload` before
  acceptance writes default keys into the JSON. Rule A (store-generated keys
  must appear in the payload) therefore needs an explicit mechanism.
- **Disposal raises no rollback event.** `RelationalTransaction.Dispose`
  without a commit or rollback raises no rollback interceptor event.
- **The EF docs support this pattern.** The EF connection-resiliency docs
  endorse `SaveChanges(false)` followed by `AcceptAllChanges` after commit.

### Plan

1. **Overrides.** Override `SaveChanges(bool)` and
   `SaveChangesAsync(bool, ct)`, and remove the `SaveChangesAsync(ct)`
   override. When there are no events, call `base.*(flag)`.
2. **Library-owned transaction.** Inside the execution strategy, each attempt
   does the following:
   - Create fresh outbox messages.
   - Begin a `ReadCommitted` transaction.
   - Call `base.SaveChanges(false)`.
   - Fix up the payload: project the pending generated values onto the entity
     objects, serialize the payload, then restore the objects. Rows whose
     payload changed get a parameterized `UPDATE`.
   - Detach the outbox entities and assign their Ids.
   - Commit.

   If the attempt fails, it detaches its outbox entities. After a successful
   commit: `AcceptAllChanges` (only if the flag is true), `ClearEvents`, then
   enqueue.
3. **Caller-owned transaction.** Do the same save, payload fix-up, and detach.
   Register the messages as pending under `TransactionId`. Call
   `AcceptAllChanges` if the flag is true. The interceptor dispatches when the
   caller commits.
4. **Interceptor.** It uses an internal `IOutboxTransactionParticipant<TKey>`
   and ignores library-owned saves.
   - On commit: take the pending messages whose transaction Id matches, clear
     the buffer, and enqueue them.
   - On rollback or failure: discard the pending messages and clear the
     buffer.
5. **Removed code.** Remove the retry snapshot/restore block (which also
   removes bug #4 on this path), the `AutoTransactionBehavior = Never` lines
   (#3, pending D4), `AddOutboxMessages`, and `GetDefaultValue*`.
6. **Tests.** Add the test project `tests/XDev.Outbox.EFCore.Tests`:
   net8–net11, xUnit, SQLite in-memory, a recording `IMessageQueue<int>`, and
   command and commit failure interceptors. Write the failing tests first.

### Decisions pending (asked of user)

- **D1:** projection plus payload `UPDATE` to keep Rule A.
- **D2:** keep the buffer when a library-owned save fails.
- **D3:** the dispatch payload when flag = false.
- **D4:** drop `AutoTransactionBehavior = Never`.
- **D5:** ambient savepoint (defer).

### Hypotheses

- Projection covers identity keys and dependent FKs on SQLite and SQL Server.
  To be verified by tests on each target framework.

### User decisions (2026-11-18)

- D1: approved. Use projection plus a parameterized payload `UPDATE` to keep
  Rule A.
- D2: approved. Keep the `IOutbox` buffer when a library-owned save fails.
- D3: approved. When flag = false, immediate dispatch sends the payload rebuilt
  from the persisted JSON (an `ExpandoObject`).
- D4: approved. Remove the `AutoTransactionBehavior = Never` lines, which also
  fixes #3.
- D5: deferred. No explicit ambient savepoint.
- Sync dispatch blocking on `EnqueueAsync` is an accepted existing limitation.

## Tests (pre-fix baseline, 2026-11-18)

Project: `tests/XDev.Outbox.EFCore.Tests`. It targets net8–net11 and uses
xUnit 2.9.3 with SQLite in-memory. It is included in `XDev.Outbox.slnx`.

Run with `dotnet test tests/XDev.Outbox.EFCore.Tests -f <tfm>`.

Result on the unfixed code, for every target framework: 56 tests, 40 failing
and 16 passing.

The failures reproduce #1, #2 and #3. They also show three further defects:

- a phantom outbox row after a failed outbox INSERT;
- outbox entities still tracked after an ambient rollback;
- a stale message enqueued after a transaction was disposed without commit.

Two tests (the payload UPDATE failure tests) only become meaningful after the
fix. Two sync ambient tests currently pass only because the sync path writes
nothing.

### Orchestrator clarification (consistent with D2)

When a caller transaction is disposed without commit:

- Its pending, already-saved messages must never be enqueued.
- Events still in the `IOutbox` buffer remain buffered. They were never
  committed, so a later save writes and dispatches them exactly once. This is
  not treated as a stale dispatch.

## Implementation (developer, 2026-11-18)

Status: implemented per "Re-plan for #1 + #2" and D1–D5. Not committed.

Files:

- `src/XDev.Outbox.EFCore/OutboxDbContext.cs`: overrides `SaveChanges(bool)` and
  `SaveChangesAsync(bool, ct)`; `SaveChangesAsync(ct)` override removed. Owned path:
  execution strategy -> ReadCommitted tx -> fresh messages -> `base.SaveChanges(false)` ->
  projected payload patch (parameterized UPDATE) -> detach + assign Ids -> commit; then
  AcceptAllChanges (flag), ClearEvents, enqueue. Ambient path registers pending messages
  by `TransactionId`. Retry snapshot, `AutoTransactionBehavior = Never`, `AddOutboxMessages`,
  `GetDefaultValue*`, `IsPrimaryKeyAutoGenerated`, `GetId` removed.
- `src/XDev.Outbox.EFCore/IOutboxTransactionParticipant.cs` (new, internal).
- `src/XDev.Outbox.EFCore/OutboxDispatchTransactionInterceptor.cs`: no tracker scans; uses the
  participant; XML cref fixed.

Observed during implementation (not a plan change): the src files listed as "uncommitted"
were already clean against HEAD (e8a9957); only `XDev.Outbox.slnx` and `tests/` were pending.

Return value: owned/ambient saves return the business row count (outbox rows subtracted),
matching the previous `SaveChangesAsync` behavior.

Tests: `dotnet test tests/XDev.Outbox.EFCore.Tests -f net8.0|net9.0|net10.0|net11.0`:
56/56 pass on every TFM. Build of `XDev.Outbox.slnx`: 0 errors, no new compiler warnings.

### Correction round (developer, 2026-11-18): B1, adversarial defects 1-4, N1-N3

Changes:

- **Defect 1 (retry FK violation).** Each attempt (owned, per execution-strategy attempt; ambient) captures, for
  Added business entries, properties that are temporary, plus store-generated or FK properties still at their
  sentinel, with their backing-field value. On any attempt failure (including commit failure) it detaches the
  attempt's outbox entries and restores: `CurrentValue`, then `IsTemporary = true`, then the backing field
  (EF writes the restored value onto the entity object; the tracker keeps it in its temporary store). Recovery is
  best-effort per item; recovery failures are thrown together with the original exception (`AggregateException`).
  Confirmed by probe (EF 8): after `SaveChanges(false)` keys/FKs are non-temporary store values; setting
  `IsTemporary = true` writes the value to the CLR object; resetting the field leaves the temporary value in the tracker.
- **B1.** Pending messages remember the save's `acceptAllChangesOnSuccess`. On rollback / failed commit or
  rollback (interceptor -> `EndTransactionWithoutCommit`) and on stale detection (next save): events saved with
  flag=false stay buffered, events saved with flag=true are removed (buffer rebuilt via `ClearEvents` + `AddEvent`,
  order preserved, matched by reference and multiplicity). Never-saved events: cleared on rollback/failed end
  (unchanged behavior), kept on stale detection (unchanged behavior).
- **Defect 2 (savepoints).** EF's savepoint interceptor events carry no savepoint name (verified in the EF 8, 9, 10
  and 11 XML docs: `TransactionEventData` has only Transaction/TransactionId/ConnectionId/IsAsync/StartTime), so
  name-based tracking per savepoint is impossible. Instead `RolledBackToSavepoint(Async)` makes the context query,
  inside the transaction, which pending outbox rows still exist (OR of parameterized `Id` equalities, batches of 100,
  no `Contains` to avoid OPENJSON); missing ones are released with the B1 rule. Ignored while any SaveChanges override
  runs (`_saveDepth`), so EF's internal `__EFSavePoint` is never acted on.
- **N1 / defect 4.** Projection uses `IProperty.FieldInfo` only; properties without a backing field known to EF are
  not projected (their payload value stays what the entity object held at INSERT time, i.e. default keys).
  Restore of projected fields is fault-tolerant.
- **N3 / defect 3.** Delimited identifiers in the payload UPDATE escape `{`/`}`.
- Participant interface: `DiscardPendingOutboxMessages` replaced by `EndTransactionWithoutCommit`,
  `RolledBackToSavepoint`, `RolledBackToSavepointAsync` (internal).

New tests: `CallerTransactionEndTests.cs` (B1 a-d, never-saved event on rollback, savepoint with flag=false,
savepoint between two saves; sync/async), `RetryInsertShapeTests.cs` (N2: retried INSERTs into Users/Orders carry
no `"Id"` column, strategy and application retry, PayloadUpdate/Commit failures).

Results (net8.0, net9.0, net10.0, net11.0, identical): 175 tests, 164 passed, 1 skipped (D5), 10 failed. All 10
failures are the payload-key assertions of `Save_EventReferencingNotificationTrackedEntity_...` (8) and
`Save_EventReferencingEntityWithSideEffectingKeySetter_...` (2): both entities map the key with no backing field EF
can discover, so field-only projection (directed for N1) cannot put the generated key into the payload. Test
expectations were not changed; needs an orchestrator/user decision (Rule A vs. no setter invocation).

### Setter fallback (developer, 2026-11-18), per user decision "properties with no backing field"

- `OutboxDbContext.cs`: new `GetClrMember(IProperty)` returns `IProperty.FieldInfo` when present, otherwise the
  `PropertyInfo` when it is readable and writable (any setter visibility), otherwise null (shadow/indexer excluded).
  Values are read/written via `GetClrValue`/`SetClrValue`. A setter is never used when a field exists.
- Rule A projection (`ProjectTrackedValues`): projects through that member (setter called once to project, once to
  restore; only when the CLR value differs from the tracker value). `FieldValue` -> `ClrValue`,
  `RestoreFieldValues` -> `RestoreClrValues`; restore still attempts every item and surfaces failures.
- Failed-attempt recovery pass 3 (`CaptureGeneratedValues`/`RestoreGeneratedValues`): same field-then-setter rule,
  so a field-less key gets its pre-attempt CLR value back instead of keeping the value EF wrote.
- Test (user-approved): `Save_EventReferencingEntityWithSideEffectingKeySetter_InvokesSetterAsOftenAsWithoutEvent`
  renamed to `..._PayloadHasGeneratedKey`; the setter-call-count assertion was removed; the payload-key assertion
  was kept. No other test was changed.
- Results: `dotnet build XDev.Outbox.slnx` 0 errors (no warnings in changed files); `dotnet test` on net8.0, net9.0,
  net10.0, net11.0: 175 total, 174 passed, 1 skipped (D5), 0 failed on each.

### Final round (developer, 2026-11-18): D-A..D-D, N4, N6, Q4

Changes:

- **D-A / N1 (commit path owned by the context).** `TakePendingOutboxMessages` replaced by
  `IOutboxTransactionParticipant.TransactionCommitted(id)`; the interceptor no longer calls `ClearEvents` for
  participants (still does for non-participant contexts). On commit the context rebuilds the buffer via a shared
  `UpdateBuffer(released, committed, keepUnsavedEvents)`: events of committed messages and never-saved events are
  removed (unchanged documented behavior); events of stale messages (other, disposed transaction) follow B1
  (not-accepted stay buffered, accepted removed). Stale messages are never dispatched.
- **D-C.** `SavedOutboxMessage.AcceptedChanges` is mutable. Any successful flag=true save in the transaction (ambient
  save with events, and the no-events branch `base.SaveChanges(true)`) marks every pending message as accepted.
- **D-B (savepoint existence check fails).** The exception is rethrown and all pending messages of the transaction are
  marked `Unconfirmed`. Buffer handling: unchanged at that moment (events stay matched to their pending messages, so
  they are neither re-saved nor dropped). Resolution happens at the next point inside the transaction: the next save
  (check runs first; if it fails the save throws before writing anything), the next rollback to a savepoint, or
  `TransactionCommitting` (new interceptor hook). Resolution = missing rows released with B1, existing rows confirmed
  (an undone row never comes back). If the check before commit fails too, the commit is NOT blocked: the failure is
  logged as a warning via EF's `IDiagnosticsLogger<Database.Transaction>`, unconfirmed messages are not dispatched in
  memory (committed rows are delivered by the poller after `LockExpiresAt`), and their events are removed from the buffer
  like committed ones. Why no duplicates/phantoms/loss: in-memory dispatch only for confirmed rows (no phantom); an
  unconfirmed event is never re-saved while its row may exist (no duplicate); a not-accepted change re-saved in the
  same transaction is preceded by a successful check that releases its undone message and re-buffers the event (no
  loss), or the save fails visibly.
- **D-D.** For setter-only projected properties, when the save ends by accepting the changes (flag=true, owned or
  ambient) the post-projection restore is deferred to `RecoverFailedAttempt` (runs first there, before the generated
  value restore); on success it is skipped, so the CLR value equals what `AcceptAllChanges` writes. Field-backed
  properties are restored as before; flag=false keeps the setter restore. Recovery pass 1 (`CurrentValue = temporary`)
  still goes through the setter for setter-only keys: avoiding it needs EF internal API (`SetTemporaryValue`) or
  marking the store value temporary, which would desynchronize dependents' FK temporaries. Documented, not changed.
- **N4.** No projection change -> no payload re-serialization and no UPDATE. When something was projected, each message
  compares the projected payload with the one serialized right after the INSERT (same CLR state the INSERT used:
  `SaveChanges(false)` writes nothing to entity objects). Pre-`SaveChanges` capture was not used because DetectChanges
  fix-up during the save can change CLR FK values before the INSERT is built.
- **N6.** Existence query uses `IgnoreQueryFilters()`; equality uses `==` when `TKey` defines it (primitives, enums,
  Guid), otherwise typed `Equals(TKey)` or `object.Equals`.
- **Q4.** `AddOutboxEntries` detaches the entries already added if adding a later one throws.

New tests: `tests/XDev.Outbox.EFCore.Tests/Round2/FinalRoundTests.cs` (14 per TFM): commit-time check also fails
(commit succeeds, nothing dispatched in memory), re-save after failed check (event written once), next-save check fails
(save throws, nothing written), D-C via no-events save, D-A with flag=true stale, Guid key savepoint, outbox query filter.

Results (`dotnet build XDev.Outbox.slnx`: 0 errors, no warnings in changed files; `dotnet test` net8.0/net9.0/net10.0/
net11.0, identical): 371 total, 364 passed, 1 skipped (D5), 6 failed. The 6 failures are test expectations that
contradict each other or the directed behavior (test assertions not changed):

- `GuardedKeySetter_RestoreThrowsOnce_...(flag: true)` x2: expects the restore to run (and fail once) on a successful
  flag=true save; D-D removes that restore. Same event sequence as `GuardedKeySetter_WithEvent_...(flag: true)`, which
  expects success: both cannot pass.
- `GuardedKeySetter_WithEvent_...(flag: false)` x2: the direction keeps the restore for flag=false, and
  `Save_EventReferencingNotificationTrackedEntity_...(flag: false)` asserts the CLR key is 0 after the save (needs the
  restore); `GuardedKeySetter_RestoreThrowsOnce_...(flag: false)` expects the identical sequence to fail.
- `SameEventInstance_SavedWithFalseThenTrue_CallerRollsBack_OneCopyStaysBuffered` x2: expects one copy kept, but the
  second (flag=true) save accepted the first save's changes; D-C (and `MixedFlags_...`) require dropping it (keeping it
  would be a phantom).

### F1 round (developer, 2026-11-18): F1, N-1, N-2

- **F1.** `OutboxDbContext` tracks `_releasedNotAcceptedEvents` (by reference, with duplicates): events B1 keeps
  buffered for released, not-accepted messages (explicit end without commit, stale release, savepoint release).
  `UpdateBuffer` keeps them regardless of `keepUnsavedEvents` and rebuilds the list from the buffer (entries no longer
  buffered are forgotten). Entries are removed when: the event is saved again in an ambient save (`CompleteAmbientSave`,
  one occurrence per saved message); an owned save succeeds (`CompleteOwnedSave` clears); any flag=true save accepts
  changes (`MarkPendingChangesAccepted` clears: ambient with events and the no-events D-C branch, owned or ambient);
  the app removed the event from the buffer (`ForgetReleasedEventsNotBuffered` in `GetEventsToSave`, and `UpdateBuffer`).
- **N-1.** Commit-time warning logged with `EventId(41001, "XDev.Outbox.EFCore.UnconfirmedPendingMessagesAtCommit")`;
  documented on the (private) field, `IOutboxTransactionParticipant.TransactionCommitting` and the public
  `OutboxDispatchTransactionInterceptor<TKey>` summary.
- **N-2.** `CompleteAmbientSave` comment corrected (the branch is the normal first-save-in-transaction path; `Clear` is
  a no-op there). State-machine summary comment added above `OutboxDbContext<TKey, TContext>`.
  `IOutboxTransactionParticipant` docs ("every other buffered event is removed") updated.
- Tests: `tests/XDev.Outbox.EFCore.Tests/Round2/ReleasedEventTrackingTests.cs` (9 theories x sync/async = 18): (a) tx2
  rollback without save, (b) tx2 commit without save, stale-dispose + two empty commits, (c) savepoint release then
  commit without re-save, (d) owned flag=true save of unrelated entity persists once then forgotten, (e) ClearEvents ->
  no resurrection, re-saved with accept in rolled-back tx -> dropped (no phantom), cleared + accepted + re-added ->
  never-saved, duplicate instance -> both copies kept. Mutation check (kept-list match disabled): 12 of 18 fail (the
  F1 cases); the 6 guard tests pass either way, as intended. No existing test changed.
- Results: `dotnet build XDev.Outbox.slnx` 0 errors (no new warnings in changed files); `dotnet test` net8.0, net9.0,
  net10.0, net11.0: 389 total, 388 passed, 1 skipped (D5), 0 failed on each.
- Limitation (unchanged by this round): a direct `ChangeTracker.AcceptAllChanges()` by the app after a release is not
  observed, so a kept event could still be saved later (as before F1).

## Review Findings (independent reviewer, 2026-11-18). Verdict: CHANGES REQUIRED

### B1 (HIGH, blocking): caller transaction ends without commit

When a caller-owned transaction ends without a commit, buffered events are
handled the same way regardless of `acceptAllChangesOnSuccess`. This causes two
failures:

- **Lost event.** With flag = false, a rollback or failed commit clears the
  buffer while the business changes are still pending. A retry then saves the
  business rows with no event. This is EF's documented user-transaction retry
  pattern.
- **Phantom event.** With flag = true, a transaction disposed without commit
  raises no event, so the buffer is kept. The next save writes and dispatches
  an event whose business rows were never committed and will not be re-saved.

**Orchestrator resolution.** This falls within the approved requirement: no lost
or phantom events, and tracker state should match plain EF. The events follow
the state of their business changes:

- flag = false: keep the events buffered when the transaction ends without a
  commit.
- flag = true: drop those events, including on stale detection after a dispose.

This supersedes the earlier "Orchestrator clarification" for the flag = true
case. The user is to be informed.

### Non-blocking findings

- **N1.** Projection falls back to property setters, which can raise
  notifications or trigger setter side effects. The restore loops are not
  fault-tolerant. Fix: project via fields only, skip non-snapshot tracking, and
  make restore best-effort per item.
- **N2.** Test gaps:
  - retry-safety on SQL Server identity (assert the retried INSERT has no `Id`
    column);
  - return value not asserted;
  - B1 scenarios;
  - ambient payload-UPDATE failure;
  - no-UPDATE-when-unchanged.
- **N3.** `{` and `}` in identifiers are not escaped for `ExecuteSqlRaw`.
- **N4.** Consumer-visible changes for release notes:
  - outbox entities are no longer tracked;
  - sync saves now use the relational path (InMemory provider and
    `TransactionScope` throw, as async already did);
  - an `ExpandoObject` is dispatched when flag = false;
  - consumer overrides run once;
  - binary compatibility is OK.

## Adversarial Test Findings (tester, 2026-11-18)

New tests are in `tests/XDev.Outbox.EFCore.Tests/Adversarial/`. Result for every
target framework: 151 tests, 132 passing, 18 failing, 1 skipped (D5). All 56
original tests pass.

### Defects

1. **HIGH: FK violation on retry.** A failure after the INSERTs succeed
   (payload UPDATE or commit) is followed by a retry. With a store-generated
   principal and a dependent (User + Order), the retry fails with an FK
   violation.
   - **Confirmed EF behavior, which contradicts the planner's claim:** after a
     successful `SaveChanges(false)`, the generated key and FK values are no
     longer temporary, and EF does not reset them when the outer transaction
     fails.
   - **Fix:** snapshot the temporary key and FK values before each attempt,
     and restore them as temporary values on failure.
2. **MEDIUM: savepoint rollback still dispatches.** A caller calls
   `RollbackToSavepoint` after a save. The message for the rolled-back row is
   still enqueued on commit (a phantom).
3. **LOW: braces break the UPDATE.** `{` or `}` in the outbox table name or
   columns breaks `ExecuteSqlRaw`. Same as reviewer N3.
4. **LOW: setters called during projection.** Projection calls property
   setters, which can have side effects. Same as reviewer N1.

### Confirmed working

- Rule A projection for:
  - computed columns on Modified entities;
  - owned types;
  - collections and nested graphs;
  - notification-tracked entities;
  - Guid outbox keys;
  - custom table names containing spaces.
- Return value excludes outbox rows.
- Cancellation.
- Sequential transactions.
- `UseTransaction`.

### Limitations

- **D5 (deferred).** An ambient payload UPDATE failure followed by a retry in
  the same transaction leaves duplicate business rows.
- A raw `DbTransaction` commit made outside EF fires no interceptor. The poller
  delivers the message later.
- The same event instance added twice is dispatched twice.

### User decision (2026-11-18): properties with no backing field

Approved: **fall back to property setters** when a projected property has no
`FieldInfo`. The setter is called once to project and once to restore, and the
restore is fault-tolerant. This keeps Rule A for every entity, so there is no
regression against HEAD.

The side-effect test changes from asserting the setter call count to asserting
that the key is present in the payload. This test change is user-approved.
Fields are still preferred whenever they exist.

## Final Review (independent reviewer, re-review, 2026-11-18)

Verdict: APPROVE WITH NON-BLOCKING COMMENTS. No blocking findings.

### Recommended before release

- **N1 (MEDIUM): stale flag=false events lost.** A caller transaction ends
  stale, after a `SaveChanges(false)` followed by dispose. A different
  transaction then commits with no save. `TakePendingOutboxMessages` and
  `ClearEvents` drop the flag=false events, so a later re-save writes the
  business rows without an event. The B1 rule must also apply here.
- **N2 (MEDIUM): no SQL Server smoke tests.** Not run on SQL Server:
  - identity retry;
  - savepoint handling;
  - payload UPDATE parameter typing;
  - insert batching with `OUTPUT`.

### Optional

- **N3: dependency on EF tracker internals.** Correctness relies on
  undocumented EF change-tracker internals. Record a decision and keep the
  test matrix that runs on every target framework.
- **N4: payload UPDATE skip decision.** Decide whether to skip the payload
  UPDATE from whether the projection changed any value, not by comparing
  re-serializations.
- **N5: payload UPDATE typing and row count.** Type the payload UPDATE
  parameters from the property's type mapping, and check the affected row
  count.
- **N6: savepoint existence query.** It needs `IgnoreQueryFilters`. A `TKey`
  without `==` would break `Expression.Equal`.
- **N7: performance.** Each event is serialized 3 times, plus one UPDATE
  round trip per changed message.
- **N8: setters that reject the restore value.** A setter that rejects the
  restore value (for example, a guard against 0) fails every save that has
  events. This follows from the approved setter fallback; tell the user.

### Questions

- **Q1:** a retry at the caller level after a failed commit in a caller
  transaction, with a principal and dependent, uses plain-EF behavior. That
  may fail on SQL Server.
- **Q2:** non-key database-default properties on retry.
- **Q4:** a partial failure inside `AddOutboxEntries`.

### Resolved by orchestrator

- **Q5:** every member removed from `OutboxDbContext`
  (`AddOutboxMessages`, `GetDefaultValue*`, `IsPrimaryKeyAutoGenerated`,
  `GetId`) was private in HEAD. Binary compatibility is OK.

## Second Adversarial Pass (tester, 2026-11-18)

New tests are in `tests/.../Round2/`. Result for every target framework: 357
tests, 346 passing, 10 failing, 1 skipped (D5). The previous 175 tests are
unchanged: 174 passing and 1 skipped.

The generated-value restore was confirmed for:

- deep, self-referencing, and mixed Guid/identity graphs;
- many-to-many relationships;
- owned collections with generated keys;
- Added dependents of Unchanged principals;
- Modified dependents re-parented to Added principals;
- setter-only and notification-tracked entities.

These cases were run with both strategy retry and app retry, and with one or
two failures. The B1 ordering and duplicate handling, nested savepoints,
savepoint release, and batching above 100 were also confirmed.

### Defects

- **D-A (MEDIUM), same as reviewer N1.** A transaction is disposed after a
  flag=false save, then a different transaction commits without saving. The
  event is dropped, so it is lost.
- **D-B (LOW–MEDIUM).** The savepoint existence query fails, then the caller
  commits. The rolled-back message is still dispatched (a phantom).
- **D-C (LOW).** Within one transaction, a flag=false save followed by a
  flag=true save: the flag=true `AcceptAllChanges` also accepts the earlier
  changes, but the earlier message keeps `AcceptedChanges=false`. After a
  rollback, that event is kept and later dispatched (a phantom).
- **D-D (MEDIUM–LOW, regression from HEAD).** If a key setter has a guard (for
  example, rejecting values of 0 or less) and there is no backing field, the
  entity can never be saved with an event, because the restore writes 0.

### Orchestrator direction for D-D

For setter-only properties, skip the post-projection restore when the save
commits with flag=true. `AcceptAllChanges` writes the same value anyway, so
this matches HEAD's setter calls. Restore stays in place for flag=false and for
failures. A guarded setter can still fail on those paths; this is a documented
limitation.

## Final-round Review (focused reviewer, 2026-11-18). Verdict: CHANGES REQUIRED (F1 only)

The 6 conflicting tests were realigned by the tester as directed by the
orchestrator:

- D-D: the restore-throws test injects a commit failure.
- The guarded-setter flag=false case is a documented limitation.
- D-C: the same-instance test expects both copies to be dropped.

Orchestrator verification after the realignment:

- Build: 0 errors.
- Tests: 370 passed, 0 failed, 1 skipped (D5) on net8, net9, net10 and net11.

### Findings

- **F1 (MEDIUM, blocking): lost event.** Sequence:
  1. `SaveChanges(false)` with an event.
  2. Explicit rollback, or a failed commit, or a savepoint release. B1 keeps
     the event buffered.
  3. Another transaction ends without a save (rollback or commit).
  4. The kept event is now indistinguishable from a never-saved event, so
     `UpdateBuffer` drops it.
  5. A later save writes the business rows with no event.

  Fix: track the events kept by B1 (by reference and count) until they are
  re-saved, accepted by a flag=true save, or cleared by an owned save.
- **N-1:** the commit-time warning has no `EventId`.
- **N-2:** a comment at `OutboxDbContext.cs:574` is wrong ("unreachable"), and
  there is no summary of the state machine.
- **Q-A:** when the savepoint check fails, the exception surfaces from the
  caller's `RollbackToSavepoint` even though the rollback itself succeeded.
- **Q-B:** a commit retry after a cancelled check is untested.
- **Q-C:** pooling does not reset pending state. This is harmless.

## Completion (orchestrator, 2026-11-18)

### Verification

Orchestrator re-ran the checks:

- `dotnet build XDev.Outbox.slnx`: 0 errors.
- Tests on net8.0, net9.0, net10.0 and net11.0: 388 passed, 0 failed,
  1 skipped (D5).
- `git diff --check`: clean.

The diff contains only the intended files:

- `src/XDev.Outbox.EFCore/OutboxDbContext.cs`
- `src/XDev.Outbox.EFCore/OutboxDispatchTransactionInterceptor.cs`
- `src/XDev.Outbox.EFCore/IOutboxTransactionParticipant.cs` (new)
- `tests/`
- `XDev.Outbox.slnx`

### Review status

- The focused re-review of the final round found F1, and F1 is fixed.
- The F1 fix itself was not re-reviewed by an independent reviewer. It was
  verified by tests, including a mutation check: disabling the fix makes 12
  tests fail.

### Durable knowledge updated

- `components/efcore-outbox-store.md` (rewritten)
- `business-rules/message-delivery.md`
- `architecture.md`
- `database/outbox-messages.md`
- `project.md`
- `components/outbox-dispatch-runtime.md`
- New: `decisions/001-deferred-acceptance-and-payload-projection.md`

### Open, for the user

- **SQL Server smoke tests (reviewer N2).** Needed for identity retry,
  savepoints, payload parameter typing, and `OUTPUT` batching.
- **N5.** Type the payload UPDATE parameters from the property's type mapping,
  and check the affected row count.
- **D5.** A payload UPDATE failure in a caller transaction, followed by a
  retry in the same transaction, duplicates rows.
- **Q-A.** When the savepoint check fails, the exception surfaces from
  `RollbackToSavepoint`.
- **EventId 41001.** Choosing this number was a developer decision; confirm it.
- **Release notes (N4 and later).** Consumer-visible changes:
  - outbox entities are no longer tracked after a save;
  - sync saves use the relational path, so InMemory and `TransactionScope` are
    unsupported;
  - with flag=false, subscribers receive an `ExpandoObject`;
  - consumer overrides of the bool overloads run once;
  - `AutoTransactionBehavior` is no longer forced;
  - setters run during saves for properties with no backing field;
  - an extra UPDATE per event whose payload references generated keys;
  - a savepoint existence query;
  - `AggregateException` when recovery fails.
- **Unrelated findings** from the 2026-11-18 bug check remain open:
  - #4–#7 (#4 is now resolved for this path);
  - the Medium and Low items.
