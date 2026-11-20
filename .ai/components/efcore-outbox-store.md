# EF Core Outbox Store (XDev.Outbox.EFCore)

This document covers `OutboxDbContext`, `OutboxDispatchTransactionInterceptor`,
`IOutboxTransactionParticipant`, `EntityFrameworkOutboxStore`, and
`AddEFCoreOutboxStore`. Table and SQL details are in
`database/outbox-messages.md`. The consumer-facing contract is in
`business-rules/message-delivery.md`.

The save path described here was introduced by the fix in
`.ai/tasks/fix-savechanges-overloads-skip-outbox.md` (2026-11-18). That fix
covers findings #1, #2, and #3 and was uncommitted at the time of writing.
`tests/XDev.Outbox.EFCore.Tests` is the executable specification. Each
behavior below is pinned by tests on net8 through net11. All of these tests
run on SQLite.

## Consumer Setup

1. Make the consumer DbContext inherit `OutboxDbContext<TKey>`. If it overrides
   `OnModelCreating`, it must call `base.OnModelCreating`.
2. Register the DbContext with `AddDbContext`, `AddDbContextPool`,
   `AddDbContextFactory` (any lifetime) or `AddPooledDbContextFactory`.
3. Call `AddOutbox(assemblies)`, which provides `IMessageQueue` and the hosted
   service.
4. Call `AddEFCoreOutboxStore<TContext>()` for an int key, or
   `AddEFCoreOutboxStore<TKey, TContext>(o => o.TableName = ...)`.
5. Add events with `dbContext.Outbox.AddEvent(...)`.

### Event buffer (since 2026-11-19, decision 002)

- Each `OutboxDbContext` instance owns its buffer: `Outbox` (type `IOutbox`,
  internal `ContextOutbox`). `IOutbox` is no longer registered in DI and the
  `Outbox` class was removed; code that injects `IOutbox` fails at resolution.
- Only saves on that context persist its events. Another context in the same
  scope does not. Unsaved events are discarded when the context is disposed or
  returned to its pool.
- **Per-lease reset.** `EnsureCurrentLease()` compares `DbContext.ContextId.Lease`
  (0 for non-pooled, incremented on every pool lease; pinned by tests on EF
  8–11) and, on change, clears the buffer and all pending/transaction state.
  It runs first in both save overrides, every `IOutboxTransactionParticipant`
  member (including the `IsOutboxSaveInProgress` getter) and every buffer
  method. EF's pool reset raises no transaction interceptor event, so cleanup
  cannot rely on `EndTransactionWithoutCommit`. Tests showed the buffer clear
  is load-bearing; clearing pending state is defense in depth (stale pending
  ids never match a new transaction).
- A stale reference to a context after its lease ended still accepts
  `Outbox.AddEvent`; the event is discarded at the next lease (misuse, as with
  any `DbContext`).

The remaining `GetService<T>()` calls resolve singletons only
(`IMessageQueue<TKey>`, `TimeProvider`, options), so resolving them from the
root provider of pooled or factory contexts is valid. They throw if
`AddOutbox`/`AddEFCoreOutboxStore` were not called.

### Interceptor registration

`AddEFCoreOutboxStore` adds the singleton interceptor to the DbContext options,
at most once per options instance (reference check), so calling it twice is
harmless. A second, consumer-created interceptor instance is attached too but
causes no double dispatch.

- **net9.0+**: `services.ConfigureDbContext<TContext>(...)`. Works for every
  registration kind; ordering relative to the DbContext registration does not
  matter.
- **net8.0**: wraps the last non-keyed `DbContextOptions<TContext>` descriptor
  in place, keeping its lifetime (scoped for `AddDbContext`, singleton for
  pools and default factories). The consumer's options action runs on every
  resolution with the real provider. The DbContext must be registered first,
  or the call throws. (Before 2026-11-19 it built a temporary provider and
  replaced the options with a frozen scoped snapshot: finding #6.)

Hand-built options need `UseApplicationServiceProvider(sp)` and
`AddInterceptors(sp.GetRequiredService<OutboxDispatchTransactionInterceptor<TKey>>())`.

## Save Path

The logic lives in the overrides `SaveChanges(bool)` and
`SaveChangesAsync(bool, CancellationToken)`. EF routes `SaveChanges()` and
`SaveChangesAsync(CancellationToken)` to these overrides, so all four public
overloads are covered. Internal saves always call `base.*(false)` explicitly.
The sync and async paths mirror each other, and the database work never wraps
async code in sync calls.

### Common steps

1. Release stale pending messages. These are messages whose caller transaction
   ended without an EF event, for example a dispose.
2. Collect buffered events that are not already pending in the current
   transaction.
3. If there are no such events, pass through to
   `base.*(acceptAllChangesOnSuccess)`. A flag=true save also marks earlier
   pending messages as accepted (rule D-C).
4. Each attempt does the following:
   - Snapshot the store-generated and temporary key and FK values of Added
     entries.
   - Add fresh `OutboxMessage` entities.
   - Call `base.SaveChanges(false)`. This is a single save, and acceptance is
     deferred.
   - Patch payloads (Rule A, below).
   - Detach the outbox entities and assign their real Ids.

### Library-owned transaction

This path applies when there is no `Database.CurrentTransaction`.

- The attempt runs inside `Database.CreateExecutionStrategy()`, in a
  `ReadCommitted` transaction, and commits.
- After success:
  1. Call `AcceptAllChanges()`, only when the flag is true.
  2. Clear the buffer.
  3. Enqueue the messages. The sync path blocks on `EnqueueAsync`.
- Dispatch happens in the save method, after acceptance. The interceptor
  ignores owned transactions; this is guarded by save depth and
  `IsOutboxSaveInProgress`.

### Caller-owned transaction

- The messages become pending in the context, keyed by `TransactionId`.
- `AcceptAllChanges()` is called when the flag is true.
- Dispatch happens when the caller commits.

### Failure recovery

This runs on any failure in an attempt, on both paths:

- Restore the snapshotted key and FK values, marking them temporary again. EF
  does **not** undo non-temporary generated values after a successful
  `SaveChanges(false)` when the outer transaction fails. Without this restore,
  a retry of a principal plus dependent fails with an FK violation.
- Detach the attempt's outbox entities.
- Keep the buffer (decision D2).

Execution-strategy retries and application-level retries both persist
everything exactly once. A retried INSERT carries no explicit identity value.

### Rule A: generated keys in payloads (decision D1)

EF keeps store-generated values in tracker side storage until acceptance, so
the CLR objects still hold defaults when the INSERT payload is serialized. The
fix-up works like this:

1. Temporarily copy pending tracker values onto the CLR objects of
   Added/Modified entries. It writes the backing field first; when there is
   none, it falls back to the property setter, a user-approved choice.
2. Re-serialize the payloads with the model's value converter.
3. Restore the CLR values. The restore is fault-tolerant: it tries every item.
4. Run a parameterized `UPDATE <table> SET <Payload> = {0} WHERE <Id> = {1}`
   for messages whose payload changed.
   - Identifiers come from the model and are delimited with
     `ISqlGenerationHelper`.
   - Braces are escaped.
   - No UPDATE runs when projection changed nothing.

Setter-only properties are not restored when the save ends with acceptance
(flag=true). In that case `AcceptAllChanges` writes the same value anyway.

### Dispatched payload

- With flag=true, the original event instance is dispatched.
- With flag=false (decision D3), an `ExpandoObject` rebuilt from the persisted
  JSON is dispatched. It has the same conversion limits as the poller path.

## Pending-message state machine (caller-owned transactions)

The authoritative summary is the comment at the top of
`OutboxDbContext<TKey,TContext>`.

**State**

- Pending messages belong to at most one transaction id.
- Each pending message records whether its business changes were accepted.
  A message can also be **unconfirmed**, after a failed savepoint check.
- Events are matched to messages by reference and count.

**Transitions**

- **Commit:**
  1. Unconfirmed messages are re-checked in `TransactionCommitting`.
  2. Confirmed messages are dispatched.
  3. Committed and never-saved events are removed from the buffer.
- **End without commit (B1).** This covers rollback, a failed commit or
  rollback, and stale detection after a dispose.
  - flag=false: the events stay buffered, and the business changes stay
    pending.
  - flag=true: the events are dropped, because the business changes were
    accepted.
  - Events kept this way are tracked until they are saved again, accepted, or
    removed from the buffer by the app. Later transactions that end without
    saving them do not drop them.
- **`RollbackToSavepoint`:**
  - An existence query checks which pending rows still exist. It runs in
    batches of 100, ignores query filters, and is skipped while a save is in
    progress.
  - Missing rows are released under the B1 rule.
  - EF savepoint interceptor events carry no savepoint name, so rows cannot be
    matched to a savepoint directly.
- **Failed existence check:**
  - The exception surfaces from the caller's `RollbackToSavepoint`, even though
    the rollback itself succeeded.
  - The messages become unconfirmed and are re-checked later.
  - If the check before commit also fails, the commit proceeds and no
    in-memory dispatch happens. The poller delivers the committed rows. The
    warning is EventId 41001,
    `XDev.Outbox.EFCore.UnconfirmedPendingMessagesAtCommit`.

## OutboxDispatchTransactionInterceptor

This is a singleton `DbTransactionInterceptor` with no state besides the queue.
It delegates to the context through the internal
`IOutboxTransactionParticipant<TKey>` for these events:

- committing;
- committed;
- rolled back;
- failed, with action `Commit` or `Rollback`;
- rolled back to savepoint.

Contexts that are not participants (plain `DbContext`) are ignored.

## EntityFrameworkOutboxStore

This store is scoped. It uses the scope's `TContext` if one is registered;
otherwise it creates one from `IDbContextFactory<TContext>`, owns it, and
disposes it with the scope (`IDisposable`/`IAsyncDisposable`); otherwise its
constructor throws naming the registration methods.

- `AddPooledDbContextFactory` registers a scoped `TContext` only on EF 10+. On
  EF 8/9 with the pooled factory alone, the store uses its own context, so a
  subscriber's context is not the store's: follow-up events a subscriber adds
  to its own context are **not** saved by `RemoveAsync` (silently). Pinned by
  `ProcessorScope_PooledFactoryOnly_DocumentsWhetherTheSubscriberSharesTheStoresContext`.

- `GetAllAsync` runs the claim query with `AsNoTracking`.
- `RemoveAsync` and `UpdateAsync` call `SaveChangesAsync`. With no buffered
  events, these behave as plain saves.

## Known Limitations

- **Caller-transaction saves are all-or-nothing only with savepoints
  (decision 003, fixes D5).** Each outbox save in a caller transaction runs
  inside savepoint `__XDevOutboxSavePoint` (reserved name) when
  `Database.AutoSavepointsEnabled && SupportsSavepoints`. On failure the
  library rolls back to it (async with `CancellationToken.None`), then
  restores the tracker, then rethrows the original exception. Without
  savepoints (SQL Server MARS, `AutoSavepointsEnabled=false`) the old
  behavior remains: roll back the transaction after any failed save. If the
  rollback fails (doomed transaction, e.g. `XACT_ABORT`, SQL error 3903), or
  no savepoint existed and EF's save had completed, warning EventId 41002
  `XDev.Outbox.EFCore.CallerTransactionSaveNotUndone` is logged.
- **Never-saved events on transaction end (2026-11-19).** Events in the
  buffer with no pending message in the ended transaction (failed saves,
  events added before the transaction or after its last save) stay buffered
  when the transaction ends without commit: explicit `Rollback()`, failed
  commit, failed rollback or dispose all behave the same. (Before, an
  explicit rollback dropped them, so a retry committed the entity without
  its outbox row.) Consequence: if the app abandons the unit of work after a
  rollback (`ChangeTracker.Clear()`, re-running logic), it must also call
  `Outbox.ClearEvents()`, or the kept event is persisted by the next save
  without its business change (payload keys 0). Pinned by
  `TesterRollbackKeepTests`.
- **Commit drops never-saved events (open, needs a decision).** A commit
  removes buffered events that were not saved in it: event added before
  `BeginTransaction` and commit without a save; event added after the last
  save; failed save then commit; never-saved events kept by an earlier
  rollback/dispose and a later transaction that commits without saving them.
  The business changes stay pending, so a later save commits them without an
  outbox row (silent event loss). Pinned by
  `NeverSavedEventsOnTransactionEndTests.NeverSavedEventThenCommit_...`.
- **Recovery re-keys outbox entries before detaching (2026-11-19).** When
  EF's own save fails after an earlier command produced a generated outbox
  key, EF reverts the entry to a temporary key but its identity map keeps
  the generated one. `RecoverFailedAttempt` calls `EntityEntry.DetectChanges()`
  on each outbox entry before detaching it (works with
  `AutoDetectChangesEnabled=false`, touches only that entry), so the retry no
  longer throws "cannot be tracked ... same key" (SQLite reuses rowids;
  latent on SQL Server). Business entries are left as plain EF leaves them:
  with auto-detect off the app must call `ChangeTracker.DetectChanges()`
  before retrying, as with plain EF.
- **Guarded setters on setter-only keys.** A setter that rejects the default
  value fails flag=false saves and failure recovery. The failure surfaces as an
  `AggregateException`.
- **Direct `ChangeTracker.AcceptAllChanges()` is not observed.** If called
  inside a caller transaction, pending flag=false messages are not marked
  accepted.
- **Commits outside EF.** A commit made directly on the raw `DbTransaction`
  passed to `UseTransaction` fires no interceptor. The poller delivers those
  rows later.
- **Duplicate events.** The same event instance added twice is persisted and
  dispatched twice.
- **Dependency on EF internals.** Correctness depends on undocumented EF
  change-tracker behavior (see `.ai/decisions/001-deferred-acceptance-and-payload-projection.md`)
  on `ContextId.Lease` incrementing per pool lease (decision 002), and on
  `EntityEntry.DetectChanges()` re-keying the identity map after a failed save.
- **SQL Server (verified 2026-11-19, LocalDB 17.0, net8–net11,
  `tests/XDev.Outbox.EFCore.SqlServer.Tests`).** Confirmed: identity values on
  execution-strategy and application retries (no FK errors, final ids in
  payloads), EF's per-save `SAVE TRANSACTION` in caller transactions, the
  savepoint existence query, `OUTPUT`/MERGE batching, pooled contexts, the
  net8 options path, and Guid keys. Payload parameters are `nvarchar(4000)`
  up to 4000 chars and `nvarchar(max)` above, with no implicit conversion;
  large and non-ASCII payloads round-trip exactly. The payload UPDATE is one
  round trip per message. D5 is fixed and verified there, including doomed
  transactions and MARS (fallback). Savepoints per save: owned 1 (EF's),
  caller with events 2, caller without events 1. Not covered: real
  transient `SqlException`s and deadlocks, commit-unknown outcomes, Azure
  SQL, factory registrations on SQL Server.
- **`TransactionScope` and InMemory.** These are unsupported on every overload:
  the save path requires relational transactions.

## Other Observed Risks (pre-existing, not addressed)

- **Design-time factory.** The sample's `DesignTimeApplicationDbContextFactory`
  calls `AddEFCoreOutboxStore` without `AddOutbox`. On net9+, `dotnet ef`
  design-time creation will likely fail.
- **Subscriber buffers then throws.** A subscriber that adds events to the
  shared scoped context and then throws has those events (and its partial
  changes) persisted by `store.UpdateAsync`'s save. Same as before the
  2026-11-19 change.

(Resolved 2026-11-19: pooled/factory contexts reading the root-provider
`IOutbox`, pending state leaking across pool leases, and the unused
`OutboxInterceptor<TKey>`, which was deleted.)
