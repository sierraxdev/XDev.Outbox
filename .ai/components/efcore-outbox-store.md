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
2. Register the DbContext with `AddDbContext`.
3. Call `AddOutbox(assemblies)`, which provides `IOutbox`, `IMessageQueue`, and
   the hosted service.
4. Call `AddEFCoreOutboxStore<TContext>()` for an int key, or
   `AddEFCoreOutboxStore<TKey, TContext>(o => o.TableName = ...)`.

Both registrations are required. Every save resolves `IOutbox`,
`IMessageQueue<TKey>`, and `TimeProvider` through `this.GetService<T>()`, which
throws if a service is missing. The transaction interceptor also depends on
`IMessageQueue<TKey>`.

`AddEFCoreOutboxStore` adds the interceptor to the DbContext options. The
mechanism depends on the target framework:

- **net9.0+**: uses `services.ConfigureDbContext<TContext>(...)`.
- **net8.0**:
  - `DbContextOptions<TContext>` must already be registered, or the call
    throws.
  - It builds a temporary service provider, which is never disposed, and
    replaces the options with a scoped factory.
  - The consumer's options lambda therefore runs once, and options that depend
    on scoped services are frozen. This is a known defect and was not fixed.

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

For contexts that are not participants, it only clears the buffer on commit
or rollback.

## EntityFrameworkOutboxStore

This store is scoped and uses the scope's `TContext`.

- `GetAllAsync` runs the claim query with `AsNoTracking`.
- `RemoveAsync` and `UpdateAsync` call `SaveChangesAsync`. With no buffered
  events, these behave as plain saves.

## Known Limitations

- **D5 (deferred by the user).** In a caller transaction, the payload UPDATE
  runs after EF releases its own savepoint. If that UPDATE fails and the caller
  retries inside the same transaction, the business rows are duplicated. The
  test for this is skipped.
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
  change-tracker behavior; see `.ai/decisions/001-deferred-acceptance-and-payload-projection.md`.
- **Not verified on SQL Server.** The tests run on SQLite only. Unverified on
  SQL Server: identity on retry, savepoint SQL, payload UPDATE parameter typing
  (reviewer N5), and `OUTPUT` batching.
- **`TransactionScope` and InMemory.** These are unsupported on every overload:
  the save path requires relational transactions.

## Other Observed Risks (pre-existing, not addressed)

- **Design-time factory.** The sample's `DesignTimeApplicationDbContextFactory`
  calls `AddEFCoreOutboxStore` without `AddOutbox`. On net9+, `dotnet ef`
  design-time creation will likely fail.
- **Pooled and factory-created contexts.** With `IDbContextFactory` or pooling,
  `IOutbox` may resolve from the root provider. Pending state is not reset on
  pool return; it is released harmlessly on the next lease.
- **Unused interceptor.** `OutboxInterceptor<TKey>` is registered but never
  attached. Adding it manually would double-insert messages.
