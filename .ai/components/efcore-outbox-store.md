# EF Core Outbox Store (XDev.Outbox.EFCore)

This document covers `OutboxDbContext`, `OutboxDispatchTransactionInterceptor`,
`EntityFrameworkOutboxStore`, and `AddEFCoreOutboxStore`. Table and SQL
details are in `database/outbox-messages.md`.

> As of 2026-11-18, `OutboxDispatchTransactionInterceptor.cs` was untracked.
> The interceptor wiring in `ServiceCollectionExtensions.cs` and the reworked
> `OutboxDbContext.SaveChangesAsync` were uncommitted working-tree changes.
> In the last commit (ae5e429), `SaveChangesAsync` enqueued messages itself,
> right after saving. Re-check this document against `git log` before relying
> on it.

## Consumer Setup

1. Make the consumer DbContext inherit `OutboxDbContext<TKey>`. If it overrides
   `OnModelCreating`, it must call `base.OnModelCreating`.
2. Register the DbContext (`AddDbContext`).
3. Call `AddOutbox(assemblies)`, which provides `IOutbox`, `IMessageQueue`, and
   the hosted service.
4. Call `AddEFCoreOutboxStore<TContext>()` (int key) or
   `AddEFCoreOutboxStore<TKey, TContext>(o => o.TableName = ...)`.

Both `AddOutbox` and `AddEFCoreOutboxStore` are required at runtime. The
transaction interceptor depends on `IMessageQueue<TKey>` from `AddOutbox`.

`AddEFCoreOutboxStore` adds the interceptor to the DbContext options. The
method differs by target framework:

- **net9.0+**: `services.ConfigureDbContext<TContext>(...)`.
- **net8.0**: It requires `DbContextOptions<TContext>` to be registered
  already, and throws `InvalidOperationException` otherwise, so `AddDbContext`
  must be called first. It builds a temporary service provider, which is never
  disposed, to read the existing options. It then replaces the options
  registration with a scoped factory that adds the interceptor. The consumer's
  `AddDbContext` options lambda therefore runs once against that temporary
  provider, and options that depend on scoped services are frozen.

## Save Path (`OutboxDbContext.SaveChangesAsync(CancellationToken)`)

1. It reads the scoped `IOutbox`, `IMessageQueue<TKey>` (unused leftover),
   `TimeProvider`, and `IOptions<OutboxConfiguration>` through
   `this.GetService<T>()`. That call falls back to the application service
   provider and **throws** if a service is missing, so every save requires
   `AddOutbox`. It then builds `OutboxMessage` objects from the buffered
   events.
2. If there are no events, it calls `base.SaveChangesAsync`.
3. If there is no ambient transaction, it uses the execution strategy and a
   `ReadCommitted` transaction. It saves the business changes first, then adds
   the outbox messages that are not already tracked (matched by `Payload`
   reference), saves again with `AutoTransactionBehavior.Never`, and commits.
   - Before a retry, it restores the snapshot of Added/Modified/Deleted entry
     states and detaches tracked `OutboxMessage` entries. For each Added entry,
     it sets the first primary-key property with `ValueGenerated.OnAdd` to
     `default(TKey)`. Here `TKey` is the **outbox** key type, not the entity's
     own key type (see Observed Risks).
4. If there is an ambient transaction, it does the same two saves without
   committing. Dispatch then happens when the caller commits.
5. The message `Id` is always reset to `default(TKey)` before `Add`, so key
   generation is left to EF and the database.

## OutboxDispatchTransactionInterceptor

This is a singleton `DbTransactionInterceptor` that holds the singleton
`IMessageQueue<TKey>`.

- **Committed**: It enqueues tracked `OutboxMessage` entries in state
  `Unchanged` whose `Payload` is reference-equal to an event still in the
  scope's `IOutbox`, then clears the outbox. The sync overload blocks on
  `EnqueueAsync`.
- **Rolled back**: It clears the outbox.
- **Failed**: If the failed action was `Commit` or `Rollback`, it clears the
  outbox. Other failed actions, such as savepoints, are ignored. If the commit
  outcome is unknown, rows that were committed are found later by the poller.

## EntityFrameworkOutboxStore

This store is scoped and uses the scope's `TContext`.

- `GetAllAsync` runs the claim query with `AsNoTracking`.
- `RemoveAsync` and `UpdateAsync` each call `SaveChangesAsync`.

These calls go through the `OutboxDbContext` override. Unless that scope's
`IOutbox` has events, they behave like plain saves.

## Observed Risks (intent unknown, not runtime-verified)

- **Sync `SaveChanges()` is not intercepted.** Buffered events are not written
  by it. If a transaction commit is observed while the events are still
  buffered, the interceptor clears them, and the events are lost silently.
- **`AutoTransactionBehavior` stays `Never`.** After any save that wrote outbox
  rows, `Database.AutoTransactionBehavior` is set to `Never` and never
  restored. Later multi-statement `SaveChanges` calls on the same context
  instance run without an automatic transaction.
- **Retry clears the buffer.** If a transient failure triggers an
  execution-strategy retry and EF raises a rollback event, the outbox buffer
  is cleared. On the retried commit, no message then matches the buffer, so
  nothing is dispatched immediately. The rows are still persisted and are
  picked up by the poller after the lock expires. A failed commit always
  clears the buffer. It is unknown whether disposing an uncommitted EF
  transaction raises a rollback event.
- **Dead null checks.** The interceptor's `outbox?.` and `events == null` guards
  never take effect, because `GetService` throws rather than returning null.
- **Retry key reset uses the wrong type.** The reset assigns
  `default(TKey)`, which is the outbox key type. If a business entity's key
  type differs, for example `TKey = int` with a `long` or `Guid` key, the
  assignment is expected to throw and the retry fails. This is inferred and
  not executed. Composite keys are only partly reset.
- **Retry detach predicate.** The detach step compares `m.Payload == x.Entity`
  (an event against an `OutboxMessage`). This is effectively always false, so
  every tracked `OutboxMessage` is detached on retry.
- **Design-time factory.** The sample's `DesignTimeApplicationDbContextFactory`
  calls `AddEFCoreOutboxStore` without `AddOutbox`. On net9+, resolving the
  interceptor needs `IMessageQueue<int>`, which is not registered there, so
  `dotnet ef` design-time creation will likely fail.
- **Contexts outside a request scope.** Contexts created with
  `IDbContextFactory` or pooling resolve `IOutbox` from whichever provider the
  options carry. It is unverified that the buffer matches the caller's scope.
- **Unused interceptor.** `OutboxInterceptor<TKey>` is registered but never
  attached, so it is dead code.
- **Wrong XML doc reference.** The interceptor's XML doc refers to a
  non-existent `TransactionalOutboxMessageQueue`.
