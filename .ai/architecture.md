# Architecture

## Overview

The system is an in-process transactional outbox:

1. Application code buffers events on its DbContext (`dbContext.Outbox`).
2. Any `OutboxDbContext<TKey>` save overload writes business changes and
   outbox rows in one database transaction.
3. After commit, the just-written messages are enqueued to a singleton
   in-memory queue (immediate dispatch). For library-owned transactions the
   save method does this itself. For caller-owned transactions an EF Core
   transaction interceptor does it.
4. A singleton hosted service consumes the queue, resolves subscribers in a new
   DI scope, invokes them, then deletes the row (success) or records the
   failure (failure).
5. The same hosted service periodically claims unprocessed rows from the
   database and enqueues them (recovery / retry path).

All dispatch happens inside the consuming application's process. No external
broker is used by the shipped components.

## Major Components

| Component | Project | Responsibility |
|---|---|---|
| `IOutbox` (implemented by `OutboxDbContext.Outbox`) | XDev.Outbox / XDev.Outbox.EFCore | Per-context event buffer, reset per pool lease. |
| `ISubscriber<T>` | XDev.Outbox | Consumer handler contract. |
| `EventPublisher` (scoped) | XDev.Outbox | Resolves and invokes subscribers for an event's runtime type. |
| `IMessageQueue<TKey>` / `MemoryMessageQueue` (singleton) | XDev.Outbox | In-memory queue with bounded concurrent handling. |
| `OutboxProcessorHostedService<TKey>` | XDev.Outbox | Polls the store, consumes the queue, deletes or records failure. |
| `IOutboxStore<TKey>` | XDev.Outbox | Store abstraction (claim, remove, update). |
| `OutboxDbContext<TKey>` | XDev.Outbox.EFCore | Base DbContext that persists buffered events atomically with business data. |
| `OutboxDispatchTransactionInterceptor<TKey>` | XDev.Outbox.EFCore | Enqueues messages after commit and tells the context how each caller transaction ended, so it can update its buffer. |
| `EntityFrameworkOutboxStore<TKey,TContext>` (scoped) | XDev.Outbox.EFCore | SQL Server implementation of `IOutboxStore`. |
| `OutboxMessageConfiguration<TKey>` | XDev.Outbox.EFCore | Table mapping and JSON payload conversion. |

Component details are in `components/outbox-dispatch-runtime.md` and
`components/efcore-outbox-store.md`.

## Dependency Direction

- `XDev.Outbox` has no dependency on EF Core or any store. It depends on
  `Microsoft.Extensions.*` abstractions (logging, hosting), `System.Text.Json`,
  and, on every target except netstandard2.0, the full
  `Microsoft.Extensions.DependencyInjection` package.
- `XDev.Outbox.EFCore` and `XDev.Outbox.ServiceBus` depend on `XDev.Outbox`.
- The sample depends on `XDev.Outbox.EFCore`.
- Store and transport implementations plug in through `IOutboxStore<TKey>` and
  `IMessageQueue<TKey>` registered in DI. Core registrations use `TryAdd*`, so
  an implementation registered earlier wins.

## Execution Flows

### Write flow (application request)

`dbContext.Outbox.AddEvent(evt)` adds to the context's buffer. Then any save overload runs. The
logic lives in `SaveChanges(bool)` and `SaveChangesAsync(bool, ct)`:

- **No buffered events:** it passes through to the base save.
- **No ambient transaction:** it runs inside the execution strategy and a
  `ReadCommitted` transaction:
  1. A single `SaveChanges(false)` writes the business rows and the outbox
     rows.
  2. Payloads are patched with generated keys.
  3. It commits.
  4. It accepts changes, if the flag is true.
  5. It clears the buffer and enqueues the messages.
- **Ambient transaction:** it runs the same save and patch, then records the
  messages as pending. The interceptor dispatches them when the caller
  commits. If the transaction ends without a commit, the events follow their
  business changes (B1).

Details are in `components/efcore-outbox-store.md`.

### Immediate dispatch flow

`MemoryMessageQueue` receive loop takes a message and calls
`OutboxProcessorHostedService.HandleMessageAsync` in a new DI scope. That
method resolves the type, converts the payload, calls
`EventPublisher.PublishAsync`, and then:

- on success, calls `IOutboxStore.RemoveAsync` (deletes the row);
- on exception, increments `Attemps`, sets `IsError` and `ErrorMessage`,
  calls `IOutboxStore.UpdateAsync`, and rethrows (the queue logs it).

### Polling / recovery flow

`OutboxProcessorHostedService.ExecuteAsync` loops:

1. It claims up to 101 rows where `LockExpiresAt` is null or past and
   `Attemps` is below the maximum. The claim sets
   `LockExpiresAt = now + MessageLockExpirationTimeSpan`.
2. It enqueues each claimed row.
3. If 100 or fewer rows came back, it waits 30 minutes. Otherwise it loops
   immediately.
4. On a store error, it logs and waits 5 minutes.

Shutdown: cancellation usually arrives during the 30-minute `Task.Delay`. The
`OperationCanceledException` is caught by the generic `catch`, which logs
"Error occurred getting messages from store". The 5-minute error delay then
throws and exits `ExecuteAsync`. As a result, `_queue.StopAsync` and the
"stopping" log are normally not reached. The queue stops through its linked
cancellation token.

## Architectural Patterns

- Transactional outbox with an in-process dispatcher and a database poller as
  fallback.
- DI registration through extension methods in the
  `Microsoft.Extensions.DependencyInjection` namespace, using `TryAdd*`.
- Multi-target conditional compilation (`NET8_0_OR_GREATER`,
  `NET9_0_OR_GREATER`) for `TimeProvider` and DbContext options
  configuration.
- Per-message DI scope for handler execution and store access.

## Persistence Architecture

A single table (`OutboxMessages` by default) is mapped into the consumer's own
`DbContext` by inheriting `OutboxDbContext<TKey>`. Rows exist only while a
message is pending or failed. Successful dispatch deletes the row. See
`database/outbox-messages.md`.

## Integration Architecture

There is no external integration in the shipped dispatch path. An Azure
Service Bus queue class exists but does not implement `IMessageQueue<TKey>`
and is not registered. See `integrations/azure-service-bus.md`.

## Constraints

- Business data and outbox rows must be written in the same database
  transaction. This is the purpose of the library.
- Messages must not be dispatched before the transaction that wrote them
  commits. That is why dispatch is triggered by the transaction-commit
  interceptor, not by `SaveChanges`.
- The poller must stay safe for multiple application instances. The claim
  query uses `UPDLOCK, READPAST, ROWLOCK` plus a lock-expiry timestamp.
- Dispatch is at-least-once. Do not introduce changes that assume
  exactly-once delivery.

## Known Technical Debt

These items materially affect future work. Business intent for each is
unknown unless stated.

- Automated tests cover the EF Core save and dispatch path
  (`tests/XDev.Outbox.EFCore.Tests`, SQLite) and subscriber registration
  (`tests/XDev.Outbox.Tests`). The queue, the SQL Server claim query, and the
  hosted service have no tests.
- The save path is a large state machine (about 1,100 lines) that depends on
  undocumented EF change-tracker internals, plus `ContextId.Lease` for the
  per-lease reset of pooled contexts. See
  `decisions/001-deferred-acceptance-and-payload-projection.md` and
  `decisions/002-event-buffer-on-dbcontext.md`.
- The store is SQL Server-specific although the package is described as EF
  Core-generic.
- The poll interval (30 min), batch size (100), error back-off (5 min), default
  max attempts (15), and queue concurrency (10) are hard-coded.
- See the "Observed risks" sections in the component documents.
