# Project

## Purpose

XDev.Outbox is a set of .NET class libraries (published to NuGet as
`XDev.Outbox` and `XDev.Outbox.EFCore`) that implement the transactional
outbox pattern for applications using Entity Framework Core.

An application records events in a scoped buffer (`IOutbox.AddEvent`). When
the application saves its EF Core `DbContext`, the events are written to an
outbox table in the same database transaction as the business data. After the
transaction commits, the events are dispatched to in-process subscribers
(`ISubscriber<TEvent>`). A background hosted service polls the outbox table to
dispatch anything that was not handled immediately (crash, failure, restart).

This repository is a library. It has no business domain of its own; the
"business rules" in `.ai/business-rules/` describe the delivery contract the
library gives to consuming applications.

## Primary Technologies

- C# / .NET, SDK-style projects, solution file `XDev.Outbox.slnx`.
- `XDev.Outbox`: targets `netstandard2.0; netstandard2.1; net8.0; net9.0; net10.0; net11.0`.
- `XDev.Outbox.EFCore`: targets `net8.0; net9.0; net10.0; net11.0`. Each target
  framework references the EF Core version with the same major number.
- `XDev.Outbox.ServiceBus`: targets `netstandard2.1`, references
  `Azure.Messaging.ServiceBus`. Experimental and not wired in (see
  `integrations/azure-service-bus.md`).
- Persistence: EF Core with **SQL Server only** in practice. The store uses
  SQL Server-specific raw SQL and `Microsoft.Data.SqlClient` (see
  `database/outbox-messages.md`).
- Serialization: `System.Text.Json`.
- Hosting: `Microsoft.Extensions.Hosting` `BackgroundService`, Microsoft DI,
  `IOptions<T>`, and `TimeProvider` on net8.0+.

## Repository Structure

- `src/XDev.Outbox/` holds the abstractions and the dispatch runtime: `IOutbox`,
  `ISubscriber<T>`, `IOutboxStore<TKey>`, `IMessageQueue<TKey>`,
  `MemoryMessageQueue`, `EventPublisher`, `OutboxProcessorHostedService`, and
  `AddOutbox(...)` DI registration.
- `src/XDev.Outbox.EFCore/` holds the EF Core / SQL Server store:
  `OutboxDbContext<TKey>`, `EntityFrameworkOutboxStore`,
  `OutboxDispatchTransactionInterceptor`, `OutboxMessageConfiguration`, and
  `AddEFCoreOutboxStore<...>()`.
- `src/XDev.Outbox.ServiceBus/` holds an incomplete Azure Service Bus queue
  prototype.
- `samples/Example/` is a console/worker host using SQL Server LocalDB, with EF
  migrations, `User` entity, and two event/subscriber pairs. It is also the only
  executable way to exercise the libraries.
- `README.md` (root) is a copy of `src/XDev.Outbox.EFCore/README.md`, which is
  the NuGet package readme.

`tests/XDev.Outbox.EFCore.Tests` (xUnit, SQLite in-memory, net8 to net11)
covers the EF Core save and dispatch path. `tests/XDev.Outbox.Tests` (xUnit,
net8 to net11) covers subscriber registration (`AddOutbox`) and
`EventPublisher`. There is no CI configuration.

## Domain Terminology

- **Event**: any CLR object passed to `IOutbox.AddEvent`. Its runtime type
  selects the subscribers.
- **Outbox (buffer)**: the scoped, in-memory `IOutbox` list of events pending
  for the current DI scope / `DbContext`.
- **Outbox message**: a persisted `OutboxMessage<TKey>` row holding the event
  type name, JSON payload, attempt count, and lock expiry.
- **Subscriber**: an `ISubscriber<TEvent>` implementation discovered by
  assembly scanning.
- **Immediate dispatch**: the path where messages are enqueued in memory right
  after the transaction commits.
- **Poller / processor**: `OutboxProcessorHostedService`, which claims pending
  rows from the store and enqueues them.
- **Lock / claim**: `LockExpiresAt`. While it is in the future, the poller will
  not pick up the row.
- **TKey**: the outbox message primary-key type. Only `int` and `Guid` are
  accepted by `AddOutbox`.

## Important Constraints

- The libraries are published NuGet packages. Public types and members
  (interfaces, `OutboxMessage<TKey>` columns, DI extension signatures,
  `OutboxDbContext` base classes) are consumer-facing API. Changing them can
  break consumers or require consumer database migrations.
- The outbox table schema is created by **consumer** EF migrations, not by
  this repository. Any change to `OutboxMessage<TKey>` or
  `OutboxMessageConfiguration` is a schema change for every consumer.
- Persisted rows store the event's assembly-qualified type name. Renaming,
  moving, or re-assembling an event type breaks dispatch of rows already
  persisted (see `business-rules/message-delivery.md`).
- Multi-targeting relies on `#if NET8_0_OR_GREATER` / `#if NET9_0_OR_GREATER`
  branches. Changes must be checked for every target framework.
- The sample's `appsettings.json` holds a LocalDB integrated-security
  connection string only. Do not add real credentials.

## Verification

### Build

```
dotnet build XDev.Outbox.slnx
```

As of 2026-11-18 (working tree) this succeeds with 0 errors. It produces
about 50 warnings stating that the `11.0.0` `Microsoft.Extensions.*` and
`System.*` packages "don't support net8.0/net9.0". These come from the
`11.0.0` package versions referenced by `XDev.Outbox.csproj`, which flow
transitively into `XDev.Outbox.EFCore`.

### Unit Tests

```
dotnet test tests/XDev.Outbox.EFCore.Tests -f net8.0
dotnet test tests/XDev.Outbox.EFCore.Tests -f net9.0
dotnet test tests/XDev.Outbox.EFCore.Tests -f net10.0
dotnet test tests/XDev.Outbox.EFCore.Tests -f net11.0
```

Run all four target frameworks; EF behavior differs by version.

- The tests use SQLite in-memory, so SQL Server is not needed.
- On 2026-11-18 each framework reported 389 tests: 388 passed and 1 skipped.
  The skipped test is the deferred D5 limitation.
- Also run `dotnet test tests/XDev.Outbox.Tests -f <tfm>` for the same four
  frameworks. On 2026-11-19 each reported 11 passed.
- Core runtime coverage is limited to subscriber registration and
  `EventPublisher`. The queue and hosted service have no tests, and the
  netstandard code paths are compiled but not executed by tests.

### Integration Tests

None exist. The only runtime verification is to run `samples/Example`
(`dotnet run --project samples/Example -- --console`) against SQL Server
LocalDB (`(localdb)\MSSQLLocalDB`, database `OutboxExample`), after applying
the sample's EF migrations.

### Additional Verification

None configured (no analyzers, linters, or CI).

## Related Knowledge

- `architecture.md` covers components, flows, and dependency direction.
- `business-rules/message-delivery.md` covers the delivery contract given to
  consumers.
- `components/outbox-dispatch-runtime.md` covers the queue, processor, and
  publisher.
- `components/efcore-outbox-store.md` covers the DbContext save path,
  interceptor, and registration.
- `database/outbox-messages.md` covers the table, claim query, transactions,
  and locking.
- `integrations/azure-service-bus.md` covers the Service Bus prototype status.
- There is no legacy system. `legacy/` is intentionally empty.
