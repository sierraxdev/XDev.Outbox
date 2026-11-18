# Message Delivery Contract

## Purpose

This document describes the observable behavior that consuming applications
rely on when they publish events through the outbox and handle them with
`ISubscriber<T>`. Future changes must preserve this behavior or deliberately
change it as a breaking change.

Each rule is labeled:

- **[Confirmed]**: directly established by source code.
- **[Observed, intent unknown]**: the code behaves this way, but nothing shows
  it is intended.

## Publishing

- [Confirmed] Events are any CLR object passed to `IOutbox.AddEvent(params object[])`.
  `IOutbox` is scoped, so events belong to the DI scope that resolved it.
- [Confirmed] Events are persisted only by
  `OutboxDbContext.SaveChangesAsync(CancellationToken)`. The sync
  `SaveChanges()` and `SaveChangesAsync(bool, CancellationToken)` are not
  overridden and do not write buffered events.
- [Confirmed] Business changes are saved first and the outbox rows second,
  inside the same transaction. If an event *references* a business entity
  object, as the sample's `UserCreatedEvent.User` does, that entity's
  store-generated key is populated before the payload is serialized. A key
  value copied into the event at `AddEvent` time is still the default.
- [Confirmed] `SaveChangesAsync` on any `OutboxDbContext` resolves `IOutbox`
  and `IMessageQueue<TKey>` through EF's `GetService<T>()`, which throws when a
  service is missing. `AddOutbox` must therefore be registered, even for saves
  with no buffered events. (The `IMessageQueue` it resolves is no longer used.)
- [Confirmed] After a commit, the buffer is cleared. After a rollback or a
  failed commit or rollback, the buffer is cleared without dispatch
  (`OutboxDispatchTransactionInterceptor`).
- [Confirmed] Each persisted row stores `Type = event.GetType().AssemblyQualifiedName`
  and `Payload` = the event serialized as JSON with
  `OutboxConfiguration.SerializerOptions`: camelCase, enums as strings, nulls
  omitted, reference cycles ignored.
- [Confirmed] A new row is created with
  `LockExpiresAt = now + MessageLockExpirationTimeSpan` (default 5 min). This
  keeps the poller from picking it up while immediate dispatch is expected to
  handle it. The commit "Fixed message double enqueueing" (d03182c) introduced
  this.

## Subscriber Resolution

- [Confirmed] Subscribers are discovered by scanning the assemblies passed to
  `AddOutbox(...)`. At least one assembly is required.
- [Confirmed] Each discovered closed `ISubscriber<X>` is registered with
  `TryAddScoped`. **Only the first discovered implementation per event type is
  registered.** Additional subscribers for the same event type are silently
  ignored, as is any implementation registered after an existing
  registration. [Observed, intent unknown] `EventPublisher` enumerates
  `IEnumerable<ISubscriber<X>>` as if many subscribers were supported.
- [Confirmed] Subscribers are matched on the event's **exact runtime type**.
  `ISubscriber<BaseType>` does not receive derived events.
- [Confirmed] The handler is invoked by reflection on a public instance method
  `HandleEventAsync(<exact event type>, CancellationToken)`. Explicit interface
  implementations are not found, which results in a `NullReferenceException`
  and counts as a failed attempt.
- [Confirmed] Multiple subscribers, when registered manually, run sequentially.
  The first exception stops the rest and fails the whole message.
- [Confirmed] An event with no registered subscriber is treated as
  successfully handled, and its row is deleted. This relies on Microsoft DI
  returning an empty `IEnumerable<>` for unregistered services. A container
  that returns null would throw at `EventPublisher.cs:23`.
- [Confirmed] Subscribers run in the same DI scope as the `IOutboxStore`, so
  they share the scope's `TContext` instance. Changes a subscriber leaves
  unsaved are written by the store's `RemoveAsync` save, atomically with the
  row delete, or by `UpdateAsync` together with the failure record.

## Delivery Semantics

- [Confirmed] Delivery is **at-least-once**. Duplicates are possible in these
  cases:
  - the handler succeeds but the row delete fails;
  - processing outlasts `MessageLockExpirationTimeSpan` and the poller
    re-claims the row;
  - the process crashes after the handler and before the delete;
  - with several subscribers, those that succeeded run again when a later one
    fails;
  - **backlog re-claim**: while more than 100 rows are claimable, the poller
    loops with no delay, claiming 101 rows per pass into the unbounded
    in-memory queue. That queue handles 10 messages at a time. Rows still
    queued when their 5-minute lock expires are claimed and enqueued again in
    the same process.

  Subscribers should be idempotent.
- [Confirmed] There is no ordering guarantee. The claim query orders by `Id`,
  but up to 10 messages are handled concurrently per process.
- [Confirmed] On success, the row is deleted. The table holds only pending or
  failed messages, and there is no processed history.
- [Confirmed] On failure, `Attemps` is incremented, `IsError = true`, and
  `ErrorMessage = exception.Message`. `LockExpiresAt` is not changed, so the
  retry happens on a later poll after the lock expires.
- [Confirmed] Retry stops when `Attemps >= MaxAttemps`. `MaxAttemps` defaults
  to 15 when `OutboxConfiguration.MaxAttemps` is null. Rows past the limit stay
  in the table permanently. There is no dead-letter handling or cleanup.
- [Confirmed] Retry timing has no backoff. A failed message is retried at the
  first poll after its lock expires. Polls happen every 30 minutes, or
  continuously while the backlog exceeds 100 rows, so the retry delay can reach
  about 30 minutes.

## Payload Received by Subscribers

- [Confirmed] **Immediate dispatch** passes the **original in-memory event
  instance** to the subscriber. This is the same object the application
  passed to `AddEvent`, including any EF entity graphs it references.
- [Confirmed] **Poller dispatch** passes a new instance rebuilt from JSON. The
  instance is created with `Activator.CreateInstance` and its public writable
  properties are populated (`ObjectExtensions.ToObject`). Init-only setters
  are writable. The rules for event types are:
  - They must be classes with a public parameterless constructor.
  - Structs are rejected.
  - Types without a parameterless constructor, such as positional records,
    fail every attempt.
  - Get-only properties are not populated.
  - Top-level properties are matched by
    `SerializerOptions.PropertyNamingPolicy.ConvertName(name)`, so
    `[JsonPropertyName]` renames are not honoured at the top level.
  - Replacing `SerializerOptions` with one that has no naming policy causes a
    `NullReferenceException`.
- [Observed, intent unknown] Subscribers can therefore see different object
  identity and fidelity depending on the path. JSON-ignored or cyclic members
  are present on the immediate path and absent on the poller path.

## Type Resolution

- [Confirmed] Dispatch uses `Type.GetType(row.Type)`. If the type cannot be
  loaded, the message fails each attempt until the maximum is reached. This
  happens when the type is renamed, moved to another namespace or assembly,
  or its assembly is not loaded.
- [Confirmed] The `Type` column is `nvarchar(200)`. Assembly-qualified names
  longer than 200 characters, such as generic events or long namespaces, fail
  to insert. The insert shares the business transaction, so the whole
  `SaveChangesAsync` fails, not just the outbox row.

## Evidence

- `src/XDev.Outbox/ServiceCollectionExtensions.cs`: subscriber scanning and `TryAddScoped`.
- `src/XDev.Outbox/EventPublisher.cs`
- `src/XDev.Outbox/OutboxProcessorHostedService.cs`: constants, success and failure handling.
- `src/XDev.Outbox/ObjectExtensions.cs`, `OutboxConfiguration.cs`
- `src/XDev.Outbox.EFCore/OutboxDbContext.cs`, `OutboxDispatchTransactionInterceptor.cs`,
  `EntityFrameworkOutboxStore.cs`, `OutboxMessageConfiguration.cs`
