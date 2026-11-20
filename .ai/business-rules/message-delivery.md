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

- [Confirmed] Events are any CLR object passed to
  `dbContext.Outbox.AddEvent(params object[])`. Events belong to that
  `OutboxDbContext` **instance**; only its saves persist them, and they are
  discarded if the context is disposed or returned to its pool unsaved.
  (Before 2026-11-19 the buffer was a DI-scoped `IOutbox`; it is no longer
  registered, and injecting it fails at resolution. Decision 002.)
- [Confirmed] All registration kinds are supported: `AddDbContext`,
  `AddDbContextPool`, `AddDbContextFactory` (any lifetime),
  `AddPooledDbContextFactory`. Pooled contexts never carry events or pending
  state into the next lease.
- [Confirmed] **All four** public save overloads persist buffered events
  atomically with the business changes: `SaveChanges()`, `SaveChanges(bool)`,
  `SaveChangesAsync(ct)`, and `SaveChangesAsync(bool, ct)`. Events are
  dispatched only after commit. (Before the 2026-11-18 fix, only
  `SaveChangesAsync(ct)` did this.)
- [Confirmed] Change acceptance is deferred until after the library-owned
  commit, and the caller's `acceptAllChangesOnSuccess` is honored. A failed
  save leaves the tracker as plain EF would:
  - business entries stay pending;
  - no outbox entities stay tracked;
  - the event buffer is kept.
  A retry then persists everything exactly once.
- [Confirmed] Rule A: if an event *references* a business entity object, as
  the sample's `UserCreatedEvent.User` does, the persisted payload contains
  that entity's store-generated keys and FKs. This holds for both flag values.
  A key value copied into the event at `AddEvent` time is still the default.
  See `components/efcore-outbox-store.md` for the mechanism.
- [Confirmed] Every save on an `OutboxDbContext` resolves
  `IMessageQueue<TKey>` and `TimeProvider` through EF's `GetService<T>()`,
  which throws when a service is missing. `AddOutbox` must therefore be
  registered, even for saves with no buffered events.
- [Confirmed] Library-owned save: after commit, the events it wrote are
  removed from the buffer (by reference and multiplicity) and its messages
  are enqueued; events added during the save stay buffered for the next save
  (since 2026-11-19). After a failure, the buffer is kept.
- [Confirmed] An event added while a save runs (`SavingChanges`/`SavedChanges`
  handlers, save interceptors) is never part of that save on any path; it is
  written by the context's next save, not atomically with the save that
  raised it. Add events before calling `SaveChanges` for atomicity.
- [Confirmed] Caller-owned transaction:
  - On commit, committed events are removed from the buffer and dispatched.
    Events never saved in the transaction stay buffered (since 2026-11-19).
  - If the transaction ends without commit (rollback, failed commit, failed
    rollback, or dispose; all behave the same since 2026-11-19), the events
    follow their business changes:
    - saved with `acceptAllChangesOnSuccess=false`: the events stay buffered;
    - saved with `true`: the events are dropped;
    - never saved in it (failed save, added before or after the saves): the
      events stay buffered.
  - An app that abandons its unit of work after any transaction end must call
    `dbContext.Outbox.ClearEvents()`; otherwise the kept events are persisted
    by the next save without their business changes.
  - Rows undone by `RollbackToSavepoint` are not dispatched.
  - Details are in `components/efcore-outbox-store.md`.
- [Confirmed] Outbox entities are not left tracked by the DbContext after a
  save.
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
- [Confirmed] Each discovered closed `ISubscriber<X>` is registered as scoped
  with `TryAddEnumerable`. **Every distinct implementation type per event type
  is registered and invoked.** (Before the 2026-11-19 fix, `TryAddScoped`
  registered only the first.) Deduplication:
  - the same implementation type is registered once across repeated
    `AddOutbox` calls, an assembly passed twice, or a manual registration by
    type made before `AddOutbox`;
  - a manual registration made **after** `AddOutbox` (`AddScoped`), or a
    factory registration whose return type is the interface
    (`AddScoped<ISubscriber<X>>(_ => new Sub())`), is not deduplicated against
    the scanned type, so that subscriber runs twice;
  - a concrete subscriber and a concrete subclass of it are two
    implementations, so both run.
- [Confirmed] An open generic subscriber class (`Sub<T> : ISubscriber<T>`) in
  a scanned assembly is registered, and `BuildServiceProvider()` then throws
  `ArgumentException`.
- [Confirmed] Subscribers are matched on the event's **exact runtime type**.
  `ISubscriber<BaseType>` does not receive derived events.
- [Confirmed] The handler is invoked by reflection on a public instance method
  `HandleEventAsync(<exact event type>, CancellationToken)`. Explicit interface
  implementations are not found, which results in a `NullReferenceException`
  and counts as a failed attempt.
- [Confirmed] Multiple subscribers for one event run sequentially, in
  registration order: manual registrations made before `AddOutbox` first; the
  order among scanned subscribers follows `Assembly.GetTypes()` and is not
  guaranteed. The first exception stops the rest and fails the whole message.
  Unsaved `DbContext` changes left by subscribers that already succeeded are
  saved together with the failure record, and those subscribers run again on
  retry.
- [Confirmed] An event with no registered subscriber is treated as
  successfully handled, and its row is deleted. This relies on Microsoft DI
  returning an empty `IEnumerable<>` for unregistered services. A container
  that returns null would throw at `EventPublisher.cs:23`.
- [Confirmed] Subscribers publish follow-up events by injecting the
  `DbContext` and calling `Outbox.AddEvent`. With a scoped `TContext` they share
  the store's context (next bullet). With `AddPooledDbContextFactory` alone on
  EF 8/9 the store creates its own context, so follow-up events added to a
  subscriber's own context are not saved by the store.
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
  `ErrorMessage = exception.Message`. Since 2026-11-19 this is the
  subscriber's own exception even when it throws synchronously (before, it
  was the reflection wrapper text "Exception has been thrown by the target of
  an invocation."). `LockExpiresAt` is not changed, so the
  retry happens on a later poll after the lock expires.
- [Confirmed] Retry stops when `Attemps >= MaxAttemps`. `MaxAttemps` defaults
  to 15 when `OutboxConfiguration.MaxAttemps` is null. Rows past the limit stay
  in the table permanently. There is no dead-letter handling or cleanup.
- [Confirmed] Retry timing has no backoff. A failed message is retried at the
  first poll after its lock expires. Polls happen every 30 minutes, or
  continuously while the backlog exceeds 100 rows, so the retry delay can reach
  about 30 minutes.

## Payload Received by Subscribers

- [Confirmed] **Immediate dispatch** after a save with
  `acceptAllChangesOnSuccess=true`, which is the default, passes the **original
  in-memory event instance** to the subscriber. This is the same object the
  application passed to `AddEvent`, including any EF entity graphs it
  references.
- [Confirmed] **Immediate dispatch** after a save with
  `acceptAllChangesOnSuccess=false` passes an `ExpandoObject` rebuilt from the
  persisted JSON (decision D3). It goes through the same conversion as the
  poller path, so the poller-path rules below apply.
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

- `src/XDev.Outbox/ServiceCollectionExtensions.cs`: subscriber scanning and `TryAddEnumerable`.
- `tests/XDev.Outbox.Tests/SubscriberRegistrationTests.cs`: subscriber registration and invocation.
- `src/XDev.Outbox/EventPublisher.cs`
- `src/XDev.Outbox/OutboxProcessorHostedService.cs`: constants, success and failure handling.
- `src/XDev.Outbox/ObjectExtensions.cs`, `OutboxConfiguration.cs`
- `src/XDev.Outbox.EFCore/OutboxDbContext.cs`, `OutboxDispatchTransactionInterceptor.cs`,
  `EntityFrameworkOutboxStore.cs`, `OutboxMessageConfiguration.cs`
