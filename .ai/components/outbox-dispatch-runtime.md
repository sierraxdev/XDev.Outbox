# Outbox Dispatch Runtime (XDev.Outbox)

This document covers `MemoryMessageQueue<TKey>`,
`OutboxProcessorHostedService<TKey>`, `EventPublisher`, and `AddOutbox`. The
delivery contract they implement is in `business-rules/message-delivery.md`.

## Registration (`AddOutbox`)

- Overloads: `AddOutbox(params Assembly[])` uses `int` keys.
  `AddOutbox<TKey>(...)` accepts an optional `Action<OutboxConfiguration>`.
- It throws `NotSupportedException` unless `TKey` is `int` or `Guid`. It throws
  `ArgumentException` if no assemblies are given.
- It registers `OutboxConfiguration` options only if no
  `IConfigureOptions<OutboxConfiguration>` exists yet. A configuration
  delegate on a later call is ignored.
- Lifetimes: `IOutbox` scoped, `IEventPublisher` scoped, subscribers scoped,
  `IMessageQueue<TKey>` singleton (`MemoryMessageQueue`), hosted service
  singleton (`TryAddEnumerable`), and `TimeProvider.System` (net8+).
- On net8.0+ the methods extend `IServiceCollection`. On netstandard2.x they
  extend the concrete `ServiceCollection` type.

`OutboxConfiguration`:

- `MaxAttemps` (null means 15);
- `MessageLockExpirationTimeSpan` (default 5 min);
- static `SerializerOptions`. This is process-global and shared by the EF
  value converter and payload rebuilding. Changing it affects how existing
  rows are read.

## MemoryMessageQueue

- It holds an unbounded `ConcurrentQueue` with no backpressure. It logs a
  warning on every enqueue while 300 or more messages are queued.
- The receive loop starts in `StartAsync` but blocks until the first
  `MessageReceived` handler is attached. Until then, enqueued messages
  accumulate.
- At most 10 handler invocations run concurrently (`SemaphoreSlim(10)`).
  Handler exceptions are logged. The message is **not** re-queued; recovery is
  via the poller.
- The queue is not durable. Queued messages are lost on process exit, and
  their rows are recovered by the poller after their lock expires.
- Each process has its own queue. Immediate dispatch happens only in the
  process that committed the transaction.

## OutboxProcessorHostedService

- `ExecuteAsync` attaches `HandleMessageAsync` to the queue, starts it, and
  then runs the poll loop described in `architecture.md`. The constants are
  hard-coded: batch size 100 (it requests 101 rows to detect a backlog), poll
  interval 30 minutes, error delay 5 minutes, default maximum attempts 15.
- `HandleMessageAsync` creates a new DI scope per message. It handles the
  payload by kind:
  - a CLR object (immediate path) is used as-is;
  - an `ExpandoObject` or `JsonElement` (poller path) is rebuilt with
    `ObjectExtensions.ToObject`.

  It then publishes the event, followed by `RemoveAsync`. If anything fails,
  it updates `Attemps` and the error fields and rethrows. The `JsonElement`
  branch matches how the Service Bus prototype deserializes `Payload`.
- The netstandard2.x `AddOutbox` does not register `TimeProvider`. That is not
  needed there, because those builds use `DateTime.UtcNow`.
- Shutdown normally exits through the error-handling path (see
  `architecture.md`, Polling / recovery flow).
- The time source is `TimeProvider` on net8+ and `DateTime.UtcNow` on
  netstandard.

## EventPublisher

It uses reflection to resolve `IEnumerable<ISubscriber<runtimeType>>` and
invokes `HandleEventAsync(runtimeType, CancellationToken)` on each subscriber
in sequence. The exact-signature lookup was introduced to allow subscribers
with overloaded `HandleEventAsync` methods (commit 452857f).

## Observed Risks (intent unknown, not runtime-verified)

- **Double dispose / null reference on stop.** `MemoryMessageQueue.StopAsync`
  disposes `_linkedCancellationTokenSource` and sets it to null. The
  receive-loop `finally` block also calls `_linkedCancellationTokenSource.Dispose()`.
  Depending on timing, this may throw a `NullReferenceException`. This is
  rarely reachable, because the hosted service normally exits before calling
  `StopAsync`.
- **Misleading shutdown log.** Every normal shutdown logs "Error occurred
  getting messages from store" with an `OperationCanceledException`.
- **Shared payload instance.** On the immediate path, with flag=true, the
  handler receives the same event object the request created, including any
  entity graph it references, while the request thread may still use it.
  Since the 2026-11-18 fix, the `OutboxMessage` entity itself is detached from
  the request's `DbContext` before it is queued.
- **Dead path in `ObjectExtensions.ToObject`.** The nested `ExpandoObject`
  branch calls `GetMethod(nameof(ToObject))`, which is ambiguous across
  overloads. With System.Text.Json, nested values are `JsonElement`, so the
  branch is not normally reached.
