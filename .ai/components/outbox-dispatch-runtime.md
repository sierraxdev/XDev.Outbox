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
- Lifetimes: `IEventPublisher` scoped, subscribers scoped
  (each scanned `ISubscriber<X>` via `TryAddEnumerable`, so every distinct
  implementation is registered; see `business-rules/message-delivery.md`),
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

- It holds an unbounded `ConcurrentQueue`. It logs a warning on every
  enqueue while 300 or more messages are queued. Enqueue never blocks (saves
  enqueue synchronously after commit); the poller paces itself instead (see
  below).
- **Dedup by Id (since 2026-11-20).** Each queued or in-flight message is
  tracked by Id with its own expected lock (a copy of `LockExpiresAt` at
  enqueue; the message object is never changed). An enqueue of an Id already
  tracked is skipped (Debug log); if it carries a newer lock (a re-claim), the
  expected lock moves to it and any revocation is cleared. The entry is
  removed after the handler finishes (success or failure), so the Id can be
  dispatched again later. A queued entry revoked by lock renewal is skipped at
  dequeue without calling the handler. Tracking is exposed to the hosted
  service through the internal `IMessageLockTracker<TKey>` (compare-and-set
  `ConfirmRenewal`/`Revoke` under a lock on the entry).
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
- **Lock renewal loop (since 2026-11-20, built-in queue only).** Runs beside
  the poll loop. Every span/3 (floor 100 ms, capped at about 24.8 days) it
  snapshots the tracked messages and calls `IOutboxStore.RenewLocksAsync`
  with their expected locks and `now + span` (same clock as the poller), in a
  new DI scope. Renewed ids get the stored value as their new expected lock.
  Missing ids lost ownership: a queued copy is dropped (the poller delivers
  the row later); an in-flight handler continues and a warning says it may be
  dispatched again (Debug if the handler already finished). A call that
  throws or returns null revokes nothing; its attempted lock is remembered
  per id (up to 8 values) and also tried on the next tick before revoking,
  which covers partial multi-chunk failures and calls whose outcome was
  unknown. Ids that still miss are retried once in the same tick with the
  tick's own lock value; this covers a retrying execution strategy re-running
  a renewal statement that had already committed. At most 1 + 8 + 1 store
  calls per tick, the extra ones only when there are misses. Every tick warns for handlers running longer than one
  span; there is no cap (a hung handler keeps its row until restart). Errors
  are logged and retried after at least 100 ms; the loop never crashes the
  service. Spans under about 300 ms cannot be kept alive.
  Residual risks (no ownership token, D4 deferred): a remembered attempted
  lock could match another instance's claim only if renewal failed for about
  2/3 of a span and that claim landed within ±5 ms of the failed attempt;
  instance clock skew close to a span breaks lock comparisons; tables with
  triggers fail renewal (SQL error 334), like the claim.
- **Store contract for renewal:** an id missing from the result means
  "owned elsewhere"; a store that cannot renew must throw (e.g.
  `NotSupportedException`) rather than return an empty result.
- **Backpressure (since 2026-11-20, built-in queue only):** after a batch of
  more than 100 rows, the poller waits until none of that batch is tracked
  (checked every second), at most one lock span, before claiming again.
- With a custom `IMessageQueue` there is no dedup, renewal or backpressure
  (previous behavior).
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
