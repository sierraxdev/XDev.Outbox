# Task: Only one subscriber per event type is registered (bug-check finding #5)

## Status

Complete, uncommitted (2026-11-19). Workflow: Bug Fix.

## Problem

`AddOutbox` scans assemblies for `ISubscriber<X>` implementations and registers
each with `services.TryAddScoped(findInterface, subscriber)`.
`TryAddScoped` is a no-op when the service type is already registered, so only
the first discovered implementation per event type is registered. A
subscriber registered before `AddOutbox` also suppresses every scanned one.
`EventPublisher.PublishAsync` resolves `IEnumerable<ISubscriber<X>>` and
invokes each in order, so it already supports several subscribers.

Reproduced during the bug check (2026-11-18).

## Root cause (confirmed)

`src/XDev.Outbox/ServiceCollectionExtensions.cs`: lines 51 (net8+ branch) and
97 (netstandard branch) use `TryAddScoped` instead of
`TryAddEnumerable(ServiceDescriptor.Scoped(serviceType, implementationType))`.

## Planned fix

Replace both calls with `TryAddEnumerable(ServiceDescriptor.Scoped(...))`.
This registers every distinct implementation per event type and still
deduplicates the same (service, implementation) pair, so calling `AddOutbox`
twice or scanning the same assembly twice registers each subscriber once.

## Behavior changes for consumers (release notes)

- All subscribers for an event type now run, sequentially, in registration
  order. The first exception stops the rest and fails the message; on retry
  the subscribers that already succeeded run again (at-least-once).
- A subscriber registered manually before `AddOutbox` no longer suppresses the
  scanned subscribers for that event type; both run.

## Constraints

- The user has unrelated uncommitted edits in progress
  (`samples/Example/AddUserHostedService.cs`,
  `src/XDev.Outbox.EFCore/OutboxDbContext.cs`). Do not touch or revert them.
  (The earlier commented-out hosted-service line in
  `ServiceCollectionExtensions.cs` has since been restored by the user.)
- No public API change.

## Progress

- [x] Regression tests (tester): new project `tests/XDev.Outbox.Tests`
  (`SubscriberRegistrationTests.cs`, 11 tests). On unfixed code, net8.0 and
  net11.0: 6 fail (multi-subscriber, duplicate-scan, manual-registration),
  5 pass. With the planned `TryAddEnumerable` change applied to a scratch
  copy: 11/11 pass (net8.0).
  Edge findings (probed in scratch, not in repo):
  - Open generic subscriber (`GenericSub<T> : ISubscriber<T>`) or generic
    class closing `ISubscriber<E>` (`Odd<T> : ISubscriber<E>`) is scanned and
    registered; `BuildServiceProvider()` then throws `ArgumentException`
    ("Cannot instantiate implementation type"). Same before and after the fix.
  - A concrete base subscriber plus a derived subclass: before, only the base
    ran; after, both run (two distinct implementation types).
  - A manual factory registration (`AddScoped<ISubscriber<E>>(_ => new Sub())`)
    of a type that is also scanned: before, only the factory ran; after, the
    subscriber runs twice (`TryAddEnumerable` cannot dedupe factory
    descriptors). Candidate release-note item.
- [x] Implementation (developer, 2026-11-19): both `TryAddScoped(findInterface, subscriber)`
  calls replaced with `TryAddEnumerable(ServiceDescriptor.Scoped(findInterface, subscriber))`.
  `dotnet build XDev.Outbox.slnx`: 0 errors (all library TFMs incl. netstandard2.0/2.1).
  `tests/XDev.Outbox.Tests`: 11/11 on net8.0, net9.0, net10.0, net11.0.
  `tests/XDev.Outbox.EFCore.Tests`: 388 passed, 1 skipped (389) on net8.0 and net11.0.
- [x] Review (reviewer): approve with comments, no blocking findings.
  Non-blocking, release-note items:
  - workaround registrations made after `AddOutbox`, and interface-typed
    factory registrations of scanned types, now run twice;
  - with several subscribers, a later failure saves earlier subscribers'
    unsaved changes with the failure record and reruns them on retry;
  - the netstandard branch is compiled but not executed by tests.
- [x] Verification (orchestrator): `dotnet build XDev.Outbox.slnx` 0 errors;
  net8.0/9.0/10.0/11.0: `XDev.Outbox.Tests` 11/11, `XDev.Outbox.EFCore.Tests`
  388 passed, 1 skipped; `git diff --check` clean.
- [x] Knowledge updates: `business-rules/message-delivery.md`,
  `components/outbox-dispatch-runtime.md`, `project.md`, `architecture.md`.

## Release notes (proposed)

1. Every `ISubscriber<T>` in the scanned assemblies is now registered and
   invoked; previously only the first per event type. Dormant handlers in
   scanned assemblies will start running.
2. Subscribers for one event run sequentially; manual registrations made
   before `AddOutbox` first, scanned order not guaranteed. The first exception
   stops the rest; on retry, those that succeeded run again. Subscribers must
   be idempotent.
3. Remove manual registrations added after `AddOutbox` as a workaround, and
   factory registrations such as `AddScoped<ISubscriber<E>>(_ => new Sub())`
   of scanned types; otherwise they run twice.
4. Behavior change to the delivery contract: ship with a minor version bump.
