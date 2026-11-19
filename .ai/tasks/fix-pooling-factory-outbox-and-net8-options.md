# Task: Pooled/factory contexts read the wrong event buffer (#7) and net8 options are frozen (#6)

## Status

Complete, uncommitted (2026-11-19). Workflow: Bug Fix (approved breaking API change).

## User decisions

- #7: **Option C** — move the event buffer onto the context: application code
  adds events to the `OutboxDbContext` itself (e.g. `context.Outbox.AddEvent`)
  instead of a DI-scoped `IOutbox` resolved separately. Breaking change to how
  consumers publish events. (Rejected: A, explicit binding + registration
  helpers; B, fail fast.)
- Fix #6 in the same change.

## Problems (from the 2026-11-18 bug check; hypotheses until reproduced)

### #7

`OutboxDbContext` obtains the buffer with `this.GetService<IOutbox>()`
(`OutboxDbContext.cs` ~127, 197, 531; also `OutboxDispatchTransactionInterceptor`
`ClearEvents` for non-participant contexts). EF resolves non-EF services from
`CoreOptionsExtension.ApplicationServiceProvider`. With `AddDbContextPool`,
`AddPooledDbContextFactory`, or `AddDbContextFactory` (default singleton
options), that is the root provider, so the scoped `IOutbox` is resolved from
root:
- with scope validation (Development): throws;
- otherwise: one root `Outbox` (non-thread-safe `List<object>`) shared by all
  contexts, different from the request-scoped `IOutbox` the application adds
  events to, so events are likely never saved.
Pooled contexts are also reused, and the context holds per-transaction
pending-message state that could leak across leases.

### #6

`src/XDev.Outbox.EFCore/ServiceCollectionExtensions.cs` `#else` (net8) branch:
builds a temporary `ServiceProvider` at registration (never disposed), resolves
`DbContextOptions<TContext>` once (running the consumer's `AddDbContext`
options action against the temporary container), then `Replace`s the options
with a **scoped** descriptor that copies that frozen snapshot and adds the
interceptor. Per-scope options logic (e.g. tenant connection strings) is
frozen; the lifetime change likely conflicts with pooling (singleton options).
net9+ uses `ConfigureDbContext` and is not affected.

## Constraints

- Published NuGet packages; this is a deliberate breaking change → release
  notes and version bump.
- No schema change to `OutboxMessage<TKey>`.
- Preserve all behavior established by `decisions/001-...` and the #1/#2 fix
  (deferred acceptance, B1, D-C, savepoints, Rule A).
- Multi-targeting: XDev.Outbox netstandard2.0/2.1 + net8–11; EFCore net8–11.

## Progress

- [x] Reproduce #6/#7 on current code (tester, 2026-11-19; scratch repro
  `scratchpad/repro67`, net8–net11, SQLite). Confirmed:
  - #7: with `AddDbContextPool`, `AddPooledDbContextFactory`, or
    `AddDbContextFactory` (default lifetime) the context's app service
    provider is ROOT. validateScopes=true: save throws "Cannot resolve scoped
    service IOutbox from root provider" (net9+). validateScopes=false: the
    business row commits, **no outbox row, no dispatch** (silent event loss);
    all contexts share one root `Outbox`; an event added via one scope's
    context was persisted/dispatched in another scope's transaction;
    concurrent `AddEvent` corrupts the `List` (exception or lost events).
    `AddDbContext` and `AddDbContextFactory(..., Scoped)` work.
  - Pool reset does not clear `_pendingTransactionId` / `_pendingOutboxMessages`
    and fires no rollback interceptor event. With flag=false, a later lease
    re-persisted and dispatched an event whose business row was rolled back
    (phantom event). Contradicts the component doc's "released harmlessly".
  - #6 (net8 only): the options action runs once at `AddEFCoreOutboxStore`
    against a temporary container; all scopes get the frozen value. The
    options lifetime becomes Scoped, so with validateScopes=true pooling,
    pooled factory, and default factory throw at context creation ("Cannot
    consume scoped DbContextOptions from singleton ..."). net9+ is per scope.
- [x] Investigation and API design (planner, 2026-11-19). Root causes
  confirmed in code and EF source. Extra gap: `AddPooledDbContextFactory`
  alone registers no scoped `TContext`, so `EntityFrameworkOutboxStore`
  (`GetRequiredService<TContext>()`) fails for every message.
  Proposed design:
  - `OutboxDbContext.Outbox` (type `IOutbox`, non-virtual, never null),
    backed by an internal lease-aware buffer; internal code uses the list
    directly. `IOutboxDbContext<TKey>` unchanged.
  - Per-lease reset via `DbContext.ContextId.Lease` (`EnsureCurrentLease()`
    at both save entry points before `_saveDepth++`, every participant member,
    every buffer method) clearing buffer and all pending state. Rejected:
    re-implementing `IResettableService.ResetState` (loses EF reset),
    `OnConfiguring` (throws `PoolingOptionsModified`), `Dispose` (not called
    for `AddDbContextPool` leases).
  - Interceptor non-participant path: no-op (no `ClearEvents`).
  - Remove DI `IOutbox` registration, `Outbox` class, `OutboxInterceptor<TKey>`.
  - #6 net8: wrap the last non-keyed `DbContextOptions<TContext>` descriptor
    in place with the same lifetime; factory invokes the original
    (instance/factory/type), then adds the interceptor if not already present.
    Remove `BuildServiceProvider` and `CreateDbContextOptions`. net9+ unchanged.
  - Version 0.5.0 proposed. Full test plan and release-note draft in the
    planner report (registration matrix, pool reuse after each failure mode,
    concurrency, existing suites on a pooled host, #6 per-scope options).
  Open hypotheses: `ResetState` with an open transaction; `ContextId.Lease`
  behavior on EF 9–11 (pin with tests).
- [x] User confirms API shape (2026-11-19):
  - Q1: remove the DI `IOutbox` registration and delete the `Outbox` class
    (code still injecting `IOutbox` fails at resolution, not silently).
  - Q2: property `OutboxDbContext.Outbox` of type `IOutbox`.
  - Q5: **fall back to `IDbContextFactory<TContext>`** in
    `EntityFrameworkOutboxStore` when no scoped `TContext` is registered
    (store owns and disposes the created context). In that setup subscribers
    do not share the store's context — document it.
  - Q8: do not change package versions.
  - Defaults taken (planner recommendations, not asked): Q3 leave
    `IOutboxDbContext<TKey>` unchanged; Q4 delete `OutboxInterceptor<TKey>`;
    Q6 document hand-built contexts (no helper); Q7 discard unsaved events
    silently at lease end/dispose; Q9 duplicate-interceptor guard on net8 and
    net9+; Q10 accept memory held until next lease.
- [x] Regression tests: written with the implementation (API change prevented test-first); the #6/#7 reproduction ran on the old code first (scratch `repro67`).
- [x] Implementation (developer, 2026-11-19). Uncommitted.
  - `OutboxDbContext.Outbox` (`IOutbox`, non-virtual, readonly) backed by
    internal `ContextOutbox` (new file; `List<object>` + lease callback).
    Internal save/transaction code uses the list directly; matching
    semantics unchanged. All `GetService<IOutbox>()` removed.
  - `EnsureCurrentLease()` (`ContextId.Lease` vs `_stateLease`) clears the
    buffer, `_pendingOutboxMessages`, `_pendingTransactionId`,
    `_releasedNotAcceptedEvents`, `_outboxSaveInProgress`, `_saveDepth`
    (all instance state). Called at both save overrides (before
    `_saveDepth++`), every participant member incl. the
    `IsOutboxSaveInProgress` getter, and every public buffer method.
  - Interceptor: non-participant contexts are ignored (no `ClearEvents`).
  - Removed `IOutbox` DI registration (both branches), `Outbox` class,
    `OutboxInterceptor<TKey>` and its registration.
  - Store: scoped `TContext` if registered, else `IDbContextFactory<TContext>`
    (store owns it; `IDisposable`/`IAsyncDisposable`), else
    `InvalidOperationException` naming the four registrations. Confirmed:
    EF 10+ `AddPooledDbContextFactory` also registers a scoped `TContext`
    (EF 8/9 do not), so the fallback is only hit on EF 8/9 there.
  - net8: last non-keyed `DbContextOptions<TContext>` descriptor wrapped in
    place, same lifetime; `BuildServiceProvider`/`CreateDbContextOptions`
    removed. Interceptor attached once on net8 and net9+ (reference guard).
  - Tests migrated to `Db.Outbox` (`AdvHost` constraint now
    `OutboxDbContext<TKey>`). New `ContextOutboxRegistrationTests` +
    `Infrastructure/RegistrationHost` (53 tests net8, 52 net9+). Confirmed
    `ContextId.Lease` increments per lease on EF 8–11; a mutation disabling
    the lease reset fails the 8 pooled-reuse tests.
  - Sample and both READMEs migrated. Build 0 errors; EFCore tests 441/442
    (net8) and 440/441 (net9–11), 1 skip (D5); core tests 11/11.
- [x] Adversarial testing (tester, 2026-11-19). 146 new tests in
  `tests/XDev.Outbox.EFCore.Tests/Adversarial/` (`PoolHost`,
  `PoolReuseAdversarialTests`, `PooledBehaviorParityTests`,
  `ContextIsolationAdversarialTests`, `OptionsRegistrationAdversarialTests`,
  `PoolConcurrencyAdversarialTests`). No product defect found. EFCore tests
  587/588 net8, 586/587 net9–11 (1 skip, D5); core 11/11 all TFMs.
  - Pool reuse (poolSize 1, sync/async dispose, AddDbContextPool, pooled
    factory, pooled factory + scoped) after: failed owned save, B1 rollback,
    commit with acceptance pending, unconfirmed savepoint rollback, retries
    exhausted, retry cancelled, ambient save throwing, tx disposed, tx left
    open: next and third lease clean, no stray rows/dispatch, own events OK.
  - Parity on reused leases/factories: 4 overloads, D-C, savepoint, owned
    failure + app retry (Rule A keys). Two contexts in one scope isolated.
    Subscriber pattern atomic with the delete (AddDbContext, pool, factories);
    pooled-factory-only on EF 8/9: subscriber cannot share the store's
    context, its follow-up event is not saved by RemoveAsync (documented by
    test; EF 10+ atomic). Concurrency: 8 workers x 6 leases, file DB, pool
    reuse confirmed, N rows/N dispatches, payloads per scope, no leaks.
  - Extra consumer interceptor (same or new instance): no double dispatch.
    Plain DbContext with the interceptor: commit/rollback/savepoint/failed
    commit do not throw. net8 #6: options action not run at registration,
    app singleton instance used, all six registrations save with
    ValidateScopes+ValidateOnBuild; scoped service per scope; order error on
    net8, works on net9+.
  - Documented (not defects): stale pooled reference — `Outbox.AddEvent`
    does not throw after the lease ended (EF ops throw
    ObjectDisposedException); discarded at next lease; but used while the
    instance is leased again, the event lands in the current lease (EF has
    the same limitation). Disposed non-pooled context: AddEvent succeeds,
    save throws ObjectDisposedException.
  - Mutation checks (scratch copy): disabling the lease reset fails 80
    adversarial tests; old net8 registration fails 40; disabling the
    duplicate-interceptor guard fails 9. Removing only the pending-state
    part of the reset (keeping the buffer clear) fails none: that part is
    defense in depth, not observable (stale pending ids never match a new
    transaction id).
- [x] Review (reviewer, 2026-11-19): approve with comments, no blocking.
  - N1 (medium): pooled reuse covered only for simple states; add leases
    ending with an unconfirmed savepoint message, non-empty released-events,
    stale pending tx where the next lease's first touch is an interceptor
    callback; consider running existing suites on a pooled host.
  - N2 (low): net8 wrapper's `ImplementationType`/`ImplementationInstance`
    branches untested (EF never registers those).
  - N3 (low): `GetEvents()` live view — same as the deleted `Outbox` class
    (`_events.AsReadOnly()`), so unchanged behavior.
  - N4 (low): `IsOutboxSaveInProgress` getter resets lease state; add a
    comment.
  - Pre-existing: a subscriber that buffers events on the shared context
    and then throws has them persisted by `store.UpdateAsync`'s save.
  - Release note: `EntityFrameworkOutboxStore` constructor is now
    `(IServiceProvider, TimeProvider)` and it implements
    `IDisposable`/`IAsyncDisposable`; a consumer member named `Outbox`
    hides the base property (CS0108).
- [x] Verification (orchestrator, 2026-11-19): `dotnet build XDev.Outbox.slnx`
  0 errors; EFCore tests net8.0 587 passed/1 skipped, net9.0–net11.0 586
  passed/1 skipped; `XDev.Outbox.Tests` 11/11 on all four; `git diff --check`
  clean; no remaining references to the `Outbox` class, `OutboxInterceptor`
  or `GetService<IOutbox>` in src, samples or README. Sample built, not run;
  no SQL Server run.
- [x] Knowledge updates: `components/efcore-outbox-store.md`,
  `business-rules/message-delivery.md`, `project.md`, `architecture.md`,
  `components/outbox-dispatch-runtime.md`; new
  `decisions/002-event-buffer-on-dbcontext.md`.

## Release notes (proposed; versions intentionally unchanged)

Breaking:
1. Add events with `dbContext.Outbox.AddEvent(...)`. `AddOutbox` no longer
   registers `IOutbox`; the `Outbox` class is removed; injecting `IOutbox`
   fails at resolution.
2. Events belong to the context instance they were added to; unsaved events
   are discarded on dispose or return to the pool. Subscribers publish
   follow-up events through their injected DbContext.
3. `OutboxInterceptor<TKey>` removed.
4. `EntityFrameworkOutboxStore` implements `IDisposable`/`IAsyncDisposable`.
5. A consumer member named `Outbox` on a derived context hides the new
   property (CS0108).

Fixed:
6. `AddDbContextPool`, `AddPooledDbContextFactory`, `AddDbContextFactory`
   contexts persist events (before: silently lost, shared across requests, or
   throwing under scope validation); pooled contexts no longer leak state
   between leases (phantom events).
7. The store works with `AddPooledDbContextFactory` alone (own context; on
   EF 8/9 subscribers do not share it).
8. net8: per-scope options actions run per resolution; pooling and factories
   work with scope validation; no temporary service provider. On net8
   `AddEFCoreOutboxStore` must follow the DbContext registration.
9. The dispatch interceptor is attached once even if `AddEFCoreOutboxStore` is
   called twice; plain DbContexts are ignored by it.
