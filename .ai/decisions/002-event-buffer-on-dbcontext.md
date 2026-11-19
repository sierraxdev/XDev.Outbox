# 002: Event buffer on the DbContext, reset per pool lease

## Status

Accepted on 2026-11-19. The user chose Option C and the sub-decisions below
during task `tasks/fix-pooling-factory-outbox-and-net8-options.md`.

## Context

`OutboxDbContext` read its event buffer with `this.GetService<IOutbox>()`.
EF resolves such services from the provider that created the context's
options. With `AddDbContextPool`, `AddPooledDbContextFactory`, or
`AddDbContextFactory` (default singleton options), that is the root provider.
Reproduced on EF 8–11:

- with scope validation, every save with events threw;
- without it, the business row committed and the event was silently lost,
  all contexts shared one non-thread-safe root buffer, and events crossed
  requests;
- pooled contexts kept pending transaction state across leases, and a later
  lease persisted and dispatched an event whose business row was rolled back.

## Decision

- **Option C.** Each `OutboxDbContext` owns its buffer:
  `public IOutbox Outbox` (internal `ContextOutbox`). Events are persisted only
  by saves on that context.
- **Q1.** `AddOutbox` no longer registers `IOutbox`, and the `Outbox` class is
  removed. Code that injects `IOutbox` fails at resolution rather than losing
  events silently. The `IOutbox` interface is kept as the buffer's type (Q2).
- **Per-lease reset.** `EnsureCurrentLease()` compares
  `DbContext.ContextId.Lease` and clears the buffer and all pending state when
  it changes. It runs first at every entry point (save overrides, participant
  members, buffer methods).
- **Q5.** `EntityFrameworkOutboxStore` uses the scoped `TContext`, else creates
  and owns one from `IDbContextFactory<TContext>`, else throws.
- **#6 (net8).** `AddEFCoreOutboxStore` wraps the existing
  `DbContextOptions<TContext>` descriptor in place with the same lifetime
  instead of building a temporary provider and freezing a scoped snapshot.
  Both TFM paths attach the interceptor at most once.
- Defaults accepted without asking: `IOutboxDbContext<TKey>` unchanged;
  `OutboxInterceptor<TKey>` deleted; hand-built contexts documented, no helper;
  unsaved events discarded silently; package versions unchanged (Q8).

## Rationale

- Removing the DI lookup removes the dependency on which provider created the
  options, so every registration kind behaves the same.
- Failing at resolution for leftover `IOutbox` injections avoids the silent
  event loss that a kept-but-unused registration would cause.

## Alternatives Considered

- **A. Explicit binding + registration helpers.** Keeps `IOutbox` in DI and
  binds the scope's buffer to pooled/factory contexts. Rejected by the user in
  favor of C.
- **B. Fail fast** on pooled/factory contexts. Smallest, but leaves them
  unsupported.
- **Pool reset hooks:** re-implementing `IResettableService.ResetState` (loses
  EF's own reset), `OnConfiguring` (throws with pooling), `Dispose` (not
  called for `AddDbContextPool` leases). Rejected for the lease-counter check.
- **Forwarding a DI `IOutbox` to the scope's context.** Ambiguous with several
  contexts and silently wrong with factory contexts.

## Consequences

- Breaking change for every consumer: `dbContext.Outbox.AddEvent(...)`;
  subscribers inject the DbContext to publish follow-up events.
- `EntityFrameworkOutboxStore` implements `IDisposable`/`IAsyncDisposable`.
- With `AddPooledDbContextFactory` alone on EF 8/9, subscribers do not share the
  store's context, so their follow-up events are not saved by the store.
- Correctness depends on `ContextId.Lease` incrementing per pool lease (pinned
  by tests on EF 8–11).

## Verification

`tests/XDev.Outbox.EFCore.Tests` on net8–net11, including
`ContextOutboxRegistrationTests` and `Adversarial/Pool*`,
`ContextIsolationAdversarialTests`, `OptionsRegistrationAdversarialTests`.
Mutation checks: disabling the lease reset fails 80 tests; restoring the old
net8 registration fails 40 on net8; removing the interceptor guard fails 9.
Pooled contexts and the net8 options path were verified on SQL Server LocalDB
(2026-11-19); factory registrations were not run there.
