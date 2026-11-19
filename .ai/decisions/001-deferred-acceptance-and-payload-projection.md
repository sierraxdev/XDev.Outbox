# 001: Deferred change acceptance with payload projection in OutboxDbContext

## Status

Accepted on 2026-11-18. The user decided D1–D5 and the setter fallback during
task `tasks/fix-savechanges-overloads-skip-outbox.md`.

## Context

The library must meet four requirements at once:

1. Persist outbox events atomically with business changes, from every
   `SaveChanges` overload.
2. Accept changes only after the commit, the way plain EF does, and honor the
   caller's `acceptAllChangesOnSuccess`. This guarantees that a failed save
   leaves the tracker retryable and creates no phantom events.
3. Keep **Rule A**: persisted payloads contain the store-generated keys of the
   entities they reference.
4. Dispatch only after commit.

The previous design saved twice: the business rows first, accepting the
changes, then the outbox rows. That satisfied Rule A only because it accepted
the changes before commit, which was defect #2.

Two findings from EF Core source and tests, confirmed on EF 8–11:

- **Generated values are held back.** Store-generated values stay in tracker
  side storage until acceptance, so CLR objects keep their defaults until
  `AcceptChanges`. Serializing during an unaccepted save therefore writes
  default keys.
- **Generated values are not undone.** After a successful `SaveChanges(false)`,
  EF does not undo the non-temporary generated values when the outer
  transaction then fails. Retrying a graph with a principal and a dependent
  then hits an FK violation.

## Decision

- **Single save.** Each save does one `base.SaveChanges(false)` per attempt,
  writing the business rows and the outbox rows together. Acceptance happens
  after commit, and only if the flag is true.
- **Payload projection (D1).** Before serializing, pending tracker values are
  copied temporarily onto the CLR objects. Backing fields are preferred; the
  property setter is used only when there is no backing field. The payloads
  are re-serialized and patched with a parameterized `UPDATE` in the same
  transaction.
- **Retry restore.** The temporary and generated key/FK values of Added
  entries are snapshotted before each attempt and restored as temporary on
  failure.
- **Library-owned saves** dispatch from the save method after commit. For
  caller-owned transactions, messages stay pending in the context, and the
  transaction interceptor dispatches them through the internal
  `IOutboxTransactionParticipant`.
- **The buffer follows the business changes.**
  - D2: on a library-owned failure, the buffer is kept.
  - B1: when a caller transaction ends without a commit, events saved with
    flag=false are kept and events saved with flag=true are dropped.
  - D-C: a later flag=true save accepts earlier pending messages.
- **D3:** after a flag=false save, subscribers receive the payload rebuilt
  from JSON.
- **D4:** `AutoTransactionBehavior` is no longer modified.
- **D5:** deferred. There is no savepoint around the payload `UPDATE` in caller
  transactions.

## Rationale

- Plain-EF semantics make failures recoverable, and they make application-level
  and execution-strategy retries persist everything exactly once.
- Projection plus an `UPDATE` keeps the existing payload contract (Rule A)
  without a breaking change. The rejected alternative was to relax Rule A.

## Constraints

- **Dependency on EF internals.** Correctness depends on undocumented EF
  change-tracker behavior:
  - generated values held in side storage until acceptance;
  - setting `IsTemporary` writes the CLR value;
  - key fix-up when a key is restored to a temporary value.
- **Run the full test matrix.** Any EF Core version bump must run
  `tests/XDev.Outbox.EFCore.Tests` on every target framework.
- **Internal saves.** Internal saves must call `base.*(false)` explicitly.
  Calling an overload that routes back through the overrides would recurse or
  process the outbox twice.
- **Setters.** Do not introduce setter calls for properties that have backing
  fields.

## Alternatives Considered

- **Throw when flag=false and events are buffered.** This was the smallest
  change, but it leaves #2 unfixed. Rejected because the user chose to fix #2
  too.
- **Ignore the flag.** Rejected: it silently overrides the caller.
- **Temporarily change entity states.** Rejected: it loses modified flags and
  original values.
- **Second internal context, raw INSERT, or `ExecuteUpdate` for outbox rows.**
  Rejected because of model drift and provider issues.

## Verification

- `tests/XDev.Outbox.EFCore.Tests` (389 tests) must pass on net8, net9, net10
  and net11. The only accepted skip is the D5 test.
- Verified on SQL Server LocalDB on 2026-11-19
  (`tests/XDev.Outbox.EFCore.SqlServer.Tests`); D5 reproduces there.
