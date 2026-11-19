# 003: Library savepoint around outbox saves in caller transactions

## Status

Accepted on 2026-11-19. The user decided Q1–Q4 during task
`tasks/fix-d5-caller-transaction-partial-save.md`. Supersedes the D5 deferral
in decision 001.

## Context

In a caller-owned transaction, an outbox save runs `base.SaveChanges(false)`,
which EF protects with its own savepoint (`__EFSavePoint`), and then payload
projection, re-serialization and per-message payload UPDATEs. A failure in
those later steps left the INSERTs (and any UPDATEs) in the caller's
transaction, while the library restored the tracker to "not saved". A retry in
the same transaction then duplicated identity rows, hit PK violations for
client keys, or `DbUpdateConcurrencyException` for Deleted / rowversion
entries; the failed attempt's outbox row with default keys was later
dispatched by the poller. Confirmed on SQLite and SQL Server.

## Decision

- In the caller-transaction branch with events only, create savepoint
  `__XDevOutboxSavePoint` (≤ 32 chars for SQL Server, distinct from
  `__EFSavePoint`) inside the try before the attempt, and release it inside
  the try on success (a release failure takes the failure path).
- On failure: roll back to it first (async with `CancellationToken.None`),
  best-effort release, then `RecoverFailedAttempt`, then rethrow the original
  exception.
- **Q1:** when savepoints are unsupported (SQL Server MARS,
  `SupportsSavepoints` false), fall back to the old behavior and document
  "roll back the transaction after a failed save".
- **Q2:** honor `Database.AutoSavepointsEnabled == false` (no library
  savepoint).
- **Q3:** keep EF's inner savepoint (one extra `SAVE TRANSACTION` round trip
  per outbox save in a caller transaction on SQL Server).
- **Q4:** if the rollback fails, swallow it (EF logs `TransactionError`),
  rethrow the original exception so retrying strategies still recognize
  transient errors, and log warning EventId 41002
  `XDev.Outbox.EFCore.CallerTransactionSaveNotUndone`. Also logged on the
  no-savepoint path when EF's save had completed
  (`SaveAttempt.BaseSaveCompleted`).
- Library-owned transactions, the interceptor and the participant logic are
  unchanged. The library's rollback runs while `_saveDepth > 0`, so the
  savepoint existence check is skipped; this is correct because the savepoint
  is created after every pending row.

## Consequences

- `__XDevOutboxSavePoint` is a reserved name. On SQL Server, where
  `ReleaseSavepoint` is a no-op, a caller savepoint with that name can be
  shadowed by a later library savepoint (same exposure as EF's own name).
- Without savepoints a failure inside EF's own save can still leave partial
  rows with no warning (plain-EF parity).

## Alternatives Considered

- Savepoint only around the payload UPDATE: does not cover projection,
  serialization or cancellation failures, and must precede the INSERTs anyway.
- Suppress EF's inner savepoint (Q3 option B): saves a round trip but mutates
  `AutoSavepointsEnabled` visibly to user interceptors.
- Throw or poison the transaction when savepoints are unavailable (Q1):
  breaks MARS users.

## Verification

SQLite (`CallerTransactionPartialSave/*`, D5 test in `AdversarialSaveTests`)
and SQL Server (`CallerTransactionPartialSaveTests`,
`TesterCallerSavepointTests`, `TesterDoomedTransactionTests`, D5 test in
`CallerTransactionTests`) on net8–net11. The new tests failed on the old code
(SQLite 40 of 54 adversarial and 46 of 58 developer cases; SQL Server 29 of
39).
