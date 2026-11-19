# Task: SQL Server smoke tests

## Status

Complete, uncommitted (2026-11-19). Workflow: Testing. Report defects; do not modify
production code.

## Goal

All automated tests so far run on SQLite, while production uses SQL Server.
Verify the save, dispatch and store paths on real SQL Server (LocalDB
`(localdb)\MSSQLLocalDB`, version 17.0.4025.3), focusing on items flagged as
"not verified on SQL Server" in `components/efcore-outbox-store.md`,
`decisions/001-...` and `decisions/002-...`:

- identity values on execution-strategy retry (principal + dependent);
- savepoints created by EF inside caller transactions, `RollbackToSavepoint`
  and the savepoint existence query;
- payload UPDATE parameter typing (reviewer N5 from task 1), large/unicode
  payloads;
- `OUTPUT` batching with many rows;
- claim SQL in `EntityFrameworkOutboxStore.GetAllAsync` (`READPAST`,
  `UPDLOCK`, schema handling), `RemoveAsync`, `UpdateAsync`;
- pooled contexts and the net8 options path on SQL Server;
- `Guid` outbox keys.

## Constraints

- Use only uniquely named throwaway databases created by the tests, dropped
  afterwards. Never touch `OutboxExample` or any other existing database.
- Test-only dependency: `Microsoft.EntityFrameworkCore.SqlServer` in the new
  test project, version-matched per TFM.

## Progress

- [x] Smoke suite written and run (tester, 2026-11-19)
- [x] Findings triaged (orchestrator): all three defects were already on the
  2026-11-18 bug-check list (Medium: claim SQL schema/quoting; Medium:
  EventPublisher reflection hides the real exception), now confirmed. No new
  defect in the #1–#7 fixes. Orchestrator re-ran the suite: 108 passed,
  5 skipped on all four TFMs; 0 leftover databases. Knowledge updated.

### Suite

`tests/XDev.Outbox.EFCore.SqlServer.Tests` (net8–net11, EF Core SqlServer
8.0.0/9.0.0/10.0.0/11.0.0, in `XDev.Outbox.slnx` under `/tests/`).

- **Server.** Defaults to LocalDB. `XDEV_OUTBOX_SQLSERVER` overrides it with
  a base connection string.
- **Skips.** `[SqlServerFact]`/`[SqlServerTheory]` skip with a reason when the
  server is unreachable or the variable is malformed (verified).
- **Databases.** Each class creates and drops `XDevOutboxSmoke_<guid>`.
  Leftovers older than 30 minutes are dropped at startup. Only names matching
  `^XDevOutboxSmoke_[0-9a-f]{32}$` can ever be dropped.
- **Parallelization** is disabled.

Result on every TFM: 113 tests, 108 passed, 5 skipped (D5 plus 4 confirmed
defects), 0 failed. Unskipped, exactly those 8 cases fail on every TFM. No
smoke databases remain afterwards.

### Confirmed on SQL Server (net8–net11)

- **Owned saves.**
  - All overloads, with flag true/false and a User with Orders: one row per
    event, with identity ids in payloads (Rule A).
  - Rows are already committed when enqueued, checked from a second
    connection with `LOCK_TIMEOUT 0`.
- **Retries.**
  - `SqlServerRetryingExecutionStrategy` retries after a failure at each step:
    business/outbox batch, dependent batch, payload UPDATE (before and after
    execution) and commit. Exactly one set of rows, final ids in payloads, one
    dispatch, no explicit identity INSERT, no FK error.
  - Application-level retry gives the same result.
- **Batching.** EF puts Users and OutboxMessages in one MERGE/INSERT command
  and Orders in a second. 100 users, 100 orders and 200 events take 11
  INSERT/MERGE commands plus **200 single-row payload UPDATEs** (one round
  trip each).
- **Caller transactions.**
  - Commit-only dispatch and the B1 rollback rules hold.
  - Two saves in one transaction work.
  - EF creates `SAVE TRANSACTION` per save and rolls back to it when a save
    fails after its INSERTs ran. A retry in the same transaction persists
    everything once.
  - Caller `RollbackToSavepoint` works, and the existence query batches
    100 + 51.
  - Re-saving after a rollback of a flag=false save works: EF does not send
    the stale identity.
- **D5** reproduces (3 Users instead of 2 and 3 outbox rows instead of 2).
  Known and deferred.
- **Payload typing.**
  - INSERT and UPDATE payload parameters are `NVarChar` with size 4000 when
    the value is 4000 chars or less, -1 (max) above that. The UPDATE id
    parameter is `Int`, or `UniqueIdentifier` for Guid keys. No CAST/CONVERT.
  - Values round-trip exactly (> 4000 chars, > 8000 bytes, emoji/CJK).
  - System.Text.Json escapes supplementary-plane characters as `\uXXXX`
    (lossless).
- **Long type names.** An AQN over 200 chars fails the whole save with
  SqlException 2628 (Type parameter `nvarchar(4000)`, no silent truncation).
  Nothing is persisted.
- **Store.**
  - The claim respects attempts and locks and claims in Id order.
  - READPAST: a second claimer skips rows locked by an open claim without
    waiting.
  - 8 concurrent claimers took 120 rows with no overlap.
  - Remove and Update work.
  - A plain custom table name works, and so does `HasDefaultSchema`.
- **Pooled contexts** (poolSize 1) are clean after a failed save or an
  abandoned caller transaction.
- **Guid keys** work for save, claim, update, remove and the savepoint check.
- **Hosted service** end to end (immediate path and startup poll) works.

### Product defects found (tests skipped with reason)

1. **Claim SQL ignores the mapped schema.** `GetTableNameWithScheme` uses
   `GetDefaultSchema()`. With `ToTable("OutboxMessages", "outbox")`,
   `GetAllAsync` throws `Invalid object name 'dbo.OutboxMessages'` (208).
   - If a `dbo.OutboxMessages` table also exists, it **silently claims that
     table's rows**.
2. **Claim SQL does not delimit or escape the table name.** With
   `TableName = "Outbox{Msgs}"`, FromSqlRaw's `string.Format` throws
   `FormatException`. Other names that need delimiting would fail too
   (not tested).
3. **Wrapped subscriber exceptions.** A subscriber that throws synchronously
   is wrapped in `TargetInvocationException` by `EventPublisher`
   (reflection), so `ErrorMessage` records "Exception has been thrown by the
   target of an invocation." This is runtime-level, not SQL-specific.

### Observations (not classified as defects)

- `@dateTimeNow` and `@lockExpiration` are inferred as SQL `datetime`, which
  is compared with and stored into `datetime2`. Stored locks are rounded to
  about 3 ms (`.6789012` → `.680`).
- The payload UPDATE is one round trip per message (200 for 200 events).

### Not covered

- Real transient SqlExceptions or connection drops (failures are injected
  through interceptors).
- A commit that succeeds on the server but is reported as failed.
- `TransactionScope`.
- MARS: EF disables savepoints with MARS.
- Poller backlog and lock-expiry re-claim timing.
- `AddPooledDbContextFactory` / `AddDbContextFactory` on SQL Server (only
  `AddDbContextPool` was run).
- Azure SQL.
