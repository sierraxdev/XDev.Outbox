# Outbox Messages Table

## Ownership

The table is mapped into the **consumer's** `DbContext` through
`OutboxDbContext<TKey>.OnModelCreating`, which calls
`OutboxMessageConfiguration<TKey>`. Consumers create it with their own EF
migrations. This repository ships no migrations for it. The sample's
migrations under `samples/Example/Migrations/` show the generated shape.

Consumers that override `OnModelCreating` must call `base.OnModelCreating`, as
the sample does, or the outbox entity will not be configured. Model building
needs `IOptions<OutboxDbContextConfiguration>`, which `AddEFCoreOutboxStore`
registers. Without it, the `OutboxMessageConfiguration` constructor throws a
`NullReferenceException` at `options.Value`.

## Shape

Table name defaults to `OutboxMessages` (`OutboxDbContextConfiguration.TableName`,
set via `AddEFCoreOutboxStore(o => o.TableName = ...)`). Columns come from
`OutboxMessage<TKey>`. The names are part of the public contract, including
the misspelling `Attemps`.

| Column | Type (sample, int key) | Notes |
|---|---|---|
| `Id` | `int` identity, or `uniqueidentifier` for `Guid` | PK. Only `int` and `Guid` keys are accepted by `AddOutbox`. |
| `Type` | `nvarchar(200) NULL` | Assembly-qualified CLR type name of the event. |
| `Payload` | `nvarchar(max) NULL` | JSON via value converter. Read back as `ExpandoObject`. |
| `Created` | `datetime2` | UTC creation time. |
| `LockExpiresAt` | `datetime2 NULL` | Claim/lock expiry (UTC). |
| `Attemps` | `int NULL` | Failed-attempt count. |
| `IsError` | `bit NULL` | Set true on any failure. Never reset. Not used in queries. |
| `ErrorMessage` | `nvarchar(max) NULL` | Last exception message. |

No indexes beyond the PK are defined.

## Writes

- **Insert**: in the same `SaveChanges(false)` call as the business changes,
  inside one transaction.
- **Payload update**: when generated keys change the serialized payload, the
  save also runs
  `UPDATE <table> SET <Payload> = {0} WHERE <Id> = {1}` in the same
  transaction:
  - parameterized, with model identifiers delimited and braces escaped;
  - one statement per changed message.

  See `components/efcore-outbox-store.md`.
- **Library savepoint**: in a caller transaction, each save with events
  runs inside `SAVE TRANSACTION [__XDevOutboxSavePoint]` (when savepoints are
  available), in addition to EF's own `__EFSavePoint`; a failure after the
  INSERTs rolls back to it, undoing the INSERTs and any payload UPDATEs.
  SQL Server never releases savepoints, so names accumulate; rollback targets
  the most recent. See `decisions/003-...`.
- **Savepoint existence check**: in a caller transaction, after
  `RollbackToSavepoint`, or before commit when messages are unconfirmed, the
  library runs `SELECT ... WHERE Id = @p0 OR ...`:
  - batches of 100;
  - query filters ignored.
- **Claim (update)**: `EntityFrameworkOutboxStore.GetAllAsync`. One statement:

  ```sql
  ;WITH cte AS (
      SELECT TOP (@take) * FROM [<schema>].[<table>] WITH (ROWLOCK, READPAST, UPDLOCK)
      WHERE (Attemps IS NULL OR Attemps < @maxAttemps)
        AND (LockExpiresAt IS NULL OR LockExpiresAt < @dateTimeNow)
      ORDER BY Id)
  UPDATE cte SET LockExpiresAt = @lockExpiration OUTPUT INSERTED.*;
  ```

  The statement is parameterized. The table name comes from the EF model, not
  from user input. `READPAST` + `UPDLOCK` lets several application instances
  claim disjoint rows concurrently. The claim lasts until `@lockExpiration`.
- **Delete**: `RemoveAsync` on successful dispatch, via `SaveChangesAsync` on
  the handler scope's `TContext`.
- **Failure update**: `UpdateAsync` uses `DbSet.Update` on a detached entity
  and rewrites all columns, including the re-serialized payload.

## Transactions and Isolation

- When there is no ambient transaction, the outbox save path opens its own
  `ReadCommitted` transaction inside `Database.CreateExecutionStrategy()`. This
  makes it compatible with `EnableRetryOnFailure`.
- With an ambient transaction, the caller's transaction and isolation level
  apply.
- "Ambient transaction" means EF's `Database.CurrentTransaction`. A
  System.Transactions `TransactionScope` is not detected, and its interaction
  with the save path is unverified.
- The claim runs as a single statement. The delete and the failure update use
  the handler scope's `TContext`, which subscribers share. Subscriber changes
  left unsaved are saved in the same EF automatic transaction as the delete or
  failure update. Changes a subscriber saved itself, or made on another
  context, are **not** atomic with the delete.

## SQL Server Specifics

- The raw SQL uses T-SQL table hints, `TOP`, and `OUTPUT INSERTED.*`, and
  passes `Microsoft.Data.SqlClient.SqlParameter`. Other providers are not
  supported by `EntityFrameworkOutboxStore`.
- **Claim table name (fixed 2026-11-19).** The claim SQL uses the table's
  own schema (`ToTable(name, schema)`), falling back to `HasDefaultSchema`,
  and is unqualified when none is set, so it resolves through the connection
  user's default schema exactly like EF's own INSERT/UPDATE/DELETE. The name is
  delimited with `ISqlGenerationHelper` and brace-escaped (internal
  `OutboxSqlText`), so `{`, `}`, `]`, quotes, spaces, unicode and 128-char
  identifiers work. Before, it used `GetDefaultSchema() ?? "dbo"` undelimited:
  error 208 or claiming a `dbo` table of the same name, and `FormatException`
  on braces. Any identifier passed to `FromSqlRaw`/`ExecuteSqlRaw` must be
  brace-escaped.
- The public `GetTableNameWithScheme<T>()` now returns the table's schema
  (falling back to model default, then `dbo`), undelimited. The library no
  longer uses it; it can disagree with the claim when no schema is set and
  the login's default schema is not `dbo`.
- **Known limitation.** The claim SQL hard-codes undelimited column names
  (`Id`, `Attemps`, `LockExpiresAt`); renaming them (`HasColumnName`, naming
  conventions) breaks the claim while saves keep working.
- **Defect (open, pre-existing).** `GetAllAsync(itemsCount: null)` fails on
  SQL Server with error 1033: the CTE keeps `ORDER BY Id` but emits
  `TOP (@take)` only when `itemsCount` has a value. The hosted poller always
  passes a count, so it is unaffected; direct callers using the default are.
- `@dateTimeNow` and `@lockExpiration` are sent as SQL `datetime`, so stored
  lock times are rounded to about 3 ms.
- Times are written as `TimeProvider.GetUtcNow().DateTime`, which is a UTC
  value with `DateTimeKind.Unspecified`.

## Idempotency and Duplicates

There is no deduplication key. Duplicate dispatch prevention relies only on
`LockExpiresAt`. Delivery is at-least-once (see
`business-rules/message-delivery.md`).
