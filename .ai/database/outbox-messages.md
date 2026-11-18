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

- **Insert**: in `OutboxDbContext.SaveChangesAsync`, in the same transaction
  as the business changes, by a second `base.SaveChangesAsync` call. See
  `components/efcore-outbox-store.md`.
- **Claim (update)**: `EntityFrameworkOutboxStore.GetAllAsync`. One statement:

  ```sql
  ;WITH cte AS (
      SELECT TOP (@take) * FROM <schema>.<table> WITH (ROWLOCK, READPAST, UPDLOCK)
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
- The schema in the raw SQL is `entityType.GetDefaultSchema() ?? "dbo"`.
  `GetDefaultSchema()` is the model default schema. [Observed, intent unknown]
  If the table lives in a non-`dbo` schema without a model-level
  `HasDefaultSchema`, the raw SQL could target the wrong table. This has not
  been verified at runtime.
- Times are written as `TimeProvider.GetUtcNow().DateTime`, which is a UTC
  value with `DateTimeKind.Unspecified`.

## Idempotency and Duplicates

There is no deduplication key. Duplicate dispatch prevention relies only on
`LockExpiresAt`. Delivery is at-least-once (see
`business-rules/message-delivery.md`).
