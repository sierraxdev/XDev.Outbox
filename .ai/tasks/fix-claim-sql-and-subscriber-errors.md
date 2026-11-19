# Task: Claim SQL schema/quoting and hidden subscriber exceptions

## Status

Complete, uncommitted (2026-11-19). Workflow: Bug Fix.

## Problems (confirmed on SQL Server LocalDB, see `tasks/sql-server-smoke-tests.md`)

1. **Claim SQL ignores the table's schema.**
   `OutboxDbContextExtensions.GetTableNameWithScheme<T>()`
   (`src/XDev.Outbox.EFCore/OutboxDbContextExtensions.cs`, public, namespace
   `Microsoft.Extensions.DependencyInjection`) uses
   `entityType.GetDefaultSchema() ?? "dbo"`. With
   `ToTable("OutboxMessages", "outbox")` `GetAllAsync` fails with error 208, or
   silently claims rows from a `dbo` table with the same name.
2. **Claim SQL does not delimit/escape the table name.** The name is
   interpolated raw into SQL passed to `FromSqlRaw`
   (`EntityFrameworkOutboxStore.GetAllAsync`), so `{`/`}` throw
   `FormatException` and names needing delimiters break.
3. **Subscriber exceptions are wrapped.** `EventPublisher.PublishAsync`
   (`src/XDev.Outbox/EventPublisher.cs`) calls `MethodInfo.Invoke`; a handler
   that throws synchronously surfaces as `TargetInvocationException`, so the
   stored `ErrorMessage` is "Exception has been thrown by the target of an
   invocation."

## Regression tests (already written, skipped)

`tests/XDev.Outbox.EFCore.SqlServer.Tests`:
`OutboxTableSchemaTests` (2 tests), `BracedTableNameTests`, and the
sync-throw test in `HostedServiceTests`.

## Planned fix

- Build the claim table name from `entityType.GetTableName()` and
  `entityType.GetSchema()` (table schema, falling back to the model default),
  delimited with EF's `ISqlGenerationHelper.DelimitIdentifier(name, schema)`,
  with `{`/`}` doubled for `FromSqlRaw`. When no schema is configured, leave
  it unqualified, as EF's own INSERT does (the old code hard-coded `dbo`).
- Fix `GetTableNameWithScheme` to use the table's schema (public API: keep the
  signature).
- `EventPublisher`: unwrap `TargetInvocationException` and rethrow the inner
  exception with `ExceptionDispatchInfo` (works on netstandard2.0).

## Progress

- [x] Implementation (developer, 2026-11-19): new internal `OutboxSqlText`
  (`DelimitedTableName` via `ISqlGenerationHelper` + `GetTableName()`/`GetSchema()`,
  shared `EscapeFormat`, also used by the payload UPDATE); claim SQL uses it
  (unqualified when no schema). `GetTableNameWithScheme` uses `GetSchema()`
  (keeps `?? "dbo"`, undelimited). `EventPublisher` unwraps
  `TargetInvocationException` via `ExceptionDispatchInfo`. Claim column names
  (`Id`, `Attemps`, `LockExpiresAt`) still hard-coded (not needed for these
  bugs). Regression tests un-skipped; claim-text assertions updated to
  `FROM [OutboxMessages]` / `FROM [app].[OutboxMessages]`; added
  `QuotedOutboxTableTests` (schema `out box]`, table `Outbox] {Msgs}`),
  a `GetTableNameWithScheme` schema test and `EventPublisherExceptionTests`.
  Build 0 errors; all suites pass on net8-net11 (SQL Server: only D5 skipped).
- [ ] Adversarial testing / review
  - Tester (2026-11-19): added
    `tests/XDev.Outbox.EFCore.SqlServer.Tests/AdversarialTableNameTests.cs` and
    `tests/XDev.Outbox.Tests/EventPublisherAdversarialTests.cs`. Confirmed on SQL
    Server (LocalDB) for 4 tables: default schema `dflt` + `ToTable(..,"tbl")`
    (table schema wins, same-named decoys in `dflt`/`dbo` untouched); hostile names
    (quotes, `]`, `.`, `{0}`, `{1}`, `{{ }}`, `--`, `/*`, `;`, `@take`); unicode
    (incl. emoji); 128-char schema and table with `]`/braces. For each: owned save +
    payload UPDATE, savepoint existence query, claim, UpdateAsync, RemoveAsync on the
    same table; claim with itemsCount 2/100 x lock yes/no x maxAttemps yes/no
    (parameters bound, semantics correct); READPAST second claimer and 6 concurrent
    claimers disjoint. No schema + `EXECUTE AS` a WITHOUT LOGIN user with default
    schema `tenant`: falls back to dbo; with a `tenant.OutboxMessages` EF's INSERT
    and the claim both use it (old code read dbo). `GetTableNameWithScheme`: dbo
    fallback, default schema, table schema, both (table wins), custom name,
    undelimited. EventPublisher: sync OCE (task Canceled), AggregateException,
    custom with inner, the subscriber's own TargetInvocationException (unwrapped only
    once), throw after await: same instance propagates; first sync thrower stops
    later subscribers; hosted service records the real ErrorMessage (sync, async,
    sync OCE). Null Task from a handler -> NullReferenceException after the handler
    ran (characterized, pre-existing).
  - **New pre-existing defect** (not introduced by this fix): `GetAllAsync` with
    `itemsCount = null` (the interface default) fails on SQL Server with error 1033
    (`ORDER BY` in a CTE without `TOP`), every table and parameter combination.
    The poller always passes a count. Test `ClaimWithoutItemsCountTests` (skipped
    with reason).
- [x] Review (reviewer): approve with comments, no blocking. Non-blocking:
  claim column names hard-coded (out of scope); `GetTableNameWithScheme` still
  undelimited with `dbo` fallback; explicit interface subscribers still get
  `NullReferenceException` (pre-existing). `QuotedOutboxTableTests` lives in
  `OutboxStoreTests.cs`.
- [x] Verification (orchestrator): build 0 errors; net8–net11:
  `XDev.Outbox.Tests` 26/26, `XDev.Outbox.EFCore.Tests` 587/586 passed + 1
  skipped, `XDev.Outbox.EFCore.SqlServer.Tests` 171 passed + 2 skipped (D5,
  itemsCount=null defect); 0 leftover smoke databases; `git diff --check`
  clean.
- [x] Knowledge updates: `database/outbox-messages.md`,
  `business-rules/message-delivery.md`.

## Release notes (proposed)

- The poller's claim uses the outbox table's mapped schema and a delimited,
  brace-escaped name. Fixes error 208 / claiming a same-named `dbo` table with
  `ToTable(name, schema)`, and `FormatException` for names with braces.
- With no schema configured the claim is unqualified instead of `dbo.`,
  matching EF's own commands (matters for logins whose default schema is not
  `dbo`).
- `GetTableNameWithScheme<T>()` returns the table's own schema.
- A subscriber that throws synchronously propagates its own exception;
  `ErrorMessage` stores the real message.

## New finding (open)

`GetAllAsync(itemsCount: null)` fails on SQL Server (error 1033, `ORDER BY` in
a CTE without `TOP`). Pre-existing; documented by the skipped
`ClaimWithoutItemsCountTests`.
