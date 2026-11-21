# Task: Claim/renewal SQL hard-codes column names; event type names over 200 characters

## Status

Complete (2026-11-20). Workflow: Bug Fix (two independent defects),
requested by the user. The user first asked for #1 (nested save) and #5, then
corrected it to #2 (column names) and #5. The nested-save analysis below is
kept for a later task; it is NOT being implemented here.

## Deferred: nested `SaveChanges` from a save handler (pre-existing, not in scope)

From `components/efcore-outbox-store.md` (Known Limitations): a
`SavingChanges` handler that calls `SaveChanges` during an owned save sees
the library's own transaction as current and takes the caller path,
re-saving the buffer: nested `SaveChanges(true)` writes the outer event twice;
nested `SaveChanges(false)` makes the outer save fail with
`DbUpdateException`. Pinned by
`PreExistingHazard_NestedSaveFromSavingChangesHandler_OuterEventDuplicatedOrSaveFails`
(`tests/XDev.Outbox.EFCore.Tests/CallerTransactionPartialSave/TesterOwnedSaveEventsAddedDuringSaveTests.cs`).

## Bug 2 — claim and renewal SQL hard-code column names

From `database/outbox-messages.md`: the claim SQL (and since 2026-11-20 the
renewal SQL) in `EntityFrameworkOutboxStore` hard-codes undelimited column
names (`Id`, `Attemps`, `LockExpiresAt`); renaming them (`HasColumnName`,
naming conventions such as snake_case) breaks the claim and renewal while
saves keep working.

### Bug 2 — confirmed (planner, 2026-11-20)

- Hard-coded columns only in `EntityFrameworkOutboxStore.cs`: claim WHERE
  (`Attemps`, `LockExpiresAt`), `ORDER BY Id`, `SET LockExpiresAt`; renewal
  `SET`, `OUTPUT INSERTED.Id/LockExpiresAt`, join/match on `t.Id`,
  `t.LockExpiresAt`. EF's own commands, the savepoint existence query and the
  payload UPDATE (`OutboxDbContext`) already use the model.
- EF8 `FromSql` maps reader columns by name (case-insensitive); a missing
  column throws, extras are ignored. `SqlQuery<RenewedLock>` (AdHocMapper)
  uses the full convention set incl. `IConventionSetPlugin`.
- Hypotheses to verify: H1 `INSERTED.*` materializes renamed columns; H2
  table-specific overrides (`ToTable(t => t.Property().HasColumnName())`)
  still fail (FromSql uses the default mapping) -> document unsupported; H3
  naming plugins rename `RenewedLock` columns -> pin with `[Column]`.
- Plan: `OutboxSqlText.DelimitedColumnName` (`GetColumnName(StoreObjectIdentifier
  .Table(...))`, `DelimitIdentifier`, clear `InvalidOperationException` if the
  table or column is null) + `EscapeFormat`; use in claim and renewal; keep
  `*`/`INSERTED.*`; alias renewal OUTPUT `AS [Id]`/`[LockExpiresAt]`;
  `[Column]` on `RenewedLock`; no caching. Unsupported (document): converters
  on Id/LockExpiresAt/Attemps, entity splitting, views/ToSqlQuery,
  table-specific overrides (if H2).
- Tests: renamed-column contexts (hostile, Unicode, 128-char, snake_case via
  HasColumnName, snake_case via a test IConventionSetPlugin, Guid key) in
  their own table/fixture; column-aware SmokeDatabase/StoreSeed/RowState
  helpers; claim combinations, concurrent claim, renewal incl. >1000
  chunking, hosted service end to end; update the SQL-shape assertions in
  `TesterAcceptAfterCommitAndClaimTests.cs` (~L212/L215).
- No user decision needed (no schema/API change). No new package
  (EFCore.NamingConventions not added).

## Bug 5 — event type names over 200 characters

From `business-rules/message-delivery.md`: the `Type` column is
`nvarchar(200)`; assembly-qualified names longer than 200 characters (generic
events, long namespaces) fail to insert, and since the insert shares the
business transaction the whole save fails. Any fix that changes the column is
a schema change and needs user approval.

## Planning findings (planner, 2026-11-20)

### Deferred nested-save analysis (not in scope)

- Root cause: no "outbox attempt in flight" state. A re-entrant save
  recomputes `GetEventsToSave` while the outer attempt has added outbox
  entries (`BeginAttempt`) and the owned transaction is current, so it takes
  the caller path: outer event saved twice (nested flag=true; in a caller
  transaction also dispatched twice) or the outer EF save fails
  (flag=false). Nested saves from `SavedChanges` re-insert unaccepted
  entries and fail.
- EF 8 `DbContext.SaveChanges`: `SavingChanges` and `SaveChangesStarting`
  run before EF's transaction; no re-entrancy guard. Plain EF: a nested save
  from `SavingChanges` is an independent committed save.
- Nested saves during an outer save WITHOUT events behave like plain EF
  (e.g. audit `SavedChanges` interceptor); keep.
- Proposed: `_outboxAttemptInProgress` around the attempt (owned and caller
  paths, incl. recovery), reset per lease; the overrides throw
  `InvalidOperationException` when set. Rejected: pass-through (silent loss
  of business changes if the outer save fails), write-new-events, silent
  no-op.
- Pre-existing gap: without a savepoint, an exception from `SavedChanges`
  after EF wrote does not log 41002.

### Bug 5 — confirmed

- Write: `ToOutboxMessageEntity` uses `AssemblyQualifiedName`; mapping
  `HasMaxLength(200)`; read: `Type.GetType` (immediate and poller). SQLite
  ignores the length.
- Hand-counted: `EntityChanged<Order>` 238 -> 128 without
  version/culture/key; `<Order, Guid>` 343 -> 166; `<Order, Customer>` 359 ->
  194; deeply nested BCL generics still too long after shortening.
- Proposed (d-conditional): full AQN if it fits the model's max length, else
  the version-less name (built recursively over `Type`), else
  `InvalidOperationException` before any write; no library schema change;
  document the consumer override (`HasMaxLength(n)` after
  `base.OnModelCreating` + own migration).
- Hypothesis to verify: simple-name binding of strong-named/TPA assemblies
  on net8–net11.

## User decisions (2026-11-20)

- Bug 5: ALWAYS store the version-less name (no Version/Culture/
  PublicKeyToken, recursively incl. generic arguments) for every new row;
  if it still exceeds the model's max length, throw InvalidOperationException
  before any write; check on ALL providers (incl. SQLite); no library schema
  change (consumers may raise the length themselves). Existing rows with full
  names keep resolving.
- Nested save: dropped from this task by the user.

## Implementation notes, Bug 5 (developer, 2026-11-20)

- New internal `src/XDev.Outbox.EFCore/OutboxTypeName.cs`: recursive over
  `System.Type`: generic definition `FullName` + `[[arg, asm],...]`; arrays
  `[]` (SZ), `[*]` (rank-1 non-SZ), `[,]...`; pointer `*`, by-ref `&`; nested
  types via `FullName` (`+`); `, <simple assembly name>` of the innermost
  element type (escaped via `AssemblyName { Name }.FullName`). Types with open
  generic parameters fall back to `AssemblyQualifiedName` (never a runtime
  event type).
- `OutboxDbContext`: `ToOutboxMessageEntity` stores `OutboxTypeName.Get`;
  new `ThrowIfEventTypeNameTooLong(events)` runs in both save cores right
  after the no-events path, before the caller-transaction/owned branches
  (no command, transaction or savepoint; buffer, tracker and caller
  transaction untouched). Max length from
  `Model.FindEntityType(OutboxMessage<TKey>).FindProperty(Type).GetMaxLength()`;
  no check when null. Message: stored name, its length, the limit, and the
  override (`modelBuilder.Entity<OutboxMessage<TKey>>().Property(x =>
  x.Type).HasMaxLength(n)` after `base.OnModelCreating` + own migration).
- `XDev.Outbox.EFCore.csproj`: `InternalsVisibleTo XDev.Outbox.EFCore.Tests`
  (helper tests). No schema, dependency or read-side change.
- Confirmed (tests, net8–net11): `Type.GetType(versionLessName)` returns the
  same type for: plain/nested types, nested in generic (`Outer`1+Inner[[..]]`),
  generics with args from the test assembly, from XDev.Outbox, from
  System.Private.CoreLib (Guid, Nullable, List, Dictionary, KeyValuePair,
  ValueTuple), strong-named framework assemblies outside CoreLib
  (System.Text.Json JsonElement, System.ObjectModel), strong-named package
  assemblies from the app folder (Microsoft.EntityFrameworkCore,
  Microsoft.Data.Sqlite); arrays SZ/jagged/multi-dim/`[*]`, arrays of
  generics, generics of arrays; pointer and pointer arrays. No case needed a
  fallback. Full AQNs (old rows) still resolve (Core test; SQL Server poller
  test with seeded full AQN).
- Measured lengths (test assembly `XDev.Outbox.EFCore.Tests`, namespace
  `XDev.Outbox.EFCore.Tests.TypeNames`): `EntityChanged<User>` 260 -> 150;
  `EntityChanged<User, Guid>` 365 -> 188; `EntityChanged<User, Order>`
  389 -> 224 (still too long with these long test namespaces);
  `EntityChanged<Dictionary<string, List<User>>>` 629 -> 318. SQL Server:
  `LongNamedEvent<User>` -> 196 (fits nvarchar(200)).
- Not verified: types in non-default `AssemblyLoadContext`s / plugin loads
  (hypothesis, untested: resolution there was limited with full names
  too); SQLite e2e through the hosted service (the claim SQL is
  SQL-Server-only) — covered on SQL Server and by the Core test instead.

## Implementation notes, Bug 2 (developer, 2026-11-20)

- `OutboxSqlText.DelimitedColumnName(context, entityType, propertyName)`:
  `GetColumnName(StoreObjectIdentifier.Table(GetTableName(), GetSchema()))`,
  delimited with `ISqlGenerationHelper`. `InvalidOperationException` when the
  entity has no table (view/SQL query/function; `DelimitedTableName` now throws
  the same instead of passing null) or the property has no column in that
  table (entity splitting). No caching.
- `EntityFrameworkOutboxStore`: claim WHERE/ORDER BY/SET and renewal
  SET/OUTPUT/join use the model's delimited, brace-escaped columns; `*` and
  `INSERTED.*` kept; renewal OUTPUT aliased `AS [Id]`/`AS [LockExpiresAt]`;
  `VALUES ... v(Id, Expected)`, parameter typing, hints and chunking
  unchanged. `RenewedLock` properties pinned with `[Column("Id")]` /
  `[Column("LockExpiresAt")]`.
- Tests (SQL Server): `RenamedColumnTests.cs` — contexts renaming all eight
  columns: hostile (`]`, `{0}`, `{{ }}`, quotes, space, `--`, `/*`, `;`,
  `@take`, `@lockExpiration`, `@maxAttemps`), Unicode/emoji with two 128-char
  names containing `]` and braces, snake_case via HasColumnName, snake_case via
  a test `IConventionSetPlugin` (registered by an `IDbContextOptionsExtension`
  in `OnConfiguring`), Guid key with hostile names; one fixture (database) per
  class. Per context (20 tests): save with payload UPDATE + claim + UpdateAsync
  + RemoveAsync, 12 claim combinations comparing every returned column with
  the stored row, concurrent claimers, renewal renew/stale/deleted, 2150-id
  chunking, renewal-vs-claim race (10 rounds), hosted service poller +
  immediate dispatch, failure recording, lock renewed during a 6 s handler
  with a competing poller. Characterization: `TableSpecificColumnOverrideTests`
  (H2), `UnsupportedOutboxMappingTests` (entity splitting, view; model only).
  Helpers made column-aware: `OutboxColumns` record (`OutboxColumns.Of(entityType)`),
  `SmokeDatabase.OutboxRows(table, columns)`, `RowState(table, id, idColumn)`,
  `SmokeHost.OutboxColumns` (also used by the recording queue's RowProbe),
  `StoreSeed.Row<TKey>(db, table, columns, ...)` (Guid ids via NEWID()).
  Shape assertions in `TesterAcceptAfterCommitAndClaimTests.cs` now expect
  `SET [LockExpiresAt] = @lockExpiration` and `ORDER BY [Id]`.
- Fail-before (net8, production unchanged): 95 of the 100 renamed-column
  tests failed (SqlException "Invalid column name 'LockExpiresAt'/'Attemps'",
  poller timeouts) plus the updated shape test; the 5 passing were the
  failure-recording hosted tests, one per context (immediate dispatch, no
  claim or renewal). The 7 characterization tests also failed before
  (other exception types; SqlException on the H2 renewal).
- H1 confirmed: `INSERTED.*` / `SELECT *` materialize renamed columns
  (Id, LockExpiresAt, Attemps, Type, IsError, ErrorMessage, Payload compared
  with the stored row in all 12 combinations x 5 contexts).
- H2 confirmed: with `ToTable("OutboxMessages", t => t.Property(x =>
  x.LockExpiresAt).HasColumnName(...))` the claim SQL uses the table's column
  but EF materializes FromSql by the default column name and throws
  `InvalidOperationException` ("The required column 'LockExpiresAt' was not
  present in the results of a 'FromSql' operation"); with a lock the statement
  has already locked the row (hidden until expiry). Renewal works. Not fixed
  (unsupported, to document).
- H3 confirmed: with `[Column]` removed from `RenewedLock`, the snake_case
  plugin renewal failed ("The required column 'lock_expires_at' was not
  present in the results of a 'FromSql' operation"); restored.
- Results (2026-11-20): solution build 0 errors, no warnings in changed files.
  Core 70; EFCore 1139 (net8) / 1138 (net9-11); SQL Server 490 (383 + 107 new)
  on net8-net11, 0 skipped. Timing-sensitive subset (renewal, hosted service,
  renamed-column scenarios, H2; 133 tests) passed 3 rounds x 4 frameworks. No
  XDevOutboxSmoke_ databases left.
- Documentation candidates (knowledge phase): `database/outbox-messages.md`
  "Known limitation" (column names) is resolved; unsupported: table-specific
  column overrides (H2: claim throws after locking the rows), entity splitting
  and view/query/function mappings (clear InvalidOperationException), value
  converters on Id/LockExpiresAt/Attemps (untested).

## Progress

- [x] Planning, Bug 5 (planner)
- [x] User decisions, Bug 5
- [x] Planning, Bug 2 (planner)
- [x] Implementation, Bug 5 (developer, 2026-11-20) — see "Implementation notes, Bug 5"
- [x] Implementation, Bug 2 (developer, 2026-11-20) — see "Implementation notes, Bug 2"
- [x] Implementation with tests first (developer)
- [x] Adversarial testing (tester, 2026-11-20): no blocking defect; two
  low-severity Bug 5 regressions and one behavior change reported.
  - Added `tests/XDev.Outbox.EFCore.SqlServer.Tests/TesterSwappedColumnAndTypeNameTests.cs`
    (74 tests): the full renamed-column scenario suite with swapped names
    (Id<->LockExpiresAt, Attemps<->IsError, others named t/v/cte/Expected;
    Id<->Attemps, LockExpiresAt<->Created, keywords Order/Select/Key/User;
    int and Guid keys); exact-length boundary 199/200/201 with surrogate
    pairs (nvarchar counts the same as .NET Length); retrying strategy (one
    InvalidOperationException, no command); 500 duplicates + one long event;
    Bug 2 + Bug 5 end to end (Type column named "t", poller + immediate).
    `RenamedColumnTests.cs`: the "no LockExpiresAt column" assertion is
    skipped when a swapped set uses that name on purpose.
  - Added `tests/XDev.Outbox.EFCore.Tests/TypeNames/TesterOutboxTypeNameAdversarialTests.cs`
    (137 tests): oracle "full name minus Version/Culture/PublicKeyToken" for
    the developer's and odd shapes (ValueTuple TRest, Nullable of generic
    struct, generics nested in generics, arrays of nested generics); dynamic
    assemblies with `, = [ " ' + & * space emoji` in assembly names and
    escaped type names resolved through AssemblyResolve; collectible ALC
    parity; SQLite boundaries; events added by a SavingChanges handler.
  - Fail-before (HEAD worktree, net8): 68 of the 74 SQL Server tests failed;
    the 6 passing are the immediate failure-recording tests, two coincidental
    claim combinations and the raw nvarchar check.
  - Confirmed: column swaps are safe (aliases are always qualified).
  - Low regression: on net8 only, a simple assembly name containing ']' as a
    generic argument does not resolve with the version-less name (the full name
    does); on net9+ neither form resolves. Pinned as a characterization test.
  - Low hazard: a failed lookup by simple name stays failed in the Default ALC
    (net8 and net10), even after the assembly is loaded later with
    LoadFrom/LoadFromAssemblyPath. A failed lookup by full name does not stay
    failed. Plugin events then fail until restart. Not pinned in a test (needs a
    separate on-disk assembly).
  - Behavior change: a consumer who widened the column with
    `HasColumnType("nvarchar(500)")` and not `HasMaxLength` keeps max length
    200. HEAD saved a 434-character full name intact; the save now throws
    (pinned in `TesterWideTypeColumnTests`). Needs a user decision or
    documentation.
  - Results: Core 70; EFCore 1276 (net8) / 1275 (net9-11); SQL Server 564 on
    net8-net11; 0 skipped. Swap scenarios 3 more rounds on net8 and net11: all
    passed. No XDevOutboxSmoke_ databases left.
- [x] Review (reviewer, 2026-11-20): APPROVE WITH NON-BLOCKING COMMENTS.
  - Finding 1 MEDIUM (to fix): a table-specific column override
    (`ToTable(t => t.Property(..).HasColumnName(..))`) is only detected after
    the claim UPDATE has locked rows (EF then fails reading `INSERTED.*` by
    default names), so every poll locks eligible rows until their lock
    expires. Before the change it failed with a SqlException and no side
    effect. Fix: throw a clear InvalidOperationException before building the
    SQL when any mapped OutboxMessage property's table column differs from
    its default column; update the H2 characterization test and the
    `DelimitedColumnName` XML doc.
  - Finding 2 LOW: stale renewal SQL in `database/outbox-messages.md`:
    fixed by the orchestrator (doc only).
  - Open question: swapped/colliding column names (Id <-> LockExpiresAt, a
    column named Expected/Id/v/t) reasoned safe, untested (optional test).
- [x] Developer corrections (developer, 2026-11-20)
  - Review finding 1: `OutboxSqlText.ThrowIfTableSpecificColumnNames(entityType)`
    runs in `GetAllAsync` (after the table/column lookups, before any
    SQL is built or sent): any mapped `OutboxMessage<TKey>` property whose
    `GetColumnName(StoreObjectIdentifier.Table(..))` differs from
    `GetColumnName()` throws `InvalidOperationException` (table-specific
    override: names property, table column, default column, and the
    `Property(x => x.P).HasColumnName("...")` alternative; null table column:
    entity-splitting message). Renewal unchanged (it works with overrides:
    own result type and aliases). `DelimitedColumnName` XML doc no longer
    implies overrides are supported. H2 test now
    `GetAllAsync_LockColumnRenamedOnlyForTheTable_ThrowsBeforeSendingTheClaim`:
    no command, lock stays NULL, twice.
  - Tester hazard 1: the length limit is `EventTypeNameMaxLength`: an
    explicit column type (`RelationalAnnotationNames.ColumnType` annotation;
    present in the runtime model; the provider's default store type is not an
    annotation) wins: `nvarchar(500)`/`varchar (250)` -> n, `(max)` or no
    parentheses (`TEXT`) -> no limit, anything else (`nvarchar(500, 1)`) ->
    falls back to `GetMaxLength()`; no explicit type -> `GetMaxLength()`
    (HasMaxLength(1000)/null unchanged). Message names the column type and
    suggests `HasColumnType(...)` when that is the limit; key type shown as
    C# name (`OutboxMessage<int>`). Tester pin flipped to
    `Save_ColumnWidenedWithHasColumnTypeOnly_..._IsSaved`; new SQL Server
    `TypeColumn250Tests` (250 saved, 257 rejected, no command); new SQLite
    `EventTypeNameColumnTypeTests` (10: nvarchar(500), NVARCHAR(MAX), TEXT
    save; varchar (250) and unreadable length reject before any command).
  - Tester hazard 3 (read side): `OutboxProcessorHostedService` resolves via
    new internal `OutboxEventTypeResolver.Resolve` =
    `Type.GetType(name, ResolveAssembly, null, throwOnError: false)`.
    DEVIATION from the requested order (deliberate, lower risk): the resolver
    first calls `Assembly.Load(name)` exactly as `Type.GetType(string)` does,
    and only when that throws `FileNotFoundException` (or
    `FileLoadException`/`BadImageFormatException`) looks in
    `AppDomain.GetAssemblies()` (same simple name, same Version/
    PublicKeyToken when given; Default ALC first, then highest version, then
    load order); a FileLoad/BadImage error with no loaded stand-in is
    rethrown (recorded failure, as before). Scanning first would let a
    plugin-context copy win over the app's own assembly not yet loaded in the
    Default context. Everything that resolved before resolves the same way;
    only names that failed before can now resolve. Confirmed: plugin loaded
    after a failed simple-name lookup (Default `LoadFromStream` and a custom
    ALC) resolves afterwards (net9-11, `PersistedAssemblyBuilder`; net8 has
    no on-disk/byte-image emit, so the plugin case is untested there);
    dynamic assemblies now resolve by name (all TFMs). Requesting a higher
    Version of an app assembly resolved before (binder) and still does.
    Tests: `tests/XDev.Outbox.Tests/TypeNameLoadedAssemblyResolutionTests.cs`.
  - Tester hazard 2: `OutboxTypeName` escapes `]` as `\]` in a generic
    argument's assembly name only (as AssemblyQualifiedName does); probe:
    on net8 escaped generic-argument form resolves, escaped top-level form
    without Version throws FileLoadException (so top level stays unescaped);
    net9+: no form resolves (parity, unchanged). Pin updated
    (`AssemblyNameWithClosingBracket_Net8EveryFormResolves_Net9PlusNeitherFormResolves`).
  - Fail-before (production reverted temporarily, net8 unless noted): H2
    tests 2/2 failed; `TesterWideTypeColumnTests` and `TypeColumn250Tests`
    failed; 8 of 10 `EventTypeNameColumnTypeTests` failed (the 2 unreadable
    -length cases pass before by design); core resolver tests: dynamic
    assembly failed on net8-11, both plugin cases failed on net9-11.
  - Results: see the developer report (build 0 errors; Core 72 net8 / 74
    net9-11; EFCore 1286 / 1285; SQL Server 565; 0 skipped).
- [x] Developer corrections, round 2 (developer, 2026-11-20)
  - F1: `TryParseColumnTypeLength` no longer treats every type without
    parentheses as unlimited. No parentheses -> no limit only when the trimmed
    name is in `UnboundedColumnTypes` (case-insensitive: text, ntext, xml,
    citext, mediumtext, longtext); any other name (`nvarchar`, `varchar`,
    `sysname`, national ... forms, unknown) -> length unreadable -> falls back
    to `GetMaxLength()`. XML docs updated. New SQLite tests
    `Save_ColumnTypeNVarCharWithoutLength_TheModelMaxLengthApplies` and
    `Save_ColumnTypeSysName_TheModelMaxLengthApplies` (sync/async; the >250
    character name is rejected at 200 before any command); TEXT still saves.
  - F3: `OutboxEventTypeResolver.FindLoadedAssembly` accepts a loaded
    assembly whose Version is >= the requested Version (binder-like);
    PublicKeyToken unchanged (null/empty requested token skips the check,
    now documented). Remarks note the fallback cost (first-chance
    FileNotFoundException + loaded-assembly scan per resolution, no caching
    by design). New core tests (dynamic assembly v2.0.0.0): requesting
    1.5.0.0 resolves via the fallback; requesting 2.0.0.1 / 3.0.0.0 records
    the failure; 2.0.0.0 resolves (control).
  - Fail-before (production logic lines reverted temporarily, net9): lower
    -version core test failed; 4 new SQLite tests failed; higher-version
    test passes before and after (guard).
  - Results: build 0 errors (no warnings in changed files); Core 74 net8 /
    76 net9-11; EFCore 1290 net8 / 1289 net9-11; SQL Server 565 net8-11;
    0 failed, 0 skipped; no XDevOutboxSmoke_ databases left.
- [x] Re-review of corrections (reviewer): APPROVE WITH NON-BLOCKING
  COMMENTS; F1 (no-length char types treated as unlimited) and F3 (exact
  version match in the fallback) fixed in round 2; F2 (exception + scan per
  fallback resolution, no caching) accepted and documented.
- [x] Verification (orchestrator, 2026-11-20): build 0 errors (netstandard
  included); net8–net11 Core 74/76, EFCore 1290/1289, SQL Server 565, all
  passed, 0 skipped; 0 leftover smoke DBs; `git diff --check` clean; no
  debug output. Round-2 corrections not re-reviewed separately (small, test-
  covered).
- [x] Knowledge updates: `database/outbox-messages.md`,
  `business-rules/message-delivery.md`, `project.md`.
