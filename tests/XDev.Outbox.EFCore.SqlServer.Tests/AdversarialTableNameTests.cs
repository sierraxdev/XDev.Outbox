using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

// Adversarial checks for the claim SQL table name (task fix-claim-sql-and-subscriber-errors): every raw statement the
// library builds (payload UPDATE, claim) and the EF-generated ones (INSERT, savepoint existence query, UPDATE, DELETE)
// must target the same mapped table, whatever its schema and name.

/// <summary>Model default schema "dflt" and the outbox table moved to "tbl" with ToTable(name, schema): the table's schema wins.</summary>
public sealed class DefaultAndTableSchemaDbContext : SmokeContextBase<int>
{
    public const string ModelSchema = "dflt";
    public const string TableSchema = "tbl";

    public DefaultAndTableSchemaDbContext(DbContextOptions<DefaultAndTableSchemaDbContext> options) : base(options)
    {
    }

    protected override string? DefaultSchema => ModelSchema;

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable("OutboxMessages", TableSchema);
}

/// <summary>
/// Schema and table names with every character that could break the text: quotes, closing bracket, a dot, a format
/// placeholder ({0}, {1}), doubled braces, comment starters, a semicolon and a parameter-like token (@take).
/// </summary>
public sealed class HostileNamesDbContext : SmokeContextBase<int>
{
    public const string Schema = "s'c \"h\" ]{1}.x";
    public const string Table = "Outbox.'q' \"dq\" ] {0} {{ }} -- /* ; @take";

    public HostileNamesDbContext(DbContextOptions<HostileNamesDbContext> options) : base(options)
    {
    }

    protected override string? DefaultSchema => "hostile users";

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable(Table, Schema);
}

/// <summary>Unicode schema and table names (accents, CJK, a supplementary-plane character).</summary>
public sealed class UnicodeNamesDbContext : SmokeContextBase<int>
{
    public const string Schema = "esquema_ü_漢字";
    public const string Table = "Bandeja ñ 😀 漢字";

    public UnicodeNamesDbContext(DbContextOptions<UnicodeNamesDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable(Table, Schema);
}

/// <summary>Schema and table names at SQL Server's 128-character identifier limit, containing ']' and braces.</summary>
public sealed class MaxLengthNamesDbContext : SmokeContextBase<int>
{
    public static readonly string Schema = "S{]".PadRight(128, 'y');
    public static readonly string Table = "Max{]}".PadRight(128, 'x');

    public MaxLengthNamesDbContext(DbContextOptions<MaxLengthNamesDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable(Table, Schema);
}

/// <summary>Save (with payload UPDATE), savepoint check, claim, update, remove and concurrent claims on one mapped table.</summary>
public abstract class TableNameScenarioTests<TContext> where TContext : SmokeContextBase<int>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    protected TableNameScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>The outbox table the model maps, delimited: [schema].[table].</summary>
    protected abstract string ExpectedOutboxTable { get; }

    protected SmokeHost<int, TContext> CreateHost()
    {
        var host = SmokeHost<int, TContext>.Create(_fixture);
        Assert.Equal(ExpectedOutboxTable, host.OutboxTable);
        Assert.Equal(1, Convert.ToInt32(host.Database.Scalar(
            "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE QUOTENAME(s.name) + '.' + QUOTENAME(t.name) = @name",
            ("@name", ExpectedOutboxTable))));
        return host;
    }

    private static IOutboxStore<int> Store(SmokeScope<TContext> scope) => scope.Services.GetRequiredService<IOutboxStore<int>>();

    private static string Delimited(DbContext db, Type entity)
    {
        var type = db.Model.FindEntityType(entity)!;
        return $"[{(type.GetSchema() ?? "dbo").Replace("]", "]]")}].[{type.GetTableName()!.Replace("]", "]]")}]";
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SavePayloadUpdateSavepointCheckClaimUpdateAndRemove_AllTargetTheMappedTable(bool async)
    {
        using var host = CreateHost();
        string usersTable, ordersTable;
        List<CommandRecord> savepointCheck;

        await using (var scope = host.CreateScope())
        {
            usersTable = Delimited(scope.Db, typeof(User));
            ordersTable = Delimited(scope.Db, typeof(Order));
            await using var tx = async ? await scope.Db.Database.BeginTransactionAsync() : scope.Db.Database.BeginTransaction();

            var kept = Graph.NewUser("kept@test", "k1");
            scope.Db.Users.Add(kept);
            scope.Db.Outbox.AddEvent(new UserCreated { User = kept }, new OrderPlaced { Order = kept.Orders[0] }, new TextEvent { Text = "kept text" });
            await Save.Invoke(scope.Db, async);

            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            var undone = Graph.NewUser("undone@test");
            scope.Db.Users.Add(undone);
            scope.Db.Outbox.AddEvent(new UserCreated { User = undone });
            await Save.Invoke(scope.Db, async);

            var before = host.Faults.Commands.Count;
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
            savepointCheck = host.Faults.Commands.Skip(before).ToList();

            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        foreach (var c in savepointCheck) _output.WriteLine("Savepoint check: " + c.Sql);
        // Exactly one existence query, and it reads the mapped table (it would fail on a wrong name).
        Assert.Single(savepointCheck, c => c.Sql.Contains("SELECT", StringComparison.OrdinalIgnoreCase) && c.Sql.Contains($"FROM {ExpectedOutboxTable}"));

        // The payload UPDATE (identity keys) ran against the mapped table and fixed the payloads.
        Assert.Contains(host.Faults.Commands, c => c.Sql.StartsWith($"UPDATE {ExpectedOutboxTable} SET [Payload] = "));
        var users = host.Database.Users(usersTable);
        var orders = host.Database.Orders(ordersTable);
        Assert.Equal("kept@test", Assert.Single(users).Email);
        var rows = host.OutboxRows();
        Assert.Equal(3, rows.Count);
        GraphAssert.RowsMatch(rows.Where(r => r.Type != typeof(TextEvent).AssemblyQualifiedName), users, orders);
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id.ToString()).OrderBy(x => int.Parse(x)));

        // Claim, record a failure on one message, delete another.
        host.Database.Execute($"UPDATE {ExpectedOutboxTable} SET [LockExpiresAt] = NULL");
        await using var claimScope = host.CreateScope();
        var store = Store(claimScope);
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var claimed = (await store.GetAllAsync(10, lockExpiration, 15)).ToList();
        var claim = host.Faults.Commands.Last(c => c.Sql.Contains("READPAST"));
        _output.WriteLine(claim.ToString());
        Assert.Contains($"FROM {ExpectedOutboxTable} WITH (ROWLOCK, READPAST, UPDLOCK)", claim.Sql);
        Assert.Equal(rows.Select(r => int.Parse(r.Id)), claimed.Select(c => c.Id));
        Assert.All(host.OutboxRows(), r => Assert.NotNull(r.LockExpiresAt));

        var failed = claimed[0];
        failed.Attemps = 1;
        failed.IsError = true;
        failed.ErrorMessage = "failed: {0} ]";
        await store.UpdateAsync(failed);
        await store.RemoveAsync(claimed[1]);

        var after = host.OutboxRows();
        Assert.Equal(new[] { claimed[0].Id, claimed[2].Id }, after.Select(r => int.Parse(r.Id)));
        Assert.Equal("failed: {0} ]", after[0].ErrorMessage);
        Assert.Equal(1, after[0].Attemps);
    }

    public static IEnumerable<object?[]> ClaimParameterCombinations()
    {
        foreach (var items in new int?[] { null, 2, 100 })
        foreach (var locks in new[] { false, true })
        foreach (var max in new int?[] { null, 3 })
            yield return new object?[] { items, locks, max };
    }

    [SqlServerTheory]
    [MemberData(nameof(ClaimParameterCombinations))]
    public async Task GetAllAsync_EveryParameterCombination_ClaimsTheEligibleRowsOfTheMappedTable(int? itemsCount, bool withLock, int? maxAttemps)
    {
        using var host = CreateHost();
        var now = DateTime.UtcNow;
        var free = StoreSeed.Row(host.Database, host.OutboxTable, null, null);
        var lockedFuture = StoreSeed.Row(host.Database, host.OutboxTable, now.AddMinutes(10), null);
        var twoAttempts = StoreSeed.Row(host.Database, host.OutboxTable, null, 2);
        var expired = StoreSeed.Row(host.Database, host.OutboxTable, now.AddMinutes(-10), 1);
        var threeAttempts = StoreSeed.Row(host.Database, host.OutboxTable, null, 3);
        var manyAttempts = StoreSeed.Row(host.Database, host.OutboxTable, now.AddMinutes(-10), 1000);
        var lockExpiration = withLock ? now.AddMinutes(30) : (DateTime?)null;

        var eligible = new[] { free, twoAttempts, expired, threeAttempts, manyAttempts }
            .Where(id => maxAttemps == null || (id != threeAttempts && id != manyAttempts))
            .ToList();
        var expected = itemsCount.HasValue ? eligible.Take(itemsCount.Value).ToList() : eligible;
        var before = host.OutboxRows().ToDictionary(r => int.Parse(r.Id));

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(itemsCount, lockExpiration, maxAttemps)).Select(x => x.Id).ToList();
        var claim = Assert.Single(host.Faults.Commands);
        _output.WriteLine(claim.ToString());

        if (itemsCount.HasValue)
        {
            Assert.Equal(expected, claimed);
        }
        else
        {
            // Without a count the claim has no ORDER BY: the order of the rows is unspecified.
            Assert.Equal(expected.OrderBy(x => x), claimed.OrderBy(x => x));
        }
        Assert.Equal((itemsCount.HasValue ? 1 : 0) + (withLock ? 1 : 0) + (maxAttemps.HasValue ? 1 : 0) + 1, claim.Parameters.Count);
        Assert.DoesNotContain(lockedFuture, claimed);
        var after = host.OutboxRows().ToDictionary(r => int.Parse(r.Id));
        foreach (var (id, row) in after)
        {
            if (withLock && expected.Contains(id))
            {
                Assert.True(Math.Abs((row.LockExpiresAt!.Value - lockExpiration!.Value).TotalMilliseconds) < 4, $"Row {id} lock not set by the claim.");
            }
            else
            {
                Assert.Equal(before[id].LockExpiresAt, row.LockExpiresAt);
            }
        }
    }

    [SqlServerFact]
    public async Task GetAllAsync_SecondClaimerWhileTheFirstHoldsItsRows_GetsTheOtherRowsWithoutBlocking()
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 6).Select(_ => StoreSeed.Row(host.Database, host.OutboxTable, null, null)).ToList();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);

        await using var first = host.CreateScope();
        await using var second = host.CreateScope();
        await using var tx = await first.Db.Database.BeginTransactionAsync();
        var claimedFirst = (await Store(first).GetAllAsync(3, lockExpiration, 15)).Select(x => x.Id).ToList();

        second.Db.Database.SetCommandTimeout(5);
        var watch = Stopwatch.StartNew();
        var claimedSecond = (await Store(second).GetAllAsync(10, lockExpiration, 15)).Select(x => x.Id).ToList();
        watch.Stop();

        Assert.Equal(ids.Take(3), claimedFirst);
        Assert.Equal(ids.Skip(3), claimedSecond);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), "The second claimer waited for the first claimer's locks.");
        await tx.CommitAsync();
    }

    [SqlServerFact]
    public async Task GetAllAsync_ConcurrentClaimers_NeverClaimTheSameRow()
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 60).Select(_ => StoreSeed.Row(host.Database, host.OutboxTable, null, null)).ToHashSet();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var start = new ManualResetEventSlim();

        var tasks = Enumerable.Range(0, 6).Select(_ => Task.Run(async () =>
        {
            await using var scope = host.CreateScope();
            await scope.Db.Database.OpenConnectionAsync();
            start.Wait();
            var claimed = new List<int>();
            for (var round = 0; round < 3; round++)
            {
                claimed.AddRange((await Store(scope).GetAllAsync(3, lockExpiration, 15)).Select(x => x.Id));
            }
            return claimed;
        })).ToList();
        start.Set();
        var all = (await Task.WhenAll(tasks)).SelectMany(x => x).ToList();

        Assert.Equal(54, all.Count);
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Subset(ids, all.ToHashSet());
    }
}

public class DefaultAndTableSchemaScenarioTests : TableNameScenarioTests<DefaultAndTableSchemaDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public DefaultAndTableSchemaScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override string ExpectedOutboxTable => "[tbl].[OutboxMessages]";

    [SqlServerFact]
    public async Task GetAllAsync_SameNamedTablesInTheModelDefaultSchemaAndDbo_ClaimsOnlyTheTableSchema()
    {
        using var host = CreateHost();
        var id = StoreSeed.Row(host.Database, host.OutboxTable, null, null);
        host.Database.Execute("SELECT TOP 0 * INTO [dflt].[OutboxMessages] FROM [tbl].[OutboxMessages]; SELECT TOP 0 * INTO [dbo].[OutboxMessages] FROM [tbl].[OutboxMessages]");
        try
        {
            StoreSeed.Row(host.Database, "[dflt].[OutboxMessages]", null, null, "{\"text\":\"decoy dflt\"}");
            StoreSeed.Row(host.Database, "[dbo].[OutboxMessages]", null, null, "{\"text\":\"decoy dbo\"}");

            await using var scope = host.CreateScope();
            var claimed = Assert.Single(await scope.Services.GetRequiredService<IOutboxStore<int>>().GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));

            Assert.Equal(id, claimed.Id);
            Assert.Null(host.Database.Scalar("SELECT [LockExpiresAt] FROM [dflt].[OutboxMessages]"));
            Assert.Null(host.Database.Scalar("SELECT [LockExpiresAt] FROM [dbo].[OutboxMessages]"));
            Assert.Equal("tbl.OutboxMessages", scope.Db.GetTableNameWithScheme<OutboxMessage<int>>());
        }
        finally
        {
            host.Database.Execute("DROP TABLE IF EXISTS [dflt].[OutboxMessages]; DROP TABLE IF EXISTS [dbo].[OutboxMessages]");
        }
    }
}

public class HostileNamesScenarioTests : TableNameScenarioTests<HostileNamesDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public HostileNamesScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override string ExpectedOutboxTable => "[s'c \"h\" ]]{1}.x].[Outbox.'q' \"dq\" ]] {0} {{ }} -- /* ; @take]";
}

public class UnicodeNamesScenarioTests : TableNameScenarioTests<UnicodeNamesDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public UnicodeNamesScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override string ExpectedOutboxTable => "[esquema_ü_漢字].[Bandeja ñ 😀 漢字]";
}

public class MaxLengthNamesScenarioTests : TableNameScenarioTests<MaxLengthNamesDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public MaxLengthNamesScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override string ExpectedOutboxTable
        => $"[{MaxLengthNamesDbContext.Schema.Replace("]", "]]")}].[{MaxLengthNamesDbContext.Table.Replace("]", "]]")}]";
}

/// <summary>
/// No schema configured: EF's commands and the claim are unqualified, so they resolve against the connection user's
/// default schema (then dbo). Simulated with a user WITHOUT LOGIN (default schema "tenant") created only inside the
/// throwaway database and impersonated with EXECUTE AS USER on the context's connection.
/// </summary>
public class NonDboDefaultSchemaUserTests : IClassFixture<SmokeDatabaseFixture>
{
    private const string UserName = "xdev_tenant_user";
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public NonDboDefaultSchemaUserTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost()
    {
        var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        host.Database.Execute($"""
            IF SCHEMA_ID(N'tenant') IS NULL EXEC(N'CREATE SCHEMA [tenant] AUTHORIZATION [dbo]');
            IF USER_ID(N'{UserName}') IS NULL CREATE USER [{UserName}] WITHOUT LOGIN WITH DEFAULT_SCHEMA = [tenant];
            ALTER ROLE [db_datareader] ADD MEMBER [{UserName}];
            ALTER ROLE [db_datawriter] ADD MEMBER [{UserName}];
            """);
        return host;
    }

    private async Task<T> AsTenantUser<T>(SmokeScope<SmokeDbContext> scope, Func<Task<T>> action)
    {
        await scope.Db.Database.OpenConnectionAsync();
        await scope.Db.Database.ExecuteSqlRawAsync($"EXECUTE AS USER = N'{UserName}'");
        try
        {
            var who = scope.Db.Database.SqlQueryRaw<string>("SELECT USER_NAME() + N'|' + SCHEMA_NAME() AS [Value]").AsEnumerable().Single();
            _output.WriteLine("Impersonated: " + who);
            Assert.Equal($"{UserName}|tenant", who);
            return await action();
        }
        finally
        {
            await scope.Db.Database.ExecuteSqlRawAsync("REVERT");
            await scope.Db.Database.CloseConnectionAsync();
        }
    }

    private void DropUser(SmokeDatabase database)
        => database.Execute($"IF USER_ID(N'{UserName}') IS NOT NULL DROP USER [{UserName}]");

    [SqlServerFact]
    public async Task SaveAndClaim_NoSchemaAndOutboxTableOnlyInDbo_ResolveToDbo()
    {
        using var host = CreateHost();
        try
        {
            await using var scope = host.CreateScope();
            var claimed = await AsTenantUser(scope, async () =>
            {
                var user = Graph.NewUser("tenant@test");
                scope.Db.Users.Add(user);
                scope.Db.Outbox.AddEvent(new UserCreated { User = user });
                await scope.Db.SaveChangesAsync();
                await scope.Db.Database.ExecuteSqlRawAsync("UPDATE [dbo].[OutboxMessages] SET [LockExpiresAt] = NULL");
                return (await scope.Services.GetRequiredService<IOutboxStore<int>>().GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15)).ToList();
            });

            var row = Assert.Single(host.OutboxRows());
            Assert.Equal(row.Id, Assert.Single(claimed).Id.ToString());
            Assert.Contains(host.Faults.Commands, c => c.Sql.Contains("FROM [OutboxMessages] WITH (ROWLOCK, READPAST, UPDLOCK)"));
        }
        finally
        {
            DropUser(host.Database);
        }
    }

    [SqlServerFact]
    public async Task SaveAndClaim_NoSchemaAndSameNamedTableInTheUsersDefaultSchema_BothUseTheSameTable()
    {
        using var host = CreateHost();
        host.Database.Execute("SELECT TOP 0 * INTO [tenant].[OutboxMessages] FROM [dbo].[OutboxMessages]");
        try
        {
            await using var scope = host.CreateScope();
            var claimed = await AsTenantUser(scope, async () =>
            {
                var user = Graph.NewUser("tenant2@test");
                scope.Db.Users.Add(user);
                scope.Db.Outbox.AddEvent(new UserCreated { User = user });
                await scope.Db.SaveChangesAsync();
                await scope.Db.Database.ExecuteSqlRawAsync("UPDATE [tenant].[OutboxMessages] SET [LockExpiresAt] = NULL");
                return (await scope.Services.GetRequiredService<IOutboxStore<int>>().GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15)).ToList();
            });

            // EF's INSERT and payload UPDATE (unqualified) went to [tenant]; the claim must read the same table.
            Assert.Empty(host.Database.OutboxRows("[dbo].[OutboxMessages]"));
            var row = Assert.Single(host.Database.OutboxRows("[tenant].[OutboxMessages]"));
            GraphAssert.UserPayloadMatches(row.Payload, host.Database.Users(), host.Database.Orders());
            Assert.Equal(row.Id, Assert.Single(claimed).Id.ToString());
        }
        finally
        {
            host.Database.Execute("DROP TABLE IF EXISTS [tenant].[OutboxMessages]");
            DropUser(host.Database);
        }
    }
}

/// <summary>The public GetTableNameWithScheme helper (undelimited "schema.table", dbo when no schema). Model only: no database.</summary>
public class GetTableNameWithSchemeTests
{
    private static string Resolve<TContext>(Action<OutboxDbContextConfiguration>? store = null) where TContext : SmokeContextBase<int>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        // Never opened: building the model needs no connection.
        services.AddDbContext<TContext>(o => o
            .UseSqlServer(@"Server=(localdb)\MSSQLLocalDB;Database=XDevOutboxModelOnly_NotCreated")
            .ConfigureWarnings(w => w.Ignore(Microsoft.EntityFrameworkCore.Diagnostics.CoreEventId.ManyServiceProvidersCreatedWarning)));
        services.AddOutbox<int>(typeof(SmokeHost<,>).Assembly);
        services.AddEFCoreOutboxStore<int, TContext>(store);
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<TContext>().GetTableNameWithScheme<OutboxMessage<int>>();
    }

    [Fact]
    public void GetTableNameWithScheme_NoSchemaConfigured_FallsBackToDbo()
        => Assert.Equal("dbo.OutboxMessages", Resolve<SmokeDbContext>());

    [Fact]
    public void GetTableNameWithScheme_ModelDefaultSchemaOnly_ReturnsTheDefaultSchema()
        => Assert.Equal("app.OutboxMessages", Resolve<DefaultSchemaDbContext>());

    [Fact]
    public void GetTableNameWithScheme_TableSchemaOnly_ReturnsTheTableSchema()
        => Assert.Equal("outbox.OutboxMessages", Resolve<OutboxSchemaDbContext>());

    [Fact]
    public void GetTableNameWithScheme_DefaultSchemaAndTableSchema_TheTableSchemaWins()
        => Assert.Equal("tbl.OutboxMessages", Resolve<DefaultAndTableSchemaDbContext>());

    [Fact]
    public void GetTableNameWithScheme_ConfiguredTableName_ReturnsItWithDbo()
        => Assert.Equal("dbo.SmokeOutbox", Resolve<CustomTableDbContext>(o => o.TableName = CustomTableDbContext.TableName));

    [Fact]
    public void GetTableNameWithScheme_NamesNeedingDelimiters_AreReturnedUndelimited()
        => Assert.Equal(QuotedOutboxTableDbContext.Schema + "." + QuotedOutboxTableDbContext.Table, Resolve<QuotedOutboxTableDbContext>());
}

/// <summary>
/// IOutboxStore.GetAllAsync declares itemsCount = null as its default: no limit, every eligible row is claimed. SQL
/// Server rejects ORDER BY in a CTE without TOP (error 1033), so the claim orders only when it takes a count; the
/// order of the rows claimed without a count is unspecified.
/// </summary>
public class ClaimWithoutItemsCountTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public ClaimWithoutItemsCountTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    [SqlServerTheory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task GetAllAsync_WithoutItemsCount_ClaimsEveryEligibleRow(bool withLock, bool withMaxAttemps)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        var now = DateTime.UtcNow;
        var free = Enumerable.Range(0, 3).Select(_ => StoreSeed.Row(host, null, null)).ToList();
        var expired = StoreSeed.Row(host, now.AddMinutes(-10), 2);
        var lockedFuture = StoreSeed.Row(host, now.AddMinutes(10), null);
        var overMaxAttemps = StoreSeed.Row(host, null, 15);
        var lockExpiration = withLock ? now.AddMinutes(5) : (DateTime?)null;

        var expected = free.Append(expired).ToList();
        if (!withMaxAttemps) expected.Add(overMaxAttemps);
        var before = host.OutboxRows().ToDictionary(r => int.Parse(r.Id));

        await using var scope = host.CreateScope();
        var claimed = (await scope.Services.GetRequiredService<IOutboxStore<int>>()
            .GetAllAsync(null, lockExpiration, withMaxAttemps ? 15 : null)).Select(x => x.Id).ToList();

        Assert.Equal(expected.OrderBy(x => x), claimed.OrderBy(x => x));
        Assert.DoesNotContain(lockedFuture, claimed);
        if (withMaxAttemps) Assert.DoesNotContain(overMaxAttemps, claimed);

        var after = host.OutboxRows().ToDictionary(r => int.Parse(r.Id));
        Assert.Equal(before.Keys.OrderBy(x => x), after.Keys.OrderBy(x => x));
        foreach (var (id, row) in after)
        {
            if (withLock && expected.Contains(id))
            {
                Assert.True(Math.Abs((row.LockExpiresAt!.Value - lockExpiration!.Value).TotalMilliseconds) < 4, $"Row {id} lock not set by the claim.");
            }
            else
            {
                Assert.Equal(before[id], row);
            }
        }
    }
}
