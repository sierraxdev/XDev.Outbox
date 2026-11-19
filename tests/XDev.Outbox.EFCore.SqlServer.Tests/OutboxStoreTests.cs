using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

internal static class StoreSeed
{
    public static int Row(SmokeHost<int, SmokeDbContext> host, DateTime? lockExpiresAt, int? attemps, string payload = "{\"text\":\"seed\",\"number\":1}")
        => Row(host.Database, host.OutboxTable, lockExpiresAt, attemps, payload);

    public static int Row(SmokeDatabase database, string table, DateTime? lockExpiresAt, int? attemps, string payload = "{\"text\":\"seed\",\"number\":1}")
        => Convert.ToInt32(database.Scalar(
            $"INSERT INTO {table} ([Type], [Payload], [Created], [LockExpiresAt], [Attemps]) OUTPUT INSERTED.[Id] VALUES (@type, @payload, @created, @lock, @attemps)",
            ("@type", typeof(ProbeEvent).AssemblyQualifiedName),
            ("@payload", payload),
            ("@created", DateTime.UtcNow),
            ("@lock", lockExpiresAt),
            ("@attemps", attemps)));
}

/// <summary>EntityFrameworkOutboxStore against SQL Server: the claim statement, concurrent claimers, delete, failure update.</summary>
public class OutboxStoreTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public OutboxStoreTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost() => SmokeHost<int, SmokeDbContext>.Create(_fixture);

    private static IOutboxStore<int> Store(SmokeScope<SmokeDbContext> scope) => scope.Services.GetRequiredService<IOutboxStore<int>>();

    [SqlServerFact]
    public async Task GetAllAsync_WithLockExpiration_ClaimsOnlyEligibleRowsInIdOrderAndPersistsTheirLock()
    {
        using var host = CreateHost();
        var now = DateTime.UtcNow;
        var free = StoreSeed.Row(host, null, null);
        var expired = StoreSeed.Row(host, now.AddMinutes(-1), 2);
        var locked = StoreSeed.Row(host, now.AddMinutes(10), null);
        var exhausted = StoreSeed.Row(host, null, 15);
        var almostExhausted = StoreSeed.Row(host, null, 14);
        var lockExpiration = new DateTime(2099, 1, 2, 3, 4, 5, 678).AddTicks(9012);

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(10, lockExpiration, 15)).ToList();

        Assert.Equal(new[] { free, expired, almostExhausted }, claimed.Select(x => x.Id));
        var command = Assert.Single(host.Faults.Commands);
        _output.WriteLine(command.ToString());
        Assert.Contains("WITH (ROWLOCK, READPAST, UPDLOCK)", command.Sql);
        Assert.Contains("FROM [OutboxMessages] WITH (ROWLOCK, READPAST, UPDLOCK)", command.Sql);

        var rows = host.OutboxRows().ToDictionary(r => int.Parse(r.Id));
        foreach (var id in new[] { free, expired, almostExhausted })
        {
            var stored = rows[id].LockExpiresAt!.Value;
            _output.WriteLine($"Row {id}: requested lock {lockExpiration:O}, stored {stored:O}, returned {claimed.Single(x => x.Id == id).LockExpiresAt:O}");
            Assert.True(Math.Abs((stored - lockExpiration).TotalMilliseconds) < 4, "Stored lock differs from the requested one by more than SQL 'datetime' rounding.");
        }
        Assert.Null(rows[exhausted].LockExpiresAt);
        Assert.True(rows[locked].LockExpiresAt > now.AddMinutes(9));
        Assert.All(claimed, m => Assert.Equal(typeof(ProbeEvent).AssemblyQualifiedName, m.Type));
        Assert.Empty(scope.Db.ChangeTracker.Entries()); // AsNoTracking
    }

    [SqlServerFact]
    public async Task GetAllAsync_ItemsCount_ClaimsOnlyTheLowestIds()
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 5).Select(_ => StoreSeed.Row(host, null, null)).ToList();

        await using var scope = host.CreateScope();
        var claimed = await Store(scope).GetAllAsync(2, DateTime.UtcNow.AddMinutes(5), 15);

        Assert.Equal(ids.Take(2), claimed.Select(x => x.Id));
        Assert.Equal(2, host.OutboxRows().Count(r => r.LockExpiresAt != null));
    }

    [SqlServerFact]
    public async Task GetAllAsync_WithoutLockExpiration_ReturnsEligibleRowsWithoutLockingThem()
    {
        using var host = CreateHost();
        var id = StoreSeed.Row(host, null, null);

        await using var scope = host.CreateScope();
        var read = await Store(scope).GetAllAsync(10, null, 15);

        Assert.Equal(id, Assert.Single(read).Id);
        Assert.Null(Assert.Single(host.OutboxRows()).LockExpiresAt);
    }

    [SqlServerFact]
    public async Task GetAllAsync_WithoutMaxAttemps_IncludesRowsWithAnyAttemptCount()
    {
        using var host = CreateHost();
        var id = StoreSeed.Row(host, null, 1000);

        await using var scope = host.CreateScope();
        var claimed = await Store(scope).GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), null);

        Assert.Equal(id, Assert.Single(claimed).Id);
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetAllAsync_WhileAnotherTransactionHoldsClaimedRows_SecondClaimerSkipsThemWithoutBlocking(bool commitFirst)
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 6).Select(_ => StoreSeed.Row(host, null, null)).ToList();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);

        await using var first = host.CreateScope();
        await using var second = host.CreateScope();
        await using var tx = await first.Db.Database.BeginTransactionAsync();
        var claimedFirst = (await Store(first).GetAllAsync(3, lockExpiration, 15)).Select(x => x.Id).ToList();

        second.Db.Database.SetCommandTimeout(5);
        var watch = Stopwatch.StartNew();
        var claimedSecond = (await Store(second).GetAllAsync(10, lockExpiration, 15)).Select(x => x.Id).ToList();
        watch.Stop();
        _output.WriteLine($"First (open transaction): {string.Join(",", claimedFirst)}; second: {string.Join(",", claimedSecond)} in {watch.ElapsedMilliseconds} ms");

        Assert.Equal(ids.Take(3), claimedFirst);
        Assert.Equal(ids.Skip(3), claimedSecond);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), "The second claimer waited for the first claimer's locks.");

        if (commitFirst) await tx.CommitAsync(); else await tx.RollbackAsync();

        await using var third = host.CreateScope();
        var claimedThird = (await Store(third).GetAllAsync(10, lockExpiration, 15)).Select(x => x.Id).ToList();
        Assert.Equal(commitFirst ? Array.Empty<int>() : ids.Take(3), claimedThird);
    }

    [SqlServerFact]
    public async Task GetAllAsync_ConcurrentClaimersOnSeparateConnections_NeverClaimTheSameRow()
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 120).Select(_ => StoreSeed.Row(host, null, null)).ToHashSet();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var start = new ManualResetEventSlim();

        var tasks = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            await using var scope = host.CreateScope();
            await scope.Db.Database.OpenConnectionAsync();
            start.Wait();
            var claimed = new List<int>();
            for (var round = 0; round < 3; round++)
            {
                claimed.AddRange((await Store(scope).GetAllAsync(5, lockExpiration, 15)).Select(x => x.Id));
            }
            return claimed;
        })).ToList();
        start.Set();
        var results = await Task.WhenAll(tasks);

        var all = results.SelectMany(x => x).ToList();
        _output.WriteLine($"Claims per claimer: {string.Join(", ", results.Select(r => r.Count))}; total {all.Count}, distinct {all.Distinct().Count()}");
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Subset(ids, all.ToHashSet());

        await using var rest = host.CreateScope();
        var remaining = (await Store(rest).GetAllAsync(1000, lockExpiration, 15)).Select(x => x.Id).ToList();
        Assert.Empty(remaining.Intersect(all));
        Assert.Equal(ids.OrderBy(x => x), all.Concat(remaining).OrderBy(x => x));
    }

    [SqlServerFact]
    public async Task RemoveAsync_ClaimedMessage_DeletesOnlyThatRow()
    {
        using var host = CreateHost();
        var first = StoreSeed.Row(host, null, null);
        var second = StoreSeed.Row(host, null, null);

        await using var scope = host.CreateScope();
        var store = Store(scope);
        var message = (await store.GetAllAsync(1, DateTime.UtcNow.AddMinutes(5), 15)).Single();
        await store.RemoveAsync(message);

        Assert.Equal(first, message.Id);
        Assert.Equal(second.ToString(), Assert.Single(host.OutboxRows()).Id);
        Assert.Empty(host.Queue.Enqueued);
    }

    [SqlServerFact]
    public async Task UpdateAsync_FailedMessage_RecordsTheFailureAndPreservesTypeAndPayload()
    {
        using var host = CreateHost();
        const string payload = "{\"text\":\"😀 漢字 \\\"quoted\\\"\",\"number\":42,\"nested\":{\"items\":[1,2,3]}}";
        var id = StoreSeed.Row(host, null, 3, payload);

        await using var scope = host.CreateScope();
        var store = Store(scope);
        var message = (await store.GetAllAsync(1, DateTime.UtcNow.AddMinutes(5), 15)).Single();
        var lockAfterClaim = Assert.Single(host.OutboxRows()).LockExpiresAt;
        message.Attemps = (message.Attemps ?? 0) + 1;
        message.IsError = true;
        message.ErrorMessage = "subscriber failed: ünïcödé";
        await store.UpdateAsync(message);

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(id.ToString(), row.Id);
        Assert.Equal(4, row.Attemps);
        Assert.True(row.IsError);
        Assert.Equal("subscriber failed: ünïcödé", row.ErrorMessage);
        Assert.Equal(typeof(ProbeEvent).AssemblyQualifiedName, row.Type);
        Assert.Equal(lockAfterClaim, row.LockExpiresAt);
        _output.WriteLine("Seeded:   " + payload);
        _output.WriteLine("Rewritten: " + row.Payload);
        Assert.True(GraphAssert.JsonEquals(payload, row.Payload), "UpdateAsync changed the payload.");
    }
}

/// <summary>
/// Outbox table moved to schema "outbox" with ToTable(name, schema) while the model default schema stays dbo. The save
/// path and the claim SQL both use the table's mapped schema (regression: the claim used to target dbo).
/// </summary>
public class OutboxTableSchemaTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public OutboxTableSchemaTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, OutboxSchemaDbContext> CreateHost() => SmokeHost<int, OutboxSchemaDbContext>.Create(_fixture);

    [SqlServerFact]
    public async Task OwnedSave_OutboxTableInNonDboSchema_InsertAndPayloadUpdateTargetThatTable()
    {
        using var host = CreateHost();
        Assert.Equal("[outbox].[OutboxMessages]", host.OutboxTable);
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("schema@test");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(new UserCreated { User = user });

        await scope.Db.SaveChangesAsync();

        var row = Assert.Single(host.OutboxRows());
        GraphAssert.UserPayloadMatches(row.Payload, host.Database.Users(), host.Database.Orders());
        Assert.Contains(host.Faults.Commands, c => c.Sql.StartsWith("UPDATE [outbox].[OutboxMessages] SET [Payload]"));
    }

    [SqlServerFact]
    public async Task GetTableNameWithScheme_OutboxTableInNonDboSchema_ReturnsTheTablesSchema()
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();

        Assert.Equal("outbox.OutboxMessages", scope.Db.GetTableNameWithScheme<OutboxMessage<int>>());
    }

    [SqlServerFact]
    public async Task Store_GetAllAsync_OutboxTableInNonDboSchema_ClaimsFromThatTable()
    {
        using var host = CreateHost();
        var id = StoreSeed.Row(host.Database, host.OutboxTable, null, null);

        await using var scope = host.CreateScope();
        var store = scope.Services.GetRequiredService<IOutboxStore<int>>();
        IEnumerable<OutboxMessage<int>>? claimed = null;
        var error = await Record.ExceptionAsync(async () => claimed = await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));
        for (var e = error; e != null; e = e.InnerException) _output.WriteLine($"{e.GetType().FullName}: {e.Message}");
        _output.WriteLine(host.Faults.Commands.LastOrDefault()?.Sql ?? "");

        Assert.Null(error);
        Assert.Equal(id, Assert.Single(claimed!).Id);
    }

    [SqlServerFact]
    public async Task Store_GetAllAsync_OutboxTableInNonDboSchemaAndADboTableWithTheSameName_ClaimsOnlyItsOwnTable()
    {
        using var host = CreateHost();
        var id = StoreSeed.Row(host.Database, host.OutboxTable, null, null);
        host.Database.Execute("SELECT TOP 0 * INTO [dbo].[OutboxMessages] FROM [outbox].[OutboxMessages]");
        try
        {
            var decoy = StoreSeed.Row(host.Database, "[dbo].[OutboxMessages]", null, null, "{\"text\":\"decoy\"}");

            await using var scope = host.CreateScope();
            var store = scope.Services.GetRequiredService<IOutboxStore<int>>();
            var claimed = (await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15)).ToList();
            _output.WriteLine($"Real row {id}, decoy row {decoy}; claimed: {string.Join(", ", claimed.Select(c => $"{c.Id} {System.Text.Json.JsonSerializer.Serialize(c.Payload)}"))}");
            _output.WriteLine("Decoy lock after claim: " + host.Database.Scalar("SELECT [LockExpiresAt] FROM [dbo].[OutboxMessages]"));

            var message = Assert.Single(claimed);
            Assert.Equal(id, message.Id);
            Assert.DoesNotContain("decoy", System.Text.Json.JsonSerializer.Serialize(message.Payload));
            Assert.Null(host.Database.Scalar("SELECT [LockExpiresAt] FROM [dbo].[OutboxMessages]"));
        }
        finally
        {
            host.Database.Execute("DROP TABLE IF EXISTS [dbo].[OutboxMessages]");
        }
    }
}

/// <summary>The whole model in schema "app" via HasDefaultSchema: save and claim both use [app].</summary>
public class DefaultSchemaTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public DefaultSchemaTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    [SqlServerFact]
    public async Task OwnedSaveClaimAndRemove_ModelDefaultSchema_UseTheSchemaTable()
    {
        using var host = SmokeHost<int, DefaultSchemaDbContext>.Create(_fixture);
        Assert.Equal("[app].[OutboxMessages]", host.OutboxTable);
        await using (var scope = host.CreateScope())
        {
            var user = Graph.NewUser("app@test");
            scope.Db.Users.Add(user);
            scope.Db.Outbox.AddEvent(new UserCreated { User = user });
            await scope.Db.SaveChangesAsync();
        }

        var row = Assert.Single(host.OutboxRows());
        GraphAssert.UserPayloadMatches(row.Payload, host.Database.Users("[app].[Users]"), host.Database.Orders("[app].[Orders]"));
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = NULL");

        await using var claimScope = host.CreateScope();
        var store = claimScope.Services.GetRequiredService<IOutboxStore<int>>();
        var claimed = Assert.Single(await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));
        Assert.Equal(row.Id, claimed.Id.ToString());
        Assert.Contains(host.Faults.Commands, c => c.Sql.Contains("FROM [app].[OutboxMessages] WITH (ROWLOCK, READPAST, UPDLOCK)"));
        await store.RemoveAsync(claimed);
        Assert.Empty(host.OutboxRows());
    }
}

/// <summary>Schema and table names that need delimiting (space, ']', braces): save, claim and remove all work.</summary>
public class QuotedOutboxTableTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public QuotedOutboxTableTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [SqlServerFact]
    public async Task OwnedSaveClaimAndRemove_SchemaAndTableNeedDelimiting_UseThatTable()
    {
        using var host = SmokeHost<int, QuotedOutboxTableDbContext>.Create(_fixture);
        Assert.Equal("[out box]]].[Outbox]] {Msgs}]", host.OutboxTable);
        await using (var scope = host.CreateScope())
        {
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "claim me" });
            await scope.Db.SaveChangesAsync();
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.Contains("claim me", row.Payload);
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = NULL");

        await using var claimScope = host.CreateScope();
        var store = claimScope.Services.GetRequiredService<IOutboxStore<int>>();
        var claimed = Assert.Single(await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));
        _output.WriteLine(host.Faults.Commands.Last(c => c.Sql.Contains("ROWLOCK")).Sql);
        Assert.Equal(row.Id, claimed.Id.ToString());
        Assert.Contains(host.Faults.Commands, c => c.Sql.Contains("FROM [out box]]].[Outbox]] {Msgs}] WITH (ROWLOCK, READPAST, UPDLOCK)"));
        Assert.NotNull(Assert.Single(host.OutboxRows()).LockExpiresAt);

        await store.RemoveAsync(claimed);
        Assert.Empty(host.OutboxRows());
    }
}
