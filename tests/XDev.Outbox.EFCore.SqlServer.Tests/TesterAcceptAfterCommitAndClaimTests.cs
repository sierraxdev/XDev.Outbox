using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Tester (adversarial) cases for fix-accept-after-owned-commit (Bug A) on SQL Server: AcceptAllChanges throwing after
/// an owned commit leaves one committed row with the store-generated keys in its payload, locked until LockExpiresAt,
/// never dispatched in memory, and claimed by the poller's claim once the lock expires.
/// </summary>
public class TesterAcceptAfterOwnedCommitSqlServerTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public TesterAcceptAfterOwnedCommitSqlServerTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    public sealed class AcceptanceFailure : Exception
    {
        public AcceptanceFailure() : base("acceptance failure")
        {
        }
    }

    [SqlServerTheory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task OwnedSave_AcceptanceThrowsOnOrder_RowCommittedWithKeys_NotDispatched_ClaimedAfterLockExpires(bool async, bool retrying)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RetryingStrategy = retrying });
        await using (var scope = host.CreateScope())
        {
            var db = scope.Db;
            var thrown = 0;
            db.ChangeTracker.StateChanged += (_, e) =>
            {
                if (thrown == 0 && e.Entry.Entity is Order && e.NewState == EntityState.Unchanged)
                {
                    thrown++;
                    throw new AcceptanceFailure();
                }
            };
            var user = Graph.NewUser("accept@test", "o1", "o2");
            db.Users.Add(user);
            db.Outbox.AddEvent(new UserCreated { User = user });

            var error = await Record.ExceptionAsync(() => async ? db.SaveChangesAsync(true) : Task.FromResult(db.SaveChanges(true)));

            Assert.IsType<AcceptanceFailure>(error);
            Assert.Equal(1, thrown);
            Assert.Empty(db.Outbox.GetEvents());
            Assert.Empty(host.Queue.Enqueued);

            db.ChangeTracker.AcceptAllChanges();
            db.Users.Add(new User { Email = "unrelated@test" });
            await Save.Invoke(db, async);
            Assert.Empty(host.Queue.Enqueued);
            Assert.Empty(db.Outbox.GetEvents());
        }

        var users = host.Database.Users();
        var orders = host.Database.Orders();
        Assert.Equal(2, users.Count);
        Assert.Equal(2, orders.Count);
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal("committed", host.Database.RowState(host.OutboxTable, int.Parse(row.Id)));
        GraphAssert.UserPayloadMatches(row.Payload, users, orders);
        Assert.NotNull(row.LockExpiresAt);
        Assert.True(row.LockExpiresAt > DateTime.UtcNow.AddMinutes(4), "The committed row stays locked for the default lock time.");

        // The poller's claim (with a count, and without) does not see the row while it is locked.
        await using (var claimScope = host.CreateScope())
        {
            var store = claimScope.Services.GetRequiredService<IOutboxStore<int>>();
            Assert.Empty(await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));
            Assert.Empty(await store.GetAllAsync(null, DateTime.UtcNow.AddMinutes(5), 15));
        }

        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = @past", ("@past", DateTime.UtcNow.AddSeconds(-1)));

        await using (var claimScope = host.CreateScope())
        {
            var store = claimScope.Services.GetRequiredService<IOutboxStore<int>>();
            var claimed = Assert.Single(await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));
            Assert.Equal(row.Id, claimed.Id.ToString());
            GraphAssert.UserPayloadMatches(Json.Serialize(claimed.Payload), users, orders);
        }
    }
}

/// <summary>
/// Tester (adversarial) cases for fix-claim-without-count (Bug B): GetAllAsync(itemsCount: null) claims every eligible
/// row with the same eligibility, locking hints and lock update as with a count, in unspecified order.
/// </summary>
public class TesterClaimWithoutItemsCountTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterClaimWithoutItemsCountTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost() => SmokeHost<int, SmokeDbContext>.Create(_fixture);

    private static IOutboxStore<int> Store(SmokeScope<SmokeDbContext> scope) => scope.Services.GetRequiredService<IOutboxStore<int>>();

    private static void SeedMany(SmokeHost<int, SmokeDbContext> host, int count, DateTime? lockExpiresAt = null)
        => host.Database.Execute(
            $$"""
            INSERT INTO {{host.OutboxTable}} ([Type], [Payload], [Created], [LockExpiresAt], [Attemps])
            SELECT TOP (@count) @type, '{"text":"bulk","number":1}', SYSUTCDATETIME(), @lock, NULL
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
            """,
            ("@count", count), ("@type", typeof(ProbeEvent).AssemblyQualifiedName), ("@lock", lockExpiresAt));

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetAllAsync_WithoutItemsCount_EmptyTable_ReturnsNothing(bool withLock)
    {
        using var host = CreateHost();

        await using var scope = host.CreateScope();
        var claimed = await Store(scope).GetAllAsync(null, withLock ? DateTime.UtcNow.AddMinutes(5) : null, 15);

        Assert.Empty(claimed);
        Assert.Empty(host.OutboxRows());
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetAllAsync_WithoutItemsCount_AllRowsIneligible_ReturnsNothing_RowsUnchanged(bool withLock)
    {
        using var host = CreateHost();
        var now = DateTime.UtcNow;
        StoreSeed.Row(host, now.AddMinutes(10), null);
        StoreSeed.Row(host, now.AddMinutes(10), 3);
        StoreSeed.Row(host, null, 15);
        StoreSeed.Row(host, now.AddMinutes(-10), 20);
        var before = host.OutboxRows();

        await using var scope = host.CreateScope();
        var claimed = await Store(scope).GetAllAsync(null, withLock ? now.AddMinutes(5) : null, 15);

        Assert.Empty(claimed);
        Assert.Equal(before, host.OutboxRows());
    }

    /// <summary>Attemps boundary: null and max-1 are eligible, max and above are not (same as with a count).</summary>
    [SqlServerTheory]
    [InlineData(null)]
    [InlineData(2)]
    public async Task GetAllAsync_WithoutItemsCount_MaxAttempsBoundary_SameAsWithCount(int? itemsCountForComparison)
    {
        using var host = CreateHost();
        var nullAttemps = StoreSeed.Row(host, null, null);
        var zero = StoreSeed.Row(host, null, 0);
        var belowMax = StoreSeed.Row(host, null, 4);
        StoreSeed.Row(host, null, 5);
        StoreSeed.Row(host, null, 6);

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(null, null, 5)).Select(x => x.Id).OrderBy(x => x).ToList();

        Assert.Equal(new[] { nullAttemps, zero, belowMax }, claimed);

        if (itemsCountForComparison.HasValue)
        {
            var withCount = new List<int>();
            await using var countScope = host.CreateScope();
            withCount.AddRange((await Store(countScope).GetAllAsync(100, null, 5)).Select(x => x.Id));
            Assert.Equal(claimed, withCount.OrderBy(x => x));
        }
    }

    /// <summary>
    /// The SQL without a count has no TOP and no ORDER BY, keeps the locking hints and the lock update; the SQL with a
    /// count keeps TOP (@take) and ORDER BY Id.
    /// </summary>
    [SqlServerFact]
    public async Task GetAllAsync_ClaimSqlShape_WithAndWithoutCount()
    {
        using var host = CreateHost();
        StoreSeed.Row(host, null, null);

        await using (var scope = host.CreateScope())
        {
            await Store(scope).GetAllAsync(null, DateTime.UtcNow.AddMinutes(5), 15);
        }
        var withoutCount = Assert.Single(host.Faults.Commands).Sql;
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = NULL");
        host.Faults.ClearLog();
        await using (var scope = host.CreateScope())
        {
            await Store(scope).GetAllAsync(5, DateTime.UtcNow.AddMinutes(5), 15);
        }
        var withCount = Assert.Single(host.Faults.Commands).Sql;
        _output.WriteLine(withoutCount);
        _output.WriteLine(withCount);

        Assert.DoesNotContain("ORDER BY", withoutCount, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("TOP", withoutCount, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WITH (ROWLOCK, READPAST, UPDLOCK)", withoutCount);
        Assert.Contains("SET LockExpiresAt = @lockExpiration", withoutCount);
        Assert.Contains("OUTPUT INSERTED.*", withoutCount);
        Assert.Contains("TOP (@take)", withCount);
        Assert.Contains("ORDER BY Id", withCount);
        Assert.Contains("WITH (ROWLOCK, READPAST, UPDLOCK)", withCount);
    }

    /// <summary>500 eligible rows: all claimed once, each lock persisted and returned.</summary>
    [SqlServerFact]
    public async Task GetAllAsync_WithoutItemsCount_500Rows_ClaimsAllOnce_LocksPersistedAndReturned()
    {
        using var host = CreateHost();
        SeedMany(host, 500);
        var lockExpiration = new DateTime(2099, 1, 2, 3, 4, 5, 677);

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(null, lockExpiration, 15)).ToList();

        var rows = host.OutboxRows();
        Assert.Equal(500, rows.Count);
        Assert.Equal(500, claimed.Count);
        Assert.Equal(rows.Select(r => int.Parse(r.Id)).OrderBy(x => x), claimed.Select(c => c.Id).OrderBy(x => x));
        // SQL 'datetime' rounding (1/300 s).
        Assert.All(rows, r => Assert.True(Math.Abs((r.LockExpiresAt!.Value - lockExpiration).TotalMilliseconds) < 4));
        Assert.All(claimed, c => Assert.True(Math.Abs((c.LockExpiresAt!.Value - lockExpiration).TotalMilliseconds) < 4));

        await using var again = host.CreateScope();
        Assert.Empty(await Store(again).GetAllAsync(null, lockExpiration, 15));
    }

    /// <summary>
    /// A claimer with a count holds its rows in an open transaction: a claimer without a count skips them (READPAST)
    /// without blocking and claims all the others; after a rollback the held rows become claimable again.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetAllAsync_CountClaimerHoldsRows_NullClaimerSkipsThemWithoutBlocking(bool commitFirst)
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 8).Select(_ => StoreSeed.Row(host, null, null)).ToList();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);

        await using var first = host.CreateScope();
        await using var second = host.CreateScope();
        await using var tx = await first.Db.Database.BeginTransactionAsync();
        var claimedFirst = (await Store(first).GetAllAsync(3, lockExpiration, 15)).Select(x => x.Id).ToList();

        second.Db.Database.SetCommandTimeout(5);
        var watch = Stopwatch.StartNew();
        var claimedSecond = (await Store(second).GetAllAsync(null, lockExpiration, 15)).Select(x => x.Id).ToList();
        watch.Stop();

        Assert.Equal(ids.Take(3), claimedFirst);
        Assert.Equal(ids.Skip(3), claimedSecond.OrderBy(x => x));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), "The null-count claimer waited for the first claimer's locks.");

        if (commitFirst) await tx.CommitAsync(); else await tx.RollbackAsync();

        await using var third = host.CreateScope();
        var claimedThird = (await Store(third).GetAllAsync(null, lockExpiration, 15)).Select(x => x.Id).OrderBy(x => x).ToList();
        Assert.Equal(commitFirst ? Array.Empty<int>() : ids.Take(3), claimedThird);
    }

    /// <summary>A claimer without a count holds every row in an open transaction: another claimer (with or without a count) gets nothing, without blocking.</summary>
    [SqlServerTheory]
    [InlineData(null)]
    [InlineData(5)]
    public async Task GetAllAsync_NullClaimerHoldsAllRows_OtherClaimerGetsNothingWithoutBlocking(int? secondCount)
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 6).Select(_ => StoreSeed.Row(host, null, null)).ToList();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);

        await using var first = host.CreateScope();
        await using var second = host.CreateScope();
        await using var tx = await first.Db.Database.BeginTransactionAsync();
        var claimedFirst = (await Store(first).GetAllAsync(null, lockExpiration, 15)).Select(x => x.Id).OrderBy(x => x).ToList();

        second.Db.Database.SetCommandTimeout(5);
        var watch = Stopwatch.StartNew();
        var claimedSecond = (await Store(second).GetAllAsync(secondCount, lockExpiration, 15)).ToList();
        watch.Stop();
        await tx.RollbackAsync();

        Assert.Equal(ids, claimedFirst);
        Assert.Empty(claimedSecond);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), "The second claimer waited for the first claimer's locks.");
    }

    /// <summary>Concurrent claimers without a count (mixed with claimers with a count), each in its own transaction, never claim the same row; all rows end up claimed.</summary>
    [SqlServerFact]
    public async Task GetAllAsync_ConcurrentNullAndCountClaimersInTransactions_NeverClaimTheSameRow()
    {
        using var host = CreateHost();
        SeedMany(host, 300);
        var ids = host.OutboxRows().Select(r => int.Parse(r.Id)).ToHashSet();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var start = new ManualResetEventSlim();

        var tasks = Enumerable.Range(0, 6).Select(i => Task.Run(async () =>
        {
            await using var scope = host.CreateScope();
            await scope.Db.Database.OpenConnectionAsync();
            start.Wait();
            var claimed = new List<int>();
            for (var round = 0; round < 3; round++)
            {
                await using var tx = await scope.Db.Database.BeginTransactionAsync();
                claimed.AddRange((await Store(scope).GetAllAsync(i % 2 == 0 ? null : 7, lockExpiration, 15)).Select(x => x.Id));
                await Task.Delay(10);
                await tx.CommitAsync();
            }
            return claimed;
        })).ToList();
        start.Set();
        var results = await Task.WhenAll(tasks);

        var all = results.SelectMany(x => x).ToList();
        _output.WriteLine($"Claims per claimer: {string.Join(", ", results.Select(r => r.Count))}; total {all.Count}, distinct {all.Distinct().Count()}");
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Equal(ids.OrderBy(x => x), all.OrderBy(x => x));
        Assert.All(host.OutboxRows(), r => Assert.NotNull(r.LockExpiresAt));
    }
}

/// <summary>Bug B with Guid outbox keys (own database: the Guid model maps the same table names).</summary>
public class TesterClaimWithoutItemsCountGuidTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public TesterClaimWithoutItemsCountGuidTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    /// <summary>Guid keys: a claim without a count claims every eligible row and locks them.</summary>
    [SqlServerFact]
    public async Task GetAllAsync_WithoutItemsCount_GuidKey_ClaimsEveryEligibleRow()
    {
        using var host = SmokeHost<Guid, GuidDbContext>.Create(_fixture);
        await using (var scope = host.CreateScope())
        {
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "one" }, new TextEvent { Text = "two" }, new TextEvent { Text = "three" });
            await scope.Db.SaveChangesAsync();
        }
        var rows = host.OutboxRows();
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = NULL WHERE [Id] <> @locked", ("@locked", Guid.Parse(rows[0].Id)));

        await using var claimScope = host.CreateScope();
        var store = claimScope.Services.GetRequiredService<IOutboxStore<Guid>>();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var claimed = (await store.GetAllAsync(null, lockExpiration, 15)).Select(c => c.Id).OrderBy(x => x).ToList();

        Assert.Equal(rows.Skip(1).Select(r => Guid.Parse(r.Id)).OrderBy(x => x), claimed);
        Assert.All(host.OutboxRows(), r => Assert.NotNull(r.LockExpiresAt));
    }
}
