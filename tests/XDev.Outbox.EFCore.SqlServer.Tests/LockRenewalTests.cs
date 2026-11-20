using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>EntityFrameworkOutboxStore.RenewLocksAsync against SQL Server.</summary>
public class LockRenewalStoreTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public LockRenewalStoreTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost() => SmokeHost<int, SmokeDbContext>.Create(_fixture);

    private static IOutboxStore<int> Store(SmokeScope<SmokeDbContext> scope) => scope.Services.GetRequiredService<IOutboxStore<int>>();

    /// <summary>A lock value with sub-millisecond ticks, which SQL 'datetime' could not hold.</summary>
    private static DateTime PreciseLock(int minutes) => new DateTime(2099, 3, 4, 5, 6, 7, DateTimeKind.Unspecified).AddMinutes(minutes).AddTicks(1234567);

    [SqlServerFact]
    public async Task RenewLocksAsync_ClaimedRows_RenewsThemExactlyAndReturnsTheStoredLocks()
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 3).Select(_ => StoreSeed.Row(host, null, null)).ToList();
        var other = StoreSeed.Row(host, DateTime.UtcNow.AddMinutes(30), null);

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(3, DateTime.UtcNow.AddMinutes(5), 15)).ToList();
        var otherBefore = host.OutboxRows().Single(r => r.Id == other.ToString()).LockExpiresAt;
        var newLock = PreciseLock(0);
        host.Faults.ClearLog();

        var renewed = await Store(scope).RenewLocksAsync(claimed.ToDictionary(m => m.Id, m => m.LockExpiresAt!.Value), newLock);

        Assert.Equal(ids, renewed.Keys.OrderBy(x => x));
        var rows = host.OutboxRows().ToDictionary(r => int.Parse(r.Id));
        foreach (var id in ids)
        {
            Assert.Equal(newLock, rows[id].LockExpiresAt);
            Assert.Equal(newLock, renewed[id]);
        }
        Assert.Equal(otherBefore, rows[other].LockExpiresAt);
        Assert.All(rows.Values, r => Assert.Null(r.Attemps));

        var command = Assert.Single(host.Faults.Commands);
        _output.WriteLine(command.ToString());
        Assert.DoesNotContain("READPAST", command.Sql);
        Assert.Contains("ROWLOCK", command.Sql);
        Assert.Contains("[OutboxMessages]", command.Sql);
        Assert.Equal(1 + 2 * ids.Count, command.Parameters.Count);
        Assert.All(command.Parameters.Where(p => p.Value is DateTime), p => Assert.Equal("DateTime2", p.SqlDbType));
        Assert.Equal(1 + ids.Count, command.Parameters.Count(p => p.Value is DateTime));
        Assert.Equal(ids.Count, command.Parameters.Count(p => p.SqlDbType == "Int"));
        Assert.Empty(scope.Db.ChangeTracker.Entries());
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_ImmediatePathRowWithSubMillisecondLock_IsRenewedFromTheEnqueuedValue()
    {
        using var host = CreateHost();
        await using (var save = host.CreateScope())
        {
            save.Db.Outbox.AddEvent(new TextEvent { Text = "immediate" });
            await save.Db.SaveChangesAsync();
        }
        var enqueued = Assert.Single(host.Queue.Enqueued);
        var expected = enqueued.LockExpiresAt!.Value;
        _output.WriteLine($"Enqueued lock {expected:O}, stored {Assert.Single(host.OutboxRows()).LockExpiresAt:O}");
        var newLock = PreciseLock(1);

        await using var scope = host.CreateScope();
        var renewed = await Store(scope).RenewLocksAsync(new Dictionary<int, DateTime> { [enqueued.Id] = expected }, newLock);

        Assert.Equal(newLock, renewed[enqueued.Id]);
        Assert.Equal(newLock, Assert.Single(host.OutboxRows()).LockExpiresAt);
    }

    [SqlServerTheory]
    [InlineData(4, true)]
    [InlineData(-4, true)]
    [InlineData(6, false)]
    [InlineData(-6, false)]
    [InlineData(1000, false)]
    public async Task RenewLocksAsync_ExpectedLockOffsetFromTheStoredValue_RenewsOnlyWithinFiveMilliseconds(int offsetMs, bool renews)
    {
        using var host = CreateHost();
        var stored = PreciseLock(2);
        var id = StoreSeed.Row(host, null, null);
        // Sent as text and converted on the server: an inferred DateTime parameter would be SQL 'datetime' (rounded).
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = CONVERT(datetime2, @lock, 126) WHERE [Id] = @id",
            ("@lock", stored.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")), ("@id", id));
        Assert.Equal(stored, Assert.Single(host.OutboxRows()).LockExpiresAt);

        await using var scope = host.CreateScope();
        var newLock = PreciseLock(3);
        var renewed = await Store(scope).RenewLocksAsync(new Dictionary<int, DateTime> { [id] = stored.AddMilliseconds(offsetMs) }, newLock);

        Assert.Equal(renews, renewed.ContainsKey(id));
        Assert.Equal(renews ? newLock : stored, Assert.Single(host.OutboxRows()).LockExpiresAt);
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_RowReclaimedByAnotherClaimer_IsNotRenewed()
    {
        using var host = CreateHost();
        var id = StoreSeed.Row(host, null, null);

        await using var first = host.CreateScope();
        var original = (await Store(first).GetAllAsync(1, DateTime.UtcNow.AddMinutes(-1), 15)).Single(); // already expired
        await using var second = host.CreateScope();
        var reclaimed = (await Store(second).GetAllAsync(1, DateTime.UtcNow.AddMinutes(10), 15)).Single();
        Assert.Equal(id, reclaimed.Id);

        var renewed = await Store(first).RenewLocksAsync(new Dictionary<int, DateTime> { [id] = original.LockExpiresAt!.Value }, PreciseLock(0));

        Assert.Empty(renewed);
        Assert.Equal(reclaimed.LockExpiresAt, Assert.Single(host.OutboxRows()).LockExpiresAt);
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_DeletedRow_IsNotResurrectedAndOtherRowsAreRenewed()
    {
        using var host = CreateHost();
        StoreSeed.Row(host, null, null);
        StoreSeed.Row(host, null, null);

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(2, DateTime.UtcNow.AddMinutes(5), 15)).ToList();
        await Store(scope).RemoveAsync(claimed[0]);

        var renewed = await Store(scope).RenewLocksAsync(claimed.ToDictionary(m => m.Id, m => m.LockExpiresAt!.Value), PreciseLock(0));

        Assert.Equal(new[] { claimed[1].Id }, renewed.Keys);
        Assert.Equal(claimed[1].Id.ToString(), Assert.Single(host.OutboxRows()).Id);
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_EmptyInput_ReturnsEmptyWithoutAnyCommand()
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();

        var renewed = await Store(scope).RenewLocksAsync(new Dictionary<int, DateTime>(), PreciseLock(0));

        Assert.Empty(renewed);
        Assert.Empty(host.Faults.Commands);
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_MoreThanOneThousandIds_RenewsAllInChunksOfAtMostOneThousand()
    {
        using var host = CreateHost();
        host.Database.Execute(
            $"INSERT INTO {host.OutboxTable} ([Type], [Payload], [Created], [LockExpiresAt]) SELECT TOP (2150) @type, N'{{}}', SYSUTCDATETIME(), NULL FROM sys.all_objects a CROSS JOIN sys.all_objects b",
            ("@type", typeof(ProbeEvent).AssemblyQualifiedName));

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(null, DateTime.UtcNow.AddMinutes(5), null)).ToList();
        Assert.Equal(2150, claimed.Count);
        var expected = claimed.ToDictionary(m => m.Id, m => m.LockExpiresAt!.Value);
        expected[-1] = DateTime.UtcNow; // no such row
        var newLock = PreciseLock(0);
        host.Faults.ClearLog();

        var renewed = await Store(scope).RenewLocksAsync(expected, newLock);

        Assert.Equal(claimed.Select(m => m.Id).OrderBy(x => x), renewed.Keys.OrderBy(x => x));
        Assert.All(host.OutboxRows(), r => Assert.Equal(newLock, r.LockExpiresAt));
        var commands = host.Faults.Commands;
        Assert.Equal(3, commands.Count);
        Assert.All(commands, c => Assert.True(c.Parameters.Count <= 1 + 2 * 1000, $"{c.Parameters.Count} parameters"));
        Assert.Equal(2151, commands.Sum(c => c.Parameters.Count(p => p.SqlDbType == "Int")));
    }

    /// <summary>
    /// A renewal and a claim race for a row whose lock has expired: exactly one of them gets it, whichever runs first.
    /// </summary>
    [SqlServerFact]
    public async Task RenewLocksAsync_RacingAClaimOnAnExpiredRow_ExactlyOneWins()
    {
        using var host = CreateHost();
        int renewalWins = 0, claimWins = 0;
        for (var round = 0; round < 30; round++)
        {
            host.Database.Execute($"DELETE FROM {host.OutboxTable}");
            var id = StoreSeed.Row(host, null, null);
            await using var owner = host.CreateScope();
            var owned = (await Store(owner).GetAllAsync(1, DateTime.UtcNow.AddSeconds(-30), 15)).Single(); // expired lock
            await using var claimerScope = host.CreateScope();
            await claimerScope.Db.Database.OpenConnectionAsync();
            await owner.Db.Database.OpenConnectionAsync();
            var renewLock = DateTime.UtcNow.AddMinutes(10);
            var claimLock = DateTime.UtcNow.AddMinutes(20);
            var start = new ManualResetEventSlim();

            var renewal = Task.Run(async () =>
            {
                start.Wait();
                return await Store(owner).RenewLocksAsync(new Dictionary<int, DateTime> { [id] = owned.LockExpiresAt!.Value }, renewLock);
            });
            var claim = Task.Run(async () =>
            {
                start.Wait();
                return (await Store(claimerScope).GetAllAsync(1, claimLock, 15)).ToList();
            });
            start.Set();
            var renewed = await renewal;
            var claimed = await claim;

            var stored = Assert.Single(host.OutboxRows()).LockExpiresAt!.Value;
            if (renewed.ContainsKey(id))
            {
                renewalWins++;
                Assert.Empty(claimed);
                Assert.Equal(renewed[id], stored);
            }
            else
            {
                claimWins++;
                Assert.Equal(id, Assert.Single(claimed).Id);
                Assert.Equal(Assert.Single(claimed).LockExpiresAt, stored);
            }
        }
        _output.WriteLine($"Renewal won {renewalWins}, claim won {claimWins}");
    }
}

/// <summary>RenewLocksAsync with Guid keys (uniqueidentifier Id parameters).</summary>
public class LockRenewalGuidTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public LockRenewalGuidTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_GuidKey_RenewsTheMatchingRowsOnly()
    {
        using var host = SmokeHost<Guid, GuidDbContext>.Create(_fixture);
        await using (var save = host.CreateScope())
        {
            save.Db.Outbox.AddEvent(new TextEvent { Text = "one" }, new TextEvent { Text = "two" });
            await save.Db.SaveChangesAsync();
        }
        var enqueued = host.Queue.Enqueued;
        Assert.Equal(2, enqueued.Count);
        var newLock = new DateTime(2099, 1, 1).AddTicks(7654321);
        host.Faults.ClearLog();

        await using var scope = host.CreateScope();
        var store = scope.Services.GetRequiredService<IOutboxStore<Guid>>();
        var renewed = await store.RenewLocksAsync(new Dictionary<Guid, DateTime>
        {
            [enqueued[0].Id] = enqueued[0].LockExpiresAt!.Value,
            [enqueued[1].Id] = enqueued[1].LockExpiresAt!.Value.AddMinutes(-1), // stale
            [Guid.NewGuid()] = enqueued[0].LockExpiresAt!.Value, // no such row
        }, newLock);

        Assert.Equal(new[] { enqueued[0].Id }, renewed.Keys);
        var command = Assert.Single(host.Faults.Commands);
        _output.WriteLine(command.ToString());
        Assert.Equal(3, command.Parameters.Count(p => p.SqlDbType == "UniqueIdentifier"));
        var rows = host.OutboxRows().ToDictionary(r => Guid.Parse(r.Id));
        Assert.Equal(newLock, rows[enqueued[0].Id].LockExpiresAt);
        Assert.Equal(enqueued[1].LockExpiresAt, rows[enqueued[1].Id].LockExpiresAt);
    }
}

public class SlowEvent
{
    public string? Text { get; set; }
}

/// <summary>Counts invocations of <see cref="SlowSubscriber"/>; each one takes <see cref="Duration"/>.</summary>
public sealed class SlowProbe
{
    private int _invocations;
    private int _completed;

    public TimeSpan Duration { get; init; }
    public int Invocations => Volatile.Read(ref _invocations);
    public int Completed => Volatile.Read(ref _completed);

    public async Task Run(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _invocations);
        await Task.Delay(Duration, cancellationToken);
        Interlocked.Increment(ref _completed);
    }
}

public sealed class SlowSubscriber : ISubscriber<SlowEvent>
{
    private readonly SlowProbe _probe;

    public SlowSubscriber(SlowProbe probe) => _probe = probe;

    public Task HandleEventAsync(SlowEvent eventMessage, CancellationToken cancellationToken = default) => _probe.Run(cancellationToken);
}

/// <summary>
/// The real runtime with a short lock span: the hosted service keeps renewing the lock of a message whose handler
/// outlasts the span, so a competing poller (another instance) never claims it.
/// </summary>
public class LockRenewalHostedServiceTests : IClassFixture<SmokeDatabaseFixture>
{
    private static readonly TimeSpan Span = TimeSpan.FromSeconds(2);
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public LockRenewalHostedServiceTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private static async Task WaitUntil(Func<bool> condition, string what, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Timed out waiting for: " + what);
            await Task.Delay(50);
        }
    }

    [SqlServerTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SlowHandler_OutlastingTheLockSeveralTimes_IsNotClaimedByACompetingPollerAndRunsOnce(bool immediatePath)
    {
        var probe = new SlowProbe { Duration = Span * 3 };
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RealQueue = true }, services =>
        {
            services.AddSingleton(probe);
            services.Configure<OutboxConfiguration>(o => o.MessageLockExpirationTimeSpan = Span);
        });
        if (!immediatePath)
        {
            StoreSeed.Row(host.Database, host.OutboxTable, null, null, "{\"text\":\"from the poller\"}");
            host.Database.Execute($"UPDATE {host.OutboxTable} SET [Type] = @type", ("@type", typeof(SlowEvent).AssemblyQualifiedName));
        }

        var hosted = host.Services.GetServices<IHostedService>().ToList();
        foreach (var service in hosted) await service.StartAsync(CancellationToken.None);
        var competitorClaims = new List<int>();
        try
        {
            if (immediatePath)
            {
                await using var save = host.CreateScope();
                save.Db.Outbox.AddEvent(new SlowEvent { Text = "immediate" });
                await save.Db.SaveChangesAsync();
            }

            await WaitUntil(() => probe.Invocations == 1, "handler started", TimeSpan.FromSeconds(20));

            // Another instance's poller: claims every row whose lock has expired, a few times per second.
            var watch = Stopwatch.StartNew();
            while (probe.Completed == 0 || host.OutboxRows().Count > 0)
            {
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(30), "The handler did not finish and delete the row in time.");
                await using (var competitor = host.CreateScope())
                {
                    var claimed = await competitor.Services.GetRequiredService<IOutboxStore<int>>().GetAllAsync(101, DateTime.UtcNow.AddMinutes(10), 15);
                    competitorClaims.AddRange(claimed.Select(m => m.Id));
                }
                if (competitorClaims.Count > 0) break;
                await Task.Delay(200);
            }
            _output.WriteLine($"Handler ran for {probe.Duration}; competitor polled for {watch.Elapsed}");
            await Task.Delay(500);
        }
        finally
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in hosted) await service.StopAsync(cts.Token);
        }

        Assert.Empty(competitorClaims);
        Assert.Equal(1, probe.Invocations);
        Assert.Equal(1, probe.Completed);
        Assert.Empty(host.OutboxRows());
    }
}
