using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

// Adversarial tests (tester) for lock renewal against SQL Server. Event and subscriber types are unique to this file.

public class TesterHeldEvent
{
    public int Key { get; set; }
}

/// <summary>Counts handler invocations per key (and per host); handlers wait until <see cref="ReleaseAll"/> when held.</summary>
public sealed class TesterHeldGate
{
    private readonly ConcurrentDictionary<int, int> _invocations = new();
    private readonly ConcurrentDictionary<string, int> _byHost = new();
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _inFlight;

    public bool Held { get; init; }
    public TimeSpan Duration { get; init; }
    public int InFlight => Volatile.Read(ref _inFlight);
    public int Invocations(int key) => _invocations.TryGetValue(key, out var c) ? c : 0;
    public IReadOnlyDictionary<int, int> AllInvocations => _invocations.ToDictionary(x => x.Key, x => x.Value);
    public IReadOnlyDictionary<string, int> ByHost => _byHost.ToDictionary(x => x.Key, x => x.Value);

    public void ReleaseAll() => _release.TrySetResult();

    public async Task Run(int key, string host, CancellationToken cancellationToken)
    {
        _invocations.AddOrUpdate(key, 1, (_, c) => c + 1);
        _byHost.AddOrUpdate(host, 1, (_, c) => c + 1);
        Interlocked.Increment(ref _inFlight);
        try
        {
            if (Held) await _release.Task.WaitAsync(cancellationToken);
            if (Duration > TimeSpan.Zero) await Task.Delay(Duration, cancellationToken);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }
}

/// <summary>Names the host a subscriber runs in (one per SmokeHost).</summary>
public sealed record TesterHostName(string Name);

public sealed class TesterHeldSubscriber(TesterHeldGate gate, TesterHostName host) : ISubscriber<TesterHeldEvent>
{
    public Task HandleEventAsync(TesterHeldEvent eventMessage, CancellationToken cancellationToken = default) => gate.Run(eventMessage.Key, host.Name, cancellationToken);
}

public sealed class TesterLogCapture : ILoggerProvider
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

    public IReadOnlyList<(LogLevel Level, string Message)> Entries => _entries.ToList();

    public ILogger CreateLogger(string categoryName) => new Logger(this);

    public void Dispose()
    {
    }

    private sealed class Logger(TesterLogCapture owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel)) owner._entries.Enqueue((logLevel, formatter(state, exception)));
        }
    }
}

internal static class TesterRenewal
{
    public static SmokeHost<int, SmokeDbContext> RealHost(SmokeDatabaseFixture fixture, TimeSpan span, TesterHeldGate gate, string name, TesterLogCapture? logs = null)
        => SmokeHost<int, SmokeDbContext>.Create(fixture, new SmokeHostOptions { RealQueue = true }, services =>
        {
            services.AddSingleton(gate);
            services.AddSingleton(new TesterHostName(name));
            if (logs != null) services.AddSingleton<ILoggerProvider>(logs);
            services.Configure<OutboxConfiguration>(o => o.MessageLockExpirationTimeSpan = span);
        });

    /// <summary>Inserts <paramref name="count"/> claimable rows of <see cref="TesterHeldEvent"/> with keys 1..count.</summary>
    public static void SeedHeldRows(SmokeDatabase database, string table, int count)
        => database.Execute(
            $$"""
            INSERT INTO {{table}} ([Type], [Payload], [Created], [LockExpiresAt])
            SELECT TOP (@count) @type, CONCAT(N'{"key":', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), N'}'), SYSUTCDATETIME(), NULL
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
            """,
            ("@count", count), ("@type", typeof(TesterHeldEvent).AssemblyQualifiedName));

    public static void SeedPlainRows(SmokeDatabase database, string table, int count)
        => database.Execute(
            $"INSERT INTO {table} ([Type], [Payload], [Created], [LockExpiresAt]) SELECT TOP (@count) @type, N'{{}}', SYSUTCDATETIME(), NULL FROM sys.all_objects a CROSS JOIN sys.all_objects b",
            ("@count", count), ("@type", typeof(ProbeEvent).AssemblyQualifiedName));

    public static async Task WaitUntil(Func<bool> condition, string what, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Timed out waiting for: " + what);
            await Task.Delay(50);
        }
    }

    public static bool IsRenewal(string sql) => sql.Contains("INNER JOIN (VALUES", StringComparison.Ordinal);

    /// <summary>
    /// Two application instances (two containers, each with its own hosted service and MemoryMessageQueue) share one
    /// database, plus a third poller that only claims rows whose lock expired and never dispatches. Handlers take longer than the
    /// lock span per batch, so queued rows survive only through renewal. Returns the competitor's claims.
    /// </summary>
    public static async Task TwoHostsWithACompetitor_EachRowIsDispatchedExactlyOnce(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        const int rows = 400;
        var span = TimeSpan.FromSeconds(3);
        var gate = new TesterHeldGate { Duration = TimeSpan.FromMilliseconds(400) };
        using var a = RealHost(fixture, span, gate, "A");
        using var b = RealHost(fixture, span, gate, "B");
        SeedHeldRows(a.Database, a.OutboxTable, rows);

        var hosted = a.Services.GetServices<IHostedService>().Concat(b.Services.GetServices<IHostedService>()).ToList();
        var competitorClaims = new List<int>();
        var watch = Stopwatch.StartNew();
        foreach (var service in hosted) await service.StartAsync(CancellationToken.None);
        try
        {
            while (a.Database.Count(a.OutboxTable) > 0)
            {
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(60), $"Rows left after 60 s: {a.Database.Count(a.OutboxTable)}; competitor claimed {competitorClaims.Count}.");
                // Claims only rows some instance claimed before and whose lock has expired (a row never claimed yet
                // is fair game for the hosts and would only make the test measure who polls first).
                competitorClaims.AddRange(a.Database.Query(
                    $"UPDATE TOP (101) t SET [LockExpiresAt] = DATEADD(minute, 10, SYSUTCDATETIME()) OUTPUT INSERTED.[Id] FROM {a.OutboxTable} AS t WITH (ROWLOCK, READPAST, UPDLOCK) WHERE t.[LockExpiresAt] IS NOT NULL AND t.[LockExpiresAt] < SYSUTCDATETIME()",
                    r => r.GetInt32(0)));
                if (competitorClaims.Count > 0) break;
                await Task.Delay(250);
            }
            await Task.Delay(500);
        }
        finally
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in hosted) await service.StopAsync(cts.Token);
        }

        output.WriteLine($"Drained in {watch.Elapsed}; by host: {string.Join(", ", gate.ByHost.Select(x => x.Key + "=" + x.Value))}; competitor claimed {competitorClaims.Count}");
        Assert.Empty(competitorClaims);
        Assert.Empty(a.OutboxRows());
        var invocations = gate.AllInvocations;
        Assert.Equal(rows, invocations.Count);
        Assert.All(Enumerable.Range(1, rows), key => Assert.Equal(1, invocations[key]));
        Assert.Equal(2, gate.ByHost.Count); // both instances dispatched
    }

    /// <summary>A renewal and a claim race for a row whose lock has expired: exactly one of them gets it.</summary>
    public static async Task<(int Renewal, int Claim)> RaceRenewalAndClaim(SmokeHost<int, SmokeDbContext> host, int rounds)
    {
        int renewalWins = 0, claimWins = 0;
        for (var round = 0; round < rounds; round++)
        {
            host.Database.Execute($"DELETE FROM {host.OutboxTable}");
            var id = StoreSeed.Row(host, null, null);
            await using var owner = host.CreateScope();
            var ownerStore = owner.Services.GetRequiredService<IOutboxStore<int>>();
            var owned = (await ownerStore.GetAllAsync(1, DateTime.UtcNow.AddSeconds(-30), 15)).Single();
            await using var claimerScope = host.CreateScope();
            var claimerStore = claimerScope.Services.GetRequiredService<IOutboxStore<int>>();
            await claimerScope.Db.Database.OpenConnectionAsync();
            await owner.Db.Database.OpenConnectionAsync();
            var start = new ManualResetEventSlim();

            var renewal = Task.Run(async () =>
            {
                start.Wait();
                return await ownerStore.RenewLocksAsync(new Dictionary<int, DateTime> { [id] = owned.LockExpiresAt!.Value }, DateTime.UtcNow.AddMinutes(10));
            });
            var claim = Task.Run(async () =>
            {
                start.Wait();
                return (await claimerStore.GetAllAsync(1, DateTime.UtcNow.AddMinutes(20), 15)).ToList();
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
        return (renewalWins, claimWins);
    }
}

/// <summary>RenewLocksAsync edge cases: chunk boundaries, NULL locks, a failure in a later chunk.</summary>
public class TesterLockRenewalStoreTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterLockRenewalStoreTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private static IOutboxStore<int> Store(SmokeScope<SmokeDbContext> scope) => scope.Services.GetRequiredService<IOutboxStore<int>>();

    [SqlServerTheory]
    [InlineData(999, 1)]
    [InlineData(1000, 1)]
    [InlineData(1001, 2)]
    [InlineData(2000, 2)]
    [InlineData(2001, 3)]
    public async Task RenewLocksAsync_AtChunkBoundaries_RenewsEveryRowWithOneCommandPerThousandIds(int count, int commands)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        TesterRenewal.SeedPlainRows(host.Database, host.OutboxTable, count);
        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(null, DateTime.UtcNow.AddMinutes(5), null)).ToList();
        Assert.Equal(count, claimed.Count);
        var newLock = new DateTime(2099, 1, 2, 3, 4, 5).AddTicks(4321);
        host.Faults.ClearLog();

        var renewed = await Store(scope).RenewLocksAsync(claimed.ToDictionary(m => m.Id, m => m.LockExpiresAt!.Value), newLock);

        Assert.Equal(count, renewed.Count);
        Assert.All(host.OutboxRows(), r => Assert.Equal(newLock, r.LockExpiresAt));
        Assert.Equal(commands, host.Faults.Commands.Count(c => TesterRenewal.IsRenewal(c.Sql)));
        Assert.All(host.Faults.Commands, c => Assert.True(c.Parameters.Count <= 2001, $"{c.Parameters.Count} parameters"));
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_RowWhoseLockIsNull_IsNotRenewedAndStaysNull()
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        var id = StoreSeed.Row(host, null, null);
        await using var scope = host.CreateScope();

        var renewed = await Store(scope).RenewLocksAsync(new Dictionary<int, DateTime> { [id] = DateTime.UtcNow }, DateTime.UtcNow.AddMinutes(5));

        Assert.Empty(renewed);
        Assert.Null(Assert.Single(host.OutboxRows()).LockExpiresAt);
    }

    /// <summary>
    /// Characterization: the chunks are separate autocommit statements. When a later chunk fails, the method throws,
    /// but the rows of the earlier chunk are already renewed - and the caller is never told their new values.
    /// </summary>
    [SqlServerFact]
    public async Task RenewLocksAsync_SecondChunkFails_ThrowsAlthoughTheFirstChunkIsAlreadyRenewed()
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        TesterRenewal.SeedPlainRows(host.Database, host.OutboxTable, 1500);
        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(null, DateTime.UtcNow.AddMinutes(5), null)).ToList();
        var newLock = new DateTime(2099, 1, 2, 3, 4, 5);
        var seen = 0;
        host.Faults.FailCommands(sql => TesterRenewal.IsRenewal(sql) && Interlocked.Increment(ref seen) == 2, FaultTiming.BeforeExecution);

        await Assert.ThrowsAsync<InjectedTransientException>(() => Store(scope).RenewLocksAsync(claimed.ToDictionary(m => m.Id, m => m.LockExpiresAt!.Value), newLock));

        var renewedRows = host.OutboxRows().Count(r => r.LockExpiresAt == newLock);
        _output.WriteLine($"Rows renewed before the failure: {renewedRows}");
        Assert.Equal(1000, renewedRows);
    }
}

/// <summary>A failed later renewal chunk makes the next tick treat the already-renewed rows as lost.</summary>
public class TesterRenewalPartialFailureTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterRenewalPartialFailureTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>
    /// Regression (was a defect repro). 1100 messages are tracked (10 in flight, the rest queued). One renewal tick
    /// fails on its second chunk: the first 1000 rows were renewed in the database, but the hosted service never
    /// records their new locks. The next tick used to send only the old expected values, miss, drop ~990 queued copies
    /// and stop renewing the in-flight ones - although no other claimer touched any row. Now a miss is retried with the
    /// failed call's attempted lock before anything is revoked.
    /// </summary>
    [SqlServerFact]
    public async Task RenewalTick_FailingOnItsSecondChunk_DoesNotDropQueuedMessagesOnTheNextTick()
    {
        const int rows = 1100;
        var span = TimeSpan.FromSeconds(3);
        var gate = new TesterHeldGate { Held = true };
        var logs = new TesterLogCapture();
        using var host = TesterRenewal.RealHost(_fixture, span, gate, "A", logs);
        TesterRenewal.SeedHeldRows(host.Database, host.OutboxTable, rows);
        var queue = host.Services.GetRequiredService<IMessageQueue<int>>();
        await using (var claimer = host.CreateScope())
        {
            var claimed = await claimer.Services.GetRequiredService<IOutboxStore<int>>().GetAllAsync(null, DateTime.UtcNow + span, 15);
            foreach (var message in claimed) await queue.EnqueueAsync(message);
        }
        var seen = 0;
        host.Faults.FailCommands(sql => TesterRenewal.IsRenewal(sql) && Interlocked.Increment(ref seen) == 2, FaultTiming.BeforeExecution);

        var hosted = host.Services.GetServices<IHostedService>().ToList();
        foreach (var service in hosted) await service.StartAsync(CancellationToken.None);
        try
        {
            await TesterRenewal.WaitUntil(() => host.Faults.CommandFailuresFired == 1, "the injected renewal failure", TimeSpan.FromSeconds(10));
            await Task.Delay(span); // two or three more ticks
            var dropped = logs.Entries.Count(e => e.Message.Contains("lost its lock while queued"));
            var inFlightLost = logs.Entries.Count(e => e.Message.Contains("lost its lock while its handler is running"));
            _output.WriteLine($"Dropped queued copies: {dropped}; in-flight lost: {inFlightLost}");

            gate.ReleaseAll();
            await TesterRenewal.WaitUntil(() => host.OutboxRows().Count == 0, "all rows handled", TimeSpan.FromSeconds(60));
            Assert.Equal(0, dropped);
            Assert.Equal(0, inFlightLost);
        }
        finally
        {
            gate.ReleaseAll();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in hosted) await service.StopAsync(cts.Token);
        }
    }
}

/// <summary>Immediate dispatch (CLR payload) and a poller copy (ExpandoObject) of the same row in one process.</summary>
public class TesterImmediateAndPollerCopyTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterImmediateAndPollerCopyTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>
    /// The immediate copy is in flight; its lock is lost (simulated renewal outage: the lock is moved into the past)
    /// and this process claims the row again, enqueuing the poller copy with a newer lock. The handler runs once, the
    /// process owns the row again (renewal resumes from the claimed lock) and the row is deleted once.
    /// </summary>
    [SqlServerFact]
    public async Task InFlightImmediateCopy_ReclaimedByThisProcess_IsHandledOnceAndRenewalResumesFromTheClaimedLock()
    {
        var span = TimeSpan.FromSeconds(2);
        var gate = new TesterHeldGate { Held = true };
        var logs = new TesterLogCapture();
        using var host = TesterRenewal.RealHost(_fixture, span, gate, "A", logs);
        var hosted = host.Services.GetServices<IHostedService>().ToList();
        foreach (var service in hosted) await service.StartAsync(CancellationToken.None);
        try
        {
            // The hosted poller's start-up claim must be over before the lock is moved into the past below, or it
            // claims the row itself (also a valid poller copy, but then this test's own claim finds nothing).
            await TesterRenewal.WaitUntil(() => host.Faults.Commands.Any(c => c.Sql.Contains("READPAST")), "the start-up claim", TimeSpan.FromSeconds(20));
            await Task.Delay(300);
            await using (var save = host.CreateScope())
            {
                save.Db.Outbox.AddEvent(new TesterHeldEvent { Key = 7 });
                await save.Db.SaveChangesAsync();
            }
            await TesterRenewal.WaitUntil(() => gate.InFlight == 1, "immediate copy in flight", TimeSpan.FromSeconds(10));

            host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = DATEADD(minute, -1, SYSUTCDATETIME())");
            OutboxMessage<int> pollerCopy;
            await using (var claimer = host.CreateScope())
            {
                pollerCopy = (await claimer.Services.GetRequiredService<IOutboxStore<int>>().GetAllAsync(101, DateTime.UtcNow + span, 15)).Single();
            }
            Assert.IsType<System.Dynamic.ExpandoObject>(pollerCopy.Payload);
            await host.Services.GetRequiredService<IMessageQueue<int>>().EnqueueAsync(pollerCopy);

            // Renewal keeps extending the re-claimed lock.
            await TesterRenewal.WaitUntil(() => Assert.Single(host.OutboxRows()).LockExpiresAt > pollerCopy.LockExpiresAt!.Value.AddMilliseconds(100), "renewal of the claimed lock", TimeSpan.FromSeconds(5));
            await Task.Delay(span + TimeSpan.FromSeconds(1));
            Assert.True(Assert.Single(host.OutboxRows()).LockExpiresAt > DateTime.UtcNow, "the row lock is still held");

            gate.ReleaseAll();
            await TesterRenewal.WaitUntil(() => host.OutboxRows().Count == 0, "row deleted", TimeSpan.FromSeconds(10));
            await Task.Delay(500);
        }
        finally
        {
            gate.ReleaseAll();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in hosted) await service.StopAsync(cts.Token);
        }

        _output.WriteLine(string.Join(Environment.NewLine, logs.Entries.Where(e => e.Level >= LogLevel.Warning).Select(e => e.Message)));
        Assert.Equal(1, gate.Invocations(7));
        Assert.DoesNotContain(logs.Entries, e => e.Message.Contains("lost its lock while queued"));
        Assert.DoesNotContain(logs.Entries, e => e.Message.Contains("Handler failed"));
    }
}

/// <summary>Two application instances sharing one database (default isolation).</summary>
public class TesterTwoHostsTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterTwoHostsTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [SqlServerFact]
    public Task TwoHostsAndACompetingPoller_SlowSubscriberAndShortSpan_EachRowIsDispatchedExactlyOnce()
        => TesterRenewal.TwoHostsWithACompetitor_EachRowIsDispatchedExactlyOnce(_fixture, _output);
}

/// <summary>The same scenarios on a database with READ_COMMITTED_SNAPSHOT ON (this class's throwaway database only).</summary>
public class TesterReadCommittedSnapshotTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterReadCommittedSnapshotTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        EnableRcsi(fixture.Database);
    }

    private static void EnableRcsi(SmokeDatabase database)
    {
        if (!SqlServerEnvironment.IsOwnDatabase(database.Name)) throw new InvalidOperationException("Not a throwaway database: " + database.Name);
        if (Convert.ToBoolean(database.Scalar("SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME()"))) return;
        SqlConnection.ClearAllPools();
        using var connection = new SqlConnection(SqlServerEnvironment.MasterConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandTimeout = 120;
        command.CommandText = $"ALTER DATABASE [{database.Name}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE";
        command.ExecuteNonQuery();
    }

    [SqlServerFact]
    public async Task Rcsi_RenewalRacingAClaimOnAnExpiredRow_ExactlyOneWins()
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        Assert.True(Convert.ToBoolean(host.Database.Scalar("SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME()")));

        var (renewal, claim) = await TesterRenewal.RaceRenewalAndClaim(host, 40);

        _output.WriteLine($"Renewal won {renewal}, claim won {claim}");
    }

    [SqlServerFact]
    public Task Rcsi_TwoHostsAndACompetingPoller_EachRowIsDispatchedExactlyOnce()
        => TesterRenewal.TwoHostsWithACompetitor_EachRowIsDispatchedExactlyOnce(_fixture, _output);
}

/// <summary>Known limitation, characterized: OUTPUT without INTO fails on a table with an enabled trigger.</summary>
public class TesterTriggerTableTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public TesterTriggerTableTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    [SqlServerFact]
    public async Task RenewLocksAsync_OnATableWithAnAfterUpdateTrigger_FailsLikeTheClaimWithError334()
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        var id = StoreSeed.Row(host, DateTime.UtcNow.AddMinutes(-1), null);
        host.Database.Execute($"CREATE OR ALTER TRIGGER [dbo].[TesterOutboxTrigger] ON {host.OutboxTable} AFTER UPDATE AS BEGIN SET NOCOUNT ON; END");
        try
        {
            await using var scope = host.CreateScope();
            var store = scope.Services.GetRequiredService<IOutboxStore<int>>();

            var claim = await Assert.ThrowsAsync<SqlException>(() => store.GetAllAsync(1, DateTime.UtcNow.AddMinutes(5), 15));
            var renewal = await Assert.ThrowsAsync<SqlException>(() => store.RenewLocksAsync(new Dictionary<int, DateTime> { [id] = DateTime.UtcNow }, DateTime.UtcNow.AddMinutes(5)));

            Assert.Equal(334, claim.Number);
            Assert.Equal(334, renewal.Number);
        }
        finally
        {
            host.Database.Execute("DROP TRIGGER IF EXISTS [dbo].[TesterOutboxTrigger]");
        }
    }
}
