using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// A renewal statement that ran and committed (autocommit) but whose call then failed, through the real hosted
/// service, MemoryMessageQueue and EF store. Without a retrying strategy the call throws; with one, EF re-runs the
/// statement with the old expected locks, which now miss, and the call returns normally without those ids.
/// </summary>
public class RenewalFaultAfterExecutionTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public RenewalFaultAfterExecutionTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RenewalTick_FaultAfterTheStatementCommitted_NextTicksKeepEveryMessageAndEachIsDispatchedOnce(bool retryingStrategy)
    {
        const int rows = 30; // 10 in flight, 20 queued
        var span = TimeSpan.FromSeconds(3);
        var gate = new TesterHeldGate { Held = true };
        var logs = new TesterLogCapture();
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RealQueue = true, RetryingStrategy = retryingStrategy }, services =>
        {
            services.AddSingleton(gate);
            services.AddSingleton(new TesterHostName("A"));
            services.AddSingleton<ILoggerProvider>(logs);
            services.Configure<OutboxConfiguration>(o => o.MessageLockExpirationTimeSpan = span);
        });
        TesterRenewal.SeedHeldRows(host.Database, host.OutboxTable, rows);
        var queue = host.Services.GetRequiredService<IMessageQueue<int>>();
        await using (var claimer = host.CreateScope())
        {
            var claimed = await claimer.Services.GetRequiredService<IOutboxStore<int>>().GetAllAsync(null, DateTime.UtcNow + span, 15);
            foreach (var message in claimed) await queue.EnqueueAsync(message);
        }
        host.Faults.FailCommands(TesterRenewal.IsRenewal, FaultTiming.AfterExecution);

        var hosted = host.Services.GetServices<IHostedService>().ToList();
        foreach (var service in hosted) await service.StartAsync(CancellationToken.None);
        try
        {
            await TesterRenewal.WaitUntil(() => host.Faults.CommandFailuresFired == 1, "the injected renewal failure", TimeSpan.FromSeconds(10));
            // Longer than the lock span: without renewal every lock would have expired.
            await Task.Delay(span + TimeSpan.FromSeconds(1));

            var dropped = logs.Entries.Count(e => e.Message.Contains("lost its lock while queued"));
            var inFlightLost = logs.Entries.Count(e => e.Message.Contains("lost its lock while its handler is running"));
            var failedTicks = logs.Entries.Count(e => e.Message.Contains("Failed to renew"));
            var renewals = host.Faults.Commands.Where(c => TesterRenewal.IsRenewal(c.Sql)).ToList();
            _output.WriteLine($"Dropped queued copies: {dropped}; in-flight lost: {inFlightLost}; failed ticks: {failedTicks}; renewal commands: {renewals.Count}");
            Assert.Equal(0, dropped);
            Assert.Equal(0, inFlightLost);
            Assert.Equal(10, gate.InFlight);
            Assert.All(host.OutboxRows(), r => Assert.True(r.LockExpiresAt > DateTime.UtcNow, $"Row {r.Id} lock {r.LockExpiresAt:O} has expired."));

            if (retryingStrategy)
            {
                // EF re-ran the faulted statement with the same values, so the call itself did not fail.
                Assert.Equal(0, failedTicks);
                Assert.Equal(renewals[0].Parameters.Select(p => p.Value), renewals[1].Parameters.Select(p => p.Value));
            }
            else
            {
                Assert.Equal(1, failedTicks);
            }

            gate.ReleaseAll();
            await TesterRenewal.WaitUntil(() => host.OutboxRows().Count == 0, "all rows handled", TimeSpan.FromSeconds(30));
            await Task.Delay(300);
        }
        finally
        {
            gate.ReleaseAll();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in hosted) await service.StopAsync(cts.Token);
        }

        var invocations = gate.AllInvocations;
        Assert.Equal(rows, invocations.Count);
        Assert.All(Enumerable.Range(1, rows), key => Assert.Equal(1, invocations[key]));
        Assert.DoesNotContain(logs.Entries, e => e.Message.Contains("lost its lock"));
    }
}
