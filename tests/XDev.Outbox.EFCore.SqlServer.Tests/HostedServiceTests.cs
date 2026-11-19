using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

public class FailingEvent
{
    public string? Text { get; set; }
}

/// <summary>Throws synchronously (before returning a Task), like any non-async handler that validates its input.</summary>
public sealed class FailingSubscriber : ISubscriber<FailingEvent>
{
    public Task HandleEventAsync(FailingEvent eventMessage, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("subscriber failed on purpose");
}

public class AsyncFailingEvent
{
    public string? Text { get; set; }
}

/// <summary>Throws from an async method: the exception is carried by the returned Task.</summary>
public sealed class AsyncFailingSubscriber : ISubscriber<AsyncFailingEvent>
{
    public async Task HandleEventAsync(AsyncFailingEvent eventMessage, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        throw new InvalidOperationException("subscriber failed on purpose");
    }
}

/// <summary>
/// The real runtime on SQL Server: OutboxProcessorHostedService + MemoryMessageQueue + EventPublisher +
/// EntityFrameworkOutboxStore. Only the immediate-dispatch path and the poller's first poll (at start) are used; the
/// 30-minute poll interval is never waited for.
/// </summary>
public class HostedServiceTests : IClassFixture<SmokeDatabaseFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private readonly SmokeDatabaseFixture _fixture;

    public HostedServiceTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    private SmokeHost<int, SmokeDbContext> CreateHost() => SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RealQueue = true });

    private static async Task<List<IHostedService>> Start(SmokeHost<int, SmokeDbContext> host)
    {
        var services = host.Services.GetServices<IHostedService>().ToList();
        Assert.Single(services, s => s is OutboxProcessorHostedService<int>);
        foreach (var service in services) await service.StartAsync(CancellationToken.None);
        return services;
    }

    private static async Task Stop(List<IHostedService> services)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        foreach (var service in services) await service.StopAsync(cts.Token);
    }

    private static async Task WaitUntil(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Timed out waiting for: " + what);
            await Task.Delay(50);
        }
    }

    [SqlServerFact]
    public async Task HostedService_ImmediateDispatchAfterOwnedSave_InvokesSubscriberOnceAndDeletesTheRow()
    {
        using var host = CreateHost();
        var probe = host.Services.GetRequiredService<SubscriberProbe>();
        var services = await Start(host);
        try
        {
            await using (var scope = host.CreateScope())
            {
                scope.Db.Outbox.AddEvent(new ProbeEvent { Text = "immediate 😀", Number = 7 });
                await scope.Db.SaveChangesAsync();
            }

            await WaitUntil(() => probe.Received.Count >= 1 && host.OutboxRows().Count == 0, "subscriber invoked and row deleted");
            await Task.Delay(300);

            var received = Assert.Single(probe.Received);
            Assert.Equal("immediate 😀", received.Text);
            Assert.Equal(7, received.Number);
            Assert.Empty(host.OutboxRows());
        }
        finally
        {
            await Stop(services);
        }
    }

    [SqlServerFact]
    public async Task HostedService_PendingRowAtStartup_IsClaimedByThePollerDispatchedOnceAndDeleted()
    {
        using var host = CreateHost();
        host.Database.Execute(
            $"INSERT INTO {host.OutboxTable} ([Type], [Payload], [Created], [LockExpiresAt]) VALUES (@type, @payload, @created, NULL)",
            ("@type", typeof(ProbeEvent).AssemblyQualifiedName),
            ("@payload", "{\"text\":\"from the poller\",\"number\":42}"),
            ("@created", DateTime.UtcNow));
        var probe = host.Services.GetRequiredService<SubscriberProbe>();

        var services = await Start(host);
        try
        {
            await WaitUntil(() => probe.Received.Count >= 1 && host.OutboxRows().Count == 0, "poller dispatch and row deleted");
            await Task.Delay(300);

            var received = Assert.Single(probe.Received);
            Assert.Equal("from the poller", received.Text);
            Assert.Equal(42, received.Number);
        }
        finally
        {
            await Stop(services);
        }
    }

    [SqlServerFact]
    public Task HostedService_AsyncSubscriberThrows_RowKeepsItsLockAndRecordsTheFailedAttemptWithTheSubscribersMessage()
        => AssertFailedAttemptRecorded(new AsyncFailingEvent { Text = "boom" });

    [SqlServerFact]
    public Task HostedService_SyncSubscriberThrows_RowKeepsItsLockAndRecordsTheFailedAttemptWithTheSubscribersMessage()
        => AssertFailedAttemptRecorded(new FailingEvent { Text = "boom" });

    private async Task AssertFailedAttemptRecorded(object @event)
    {
        using var host = CreateHost();
        var services = await Start(host);
        try
        {
            await using (var scope = host.CreateScope())
            {
                scope.Db.Outbox.AddEvent(@event);
                await scope.Db.SaveChangesAsync();
            }
            var lockAfterSave = Assert.Single(host.OutboxRows()).LockExpiresAt;

            await WaitUntil(() => host.OutboxRows().SingleOrDefault()?.Attemps == 1, "failed attempt recorded");

            var row = Assert.Single(host.OutboxRows());
            Assert.True(row.IsError);
            Assert.Equal(lockAfterSave, row.LockExpiresAt);
            Assert.Contains("boom", row.Payload);
            Assert.Equal("subscriber failed on purpose", row.ErrorMessage);
        }
        finally
        {
            await Stop(services);
        }
    }
}
