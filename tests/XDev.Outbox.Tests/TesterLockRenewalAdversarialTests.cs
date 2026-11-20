using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace XDev.Outbox.Tests;

// Adversarial tests (tester) for in-process dedup, poller backpressure and lock renewal.
// Event and subscriber types are unique to this file (AddOutbox scans the whole test assembly).

public sealed class PacedRenewalEvent
{
    public int Key { get; set; }
}

/// <summary>Like <see cref="RenewalTestSubscriber"/>, but each handler first waits a timer tick, so dequeues are spread over many renewal ticks.</summary>
public sealed class PacedRenewalSubscriber(RenewalGate gate) : ISubscriber<PacedRenewalEvent>
{
    public async Task HandleEventAsync(PacedRenewalEvent eventMessage, CancellationToken cancellationToken = default)
    {
        await Task.Delay(1, cancellationToken);
        await gate.Run(eventMessage.Key, cancellationToken);
    }
}

/// <summary>A store whose renewal is asynchronous and observes cancellation; claims return nothing.</summary>
public sealed class BlockingRenewStore : IOutboxStore<int>
{
    private int _renewCalls;
    private int _renewCanceled;

    public TaskCompletionSource RenewEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public int RenewCalls => Volatile.Read(ref _renewCalls);
    public int RenewCanceled => Volatile.Read(ref _renewCanceled);

    public Task<IEnumerable<OutboxMessage<int>>> GetAllAsync(int? itemsCount = null, DateTime? lockExpiration = null, int? maxAttemps = null, CancellationToken cancellationToken = default)
        => Task.FromResult<IEnumerable<OutboxMessage<int>>>(Array.Empty<OutboxMessage<int>>());

    public Task RemoveAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task UpdateAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default) => Task.CompletedTask;

    public async Task<IReadOnlyDictionary<int, DateTime>> RenewLocksAsync(IReadOnlyDictionary<int, DateTime> expectedLocks, DateTime lockExpiration, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _renewCalls);
        RenewEntered.TrySetResult();
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            Interlocked.Increment(ref _renewCanceled);
            throw;
        }
        return new Dictionary<int, DateTime>();
    }
}

/// <summary>A TimeProvider frozen at a fixed instant.</summary>
public sealed class FrozenTimeProvider(DateTimeOffset now) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => now;
}

/// <summary>The real hosted service and queue with any store instance and optional TimeProvider.</summary>
public sealed class TesterHost : IAsyncDisposable
{
    private readonly ServiceProvider _provider;

    private TesterHost(ServiceProvider provider)
    {
        _provider = provider;
        Hosted = provider.GetServices<IHostedService>().OfType<OutboxProcessorHostedService<int>>().Single();
        Logs = provider.GetRequiredService<LogSink>();
        Queue = provider.GetRequiredService<IMessageQueue<int>>();
        Gate = provider.GetRequiredService<RenewalGate>();
    }

    public OutboxProcessorHostedService<int> Hosted { get; }
    public LogSink Logs { get; }
    public IMessageQueue<int> Queue { get; }
    public RenewalGate Gate { get; }

    public static async Task<TesterHost> StartAsync(IOutboxStore<int> store, TimeSpan span, TimeProvider? timeProvider = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<LogSink>();
        services.AddSingleton(typeof(ILogger<>), typeof(SinkLogger<>));
        services.AddSingleton(new RenewalGate());
        if (timeProvider != null) services.AddSingleton(timeProvider);
        services.AddScoped(_ => store);
        services.AddOutbox<int>(o => o.MessageLockExpirationTimeSpan = span, typeof(TesterHost).Assembly);
        var host = new TesterHost(services.BuildServiceProvider());
        await host.Hosted.StartAsync(CancellationToken.None);
        return host;
    }

    public Task EnqueueAsync(int id, DateTime? lockExpiresAt) => Queue.EnqueueAsync(RenewalHost.Message(id, lockExpiresAt));

    /// <summary>Stops without releasing held handlers first.</summary>
    public async Task<TimeSpan> StopAsync()
    {
        var watch = Stopwatch.StartNew();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await Hosted.StopAsync(cts.Token);
        return watch.Elapsed;
    }

    public async ValueTask DisposeAsync()
    {
        Gate.ReleaseAll();
        await StopAsync();
        await _provider.DisposeAsync();
    }
}

/// <summary>Collects unobserved task exceptions raised by the library while a test runs.</summary>
public sealed class UnobservedLibraryExceptions : IDisposable
{
    private readonly ConcurrentQueue<Exception> _seen = new();

    public UnobservedLibraryExceptions() => TaskScheduler.UnobservedTaskException += OnUnobserved;

    public IReadOnlyList<Exception> Collect()
    {
        for (var i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        return _seen.ToList();
    }

    private void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        // Other test classes run in parallel (and fault their own queues' receive tasks): only exceptions raised by the
        // hosted service's poll, backpressure or renewal code count.
        var text = e.Exception.ToString();
        if (text.Contains("RunLockRenewalAsync") || text.Contains("WaitUntilHandledAsync") || text.Contains("OutboxProcessorHostedService`1.ExecuteAsync") || text.Contains("OutboxProcessorHostedService`1.RenewLocksAsync"))
        {
            _seen.Enqueue(e.Exception);
        }
    }

    public void Dispose() => TaskScheduler.UnobservedTaskException -= OnUnobserved;
}

public class TesterLockRenewalAdversarialTests(Xunit.Abstractions.ITestOutputHelper output)
{
    private static DateTime Lock(int seconds = 60) => DateTime.UtcNow.AddSeconds(seconds);

    private static readonly Regex DroppedQueued = new(@"Outbox message (\d+) lost its lock while queued", RegexOptions.CultureInvariant);

    /// <summary>
    /// Regression (was a defect repro). A lock span of 200 days gives a renewal interval of about 66.7 days, above
    /// Task.Delay's maximum (about 49.7 days): the renewal loop's first statement threw ArgumentOutOfRangeException,
    /// which was caught, logged as a warning and retried immediately - a hot loop at full CPU that flooded the log,
    /// starting at host start even with nothing tracked. The interval is now clamped to int.MaxValue ms.
    /// </summary>
    [Fact]
    public async Task RenewalLoop_LockSpanWhoseThirdExceedsTheTaskDelayMaximum_DoesNotSpinOrFloodWarnings()
    {
        var store = new FakeLockStore();
        await using var host = await TesterHost.StartAsync(store, TimeSpan.FromDays(200));
        await Task.Delay(500);

        var failures = host.Logs.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("Failed to renew"));
        Assert.True(failures <= 1, $"{failures} 'Failed to renew' warnings in 500 ms with nothing tracked.");
    }

    [Fact]
    public async Task RenewalLoop_LockSpanJustBelowTheTaskDelayLimit_StartsWithoutRenewalWarnings()
    {
        var store = new FakeLockStore();
        await using var host = await TesterHost.StartAsync(store, TimeSpan.FromDays(140));
        await Task.Delay(500);

        Assert.DoesNotContain(host.Logs.Entries, e => e.Level == LogLevel.Warning);
    }

    /// <summary>
    /// Every renewal misses for odd Ids while thousands of messages are dequeued concurrently: each odd message is
    /// either dropped (never handled) or handled once, never both; even messages are always handled exactly once; the
    /// queue neither stalls nor loses later messages.
    /// </summary>
    [Fact]
    public async Task Revoke_RacingDequeueOfThousandsOfMessages_EachMessageRunsOnceOrIsDroppedNeverBoth()
    {
        const int count = 3000;
        var store = new FakeLockStore
        {
            Renew = (expected, lockExpiration) => expected.Keys.Where(id => id % 2 == 0).ToDictionary(id => id, _ => lockExpiration),
        };
        await using var host = await TesterHost.StartAsync(store, TimeSpan.FromMilliseconds(300));
        for (var id = 1; id <= count; id++)
        {
            await host.Queue.EnqueueAsync(new OutboxMessage<int>
            {
                Id = id,
                Type = typeof(PacedRenewalEvent).AssemblyQualifiedName,
                Payload = new PacedRenewalEvent { Key = id },
                LockExpiresAt = Lock(),
            });
        }

        await RenewalHost.WaitUntil(() =>
        {
            var dropped = host.Logs.Entries.Count(e => DroppedQueued.IsMatch(e.Message));
            var handled = Enumerable.Range(1, count).Count(id => host.Gate.Completions(id) > 0);
            return dropped + handled >= count;
        }, "every message handled or dropped", 60000);
        await Task.Delay(300);

        var droppedIds = host.Logs.Entries.Select(e => DroppedQueued.Match(e.Message)).Where(m => m.Success).Select(m => int.Parse(m.Groups[1].Value)).ToList();
        Assert.Equal(droppedIds.Count, droppedIds.Distinct().Count());
        Assert.All(droppedIds, id => Assert.True(id % 2 == 1, $"even id {id} dropped"));
        Assert.All(droppedIds, id => Assert.Equal(0, host.Gate.Invocations(id)));
        Assert.All(Enumerable.Range(1, count), id => Assert.True(host.Gate.Invocations(id) <= 1, $"id {id} handled {host.Gate.Invocations(id)} times"));
        Assert.All(Enumerable.Range(1, count).Where(id => id % 2 == 0), id => Assert.Equal(1, host.Gate.Invocations(id)));
        Assert.Equal(count, droppedIds.Count + Enumerable.Range(1, count).Count(id => host.Gate.Invocations(id) == 1));
        var inFlightRevoked = host.Logs.Entries.Count(e => e.Message.Contains("lost its lock while its handler is running"));
        output.WriteLine($"dropped while queued: {droppedIds.Count}, revoked in flight: {inFlightRevoked}, handled: {Enumerable.Range(1, count).Count(id => host.Gate.Invocations(id) == 1)}");
        Assert.NotEmpty(droppedIds); // the race was exercised

        await host.EnqueueAsync(count + 1, Lock());
        await RenewalHost.WaitUntil(() => host.Gate.Completions(count + 1) == 1, "a later message is still handled");
    }

    /// <summary>
    /// Renewal fails on every tick for longer than the span: the poller claims the same rows again (newer locks) while
    /// they are still queued or in flight. Each is still handled exactly once, and once renewal recovers it renews the
    /// re-claimed (newer) locks, not the stale ones.
    /// </summary>
    [Fact]
    public async Task RenewalFailingLongerThanTheSpan_PollerReclaimsTheSameRows_EachIsHandledOnceAndTheNewerLockIsRenewed()
    {
        var span = TimeSpan.FromSeconds(1.5);
        var firstLock = Lock(60);
        var secondLock = Lock(120);
        var failing = 1;
        var ids = Enumerable.Range(1, 101).ToArray();
        await using var host = await RenewalHost.StartAsync(span, store =>
        {
            store.AddBatch(ids.Select(id => RenewalHost.Message(id, firstLock)));
            store.AddBatch(ids.Select(id => RenewalHost.Message(id, secondLock)).Append(RenewalHost.Message(500, secondLock)));
            store.Renew = (expected, lockExpiration) =>
            {
                if (Volatile.Read(ref failing) == 1) throw new TimeoutException("store down");
                return expected.ToDictionary(x => x.Key, x => x.Value); // renewed, stored value unchanged (keeps assertions simple)
            };
        }, ids);

        await RenewalHost.WaitUntil(() => host.Store.Claims.Count >= 2, "the poller claimed again after the bound", 10000);
        await Task.Delay(300);
        Volatile.Write(ref failing, 0);
        var recovered = DateTime.UtcNow;
        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Any(c => c.At > recovered && c.Result != null), "a successful renewal");

        var renewal = host.Store.RenewCalls.First(c => c.At > recovered && c.Result != null);
        Assert.Equal(ids, renewal.Expected.Keys.Where(id => id <= 101).OrderBy(x => x));
        Assert.All(ids, id => Assert.Equal(secondLock, renewal.Expected[id]));

        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => ids.All(id => host.Gate.Completions(id) == 1) && host.Gate.Completions(500) == 1, "all handled");
        await Task.Delay(300);
        Assert.All(ids.Append(500), id => Assert.Equal(1, host.Gate.Invocations(id)));
        Assert.Contains(host.Logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("Failed to renew"));
    }

    /// <summary>
    /// A queued copy that lost its lock stays revoked when the same Id is enqueued again with the same lock (still not
    /// owned), and is owned and handled once when it is enqueued with a newer lock (this process re-claimed it).
    /// </summary>
    [Fact]
    public async Task RevokedQueuedCopy_EnqueuedAgainWithSameLockStaysDropped_WithANewerLockIsHandledOnce()
    {
        var lost = 1;
        var original = Lock(60);
        var newer = Lock(120);
        await using var host = await RenewalHost.StartAsync(TimeSpan.FromMilliseconds(600), store => store.Renew = (expected, lockExpiration) =>
            expected.Keys.Where(id => !(id == 11 && Volatile.Read(ref lost) == 1)).ToDictionary(id => id, _ => lockExpiration));
        var held = Enumerable.Range(1, 10).ToArray();
        host.Gate.Hold(held);
        foreach (var id in held) await host.EnqueueAsync(id, Lock());
        await RenewalHost.WaitUntil(() => host.Gate.InFlight == 10, "ten in flight");
        await host.EnqueueAsync(11, original);
        await RenewalHost.WaitUntil(() => host.Logs.Entries.Any(e => DroppedQueued.IsMatch(e.Message)), "queued copy revoked");

        Volatile.Write(ref lost, 0);
        await host.EnqueueAsync(11, original); // same lock: still not ours
        var afterSame = DateTime.UtcNow;
        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count(c => c.At > afterSame) >= 2, "renewals after the same-lock enqueue");
        Assert.DoesNotContain(host.Store.RenewCalls.Where(c => c.At > afterSame), c => c.Expected.ContainsKey(11));

        await host.EnqueueAsync(11, newer);
        var afterNewer = DateTime.UtcNow;
        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Any(c => c.At > afterNewer && c.Expected.ContainsKey(11)), "renewal of the re-claimed copy");
        Assert.Equal(newer, host.Store.RenewCalls.First(c => c.At > afterNewer && c.Expected.ContainsKey(11)).Expected[11]);

        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => host.Gate.Completions(11) == 1, "re-claimed copy handled");
        await Task.Delay(300);
        Assert.Equal(1, host.Gate.Invocations(11));
    }

    [Fact]
    public async Task Renewal_StoreReturnsNull_IsLoggedAndRetriedAndNothingIsDropped()
    {
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(TimeSpan.FromMilliseconds(600), store => store.Renew = (expected, lockExpiration) =>
            Interlocked.Increment(ref calls) <= 2 ? null! : expected.Keys.ToDictionary(id => id, _ => lockExpiration));
        host.Gate.Hold(1);
        await host.EnqueueAsync(1, Lock());

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 4, "renewals after the null results");

        Assert.All(host.Store.RenewCalls, c => Assert.Contains(1, c.Expected.Keys));
        Assert.Equal(2, host.Logs.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("Failed to renew")));
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("lost its lock"));
        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => host.Gate.Completions(1) == 1, "handled");
    }

    /// <summary>The renewal lock and the poller's lock use the configured TimeProvider, not the machine clock.</summary>
    [Fact]
    public async Task Renewal_WithAFrozenTimeProvider_RequestsTheProvidersNowPlusSpan()
    {
        var now = new DateTimeOffset(2031, 5, 6, 7, 8, 9, TimeSpan.Zero);
        var span = TimeSpan.FromMilliseconds(600);
        var store = new FakeLockStore();
        await using var host = await TesterHost.StartAsync(store, span, new FrozenTimeProvider(now));
        host.Gate.Hold(1);
        await host.EnqueueAsync(1, Lock());

        await RenewalHost.WaitUntil(() => store.RenewCalls.Count >= 1, "a renewal");

        Assert.Equal(now.DateTime + span, store.RenewCalls[0].LockExpiration);
    }

    /// <summary>Shutdown while the poller waits for a full batch whose handlers never finish on their own.</summary>
    [Fact]
    public async Task Shutdown_DuringBackpressureWaitWithStuckHandlers_StopsPromptlyWithoutUnobservedExceptions()
    {
        using var unobserved = new UnobservedLibraryExceptions();
        var ids = Enumerable.Range(1, 101).ToArray();
        var host = await RenewalHost.StartAsync(TimeSpan.FromSeconds(30), store => store.AddBatch(ids.Select(id => RenewalHost.Message(id, Lock()))), ids);
        await RenewalHost.WaitUntil(() => host.Gate.InFlight == 10, "batch dispatched");
        await Task.Delay(1500); // inside WaitUntilHandledAsync

        var watch = Stopwatch.StartNew();
        // Not RenewalHost.StopAsync: that releases the gates first.
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
        {
            var hosted = typeof(RenewalHost).GetField("_hosted", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(host) as OutboxProcessorHostedService<int>;
            await hosted!.StopAsync(cts.Token);
        }
        watch.Stop();

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), $"Stop took {watch.Elapsed}");
        Assert.True(host.ExecuteTask!.IsCompleted);
        Assert.Single(host.Store.Claims);
        await RenewalHost.WaitUntil(() => host.Gate.InFlight == 0, "held handlers canceled");
        await host.DisposeAsync();
        Assert.Empty(unobserved.Collect());
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("Failed to renew"));
    }

    /// <summary>Shutdown while a renewal call is in progress in the store: the call is canceled and nothing is logged as a failure.</summary>
    [Fact]
    public async Task Shutdown_DuringARenewalCall_CancelsItAndStopsPromptly()
    {
        using var unobserved = new UnobservedLibraryExceptions();
        var store = new BlockingRenewStore();
        var host = await TesterHost.StartAsync(store, TimeSpan.FromMilliseconds(600));
        host.Gate.Hold(1);
        await host.EnqueueAsync(1, Lock());
        await store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var elapsed = await host.StopAsync();

        Assert.True(elapsed < TimeSpan.FromSeconds(3), $"Stop took {elapsed}");
        Assert.True(host.Hosted.ExecuteTask!.IsCompleted);
        Assert.Equal(1, store.RenewCalls);
        Assert.Equal(1, store.RenewCanceled);
        await host.DisposeAsync();
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("Failed to renew"));
        Assert.Empty(unobserved.Collect());
    }
}

/// <summary>MemoryMessageQueue dedup under handler faults, high enqueue rates and concurrent duplicate enqueues.</summary>
public class TesterMemoryMessageQueueAdversarialTests
{
    private static OutboxMessage<int> Message(int id, DateTime? lockExpiresAt = null)
        => new() { Id = id, Type = "t", Payload = new object(), LockExpiresAt = lockExpiresAt ?? DateTime.UtcNow.AddMinutes(5) };

    private static int TrackedCount(MemoryMessageQueue<int> queue)
    {
        var field = typeof(MemoryMessageQueue<int>).GetField("_tracked", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (int)field.FieldType.GetProperty("Count")!.GetValue(field.GetValue(queue))!;
    }

    /// <summary>A handler that throws synchronously (not through a faulted task) still releases the Id.</summary>
    [Theory]
    [InlineData("sync-throw")]
    [InlineData("faulted-task")]
    [InlineData("canceled-task")]
    [InlineData("null-task")]
    public async Task Handler_FailingInAnyWay_ReleasesTheIdSoItCanBeHandledAgain(string mode)
    {
        var queue = new MemoryMessageQueue<int>(NullLogger<MemoryMessageQueue<int>>.Instance);
        var calls = 0;
        using var cts = new CancellationTokenSource();
        queue.MessageReceived += (message, _) =>
        {
            if (Interlocked.Increment(ref calls) > 1) return Task.CompletedTask;
            return mode switch
            {
                "sync-throw" => throw new InvalidOperationException("sync"),
                "faulted-task" => Task.FromException(new InvalidOperationException("async")),
                "canceled-task" => Task.FromCanceled(new CancellationToken(true)),
                _ => null!,
            };
        };
        await queue.StartAsync(cts.Token);
        try
        {
            await queue.EnqueueAsync(Message(1));
            await QueueHarness.WaitUntil(() => Volatile.Read(ref calls) == 1 && TrackedCount(queue) == 0, "first call finished and forgotten");

            await queue.EnqueueAsync(Message(1));
            await QueueHarness.WaitUntil(() => Volatile.Read(ref calls) == 2, "handled again");
            await queue.EnqueueAsync(Message(2));
            await QueueHarness.WaitUntil(() => Volatile.Read(ref calls) == 3, "queue keeps running");
        }
        finally
        {
            cts.Cancel();
        }
    }

    /// <summary>
    /// Eight producers enqueue the same 3000 Ids concurrently while handlers run (a third of them failing): an Id is
    /// never handled by two handlers at the same time, every Id is handled, and the tracking map returns to empty.
    /// </summary>
    [Fact]
    public async Task ConcurrentDuplicateEnqueues_UnderLoadWithFailures_NeverOverlapAndTrackingReturnsToEmpty()
    {
        const int ids = 3000;
        var queue = new MemoryMessageQueue<int>(NullLogger<MemoryMessageQueue<int>>.Instance);
        var running = new ConcurrentDictionary<int, byte>();
        var counts = new ConcurrentDictionary<int, int>();
        var overlaps = 0;
        var total = 0;
        using var cts = new CancellationTokenSource();
        queue.MessageReceived += async (message, ct) =>
        {
            if (!running.TryAdd(message.Id, 0)) Interlocked.Increment(ref overlaps);
            counts.AddOrUpdate(message.Id, 1, (_, c) => c + 1);
            try
            {
                await Task.Yield();
                if (message.Id % 3 == 0) throw new InvalidOperationException("fail");
            }
            finally
            {
                running.TryRemove(message.Id, out _);
                Interlocked.Increment(ref total);
            }
        };
        await queue.StartAsync(cts.Token);
        try
        {
            var producers = Enumerable.Range(0, 8).Select(p => Task.Run(async () =>
            {
                for (var id = 1; id <= ids; id++) await queue.EnqueueAsync(Message(id));
            })).ToArray();
            await Task.WhenAll(producers);

            await QueueHarness.WaitUntil(() => TrackedCount(queue) == 0, "everything handled and forgotten", 60000);

            Assert.Equal(0, Volatile.Read(ref overlaps));
            Assert.All(Enumerable.Range(1, ids), id => Assert.True(counts.TryGetValue(id, out var c) && c >= 1 && c <= 8, $"id {id}: {c}"));

            // Every Id was released: enqueuing each once more handles each exactly once more.
            var before = counts.ToDictionary(x => x.Key, x => x.Value);
            for (var id = 1; id <= ids; id++) await queue.EnqueueAsync(Message(id));
            await QueueHarness.WaitUntil(() => TrackedCount(queue) == 0 && Enumerable.Range(1, ids).All(id => counts[id] == before[id] + 1), "second round", 60000);
        }
        finally
        {
            cts.Cancel();
        }
    }
}
