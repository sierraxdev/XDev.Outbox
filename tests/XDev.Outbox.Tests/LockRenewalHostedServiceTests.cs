using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace XDev.Outbox.Tests;

// Event and subscriber types are unique to this file (AddOutbox scans the whole test assembly).

public sealed class RenewalTestEvent
{
    public int Key { get; set; }
}

/// <summary>Counts handler invocations per message Id and can hold a handler until the test releases it.</summary>
public sealed class RenewalGate
{
    private readonly ConcurrentDictionary<int, int> _invocations = new();
    private readonly ConcurrentDictionary<int, int> _completions = new();
    private readonly ConcurrentDictionary<int, TaskCompletionSource> _gates = new();
    private int _inFlight;

    public int InFlight => Volatile.Read(ref _inFlight);
    public int Invocations(int id) => _invocations.TryGetValue(id, out var c) ? c : 0;
    public int Completions(int id) => _completions.TryGetValue(id, out var c) ? c : 0;
    public int TotalCompletions => _completions.Values.Sum();

    public void Hold(params int[] ids)
    {
        foreach (var id in ids) _gates[id] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void ReleaseAll()
    {
        foreach (var id in _gates.Keys.ToList())
        {
            if (_gates.TryRemove(id, out var gate)) gate.TrySetResult();
        }
    }

    public async Task Run(int id, CancellationToken cancellationToken)
    {
        _invocations.AddOrUpdate(id, 1, (_, c) => c + 1);
        Interlocked.Increment(ref _inFlight);
        try
        {
            await Task.Yield();
            if (_gates.TryGetValue(id, out var gate)) await gate.Task.WaitAsync(cancellationToken);
            _completions.AddOrUpdate(id, 1, (_, c) => c + 1);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }
}

public sealed class RenewalTestSubscriber(RenewalGate gate) : ISubscriber<RenewalTestEvent>
{
    public Task HandleEventAsync(RenewalTestEvent eventMessage, CancellationToken cancellationToken = default) => gate.Run(eventMessage.Key, cancellationToken);
}

public sealed record RenewCall(DateTime At, IReadOnlyDictionary<int, DateTime> Expected, DateTime LockExpiration, IReadOnlyDictionary<int, DateTime>? Result);

/// <summary>A store whose claims are scripted and whose lock renewal is observable and programmable.</summary>
public sealed class FakeLockStore : IOutboxStore<int>
{
    private readonly object _gate = new();
    private readonly Queue<List<OutboxMessage<int>>> _batches = new();
    private readonly List<DateTime> _claims = new();
    private readonly List<RenewCall> _renewCalls = new();

    /// <summary>Decides what is renewed. Default: every expected Id, stored exactly at the new lock.</summary>
    public Func<IReadOnlyDictionary<int, DateTime>, DateTime, IReadOnlyDictionary<int, DateTime>> Renew { get; set; }
        = (expected, lockExpiration) => expected.Keys.ToDictionary(id => id, _ => lockExpiration);

    public ConcurrentBag<int> Removed { get; } = new();

    public IReadOnlyList<DateTime> Claims
    {
        get { lock (_gate) return _claims.ToList(); }
    }

    public IReadOnlyList<RenewCall> RenewCalls
    {
        get { lock (_gate) return _renewCalls.ToList(); }
    }

    public void AddBatch(IEnumerable<OutboxMessage<int>> batch)
    {
        lock (_gate) _batches.Enqueue(batch.ToList());
    }

    public Task<IEnumerable<OutboxMessage<int>>> GetAllAsync(int? itemsCount = null, DateTime? lockExpiration = null, int? maxAttemps = null, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _claims.Add(DateTime.UtcNow);
            var batch = _batches.Count > 0 ? _batches.Dequeue() : new List<OutboxMessage<int>>();
            return Task.FromResult<IEnumerable<OutboxMessage<int>>>(batch);
        }
    }

    public Task RemoveAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default)
    {
        Removed.Add(message.Id);
        return Task.CompletedTask;
    }

    public Task UpdateAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task<IReadOnlyDictionary<int, DateTime>> RenewLocksAsync(IReadOnlyDictionary<int, DateTime> expectedLocks, DateTime lockExpiration, CancellationToken cancellationToken = default)
    {
        var copy = expectedLocks.ToDictionary(x => x.Key, x => x.Value);
        IReadOnlyDictionary<int, DateTime>? result = null;
        try
        {
            result = Renew(copy, lockExpiration);
            return Task.FromResult(result);
        }
        finally
        {
            lock (_gate) _renewCalls.Add(new RenewCall(DateTime.UtcNow, copy, lockExpiration, result));
        }
    }
}

public sealed class LogSink
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

    public IReadOnlyList<(LogLevel Level, string Message)> Entries => _entries.ToList();

    public void Add(LogLevel level, string message) => _entries.Enqueue((level, message));
}

public sealed class SinkLogger<T>(LogSink sink) : ILogger<T>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => sink.Add(logLevel, formatter(state, exception));
}

/// <summary>The real hosted service and MemoryMessageQueue with a fake store and a short lock span.</summary>
public sealed class RenewalHost : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly OutboxProcessorHostedService<int> _hosted;
    private bool _stopped;

    private RenewalHost(ServiceProvider provider, FakeLockStore store, TimeSpan span)
    {
        _provider = provider;
        Store = store;
        Span = span;
        Gate = provider.GetRequiredService<RenewalGate>();
        Logs = provider.GetRequiredService<LogSink>();
        Queue = provider.GetRequiredService<IMessageQueue<int>>();
        _hosted = provider.GetServices<IHostedService>().OfType<OutboxProcessorHostedService<int>>().Single();
    }

    public FakeLockStore Store { get; }
    public TimeSpan Span { get; }
    public RenewalGate Gate { get; }
    public LogSink Logs { get; }
    public IMessageQueue<int> Queue { get; }
    public Task? ExecuteTask => _hosted.ExecuteTask;

    /// <param name="held">Ids whose handlers wait for <see cref="RenewalGate.ReleaseAll"/>, set before the first claim.</param>
    public static async Task<RenewalHost> StartAsync(TimeSpan span, Action<FakeLockStore>? arrange = null, params int[] held)
    {
        var store = new FakeLockStore();
        arrange?.Invoke(store);
        var gate = new RenewalGate();
        gate.Hold(held);
        var services = new ServiceCollection();
        services.AddSingleton<LogSink>();
        services.AddSingleton(typeof(ILogger<>), typeof(SinkLogger<>));
        services.AddSingleton(gate);
        services.AddScoped<IOutboxStore<int>>(_ => store);
        services.AddOutbox<int>(o => o.MessageLockExpirationTimeSpan = span, typeof(RenewalHost).Assembly);
        var provider = services.BuildServiceProvider();
        var host = new RenewalHost(provider, store, span);
        await host._hosted.StartAsync(CancellationToken.None);
        return host;
    }

    public static OutboxMessage<int> Message(int id, DateTime? lockExpiresAt) => new()
    {
        Id = id,
        Type = typeof(RenewalTestEvent).AssemblyQualifiedName,
        Payload = new RenewalTestEvent { Key = id },
        Created = DateTime.UtcNow,
        LockExpiresAt = lockExpiresAt,
    };

    public Task EnqueueAsync(int id, DateTime? lockExpiresAt) => Queue.EnqueueAsync(Message(id, lockExpiresAt));

    public async Task StopAsync()
    {
        if (_stopped) return;
        _stopped = true;
        Gate.ReleaseAll();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await _hosted.StopAsync(cts.Token);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _provider.DisposeAsync();
    }

    public static async Task WaitUntil(Func<bool> condition, string what, int timeoutMs = 10000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Timed out waiting for: " + what);
            await Task.Delay(20);
        }
    }
}

/// <summary>The hosted service renews the locks of queued and in-flight messages of the built-in queue.</summary>
public class LockRenewalHostedServiceTests
{
    /// <summary>Renewal every 200 ms.</summary>
    private static readonly TimeSpan ShortSpan = TimeSpan.FromMilliseconds(600);

    private static DateTime Lock(int seconds = 60) => DateTime.UtcNow.AddSeconds(seconds);

    /// <summary>Holds ten messages in flight (Ids 1-10) so later messages stay queued.</summary>
    private static async Task FillHandlers(RenewalHost host)
    {
        var ids = Enumerable.Range(1, 10).ToArray();
        host.Gate.Hold(ids);
        foreach (var id in ids) await host.EnqueueAsync(id, Lock());
        await RenewalHost.WaitUntil(() => host.Gate.InFlight == 10, "ten handlers in flight");
    }

    [Fact]
    public async Task Renewal_QueuedAndInFlightMessages_AreRenewedWithTheirExpectedLocksAndNowPlusSpan()
    {
        await using var host = await RenewalHost.StartAsync(ShortSpan);
        await FillHandlers(host);
        var queuedLock = Lock(30);
        await host.EnqueueAsync(11, queuedLock);
        var before = DateTime.UtcNow;

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count(c => c.At > before) >= 2, "two renewals");

        var first = host.Store.RenewCalls.First(c => c.At > before && c.Expected.ContainsKey(11));
        Assert.Equal(Enumerable.Range(1, 11), first.Expected.Keys.OrderBy(x => x));
        Assert.Equal(queuedLock, first.Expected[11]);
        var lag = first.LockExpiration - ShortSpan - first.At;
        Assert.True(lag.Duration() < TimeSpan.FromSeconds(1), $"Lock {first.LockExpiration:O} is not about now + span (call at {first.At:O}).");
        Assert.Equal(0, host.Gate.Invocations(11));
    }

    [Fact]
    public async Task Renewal_StoredValueReturned_IsTheExpectedLockOfTheNextRenewal()
    {
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) =>
        {
            Interlocked.Increment(ref calls);
            // Like SQL 'datetime' rounding: the stored value differs from the requested one.
            return expected.Keys.ToDictionary(id => id, _ => lockExpiration.AddTicks(-30000 + Volatile.Read(ref calls)));
        });
        host.Gate.Hold(1);
        var initial = Lock();
        await host.EnqueueAsync(1, initial);

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 3, "three renewals");

        var renewals = host.Store.RenewCalls.Take(3).ToList();
        Assert.Equal(initial, renewals[0].Expected[1]);
        Assert.Equal(renewals[0].Result![1], renewals[1].Expected[1]);
        Assert.Equal(renewals[1].Result![1], renewals[2].Expected[1]);
    }

    [Fact]
    public async Task Renewal_QueuedMessageWhoseLockWasLost_IsDroppedAndNeverHandled()
    {
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) =>
            expected.Keys.Where(id => id != 11).ToDictionary(id => id, _ => lockExpiration));
        await FillHandlers(host);
        await host.EnqueueAsync(11, Lock());
        await host.EnqueueAsync(12, Lock());

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Any(c => c.Expected.ContainsKey(11)), "renewal attempted for the queued message");
        var missAt = host.Store.RenewCalls.First(c => c.Expected.ContainsKey(11)).At;
        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count(c => c.At > missAt) >= 2, "later renewals");
        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => host.Gate.Completions(12) == 1 && Enumerable.Range(1, 10).All(id => host.Gate.Completions(id) == 1), "other messages handled");
        await Task.Delay(300);

        Assert.Equal(0, host.Gate.Invocations(11));
        Assert.DoesNotContain(11, host.Store.Removed);
        // One tick: its expected lock, then (still missed) the tick's own lock; never again after the revocation.
        var calls11 = host.Store.RenewCalls.Where(c => c.Expected.ContainsKey(11)).ToList();
        Assert.Equal(2, calls11.Count);
        Assert.Equal(calls11[0].LockExpiration, calls11[1].LockExpiration);
        Assert.Equal(calls11[0].LockExpiration, calls11[1].Expected[11]);
        Assert.Equal(new[] { 11 }, calls11[1].Expected.Keys);
        Assert.Contains(host.Logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("lost its lock while queued") && e.Message.Contains("11"));
    }

    [Fact]
    public async Task Renewal_InFlightMessageWhoseLockWasLost_HandlerContinuesWithAWarningAndIsNotRenewedAgain()
    {
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (_, _) => new Dictionary<int, DateTime>());
        host.Gate.Hold(1);
        await host.EnqueueAsync(1, Lock());
        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 1, "renewal attempted");
        await Task.Delay(ShortSpan * 2);

        // One tick only: its expected lock, then the tick's own lock (both missed); never renewed again.
        Assert.Equal(2, host.Store.RenewCalls.Count);
        Assert.Equal(host.Store.RenewCalls[0].LockExpiration, host.Store.RenewCalls[1].Expected[1]);
        Assert.Equal(1, host.Gate.InFlight);
        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => host.Store.Removed.Contains(1), "handler completed and removed the row");

        Assert.Equal(1, host.Gate.Completions(1));
        Assert.Contains(host.Logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("lost its lock while its handler is running"));
    }

    [Fact]
    public async Task Renewal_DuplicateEnqueueWithANewerLock_MovesTheExpectedLockAndAStaleMissDoesNotRevoke()
    {
        RenewalHost? hostRef = null;
        var newer = Lock(120);
        var newest = Lock(180);
        var raced = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) =>
        {
            if (expected.TryGetValue(11, out var sent) && sent == newer && Interlocked.Exchange(ref raced, 1) == 0)
            {
                // While this renewal runs, the poller re-claims the row: the entry now expects 'newest'. The renewal
                // of 'newer' then misses, which must not revoke the re-claimed message.
                hostRef!.EnqueueAsync(11, newest).GetAwaiter().GetResult();
                return expected.Keys.Where(id => id != 11).ToDictionary(id => id, _ => lockExpiration);
            }
            return expected.Keys.Where(id => id != 11 || expected[11] == newest).ToDictionary(id => id, id => id == 11 ? newest : lockExpiration);
        });
        hostRef = host;
        await FillHandlers(host);
        await host.EnqueueAsync(11, Lock(60));
        await host.EnqueueAsync(11, newer);

        await RenewalHost.WaitUntil(() => Volatile.Read(ref raced) == 1, "the racing renewal");
        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count(c => c.Expected.TryGetValue(11, out var e) && e == newest) >= 2, "renewals of the newest lock");
        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => host.Gate.Completions(11) == 1, "message 11 handled");
        await Task.Delay(300);

        Assert.Equal(1, host.Gate.Invocations(11));
        // Calls retrying a miss with the tick's own lock (Expected == LockExpiration) are not renewals of a claimed lock.
        var afterRace = host.Store.RenewCalls.SkipWhile(c => !(c.Expected.TryGetValue(11, out var e) && e == newer)).Skip(1)
            .Where(c => c.Expected.TryGetValue(11, out var e) && e != c.LockExpiration).ToList();
        Assert.NotEmpty(afterRace);
        Assert.All(afterRace, c => Assert.Equal(newest, c.Expected[11]));
    }

    [Fact]
    public async Task Renewal_DuplicateEnqueueWithAnOlderLock_KeepsTheExpectedLock()
    {
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, _) => expected.ToDictionary(x => x.Key, x => x.Value));
        host.Gate.Hold(1);
        var current = Lock(120);
        await host.EnqueueAsync(1, current);
        await host.EnqueueAsync(1, Lock(30));

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 2, "two renewals");

        Assert.All(host.Store.RenewCalls, c => Assert.Equal(current, c.Expected[1]));
    }

    [Fact]
    public async Task Renewal_StoreThrows_IsLoggedAndRetriedAndNothingIsDropped()
    {
        var failures = 2;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) =>
        {
            if (Interlocked.Decrement(ref failures) >= 0) throw new InvalidOperationException("renewal store failure");
            return expected.Keys.ToDictionary(id => id, _ => lockExpiration);
        });
        await FillHandlers(host);
        await host.EnqueueAsync(11, Lock());

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 4, "renewals after the failures");
        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => host.Gate.Completions(11) == 1, "queued message handled");

        Assert.All(host.Store.RenewCalls.Skip(2), c => Assert.Contains(11, c.Expected.Keys));
        Assert.Equal(2, host.Logs.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("Failed to renew")));
        Assert.False(host.ExecuteTask!.IsCompleted);
    }

    /// <summary>
    /// Rows modeled like the EF store: a renewal applies only where the stored lock matches the expected value within
    /// ±5 ms. <paramref name="failApplied"/> decides, for a failing call, which rows it still applies to before throwing.
    /// </summary>
    private sealed class StoredLocks
    {
        private readonly object _gate = new();
        private readonly Dictionary<int, DateTime> _rows = new();

        public void Set(int id, DateTime value)
        {
            lock (_gate) _rows[id] = value;
        }

        public DateTime Get(int id)
        {
            lock (_gate) return _rows[id];
        }

        public IReadOnlyDictionary<int, DateTime> Renew(IReadOnlyDictionary<int, DateTime> expected, DateTime lockExpiration, Func<int, bool>? failApplied = null)
        {
            lock (_gate)
            {
                var result = new Dictionary<int, DateTime>();
                foreach (var (id, value) in expected)
                {
                    if (failApplied != null && !failApplied(id)) continue;
                    if (_rows.TryGetValue(id, out var stored) && (stored - value).Duration() <= TimeSpan.FromMilliseconds(5))
                    {
                        _rows[id] = lockExpiration;
                        result[id] = lockExpiration;
                    }
                }
                if (failApplied != null) throw new TimeoutException("acknowledgement lost");
                return result;
            }
        }
    }

    /// <summary>
    /// The first renewal call stores the new lock on every row and then throws (lost acknowledgement). The next tick
    /// misses with the old expected locks, retries with the attempted lock, renews every message and drops nothing.
    /// </summary>
    [Fact]
    public async Task Renewal_FailedCallThatAppliedTheLock_NextTickRenewsWithTheAttemptedLockAndDropsNothing()
    {
        var rows = new StoredLocks();
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) =>
            Interlocked.Increment(ref calls) == 1 ? rows.Renew(expected, lockExpiration, _ => true) : rows.Renew(expected, lockExpiration));
        var ids = Enumerable.Range(1, 20).ToArray();
        foreach (var id in ids) rows.Set(id, Lock());
        host.Gate.Hold(ids);
        foreach (var id in ids) await host.EnqueueAsync(id, rows.Get(id));
        await RenewalHost.WaitUntil(() => host.Gate.InFlight == 10, "ten in flight, ten queued");

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count(c => c.Result != null) >= 4, "renewals after the failure");

        var failed = host.Store.RenewCalls[0];
        Assert.Null(failed.Result);
        Assert.Equal(ids, failed.Expected.Keys.OrderBy(x => x));
        var retry = host.Store.RenewCalls.First(c => c.Expected.Values.Contains(failed.LockExpiration));
        Assert.Equal(ids, retry.Expected.Keys.OrderBy(x => x));
        Assert.Equal(ids, retry.Result!.Keys.OrderBy(x => x));
        var later = host.Store.RenewCalls.Last(c => c.Result != null);
        Assert.Equal(ids, later.Result!.Keys.OrderBy(x => x));
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("lost its lock"));

        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => ids.All(id => host.Gate.Completions(id) == 1), "every message handled");
        Assert.All(ids, id => Assert.Equal(1, host.Gate.Invocations(id)));
    }

    /// <summary>
    /// The first renewal call applies to some rows only (like a later chunk failing after earlier chunks committed),
    /// and a second tick fails too before the store recovers: each message is still renewed from the value its row
    /// actually holds, and nothing is dropped.
    /// </summary>
    [Fact]
    public async Task Renewal_FailedCallsThatAppliedToSomeRows_NextSuccessfulTickRenewsEveryRowFromItsStoredValue()
    {
        var rows = new StoredLocks();
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) => Interlocked.Increment(ref calls) switch
        {
            1 => rows.Renew(expected, lockExpiration, id => id <= 12),
            2 => rows.Renew(expected, lockExpiration, _ => false),
            _ => rows.Renew(expected, lockExpiration),
        });
        var ids = Enumerable.Range(1, 20).ToArray();
        foreach (var id in ids) rows.Set(id, Lock());
        host.Gate.Hold(ids);
        foreach (var id in ids) await host.EnqueueAsync(id, rows.Get(id));

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count(c => c.Result != null) >= 3, "renewals after the failures");

        Assert.Equal(2, host.Logs.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("Failed to renew")));
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("lost its lock"));
        // Steady state again: one call per tick renews every message.
        var last = host.Store.RenewCalls.Last(c => c.Result != null);
        Assert.Equal(ids, last.Expected.Keys.OrderBy(x => x));
        Assert.Equal(ids, last.Result!.Keys.OrderBy(x => x));

        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => ids.All(id => host.Gate.Completions(id) == 1), "every message handled");
    }

    /// <summary>
    /// A failed call that applied nothing, after which the row is claimed elsewhere: the queued copy is still dropped
    /// once the next tick misses with both the expected and the attempted lock.
    /// </summary>
    [Fact]
    public async Task Renewal_FailedCallThenRowClaimedElsewhere_QueuedCopyIsDroppedAfterTryingEveryCandidate()
    {
        var rows = new StoredLocks();
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                rows.Set(11, Lock(600)); // another process claims the row
                return rows.Renew(expected, lockExpiration, _ => false);
            }
            return rows.Renew(expected, lockExpiration);
        });
        foreach (var id in Enumerable.Range(1, 11)) rows.Set(id, Lock());
        host.Gate.Hold(Enumerable.Range(1, 10).ToArray());
        // Ids 1-10 take every handler slot; 11 stays queued.
        foreach (var id in Enumerable.Range(1, 11)) await host.EnqueueAsync(id, rows.Get(id));

        await RenewalHost.WaitUntil(() => host.Logs.Entries.Any(e => e.Message.Contains("lost its lock while queued") && e.Message.Contains("11")), "queued copy dropped");

        var attempted = host.Store.RenewCalls[0].LockExpiration;
        Assert.Contains(host.Store.RenewCalls, c => c.Expected.TryGetValue(11, out var e) && e == attempted);
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("lost its lock") && !e.Message.Contains("11"));
        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => Enumerable.Range(1, 10).All(id => host.Gate.Completions(id) == 1), "others handled");
        await Task.Delay(300);
        Assert.Equal(0, host.Gate.Invocations(11));
    }

    /// <summary>
    /// A retrying execution strategy re-runs a renewal statement whose commit's acknowledgement was lost: the rows
    /// (here ids 1-12, like a first chunk) already hold the new lock, the re-run misses them, and the call returns
    /// normally without them. The same tick retries them once with its own lock, renews them, and drops nothing.
    /// </summary>
    [Fact]
    public async Task Renewal_CallReRunAfterItsCommitWasLost_SameTickRetriesWithItsOwnLockAndDropsNothing()
    {
        var rows = new StoredLocks();
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                // First execution committed on ids 1-12, then the strategy re-ran the whole call with the same values.
                rows.Renew(expected.Where(x => x.Key <= 12).ToDictionary(x => x.Key, x => x.Value), lockExpiration);
            }
            return rows.Renew(expected, lockExpiration);
        });
        var ids = Enumerable.Range(1, 20).ToArray();
        foreach (var id in ids) rows.Set(id, Lock());
        host.Gate.Hold(ids);
        foreach (var id in ids) await host.EnqueueAsync(id, rows.Get(id));
        await RenewalHost.WaitUntil(() => host.Gate.InFlight == 10, "ten in flight, ten queued");

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 5, "renewals after the re-run");

        var first = host.Store.RenewCalls[0];
        Assert.Equal(Enumerable.Range(13, 8), first.Result!.Keys.OrderBy(x => x));
        var retry = host.Store.RenewCalls[1];
        Assert.Equal(first.LockExpiration, retry.LockExpiration);
        Assert.Equal(Enumerable.Range(1, 12), retry.Expected.Keys.OrderBy(x => x));
        Assert.All(retry.Expected.Values, v => Assert.Equal(first.LockExpiration, v));
        Assert.Equal(Enumerable.Range(1, 12), retry.Result!.Keys.OrderBy(x => x));
        // Steady state: one call per tick renews every message from the value its row holds.
        var later = host.Store.RenewCalls.Last();
        Assert.Equal(ids, later.Result!.Keys.OrderBy(x => x));
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("lost its lock") || e.Message.Contains("Failed to renew"));

        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => ids.All(id => host.Gate.Completions(id) == 1), "every message handled");
        Assert.All(ids, id => Assert.Equal(1, host.Gate.Invocations(id)));
    }

    /// <summary>
    /// The first call stores its lock on ids 1-10 only and throws. In the next tick, pass 0 renews ids 11-20, and
    /// the retry of ids 1-10 with the remembered lock fails: nothing is revoked, ids 11-20 keep their confirmed lock,
    /// and the tick after renews ids 1-10 with the remembered lock.
    /// </summary>
    [Fact]
    public async Task Renewal_LaterPassFailsAfterPassZeroPartlySucceeded_RevokesNothingAndRecoversOnTheNextTick()
    {
        var rows = new StoredLocks();
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, lockExpiration) => Interlocked.Increment(ref calls) switch
        {
            1 => rows.Renew(expected, lockExpiration, id => id <= 10),
            3 => rows.Renew(expected, lockExpiration, _ => false),
            _ => rows.Renew(expected, lockExpiration),
        });
        var ids = Enumerable.Range(1, 20).ToArray();
        foreach (var id in ids) rows.Set(id, Lock());
        host.Gate.Hold(ids);
        foreach (var id in ids) await host.EnqueueAsync(id, rows.Get(id));

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 7, "renewals after the failures");

        var calls0 = host.Store.RenewCalls;
        var tick1 = calls0[0].LockExpiration;
        var tick2 = calls0[1].LockExpiration;
        Assert.NotEqual(tick1, tick2);
        Assert.Equal(Enumerable.Range(11, 10), calls0[1].Result!.Keys.OrderBy(x => x)); // pass 0 of tick 2
        Assert.Equal(tick2, calls0[2].LockExpiration); // the remembered-lock pass of tick 2, which failed
        Assert.Equal(Enumerable.Range(1, 10), calls0[2].Expected.Keys.OrderBy(x => x));
        Assert.All(calls0[2].Expected.Values, v => Assert.Equal(tick1, v));
        Assert.Null(calls0[2].Result);

        // Tick 3: ids 11-20 expect tick 2's lock; ids 1-10 miss and are renewed with tick 1's lock.
        var tick3 = calls0.Where(c => c.LockExpiration != tick1 && c.LockExpiration != tick2).GroupBy(c => c.LockExpiration).First().ToList();
        Assert.All(Enumerable.Range(11, 10), id => Assert.Equal(tick2, tick3[0].Expected[id]));
        Assert.Equal(Enumerable.Range(11, 10), tick3[0].Result!.Keys.OrderBy(x => x));
        Assert.Equal(Enumerable.Range(1, 10), tick3[1].Result!.Keys.OrderBy(x => x));
        Assert.All(tick3[1].Expected.Values, v => Assert.Equal(tick1, v));
        Assert.Equal(2, host.Logs.Entries.Count(e => e.Message.Contains("Failed to renew"))); // ticks 1 and 2
        Assert.DoesNotContain(host.Logs.Entries, e => e.Message.Contains("lost its lock"));

        host.Gate.ReleaseAll();
        await RenewalHost.WaitUntil(() => ids.All(id => host.Gate.Completions(id) == 1), "every message handled");
        Assert.All(ids, id => Assert.Equal(1, host.Gate.Invocations(id)));
    }

    /// <summary>
    /// Ten failed ticks, then the row is owned elsewhere: the next tick tries the expected lock, the 8 most recent
    /// failed attempts (oldest first) and its own lock - 10 calls - and only then drops the message.
    /// </summary>
    [Fact]
    public async Task Renewal_ManyFailedTicks_RemembersOnlyTheEightMostRecentAttempts()
    {
        var calls = 0;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (_, _) =>
        {
            if (Interlocked.Increment(ref calls) <= 10) throw new TimeoutException("outage");
            return new Dictionary<int, DateTime>();
        });
        host.Gate.Hold(1);
        var initial = Lock();
        await host.EnqueueAsync(1, initial);

        await RenewalHost.WaitUntil(() => host.Logs.Entries.Any(e => e.Message.Contains("lost its lock while its handler is running")), "the lock given up", 20000);

        var all = host.Store.RenewCalls;
        var failed = all.Take(10).Select(c => c.LockExpiration).ToList();
        Assert.Equal(10, failed.Distinct().Count());
        var tick = all.Skip(10).ToList();
        Assert.Equal(10, tick.Count);
        Assert.All(tick, c => Assert.Equal(tick[0].LockExpiration, c.LockExpiration));
        Assert.Equal(new[] { initial }.Concat(failed.Skip(2)).Append(tick[0].LockExpiration), tick.Select(c => c.Expected[1]));
        Assert.Equal(10, host.Logs.Entries.Count(e => e.Message.Contains("Failed to renew")));
    }

    /// <summary>The handler deletes the row and returns while a renewal call that missed it is running: no lost-lock warning.</summary>
    [Fact]
    public async Task Renewal_HandlerFinishesDuringTheRenewalThatMissesIt_NoLostLockWarning()
    {
        RenewalHost? hostRef = null;
        await using var host = await RenewalHost.StartAsync(ShortSpan, store => store.Renew = (expected, _) =>
        {
            // The row is deleted by the handler: release it and let it finish before answering.
            hostRef!.Gate.ReleaseAll();
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!hostRef.Store.Removed.Contains(1) && DateTime.UtcNow < deadline) Thread.Sleep(5);
            Thread.Sleep(50);
            return new Dictionary<int, DateTime>();
        });
        hostRef = host;
        host.Gate.Hold(1);
        await host.EnqueueAsync(1, Lock());

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 1, "renewal attempted");
        await Task.Delay(ShortSpan);

        Assert.Equal(1, host.Gate.Completions(1));
        Assert.DoesNotContain(host.Logs.Entries, e => e.Level >= LogLevel.Warning && e.Message.Contains("lost its lock"));
    }

    [Fact]
    public async Task Renewal_CompletedMessagesAndMessagesWithoutALock_AreNotRenewed()
    {
        await using var host = await RenewalHost.StartAsync(ShortSpan);
        await host.EnqueueAsync(1, Lock());
        await RenewalHost.WaitUntil(() => host.Store.Removed.Contains(1), "message 1 handled");
        await Task.Delay(100);
        host.Gate.Hold(2, 3);
        await host.EnqueueAsync(2, null);
        await host.EnqueueAsync(3, Lock());
        var after = DateTime.UtcNow;

        await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count(c => c.At > after) >= 2, "two renewals");

        Assert.All(host.Store.RenewCalls.Where(c => c.At > after), c => Assert.Equal(new[] { 3 }, c.Expected.Keys));
        Assert.DoesNotContain(host.Store.RenewCalls, c => c.Expected.ContainsKey(1) && c.At > after);
    }

    [Fact]
    public async Task Renewal_NothingTracked_DoesNotCallTheStore()
    {
        await using var host = await RenewalHost.StartAsync(ShortSpan);
        await Task.Delay(ShortSpan * 2);

        Assert.Empty(host.Store.RenewCalls);
    }

    [Fact]
    public async Task Renewal_InFlightLongerThanOneSpan_WarnsAndKeepsRenewing()
    {
        await using var host = await RenewalHost.StartAsync(ShortSpan);
        host.Gate.Hold(1);
        await host.EnqueueAsync(1, Lock());
        await Task.Delay(ShortSpan * 3);

        Assert.Contains(host.Logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("longer than the lock span"));
        Assert.True(host.Store.RenewCalls.Count >= 5, $"{host.Store.RenewCalls.Count} renewals");
        Assert.All(host.Store.RenewCalls, c => Assert.Contains(1, c.Expected.Keys));
    }

    [Fact]
    public async Task Shutdown_WithRenewalAndAHandlerInFlight_StopsPromptlyAndRenewsNoMore()
    {
        var host = await RenewalHost.StartAsync(ShortSpan);
        try
        {
            host.Gate.Hold(1);
            await host.EnqueueAsync(1, Lock());
            await RenewalHost.WaitUntil(() => host.Store.RenewCalls.Count >= 1, "renewal running");

            var watch = Stopwatch.StartNew();
            await host.StopAsync();
            watch.Stop();
            var callsAtStop = host.Store.RenewCalls.Count;
            await Task.Delay(ShortSpan * 2);

            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), $"Stop took {watch.Elapsed}");
            Assert.True(host.ExecuteTask!.IsCompleted);
            Assert.Equal(callsAtStop, host.Store.RenewCalls.Count);
        }
        finally
        {
            await host.DisposeAsync();
        }
    }
}

/// <summary>After a full batch the poller waits for that batch to be handled (bounded by the lock span) before claiming again.</summary>
public class PollerBackpressureTests
{
    private static IEnumerable<OutboxMessage<int>> Batch(int count) => Enumerable.Range(1, count).Select(id => RenewalHost.Message(id, DateTime.UtcNow.AddMinutes(1)));

    [Fact]
    public async Task FullBatch_DoesNotClaimAgainUntilTheBatchIsHandled()
    {
        var host = await RenewalHost.StartAsync(TimeSpan.FromSeconds(30), store => store.AddBatch(Batch(101)), Enumerable.Range(1, 101).ToArray());
        try
        {
            await RenewalHost.WaitUntil(() => host.Gate.InFlight == 10, "first claim dispatched");
            await Task.Delay(2500);
            Assert.Single(host.Store.Claims);

            host.Gate.ReleaseAll();
            await RenewalHost.WaitUntil(() => host.Gate.TotalCompletions == 101, "batch handled");
            await RenewalHost.WaitUntil(() => host.Store.Claims.Count == 2, "second claim after the batch was handled", 5000);
            Assert.All(Enumerable.Range(1, 101), id => Assert.Equal(1, host.Gate.Invocations(id)));
        }
        finally
        {
            await host.DisposeAsync();
        }
    }

    [Fact]
    public async Task FullBatch_StillNotHandledAfterOneLockSpan_ClaimsAgain()
    {
        var span = TimeSpan.FromSeconds(1.5);
        var host = await RenewalHost.StartAsync(span, store => store.AddBatch(Batch(101)), Enumerable.Range(1, 101).ToArray());
        try
        {
            await RenewalHost.WaitUntil(() => host.Store.Claims.Count == 2, "second claim after the bound", 8000);

            var claims = host.Store.Claims;
            var gap = claims[1] - claims[0];
            Assert.True(gap >= span - TimeSpan.FromMilliseconds(250), $"Claimed again after {gap}");
            Assert.True(gap < span + TimeSpan.FromSeconds(3), $"Claimed again after {gap}");
            await Task.Delay(1000);
            Assert.Equal(2, host.Store.Claims.Count); // the second claim returned nothing: back to the long interval
        }
        finally
        {
            await host.DisposeAsync();
        }
    }

    [Fact]
    public async Task BatchOfOneHundredOrFewer_DoesNotClaimAgainSoon()
    {
        var host = await RenewalHost.StartAsync(TimeSpan.FromSeconds(1), store => store.AddBatch(Batch(100)));
        try
        {
            await RenewalHost.WaitUntil(() => host.Gate.TotalCompletions == 100, "batch handled");
            await Task.Delay(2500);

            Assert.Single(host.Store.Claims);
        }
        finally
        {
            await host.DisposeAsync();
        }
    }
}
