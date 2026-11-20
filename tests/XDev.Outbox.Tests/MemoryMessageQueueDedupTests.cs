using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;

namespace XDev.Outbox.Tests;

/// <summary>
/// Drives a real <see cref="MemoryMessageQueue{TKey}"/> with a handler that counts invocations per Id and can hold a
/// message in flight until the test releases it.
/// </summary>
public sealed class QueueHarness : IAsyncDisposable
{
    private readonly ConcurrentDictionary<int, int> _invocations = new();
    private readonly ConcurrentDictionary<int, TaskCompletionSource> _gates = new();
    private readonly ConcurrentDictionary<int, int> _failuresLeft = new();
    private readonly CancellationTokenSource _cts = new();
    private int _inFlight;
    private int _maxInFlight;
    private int _completed;

    public QueueHarness()
    {
        Queue = new MemoryMessageQueue<int>(NullLogger<MemoryMessageQueue<int>>.Instance);
    }

    public MemoryMessageQueue<int> Queue { get; }

    public int MaxInFlight => Volatile.Read(ref _maxInFlight);
    public int InFlight => Volatile.Read(ref _inFlight);
    public int Completed => Volatile.Read(ref _completed);

    public int Invocations(int id) => _invocations.TryGetValue(id, out var count) ? count : 0;

    /// <summary>The handler for <paramref name="id"/> waits until <see cref="Release"/> is called.</summary>
    public void Hold(int id) => _gates[id] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Release(int id)
    {
        if (_gates.TryRemove(id, out var gate)) gate.TrySetResult();
    }

    public void ReleaseAll()
    {
        foreach (var id in _gates.Keys.ToList()) Release(id);
    }

    public void FailNext(int id, int times = 1) => _failuresLeft[id] = times;

    public async Task StartAsync()
    {
        Queue.MessageReceived += HandleAsync;
        await Queue.StartAsync(_cts.Token);
    }

    public Task EnqueueAsync(int id, DateTime? lockExpiresAt = null)
        => Queue.EnqueueAsync(new OutboxMessage<int> { Id = id, Type = "t", Payload = new object(), LockExpiresAt = lockExpiresAt ?? DateTime.UtcNow.AddMinutes(5) });

    private async Task HandleAsync(OutboxMessage<int> message, CancellationToken cancellationToken)
    {
        _invocations.AddOrUpdate(message.Id, 1, (_, c) => c + 1);
        var now = Interlocked.Increment(ref _inFlight);
        for (var max = Volatile.Read(ref _maxInFlight); now > max; max = Volatile.Read(ref _maxInFlight))
        {
            if (Interlocked.CompareExchange(ref _maxInFlight, now, max) == max) break;
        }
        try
        {
            await Task.Yield();
            if (_gates.TryGetValue(message.Id, out var gate)) await gate.Task.WaitAsync(cancellationToken);
            if (_failuresLeft.TryGetValue(message.Id, out var left) && left > 0)
            {
                _failuresLeft[message.Id] = left - 1;
                throw new InvalidOperationException("handler failed on purpose");
            }
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
            Interlocked.Increment(ref _completed);
        }
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

    public async ValueTask DisposeAsync()
    {
        ReleaseAll();
        // Stopped by cancellation, as the hosted service does on shutdown. (MemoryMessageQueue.StopAsync can throw
        // NullReferenceException when the receive loop's finally disposes the token source first: a known issue
        // outside this task.)
        _cts.Cancel();
        await Task.Delay(50);
    }
}

/// <summary>MemoryMessageQueue does not hold or run two copies of the same message Id at the same time.</summary>
public class MemoryMessageQueueDedupTests
{
    /// <summary>
    /// Time allowed for the queue to forget a message after its handler returned (the entry is removed in the
    /// queue's own continuation, just after the handler's task completes).
    /// </summary>
    private static readonly TimeSpan ForgetMargin = TimeSpan.FromMilliseconds(300);

    [Fact]
    public async Task Enqueue_SameIdTwiceWhileQueued_HandlesItOnce()
    {
        await using var harness = new QueueHarness();
        await harness.EnqueueAsync(1);
        await harness.EnqueueAsync(1);
        await harness.EnqueueAsync(2);

        await harness.StartAsync();
        await QueueHarness.WaitUntil(() => harness.Completed >= 2, "both distinct messages handled");
        await Task.Delay(300);

        Assert.Equal(1, harness.Invocations(1));
        Assert.Equal(1, harness.Invocations(2));
        Assert.Equal(2, harness.Completed);
    }

    [Fact]
    public async Task Enqueue_SameIdWhileInFlight_DoesNotRunItAgain()
    {
        await using var harness = new QueueHarness();
        harness.Hold(1);
        await harness.StartAsync();
        await harness.EnqueueAsync(1);
        await QueueHarness.WaitUntil(() => harness.InFlight == 1, "message in flight");

        await harness.EnqueueAsync(1);
        await Task.Delay(300);
        harness.Release(1);
        await QueueHarness.WaitUntil(() => harness.Completed >= 1, "handler completed");
        await Task.Delay(300);

        Assert.Equal(1, harness.Invocations(1));
    }

    [Fact]
    public async Task Enqueue_SameIdAfterTheHandlerSucceeded_HandlesItAgain()
    {
        await using var harness = new QueueHarness();
        await harness.StartAsync();
        await harness.EnqueueAsync(1);
        await QueueHarness.WaitUntil(() => harness.Completed == 1, "first handling");
        await Task.Delay(ForgetMargin);

        await harness.EnqueueAsync(1);
        await QueueHarness.WaitUntil(() => harness.Completed == 2, "second handling");

        Assert.Equal(2, harness.Invocations(1));
    }

    [Fact]
    public async Task Enqueue_SameIdAfterTheHandlerFailed_HandlesItAgain()
    {
        await using var harness = new QueueHarness();
        harness.FailNext(1);
        await harness.StartAsync();
        await harness.EnqueueAsync(1);
        await QueueHarness.WaitUntil(() => harness.Completed == 1, "failed handling");
        await Task.Delay(ForgetMargin);

        await harness.EnqueueAsync(1);
        await QueueHarness.WaitUntil(() => harness.Completed == 2, "second handling");

        Assert.Equal(2, harness.Invocations(1));
    }

    [Fact]
    public async Task Enqueue_DistinctIds_RunsAtMostTenConcurrentlyAndHandlesAll()
    {
        await using var harness = new QueueHarness();
        for (var id = 1; id <= 15; id++) harness.Hold(id);
        await harness.StartAsync();
        for (var id = 1; id <= 15; id++) await harness.EnqueueAsync(id);

        await QueueHarness.WaitUntil(() => harness.InFlight == 10, "ten handlers in flight");
        await Task.Delay(300);
        Assert.Equal(10, harness.InFlight);

        harness.ReleaseAll();
        for (var id = 1; id <= 15; id++) harness.Release(id);
        await QueueHarness.WaitUntil(() => harness.Completed == 15, "all handled");
        // Gates registered after ReleaseAll do not exist: the remaining five run straight through.
        Assert.Equal(10, harness.MaxInFlight);
        Assert.All(Enumerable.Range(1, 15), id => Assert.Equal(1, harness.Invocations(id)));
    }

    [Fact]
    public async Task Enqueue_ManyDuplicatesWhileQueued_HandlesEachIdOnceAndLaterMessagesStillRun()
    {
        await using var harness = new QueueHarness();
        for (var round = 0; round < 5; round++)
        {
            for (var id = 1; id <= 20; id++) await harness.EnqueueAsync(id);
        }

        await harness.StartAsync();
        await QueueHarness.WaitUntil(() => harness.Completed >= 20, "every distinct id handled");
        await Task.Delay(300);
        await harness.EnqueueAsync(99);
        await QueueHarness.WaitUntil(() => harness.Invocations(99) == 1, "a later message");

        Assert.All(Enumerable.Range(1, 20), id => Assert.Equal(1, harness.Invocations(id)));
        Assert.Equal(21, harness.Completed);
    }
}
