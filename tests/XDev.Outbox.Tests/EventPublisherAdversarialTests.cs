using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace XDev.Outbox.Tests;

// Event and subscriber types are unique to this file (AddOutbox scans the whole test assembly).

/// <summary>Holds the exception instance a subscriber is about to throw, so tests can check the very same object propagates.</summary>
public sealed class ThrownExceptions
{
    private readonly List<Exception> _thrown = new();

    public IReadOnlyList<Exception> All
    {
        get { lock (_thrown) return _thrown.ToList(); }
    }

    public T Remember<T>(T exception) where T : Exception
    {
        lock (_thrown) _thrown.Add(exception);
        return exception;
    }
}

public sealed class OuterSubscriberException(string message, Exception inner) : Exception(message, inner);

// --- Sync throws of special exception types ---
public sealed class SyncCanceledEvent { }

public sealed class SyncCanceledSubscriber(ThrownExceptions thrown) : ISubscriber<SyncCanceledEvent>
{
    public Task HandleEventAsync(SyncCanceledEvent eventMessage, CancellationToken cancellationToken = default)
        => throw thrown.Remember(new OperationCanceledException("sync cancel on purpose", new CancellationToken(true)));
}

public sealed class SyncAggregateEvent { }

public sealed class SyncAggregateSubscriber(ThrownExceptions thrown) : ISubscriber<SyncAggregateEvent>
{
    public Task HandleEventAsync(SyncAggregateEvent eventMessage, CancellationToken cancellationToken = default)
        => throw thrown.Remember(new AggregateException("sync aggregate on purpose", new InvalidOperationException("first"), new ArgumentException("second")));
}

public sealed class SyncInnerEvent { }

public sealed class SyncInnerSubscriber(ThrownExceptions thrown) : ISubscriber<SyncInnerEvent>
{
    public Task HandleEventAsync(SyncInnerEvent eventMessage, CancellationToken cancellationToken = default)
        => throw thrown.Remember(new OuterSubscriberException("outer on purpose", new FormatException("inner cause")));
}

/// <summary>A subscriber that itself throws TargetInvocationException: it must not be unwrapped twice.</summary>
public sealed class SyncOwnTargetInvocationEvent { }

public sealed class SyncOwnTargetInvocationSubscriber(ThrownExceptions thrown) : ISubscriber<SyncOwnTargetInvocationEvent>
{
    public Task HandleEventAsync(SyncOwnTargetInvocationEvent eventMessage, CancellationToken cancellationToken = default)
        => throw thrown.Remember(new TargetInvocationException("subscriber's own TIE", new InvalidDataException("its inner")));
}

public sealed class SyncOwnTargetInvocationNoInnerEvent { }

public sealed class SyncOwnTargetInvocationNoInnerSubscriber(ThrownExceptions thrown) : ISubscriber<SyncOwnTargetInvocationNoInnerEvent>
{
    public Task HandleEventAsync(SyncOwnTargetInvocationNoInnerEvent eventMessage, CancellationToken cancellationToken = default)
        => throw thrown.Remember(new TargetInvocationException("subscriber's own TIE without inner", null));
}

// --- Throw after an await ---
public sealed class ThrowAfterAwaitEvent { }

public sealed class ThrowAfterAwaitSubscriber(ThrownExceptions thrown) : ISubscriber<ThrowAfterAwaitEvent>
{
    public async Task HandleEventAsync(ThrowAfterAwaitEvent eventMessage, CancellationToken cancellationToken = default)
    {
        await Task.Delay(1, CancellationToken.None);
        throw thrown.Remember(new OuterSubscriberException("after await on purpose", new TimeoutException("inner after await")));
    }
}

// --- A handler returning a null Task ---
public sealed class NullTaskEvent { }

public sealed class NullTaskSubscriber(InvocationLog log) : ISubscriber<NullTaskEvent>
{
    public Task HandleEventAsync(NullTaskEvent eventMessage, CancellationToken cancellationToken = default)
    {
        log.Record(this, eventMessage);
        return null!;
    }
}

// --- Two subscribers that both record then throw synchronously: only the first one runs ---
public sealed class FirstThrowsEvent { }

public sealed class FirstThrowsSubscriberA(InvocationLog log, ThrownExceptions thrown) : ISubscriber<FirstThrowsEvent>
{
    public Task HandleEventAsync(FirstThrowsEvent eventMessage, CancellationToken cancellationToken = default)
    {
        log.Record(this, eventMessage);
        throw thrown.Remember(new InvalidOperationException(nameof(FirstThrowsSubscriberA)));
    }
}

public sealed class FirstThrowsSubscriberB(InvocationLog log, ThrownExceptions thrown) : ISubscriber<FirstThrowsEvent>
{
    public Task HandleEventAsync(FirstThrowsEvent eventMessage, CancellationToken cancellationToken = default)
    {
        log.Record(this, eventMessage);
        throw thrown.Remember(new InvalidOperationException(nameof(FirstThrowsSubscriberB)));
    }
}

// --- Two subscribers: the one registered first throws synchronously, the second records ---
public sealed class OrderedThrowEvent { }

public sealed class OrderedThrowFirst : ISubscriber<OrderedThrowEvent>
{
    public Task HandleEventAsync(OrderedThrowEvent eventMessage, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("first subscriber failed");
}

public sealed class OrderedThrowSecond(InvocationLog log) : ISubscriber<OrderedThrowEvent>
{
    public Task HandleEventAsync(OrderedThrowEvent eventMessage, CancellationToken cancellationToken = default)
    {
        log.Record(this, eventMessage);
        return Task.CompletedTask;
    }
}

// --- Poller: the hosted service records the subscriber's message ---
public sealed class PollerSyncEvent
{
    public string? Text { get; set; }
}

public sealed class PollerSyncSubscriber : ISubscriber<PollerSyncEvent>
{
    public const string Message = "poller sync subscriber failed";

    public Task HandleEventAsync(PollerSyncEvent eventMessage, CancellationToken cancellationToken = default) => Throw();

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static Task Throw() => throw new InvalidOperationException(Message);
}

public sealed class PollerAsyncEvent
{
    public string? Text { get; set; }
}

public sealed class PollerAsyncSubscriber : ISubscriber<PollerAsyncEvent>
{
    public const string Message = "poller async subscriber failed";

    public async Task HandleEventAsync(PollerAsyncEvent eventMessage, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        throw new InvalidOperationException(Message);
    }
}

public sealed class PollerCanceledEvent
{
    public string? Text { get; set; }
}

public sealed class PollerCanceledSubscriber : ISubscriber<PollerCanceledEvent>
{
    public const string Message = "poller subscriber canceled synchronously";

    public Task HandleEventAsync(PollerCanceledEvent eventMessage, CancellationToken cancellationToken = default)
        => throw new OperationCanceledException(Message);
}

/// <summary>In-memory store recording updates and removals.</summary>
public sealed class RecordingStore : IOutboxStore<int>
{
    public List<OutboxMessage<int>> Updated { get; } = new();
    public List<OutboxMessage<int>> Removed { get; } = new();

    public Task<IEnumerable<OutboxMessage<int>>> GetAllAsync(int? itemsCount = null, DateTime? lockExpiration = null, int? maxAttemps = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Enumerable.Empty<OutboxMessage<int>>());

    public Task RemoveAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default)
    {
        lock (Removed) Removed.Add(message);
        return Task.CompletedTask;
    }

    public Task UpdateAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default)
    {
        lock (Updated) Updated.Add(message);
        return Task.CompletedTask;
    }

    // Not used by these tests (their queue is not the built-in one, so nothing is renewed). A store that cannot renew
    // throws: an empty result would mean "every lock was lost" and drop queued messages.
    public Task<IReadOnlyDictionary<int, DateTime>> RenewLocksAsync(IReadOnlyDictionary<int, DateTime> expectedLocks, DateTime lockExpiration, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("RecordingStore does not renew locks.");
}

/// <summary>Captures the hosted service's MessageReceived handler so a test can deliver a message to it directly.</summary>
public sealed class CapturingQueue : IMessageQueue<int>
{
    public Func<OutboxMessage<int>, CancellationToken, Task>? Handler { get; private set; }

    public Task EnqueueAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default) => Task.CompletedTask;

    public event Func<OutboxMessage<int>, CancellationToken, Task>? MessageReceived
    {
        add => Handler += value;
        remove => Handler -= value;
    }

    public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}

/// <summary>Adversarial checks of EventPublisher's exception propagation (TargetInvocationException unwrapping).</summary>
public class EventPublisherAdversarialTests
{
    private static async Task<(Exception? Error, ThrownExceptions Thrown, InvocationLog Log)> Publish<TEvent>(TEvent evt)
    {
        var services = new ServiceCollection();
        services.AddOutbox(typeof(EventPublisherAdversarialTests).Assembly);
        services.AddSingleton<InvocationLog>();
        services.AddSingleton<ThrownExceptions>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var error = await Record.ExceptionAsync(() => scope.ServiceProvider.GetRequiredService<IEventPublisher>().PublishAsync(evt));
        return (error, provider.GetRequiredService<ThrownExceptions>(), provider.GetRequiredService<InvocationLog>());
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsOperationCanceledSynchronously_PropagatesTheSameInstance()
    {
        var (error, thrown, _) = await Publish(new SyncCanceledEvent());

        Assert.Same(Assert.Single(thrown.All), error);
        var canceled = Assert.IsType<OperationCanceledException>(error);
        Assert.Equal("sync cancel on purpose", canceled.Message);
        Assert.True(canceled.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsOperationCanceledSynchronously_ReturnedTaskIsCanceledNotFaulted()
    {
        var services = new ServiceCollection();
        services.AddOutbox(typeof(EventPublisherAdversarialTests).Assembly);
        services.AddSingleton<ThrownExceptions>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var task = scope.ServiceProvider.GetRequiredService<IEventPublisher>().PublishAsync(new SyncCanceledEvent());
        await Record.ExceptionAsync(() => task);

        // Same status an async-thrown OperationCanceledException gives.
        Assert.True(task.IsCanceled);
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsAggregateExceptionSynchronously_PropagatesItWithItsInnerExceptions()
    {
        var (error, thrown, _) = await Publish(new SyncAggregateEvent());

        Assert.Same(Assert.Single(thrown.All), error);
        var aggregate = Assert.IsType<AggregateException>(error);
        Assert.StartsWith("sync aggregate on purpose", aggregate.Message);
        Assert.Collection(aggregate.InnerExceptions,
            e => Assert.Equal("first", Assert.IsType<InvalidOperationException>(e).Message),
            e => Assert.Equal("second", Assert.IsType<ArgumentException>(e).Message));
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsCustomExceptionWithInnerSynchronously_PropagatesTypeMessageAndInnerUnchanged()
    {
        var (error, thrown, _) = await Publish(new SyncInnerEvent());

        Assert.Same(Assert.Single(thrown.All), error);
        var outer = Assert.IsType<OuterSubscriberException>(error);
        Assert.Equal("outer on purpose", outer.Message);
        Assert.Equal("inner cause", Assert.IsType<FormatException>(outer.InnerException).Message);
        Assert.Contains(nameof(SyncInnerSubscriber), outer.StackTrace);
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsItsOwnTargetInvocationExceptionSynchronously_UnwrapsOnlyTheReflectionLayer()
    {
        var (error, thrown, _) = await Publish(new SyncOwnTargetInvocationEvent());

        Assert.Same(Assert.Single(thrown.All), error);
        var tie = Assert.IsType<TargetInvocationException>(error);
        Assert.Equal("its inner", Assert.IsType<InvalidDataException>(tie.InnerException).Message);
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsItsOwnTargetInvocationExceptionWithoutInner_PropagatesThatInstance()
    {
        var (error, thrown, _) = await Publish(new SyncOwnTargetInvocationNoInnerEvent());

        Assert.Same(Assert.Single(thrown.All), error);
        Assert.Null(error!.InnerException);
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsAfterAnAwait_PropagatesTheSameInstance()
    {
        var (error, thrown, _) = await Publish(new ThrowAfterAwaitEvent());

        Assert.Same(Assert.Single(thrown.All), error);
        var outer = Assert.IsType<OuterSubscriberException>(error);
        Assert.Equal("after await on purpose", outer.Message);
        Assert.IsType<TimeoutException>(outer.InnerException);
    }

    /// <summary>
    /// Characterization (not changed by this fix): a handler that returns a null Task makes PublishAsync fail with
    /// NullReferenceException after the handler ran. It does not silently succeed.
    /// </summary>
    [Fact]
    public async Task Publish_WhenSubscriberReturnsNullTask_FailsWithNullReferenceAfterTheHandlerRan()
    {
        var (error, _, log) = await Publish(new NullTaskEvent());

        Assert.Equal(1, log.Count<NullTaskSubscriber>());
        Assert.IsType<NullReferenceException>(error);
    }

    [Fact]
    public async Task Publish_WhenEverySubscriberThrowsSynchronously_OnlyTheFirstRunsAndItsExceptionPropagates()
    {
        var (error, thrown, log) = await Publish(new FirstThrowsEvent());

        var entry = Assert.Single(log.Entries);
        var only = Assert.Single(thrown.All);
        Assert.Same(only, error);
        Assert.Equal(entry.Subscriber.Name, only.Message);
    }

    [Fact]
    public async Task Publish_WhenTheFirstRegisteredSubscriberThrowsSynchronously_LaterSubscribersAreNotInvoked()
    {
        var services = new ServiceCollection();
        // Registered explicitly first (AddOutbox's TryAddEnumerable keeps this order and skips the duplicates).
        services.AddScoped<ISubscriber<OrderedThrowEvent>, OrderedThrowFirst>();
        services.AddScoped<ISubscriber<OrderedThrowEvent>, OrderedThrowSecond>();
        services.AddOutbox(typeof(EventPublisherAdversarialTests).Assembly);
        services.AddSingleton<InvocationLog>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        Assert.Equal(new[] { typeof(OrderedThrowFirst), typeof(OrderedThrowSecond) }, scope.ServiceProvider.GetServices<ISubscriber<OrderedThrowEvent>>().Select(s => s.GetType()));

        var error = await Record.ExceptionAsync(() => scope.ServiceProvider.GetRequiredService<IEventPublisher>().PublishAsync(new OrderedThrowEvent()));

        Assert.Equal("first subscriber failed", Assert.IsType<InvalidOperationException>(error).Message);
        Assert.Equal(0, provider.GetRequiredService<InvocationLog>().Count<OrderedThrowSecond>());
    }
}

/// <summary>OutboxProcessorHostedService records the subscriber's own message as ErrorMessage (no reflection wrapper text).</summary>
public class HostedServiceErrorMessageTests
{
    private static async Task<(Exception? Error, RecordingStore Store)> Deliver(object payload)
    {
        var store = new RecordingStore();
        var queue = new CapturingQueue();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddScoped<IOutboxStore<int>>(_ => store);
        services.AddOutbox(typeof(HostedServiceErrorMessageTests).Assembly);
        services.AddSingleton<InvocationLog>();
        services.AddSingleton<ThrownExceptions>();
        using var provider = services.BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().OfType<OutboxProcessorHostedService<int>>().Single();
        using var cts = new CancellationTokenSource();
        await hosted.StartAsync(cts.Token);
        try
        {
            for (var i = 0; queue.Handler == null && i < 200; i++) await Task.Delay(10);
            Assert.NotNull(queue.Handler);

            using var json = JsonDocument.Parse(JsonSerializer.Serialize(payload, OutboxConfiguration.SerializerOptions));
            var message = new OutboxMessage<int>
            {
                Id = 7,
                Type = payload.GetType().AssemblyQualifiedName,
                Payload = json.RootElement.Clone(),
                Created = DateTime.UtcNow,
                Attemps = 2,
            };
            var error = await Record.ExceptionAsync(() => queue.Handler!(message, CancellationToken.None));
            return (error, store);
        }
        finally
        {
            cts.Cancel();
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Theory]
    [InlineData(typeof(PollerSyncEvent), PollerSyncSubscriber.Message)]
    [InlineData(typeof(PollerAsyncEvent), PollerAsyncSubscriber.Message)]
    [InlineData(typeof(PollerCanceledEvent), PollerCanceledSubscriber.Message)]
    public async Task HandleMessage_WhenSubscriberThrows_RecordsTheSubscribersMessageAndRethrows(Type eventType, string expectedMessage)
    {
        var payload = Activator.CreateInstance(eventType)!;
        eventType.GetProperty("Text")!.SetValue(payload, "boom");

        var (error, store) = await Deliver(payload);

        Assert.NotNull(error);
        Assert.IsNotType<TargetInvocationException>(error);
        Assert.Equal(expectedMessage, error!.Message);
        var updated = Assert.Single(store.Updated);
        Assert.Equal(expectedMessage, updated.ErrorMessage);
        Assert.Equal(3, updated.Attemps);
        Assert.True(updated.IsError);
        Assert.Empty(store.Removed);
    }
}
