using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

namespace XDev.Outbox.Tests;

// Event and subscriber types are unique to this file (AddOutbox scans the whole test assembly).

public sealed class SubscriberFailureException(string message) : Exception(message);

// --- Scenario: a subscriber that throws synchronously (before returning a Task) ---
public sealed class SyncThrowingEvent { }

public sealed class SyncThrowingSubscriber : ISubscriber<SyncThrowingEvent>
{
    public const string Message = "sync subscriber failed on purpose";

    public Task HandleEventAsync(SyncThrowingEvent eventMessage, CancellationToken cancellationToken = default) => Throw();

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static Task Throw() => throw new SubscriberFailureException(Message);
}

// --- Scenario: a subscriber that throws inside its async body ---
public sealed class AsyncThrowingEvent { }

public sealed class AsyncThrowingSubscriber : ISubscriber<AsyncThrowingEvent>
{
    public const string Message = "async subscriber failed on purpose";

    public async Task HandleEventAsync(AsyncThrowingEvent eventMessage, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        throw new SubscriberFailureException(Message);
    }
}

/// <summary>EventPublisher propagates the subscriber's own exception, not the reflection wrapper.</summary>
public class EventPublisherExceptionTests
{
    private static async Task Publish<TEvent>(TEvent evt)
    {
        var services = new ServiceCollection();
        services.AddOutbox(typeof(EventPublisherExceptionTests).Assembly);
        services.AddSingleton<InvocationLog>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IEventPublisher>().PublishAsync(evt);
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsSynchronously_PropagatesTheSubscribersExceptionUnwrapped()
    {
        var error = await Record.ExceptionAsync(() => Publish(new SyncThrowingEvent()));

        Assert.NotNull(error);
        Assert.IsNotType<TargetInvocationException>(error);
        var failure = Assert.IsType<SubscriberFailureException>(error);
        Assert.Equal(SyncThrowingSubscriber.Message, failure.Message);
        Assert.Null(failure.InnerException);
        Assert.Contains(nameof(SyncThrowingSubscriber), failure.StackTrace);
    }

    [Fact]
    public async Task Publish_WhenSubscriberThrowsAsynchronously_PropagatesTheSubscribersException()
    {
        var error = await Record.ExceptionAsync(() => Publish(new AsyncThrowingEvent()));

        var failure = Assert.IsType<SubscriberFailureException>(error);
        Assert.Equal(AsyncThrowingSubscriber.Message, failure.Message);
        Assert.Null(failure.InnerException);
        Assert.Contains(nameof(AsyncThrowingSubscriber), failure.StackTrace);
    }
}
