using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace XDev.Outbox.Tests;

// IMPORTANT: AddOutbox scans the whole test assembly, so every ISubscriber<> implementation in this
// assembly is discovered by every test. Each scenario therefore uses its own event type, and all
// subscribers must be concrete, non-generic classes so that scanning this assembly stays valid.

/// <summary>Records subscriber invocations. Registered as a singleton per service provider.</summary>
public sealed class InvocationLog
{
    private readonly List<(Type Subscriber, object Event)> _entries = new();

    public IReadOnlyList<(Type Subscriber, object Event)> Entries
    {
        get { lock (_entries) return _entries.ToList(); }
    }

    public void Record(object subscriber, object evt)
    {
        lock (_entries) _entries.Add((subscriber.GetType(), evt));
    }

    public int Count<TSubscriber>() => Entries.Count(e => e.Subscriber == typeof(TSubscriber));
}

public abstract class RecordingSubscriber
{
    private readonly InvocationLog _log;

    protected RecordingSubscriber(InvocationLog log) => _log = log;

    protected Task Record(object evt)
    {
        _log.Record(this, evt);
        return Task.CompletedTask;
    }
}

// --- Scenario: two subscriber classes for the same event type ---
public sealed class TwoSubscribersEvent { }

public sealed class TwoSubscribersFirst(InvocationLog log) : RecordingSubscriber(log), ISubscriber<TwoSubscribersEvent>
{
    public Task HandleEventAsync(TwoSubscribersEvent eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);
}

public sealed class TwoSubscribersSecond(InvocationLog log) : RecordingSubscriber(log), ISubscriber<TwoSubscribersEvent>
{
    public Task HandleEventAsync(TwoSubscribersEvent eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);
}

// --- Scenario: one class subscribing to two event types ---
public sealed class MultiInterfaceEventA { }

public sealed class MultiInterfaceEventB { }

public sealed class MultiInterfaceSubscriber(InvocationLog log) : RecordingSubscriber(log), ISubscriber<MultiInterfaceEventA>, ISubscriber<MultiInterfaceEventB>
{
    public Task HandleEventAsync(MultiInterfaceEventA eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);

    public Task HandleEventAsync(MultiInterfaceEventB eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);
}

// --- Scenario: a manual registration before AddOutbox ---
public sealed class ManualRegistrationEvent { }

public sealed class ManualRegistrationManual(InvocationLog log) : RecordingSubscriber(log), ISubscriber<ManualRegistrationEvent>
{
    public Task HandleEventAsync(ManualRegistrationEvent eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);
}

public sealed class ManualRegistrationScanned(InvocationLog log) : RecordingSubscriber(log), ISubscriber<ManualRegistrationEvent>
{
    public Task HandleEventAsync(ManualRegistrationEvent eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);
}

// --- Scenario: single subscriber (baseline) ---
public sealed class SingleSubscriberEvent { }

public sealed class SingleSubscriber(InvocationLog log) : RecordingSubscriber(log), ISubscriber<SingleSubscriberEvent>
{
    public Task HandleEventAsync(SingleSubscriberEvent eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);
}

// --- Scenario: no subscriber ---
public sealed class NoSubscriberEvent { }

// --- Scenario: a failing subscriber ---
public sealed class FailingEvent { }

public sealed class FailingEventException : Exception
{
    public FailingEventException() : base("subscriber failed") { }
}

public sealed class FailingEventThrower(InvocationLog log) : RecordingSubscriber(log), ISubscriber<FailingEvent>
{
    public async Task HandleEventAsync(FailingEvent eventMessage, CancellationToken cancellationToken = default)
    {
        await Record(eventMessage);
        await Task.Yield();
        throw new FailingEventException();
    }
}

public sealed class FailingEventOther(InvocationLog log) : RecordingSubscriber(log), ISubscriber<FailingEvent>
{
    public Task HandleEventAsync(FailingEvent eventMessage, CancellationToken cancellationToken = default) => Record(eventMessage);
}

/// <summary>
/// Bug-check finding #5: AddOutbox must register every distinct ISubscriber&lt;X&gt; implementation it
/// discovers, so EventPublisher invokes all of them, without duplicating any registration.
/// </summary>
public class SubscriberRegistrationTests
{
    private static readonly Assembly Scanned = typeof(SubscriberRegistrationTests).Assembly;

    private static ServiceProvider Build(Action<IServiceCollection>? beforeAddOutbox = null, int addOutboxCalls = 1)
    {
        var services = new ServiceCollection();
        services.AddSingleton<InvocationLog>();
        beforeAddOutbox?.Invoke(services);
        for (var i = 0; i < addOutboxCalls; i++)
        {
            services.AddOutbox(Scanned);
        }
        return services.BuildServiceProvider();
    }

    private static async Task Publish<TEvent>(ServiceProvider provider, TEvent evt)
    {
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IEventPublisher>().PublishAsync(evt);
    }

    private static Type[] ResolvedSubscriberTypes<TEvent>(ServiceProvider provider)
    {
        using var scope = provider.CreateScope();
        return scope.ServiceProvider.GetServices<ISubscriber<TEvent>>().Select(s => s!.GetType()).ToArray();
    }

    [Fact]
    public async Task Publish_WhenSingleSubscriberExists_InvokesItOnce()
    {
        using var provider = Build();
        var evt = new SingleSubscriberEvent();

        await Publish(provider, evt);

        var entry = Assert.Single(provider.GetRequiredService<InvocationLog>().Entries);
        Assert.Equal(typeof(SingleSubscriber), entry.Subscriber);
        Assert.Same(evt, entry.Event);
    }

    [Fact]
    public void AddOutbox_WhenTwoSubscribersHandleSameEvent_RegistersBoth()
    {
        using var provider = Build();

        var resolved = ResolvedSubscriberTypes<TwoSubscribersEvent>(provider);

        Assert.Equal(2, resolved.Length);
        Assert.Contains(typeof(TwoSubscribersFirst), resolved);
        Assert.Contains(typeof(TwoSubscribersSecond), resolved);
    }

    [Fact]
    public async Task Publish_WhenTwoSubscribersHandleSameEvent_InvokesEachExactlyOnce()
    {
        using var provider = Build();
        var evt = new TwoSubscribersEvent();

        await Publish(provider, evt);

        var log = provider.GetRequiredService<InvocationLog>();
        Assert.Equal(2, log.Entries.Count);
        Assert.Equal(1, log.Count<TwoSubscribersFirst>());
        Assert.Equal(1, log.Count<TwoSubscribersSecond>());
        Assert.All(log.Entries, e => Assert.Same(evt, e.Event));
    }

    [Fact]
    public async Task Publish_WhenOneClassSubscribesToTwoEvents_DispatchesEachEventToItOnce()
    {
        using var provider = Build();
        var a = new MultiInterfaceEventA();
        var b = new MultiInterfaceEventB();

        Assert.Equal(new[] { typeof(MultiInterfaceSubscriber) }, ResolvedSubscriberTypes<MultiInterfaceEventA>(provider));
        Assert.Equal(new[] { typeof(MultiInterfaceSubscriber) }, ResolvedSubscriberTypes<MultiInterfaceEventB>(provider));

        await Publish(provider, a);
        await Publish(provider, b);

        var entries = provider.GetRequiredService<InvocationLog>().Entries;
        Assert.Equal(2, entries.Count);
        Assert.Single(entries, e => ReferenceEquals(e.Event, a));
        Assert.Single(entries, e => ReferenceEquals(e.Event, b));
        Assert.All(entries, e => Assert.Equal(typeof(MultiInterfaceSubscriber), e.Subscriber));
    }

    [Fact]
    public async Task AddOutbox_WhenCalledTwiceWithSameAssembly_RegistersAndInvokesEachSubscriberOnce()
    {
        using var provider = Build(addOutboxCalls: 2);

        var resolved = ResolvedSubscriberTypes<TwoSubscribersEvent>(provider);
        Assert.Equal(2, resolved.Length);
        Assert.Equal(2, resolved.Distinct().Count());
        Assert.Single(ResolvedSubscriberTypes<SingleSubscriberEvent>(provider));
        Assert.Single(ResolvedSubscriberTypes<MultiInterfaceEventA>(provider));

        await Publish(provider, new TwoSubscribersEvent());
        await Publish(provider, new SingleSubscriberEvent());

        var log = provider.GetRequiredService<InvocationLog>();
        Assert.Equal(1, log.Count<TwoSubscribersFirst>());
        Assert.Equal(1, log.Count<TwoSubscribersSecond>());
        Assert.Equal(1, log.Count<SingleSubscriber>());
        Assert.Equal(3, log.Entries.Count);
    }

    [Fact]
    public async Task AddOutbox_WhenSameAssemblyPassedTwiceInOneCall_InvokesEachSubscriberOnce()
    {
        var services = new ServiceCollection();
        services.AddSingleton<InvocationLog>();
        services.AddOutbox(Scanned, Scanned);
        using var provider = services.BuildServiceProvider();

        await Publish(provider, new TwoSubscribersEvent());

        var log = provider.GetRequiredService<InvocationLog>();
        Assert.Equal(1, log.Count<TwoSubscribersFirst>());
        Assert.Equal(1, log.Count<TwoSubscribersSecond>());
        Assert.Equal(2, log.Entries.Count);
    }

    [Fact]
    public async Task AddOutbox_WhenSubscriberRegisteredManuallyFirst_RegistersOtherScannedSubscribersWithoutDuplicatingTheManualOne()
    {
        // ManualRegistrationManual is registered by type before AddOutbox and is also discovered by
        // the scan; ManualRegistrationScanned is only registered by the scan.
        using var provider = Build(s => s.AddScoped<ISubscriber<ManualRegistrationEvent>, ManualRegistrationManual>());

        var resolved = ResolvedSubscriberTypes<ManualRegistrationEvent>(provider);
        Assert.Equal(2, resolved.Length);
        Assert.Contains(typeof(ManualRegistrationManual), resolved);
        Assert.Contains(typeof(ManualRegistrationScanned), resolved);

        await Publish(provider, new ManualRegistrationEvent());

        var log = provider.GetRequiredService<InvocationLog>();
        Assert.Equal(1, log.Count<ManualRegistrationManual>());
        Assert.Equal(1, log.Count<ManualRegistrationScanned>());
        Assert.Equal(2, log.Entries.Count);
    }

    [Fact]
    public async Task AddOutbox_WhenSubscriberRegisteredManuallyFirst_ManualSubscriberRunsFirst()
    {
        using var provider = Build(s => s.AddScoped<ISubscriber<ManualRegistrationEvent>, ManualRegistrationManual>());

        await Publish(provider, new ManualRegistrationEvent());

        var entries = provider.GetRequiredService<InvocationLog>().Entries;
        Assert.NotEmpty(entries);
        Assert.Equal(typeof(ManualRegistrationManual), entries[0].Subscriber);
    }

    [Fact]
    public async Task Publish_WhenEventHasNoSubscribers_CompletesWithoutError()
    {
        using var provider = Build();

        Assert.Empty(ResolvedSubscriberTypes<NoSubscriberEvent>(provider));
        await Publish(provider, new NoSubscriberEvent());

        Assert.Empty(provider.GetRequiredService<InvocationLog>().Entries);
    }

    [Fact]
    public async Task Publish_WhenFirstRegisteredSubscriberThrows_PropagatesExceptionAndSkipsLaterSubscribers()
    {
        // Explicit registration before AddOutbox makes the order deterministic: thrower, then the other.
        using var provider = Build(s =>
        {
            s.AddScoped<ISubscriber<FailingEvent>, FailingEventThrower>();
            s.AddScoped<ISubscriber<FailingEvent>, FailingEventOther>();
        });

        Assert.Equal(new[] { typeof(FailingEventThrower), typeof(FailingEventOther) }, ResolvedSubscriberTypes<FailingEvent>(provider));

        await Assert.ThrowsAsync<FailingEventException>(() => Publish(provider, new FailingEvent()));

        var log = provider.GetRequiredService<InvocationLog>();
        Assert.Equal(1, log.Count<FailingEventThrower>());
        Assert.Equal(0, log.Count<FailingEventOther>());
    }

    [Fact]
    public async Task Publish_WhenOneOfSeveralScannedSubscribersThrows_PropagatesExceptionAndInvokesThrowerOnce()
    {
        using var provider = Build();

        Assert.Equal(2, ResolvedSubscriberTypes<FailingEvent>(provider).Length);

        await Assert.ThrowsAsync<FailingEventException>(() => Publish(provider, new FailingEvent()));

        Assert.Equal(1, provider.GetRequiredService<InvocationLog>().Count<FailingEventThrower>());
    }
}
