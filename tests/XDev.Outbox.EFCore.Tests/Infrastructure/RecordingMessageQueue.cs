namespace XDev.Outbox.EFCore.Tests.Infrastructure;

/// <summary>A message as observed at the moment it was enqueued.</summary>
public sealed record EnqueuedMessage(int Id, object? Payload, string? Type, OutboxMessage<int> Message);

/// <summary>
/// Test double for the in-memory dispatcher. Records every enqueue and never dispatches.
/// Registered before AddOutbox so the library's TryAddSingleton keeps it.
/// </summary>
public sealed class RecordingMessageQueue : IMessageQueue<int>
{
    private readonly object _gate = new();
    private readonly List<EnqueuedMessage> _items = new();

    public IReadOnlyList<EnqueuedMessage> Enqueued
    {
        get { lock (_gate) return _items.ToArray(); }
    }

    public Task EnqueueAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _items.Add(new EnqueuedMessage(message.Id, message.Payload, message.Type, message));
        }
        return Task.CompletedTask;
    }

    public event Func<OutboxMessage<int>, CancellationToken, Task>? MessageReceived
    {
        add { }
        remove { }
    }

    public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}
