using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace XDev.Outbox.Tests;

// Event and subscriber types are unique to this file (AddOutbox scans the whole test assembly).

public sealed class TypeNameItem
{
    public int Id { get; set; }
}

public sealed class TypeNameEvent<TEntity, TKey>
{
    public TEntity? Entity { get; set; }
    public TKey? Key { get; set; }
}

public sealed class TypeNameEventSubscriber(InvocationLog log) : ISubscriber<TypeNameEvent<TypeNameItem, Guid>>
{
    public Task HandleEventAsync(TypeNameEvent<TypeNameItem, Guid> eventMessage, CancellationToken cancellationToken = default)
    {
        log.Record(this, eventMessage);
        return Task.CompletedTask;
    }
}

/// <summary>
/// OutboxProcessorHostedService resolves the stored type name with Type.GetType: the version-less name the EF store
/// writes for new rows (no Version/Culture/PublicKeyToken, generic arguments included) and the full
/// assembly-qualified name of rows written before that change.
/// </summary>
public class TypeNameResolutionTests
{
    private static readonly Guid Key = Guid.Parse("8f0b5a52-63c5-4d4f-9a2c-0d6f1f3a9b11");

    private static async Task<(Exception? Error, RecordingStore Store, InvocationLog Log)> Deliver(string typeName)
    {
        var store = new RecordingStore();
        var queue = new CapturingQueue();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddScoped<IOutboxStore<int>>(_ => store);
        services.AddOutbox(typeof(TypeNameResolutionTests).Assembly);
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

            var payload = new TypeNameEvent<TypeNameItem, Guid> { Entity = new TypeNameItem { Id = 5 }, Key = Key };
            using var json = JsonDocument.Parse(JsonSerializer.Serialize(payload, OutboxConfiguration.SerializerOptions));
            var message = new OutboxMessage<int>
            {
                Id = 11,
                Type = typeName,
                Payload = json.RootElement.Clone(),
                Created = DateTime.UtcNow,
            };
            var error = await Record.ExceptionAsync(() => queue.Handler!(message, CancellationToken.None));
            return (error, store, provider.GetRequiredService<InvocationLog>());
        }
        finally
        {
            cts.Cancel();
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    public static TheoryData<string> ResolvableNames() => new()
    {
        // Version-less, as stored for new rows.
        $"{typeof(TypeNameEvent<,>).FullName}[[{typeof(TypeNameItem).FullName}, XDev.Outbox.Tests],[System.Guid, System.Private.CoreLib]], XDev.Outbox.Tests",
        // Full assembly-qualified name, as stored by earlier versions.
        typeof(TypeNameEvent<TypeNameItem, Guid>).AssemblyQualifiedName!,
    };

    [Theory]
    [MemberData(nameof(ResolvableNames))]
    public async Task HandleMessage_ResolvesTheStoredTypeNameAndDispatchesTheTypedPayload(string typeName)
    {
        var (error, store, log) = await Deliver(typeName);

        Assert.Null(error);
        var entry = Assert.Single(log.Entries);
        Assert.Equal(typeof(TypeNameEventSubscriber), entry.Subscriber);
        var delivered = Assert.IsType<TypeNameEvent<TypeNameItem, Guid>>(entry.Event);
        Assert.Equal(5, delivered.Entity!.Id);
        Assert.Equal(Key, delivered.Key);
        Assert.Equal(11, Assert.Single(store.Removed).Id);
        Assert.Empty(store.Updated);
    }

    [Fact]
    public async Task HandleMessage_WhenTheTypeCannotBeLoaded_RecordsTheFailure()
    {
        var (error, store, log) = await Deliver("Missing.Namespace.MissingEvent, Missing.Assembly");

        Assert.NotNull(error);
        Assert.Empty(log.Entries);
        var updated = Assert.Single(store.Updated);
        Assert.Equal(1, updated.Attemps);
        Assert.Empty(store.Removed);
    }
}
