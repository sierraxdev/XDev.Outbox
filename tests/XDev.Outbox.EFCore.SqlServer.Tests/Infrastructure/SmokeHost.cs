using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

/// <summary>An enqueued message; <see cref="RowStateAtEnqueue"/> is "committed", "absent" or "locked" (uncommitted).</summary>
public sealed record EnqueuedMessage<TKey>(TKey Id, object? Payload, string? Type, string? RowStateAtEnqueue, DateTime? LockExpiresAt = null) where TKey : struct;

/// <summary>Records every enqueue and never dispatches. Registered before AddOutbox so its TryAdd keeps it.</summary>
public sealed class RecordingMessageQueue<TKey> : IMessageQueue<TKey> where TKey : struct
{
    private readonly object _gate = new();
    private readonly List<EnqueuedMessage<TKey>> _items = new();

    public IReadOnlyList<EnqueuedMessage<TKey>> Enqueued
    {
        get { lock (_gate) return _items.ToArray(); }
    }

    /// <summary>Called at enqueue time to observe, from another connection, whether the row is committed.</summary>
    public Func<TKey, string>? RowProbe { get; set; }

    public Task EnqueueAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
    {
        var state = RowProbe?.Invoke(message.Id);
        lock (_gate) _items.Add(new EnqueuedMessage<TKey>(message.Id, message.Payload, message.Type, state, message.LockExpiresAt));
        return Task.CompletedTask;
    }

    public event Func<OutboxMessage<TKey>, CancellationToken, Task>? MessageReceived
    {
        add { }
        remove { }
    }

    public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}

public enum Registration
{
    AddDbContext,
    AddDbContextPool,
}

public sealed class SmokeHostOptions
{
    public bool RetryingStrategy { get; init; }
    public Registration Registration { get; init; } = Registration.AddDbContext;
    public int PoolSize { get; init; } = 1;
    public Action<OutboxDbContextConfiguration>? StoreConfiguration { get; init; }

    /// <summary>Use the library's MemoryMessageQueue (real dispatch) instead of the recording queue.</summary>
    public bool RealQueue { get; init; }

    /// <summary>MultipleActiveResultSets=True on the context's connection string (EF then uses no savepoints).</summary>
    public bool MultipleActiveResultSets { get; init; }
}

/// <summary>
/// A DI container wired the way a consumer wires it, against the fixture's SQL Server database: the context
/// registration under test, AddOutbox, AddEFCoreOutboxStore, a recording queue (unless <see cref="SmokeHostOptions.RealQueue"/>)
/// and a fault injector that also records SQL. The hosted service is registered but not started.
/// Tables are created on first use and emptied for every new host.
/// </summary>
public sealed class SmokeHost<TKey, TContext> : IDisposable
    where TKey : struct
    where TContext : SmokeContextBase<TKey>
{
    private SmokeHost(SmokeDatabase database, ServiceProvider services, RecordingMessageQueue<TKey> queue, FaultInjector faults, string outboxTable, OutboxColumns outboxColumns)
    {
        OutboxTable = outboxTable;
        OutboxColumns = outboxColumns;
        Database = database;
        Services = services;
        Queue = queue;
        Faults = faults;
    }

    public SmokeDatabase Database { get; }
    public ServiceProvider Services { get; }
    public RecordingMessageQueue<TKey> Queue { get; }
    public FaultInjector Faults { get; }

    /// <summary>The outbox table as mapped by the model, delimited: [schema].[table].</summary>
    public string OutboxTable { get; }

    /// <summary>The outbox columns as mapped by the model, delimited.</summary>
    public OutboxColumns OutboxColumns { get; }

    public List<OutboxRow> OutboxRows() => Database.OutboxRows(OutboxTable, OutboxColumns);

    public static SmokeHost<TKey, TContext> Create(SmokeDatabaseFixture fixture, SmokeHostOptions? options = null, Action<IServiceCollection>? configure = null)
    {
        options ??= new SmokeHostOptions();
        var database = fixture.Database;
        var queue = new RecordingMessageQueue<TKey>();
        var faults = new FaultInjector();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<SubscriberProbe>();
        if (!options.RealQueue) services.AddSingleton<IMessageQueue<TKey>>(queue);

        void ConfigureContext(DbContextOptionsBuilder builder)
        {
            var connectionString = options.MultipleActiveResultSets
                ? new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(database.ConnectionString) { MultipleActiveResultSets = true }.ConnectionString
                : database.ConnectionString;
            builder.UseSqlServer(connectionString, sql =>
            {
                if (options.RetryingStrategy) sql.ExecutionStrategy(d => new SmokeRetryingExecutionStrategy(d));
            });
            builder.AddInterceptors(new FaultCommandInterceptor(faults), new FaultTransactionInterceptor(faults));
            builder.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        }

        // The DbContext registration precedes AddEFCoreOutboxStore (required on net8.0).
        if (options.Registration == Registration.AddDbContextPool)
        {
            services.AddDbContextPool<TContext>(ConfigureContext, options.PoolSize);
        }
        else
        {
            services.AddDbContext<TContext>(ConfigureContext);
        }

        services.AddOutbox<TKey>(typeof(SmokeHost<,>).Assembly);
        services.AddEFCoreOutboxStore<TKey, TContext>(options.StoreConfiguration);
        configure?.Invoke(services);

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        string outboxTable;
        OutboxColumns outboxColumns;
        using (var scope = provider.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TContext>();
            fixture.EnsureTables(context);
            var entityType = context.Model.FindEntityType(typeof(OutboxMessage<TKey>))!;
            outboxTable = $"[{(entityType.GetSchema() ?? "dbo").Replace("]", "]]")}].[{entityType.GetTableName()!.Replace("]", "]]")}]";
            outboxColumns = OutboxColumns.Of(entityType);
        }
        database.DeleteAllRows();
        faults.ClearLog();
        queue.RowProbe = id => database.RowState(outboxTable, id!, outboxColumns.Id);

        return new SmokeHost<TKey, TContext>(database, provider, queue, faults, outboxTable, outboxColumns);
    }

    public SmokeScope<TContext> CreateScope() => new(Services.CreateAsyncScope());

    public void Dispose() => Services.Dispose();
}

public sealed class SmokeScope<TContext> : IDisposable, IAsyncDisposable where TContext : DbContext
{
    private readonly AsyncServiceScope _scope;

    public SmokeScope(AsyncServiceScope scope)
    {
        _scope = scope;
        Db = scope.ServiceProvider.GetRequiredService<TContext>();
    }

    public IServiceProvider Services => _scope.ServiceProvider;
    public TContext Db { get; }

    public void Dispose() => _scope.Dispose();

    public ValueTask DisposeAsync() => _scope.DisposeAsync();
}

/// <summary>The public save overloads (and the flag value for the bool ones).</summary>
public enum SaveCall
{
    SaveChanges,
    SaveChangesTrue,
    SaveChangesFalse,
    SaveChangesAsync,
    SaveChangesAsyncTrue,
    SaveChangesAsyncFalse,
}

public static class Save
{
    public static async Task<int> Invoke(DbContext db, SaveCall call) => call switch
    {
        SaveCall.SaveChanges => db.SaveChanges(),
        SaveCall.SaveChangesTrue => db.SaveChanges(true),
        SaveCall.SaveChangesFalse => db.SaveChanges(false),
        SaveCall.SaveChangesAsync => await db.SaveChangesAsync(CancellationToken.None),
        SaveCall.SaveChangesAsyncTrue => await db.SaveChangesAsync(true, CancellationToken.None),
        SaveCall.SaveChangesAsyncFalse => await db.SaveChangesAsync(false, CancellationToken.None),
        _ => throw new ArgumentOutOfRangeException(nameof(call)),
    };

    public static bool Accepts(SaveCall call) => call is not (SaveCall.SaveChangesFalse or SaveCall.SaveChangesAsyncFalse);

    public static async Task<int> Invoke(DbContext db, bool async)
        => async ? await db.SaveChangesAsync(CancellationToken.None) : db.SaveChanges();
}

public static class Json
{
    public static JsonElement Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    /// <summary>Serializes an in-memory payload (event or ExpandoObject) the way the library does.</summary>
    public static string Serialize(object? payload) => JsonSerializer.Serialize(payload, OutboxConfiguration.SerializerOptions);
}

/// <summary>Invocations observed by <see cref="ProbeSubscriber"/>.</summary>
public sealed class SubscriberProbe
{
    private readonly object _gate = new();
    private readonly List<ProbeEvent> _received = new();

    public IReadOnlyList<ProbeEvent> Received
    {
        get { lock (_gate) return _received.ToArray(); }
    }

    public void Record(ProbeEvent @event)
    {
        lock (_gate) _received.Add(@event);
    }
}

public sealed class ProbeSubscriber : ISubscriber<ProbeEvent>
{
    private readonly SubscriberProbe _probe;

    public ProbeSubscriber(SubscriberProbe probe) => _probe = probe;

    public Task HandleEventAsync(ProbeEvent eventMessage, CancellationToken cancellationToken = default)
    {
        _probe.Record(eventMessage);
        return Task.CompletedTask;
    }
}
