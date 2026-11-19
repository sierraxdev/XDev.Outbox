using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>
/// Like <see cref="RegistrationHost"/>, plus fault injection and an optional retrying execution strategy. All contexts
/// share one open SQLite in-memory connection, so contexts must be used sequentially. Scope validation is on.
/// </summary>
public sealed class PoolHost : IDisposable
{
    private PoolHost(SqliteConnection connection, ServiceProvider services, RecordingMessageQueue queue, FaultInjector faults, ContextRegistration registration)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
        Registration = registration;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingMessageQueue Queue { get; }
    public FaultInjector Faults { get; }
    public ContextRegistration Registration { get; }

    public bool IsPooled => Registration is ContextRegistration.AddDbContextPool or ContextRegistration.PooledFactory or ContextRegistration.PooledFactoryWithScopedContext;

    public static PoolHost Create(ContextRegistration registration, int poolSize = 1, bool retrying = false, Action<IServiceCollection>? configure = null)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        var schemaOptions = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(connection)
            .UseApplicationServiceProvider(new ServiceCollection().AddOptions().BuildServiceProvider())
            .ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning))
            .Options;
        using (var schema = new TestDbContext(schemaOptions))
        {
            schema.Database.EnsureCreated();
        }

        var queue = new RecordingMessageQueue();
        var faults = new FaultInjector();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        RegistrationHost.RegisterContext(services, registration, options =>
        {
            options.UseSqlite(connection, sqlite =>
            {
                if (retrying) sqlite.ExecutionStrategy(d => new TestRetryingExecutionStrategy(d));
            });
            options.AddInterceptors(new FaultCommandInterceptor(faults), new FaultCommitInterceptor(faults));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        }, poolSize);
        services.AddOutbox(typeof(PoolHost).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        configure?.Invoke(services);

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        return new PoolHost(connection, provider, queue, faults, registration);
    }

    /// <summary>A scope plus the context the application uses in it (scoped registration or factory-created).</summary>
    public PoolLease Lease()
    {
        var scope = Services.CreateAsyncScope();
        switch (Registration)
        {
            case ContextRegistration.AddDbContext:
            case ContextRegistration.AddDbContextPool:
            case ContextRegistration.PooledFactoryWithScopedContext:
                return new PoolLease(scope, scope.ServiceProvider.GetRequiredService<TestDbContext>(), owned: false);
            default:
                return new PoolLease(scope, scope.ServiceProvider.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext(), owned: true);
        }
    }

    public long Count(string table)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM \"{table}\"";
        return (long)command.ExecuteScalar()!;
    }

    public IReadOnlyList<OutboxRow> OutboxRows()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Type\", \"Payload\" FROM \"OutboxMessages\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<OutboxRow>();
        while (reader.Read())
        {
            rows.Add(new OutboxRow(reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
        }
        return rows;
    }

    public IReadOnlyList<(int Id, string Email)> Users()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Email\" FROM \"Users\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, string)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    public int SeedOutboxRow()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "INSERT INTO \"OutboxMessages\" (\"Type\", \"Payload\", \"Created\", \"LockExpiresAt\") VALUES ('t', '{}', $created, $lock) RETURNING \"Id\"";
        command.Parameters.AddWithValue("$created", DateTime.UtcNow);
        command.Parameters.AddWithValue("$lock", DateTime.UtcNow.AddMinutes(5));
        return Convert.ToInt32(command.ExecuteScalar());
    }

    /// <summary>No phantom: every enqueued message has a committed outbox row, and none is enqueued twice.</summary>
    public void AssertEveryDispatchHasACommittedRow()
    {
        var rowIds = OutboxRows().Select(x => x.Id).ToHashSet();
        var enqueued = Queue.Enqueued.Select(x => x.Id).ToList();
        Assert.All(enqueued, id => Assert.Contains(id, rowIds));
        Assert.Equal(enqueued.Count, enqueued.Distinct().Count());
    }

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}

public sealed class PoolLease
{
    private readonly AsyncServiceScope _scope;
    private readonly bool _owned;

    public PoolLease(AsyncServiceScope scope, TestDbContext db, bool owned)
    {
        _scope = scope;
        Db = db;
        _owned = owned;
    }

    public TestDbContext Db { get; }
    public IServiceProvider Services => _scope.ServiceProvider;

    /// <summary>Ends the lease: disposes the factory-created context (if any) and the scope, sync or async.</summary>
    public async Task End(bool asyncDispose)
    {
        if (_owned)
        {
            if (asyncDispose) await Db.DisposeAsync(); else Db.Dispose();
        }
        if (asyncDispose) await _scope.DisposeAsync(); else _scope.Dispose();
    }
}
