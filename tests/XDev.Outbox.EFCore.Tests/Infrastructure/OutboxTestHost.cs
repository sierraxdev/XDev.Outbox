using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace XDev.Outbox.EFCore.Tests.Infrastructure;

public sealed record OutboxRow(int Id, string Type, string Payload);

/// <summary>
/// One isolated test environment: a private SQLite in-memory database (kept alive by an open connection),
/// a DI container wired the way a consumer would wire it, a recording message queue and a fault injector.
/// The outbox hosted service is registered by AddOutbox but never started.
/// </summary>
public sealed class OutboxTestHost : IDisposable
{
    private OutboxTestHost(SqliteConnection connection, ServiceProvider services, RecordingMessageQueue queue, FaultInjector faults)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingMessageQueue Queue { get; }
    public FaultInjector Faults { get; }

    public static OutboxTestHost Create(bool retryingStrategy = false)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        var queue = new RecordingMessageQueue();
        var faults = new FaultInjector();

        var services = new ServiceCollection();

        // Registered before AddOutbox so its TryAddSingleton keeps this instance.
        services.AddSingleton<IMessageQueue<int>>(queue);

        // AddDbContext must precede AddEFCoreOutboxStore (required on net8.0).
        services.AddDbContext<TestDbContext>(options =>
        {
            options.UseSqlite(connection, sqlite =>
            {
                if (retryingStrategy)
                {
                    sqlite.ExecutionStrategy(d => new TestRetryingExecutionStrategy(d));
                }
            });
            options.AddInterceptors(new FaultCommandInterceptor(faults), new FaultCommitInterceptor(faults));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        });

        // The test assembly declares no ISubscriber<>; the hosted service is never started.
        services.AddOutbox(typeof(OutboxTestHost).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        using (var scope = provider.CreateScope())
        {
            scope.ServiceProvider.GetRequiredService<TestDbContext>().Database.EnsureCreated();
        }

        return new OutboxTestHost(connection, provider, queue, faults);
    }

    public TestScope CreateScope() => new(Services.CreateScope());

    // ---- Direct database inspection (bypasses EF and the outbox). Do not call while a transaction is open. ----

    public long Count(string table)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM \"{table}\"";
        return (long)command.ExecuteScalar()!;
    }

    public IReadOnlyList<int> UserIds() => Ints("SELECT \"Id\" FROM \"Users\" ORDER BY \"Id\"");

    public IReadOnlyList<int> OrderIds() => Ints("SELECT \"Id\" FROM \"Orders\" ORDER BY \"Id\"");

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

    public (string Name, int Version) ReadVersioned(int id)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Name\", \"Version\" FROM \"VersionedItems\" WHERE \"Id\" = $id";
        command.Parameters.AddWithValue("$id", id);
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read(), $"VersionedItems row {id} not found");
        return (reader.GetString(0), reader.GetInt32(1));
    }

    public int SeedVersioned(string name, int version)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "INSERT INTO \"VersionedItems\" (\"Name\", \"Version\") VALUES ($name, $version) RETURNING \"Id\"";
        command.Parameters.AddWithValue("$name", name);
        command.Parameters.AddWithValue("$version", version);
        return Convert.ToInt32(command.ExecuteScalar());
    }

    public int SeedOutboxRow(string type, string payload)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "INSERT INTO \"OutboxMessages\" (\"Type\", \"Payload\", \"Created\", \"LockExpiresAt\") VALUES ($type, $payload, $created, $lock) RETURNING \"Id\"";
        command.Parameters.AddWithValue("$type", type);
        command.Parameters.AddWithValue("$payload", payload);
        command.Parameters.AddWithValue("$created", DateTime.UtcNow);
        command.Parameters.AddWithValue("$lock", DateTime.UtcNow.AddMinutes(5));
        return Convert.ToInt32(command.ExecuteScalar());
    }

    public (int? Attemps, bool? IsError, string? ErrorMessage) ReadOutboxFailureColumns(int id)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Attemps\", \"IsError\", \"ErrorMessage\" FROM \"OutboxMessages\" WHERE \"Id\" = $id";
        command.Parameters.AddWithValue("$id", id);
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read(), $"OutboxMessages row {id} not found");
        return (
            reader.IsDBNull(0) ? null : reader.GetInt32(0),
            reader.IsDBNull(1) ? null : reader.GetBoolean(1),
            reader.IsDBNull(2) ? null : reader.GetString(2));
    }

    private IReadOnlyList<int> Ints(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var values = new List<int>();
        while (reader.Read()) values.Add(reader.GetInt32(0));
        return values;
    }

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}

public sealed class TestScope : IDisposable
{
    private readonly IServiceScope _scope;

    public TestScope(IServiceScope scope)
    {
        _scope = scope;
        Db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Outbox = Db.Outbox;
    }

    public IServiceProvider Services => _scope.ServiceProvider;
    public TestDbContext Db { get; }
    public IOutbox Outbox { get; }

    public int TrackedOutboxMessages => Db.ChangeTracker.Entries<OutboxMessage<int>>().Count();

    public void Dispose() => _scope.Dispose();
}

public enum SaveCall
{
    SaveChanges,
    SaveChangesAccept,
    SaveChangesAsync,
    SaveChangesAsyncAccept,
}

public static class Save
{
    /// <summary>Invokes one specific public save overload.</summary>
    public static async Task<int> Invoke(DbContext db, SaveCall call) => call switch
    {
        SaveCall.SaveChanges => db.SaveChanges(),
        SaveCall.SaveChangesAccept => db.SaveChanges(acceptAllChangesOnSuccess: true),
        SaveCall.SaveChangesAsync => await db.SaveChangesAsync(CancellationToken.None),
        SaveCall.SaveChangesAsyncAccept => await db.SaveChangesAsync(acceptAllChangesOnSuccess: true, CancellationToken.None),
        _ => throw new ArgumentOutOfRangeException(nameof(call)),
    };

    /// <summary>Invokes the bool overload, sync or async.</summary>
    public static async Task<int> Invoke(DbContext db, bool async, bool acceptAllChangesOnSuccess)
        => async
            ? await db.SaveChangesAsync(acceptAllChangesOnSuccess, CancellationToken.None)
            : db.SaveChanges(acceptAllChangesOnSuccess);

    /// <summary>Invokes the parameterless overload, sync or async.</summary>
    public static async Task<int> Invoke(DbContext db, bool async)
        => async ? await db.SaveChangesAsync(CancellationToken.None) : db.SaveChanges();
}

public static class Payload
{
    /// <summary>Parses a persisted payload and returns its "user" object.</summary>
    public static JsonElement User(string payloadJson)
    {
        using var doc = JsonDocument.Parse(payloadJson);
        return doc.RootElement.GetProperty("user").Clone();
    }

    /// <summary>Serializes an in-memory payload (event instance or ExpandoObject) the way the library does.</summary>
    public static string ToJson(object? payload) => JsonSerializer.Serialize(payload, OutboxConfiguration.SerializerOptions);
}

public static class ExceptionChain
{
    public static bool Contains<T>(Exception? exception) where T : Exception
    {
        for (var e = exception; e != null; e = e.InnerException)
        {
            if (e is T) return true;
        }
        return false;
    }
}
