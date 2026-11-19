using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace XDev.Outbox.EFCore.Tests.Infrastructure;

/// <summary>
/// SQLite in-memory host where <see cref="TestDbContext"/> is registered with a chosen
/// <see cref="ContextRegistration"/> (scoped, pooled, factory). Scope validation is on. All contexts (pooled ones
/// included) share one open connection, so tests must use contexts sequentially.
/// </summary>
public sealed class RegistrationHost : IDisposable
{
    private RegistrationHost(SqliteConnection connection, ServiceProvider services, RecordingMessageQueue queue, ContextRegistration registration)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Registration = registration;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingMessageQueue Queue { get; }
    public ContextRegistration Registration { get; }

    public static RegistrationHost Create(
        ContextRegistration registration,
        int poolSize = 1024,
        Action<IServiceCollection>? beforeOutbox = null,
        Action<IServiceCollection>? configure = null)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        // The schema is created by a standalone context so no lease of the registration under test is used.
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
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        RegisterContext(services, registration, options =>
        {
            options.UseSqlite(connection);
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        }, poolSize);
        beforeOutbox?.Invoke(services);
        services.AddOutbox(typeof(RegistrationHost).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        configure?.Invoke(services);

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        return new RegistrationHost(connection, provider, queue, registration);
    }

    public static void RegisterContext(IServiceCollection services, ContextRegistration registration, Action<DbContextOptionsBuilder> options, int poolSize)
    {
        switch (registration)
        {
            case ContextRegistration.AddDbContext:
                services.AddDbContext<TestDbContext>(options);
                break;
            case ContextRegistration.AddDbContextPool:
                services.AddDbContextPool<TestDbContext>(options, poolSize);
                break;
            case ContextRegistration.PooledFactory:
                services.AddPooledDbContextFactory<TestDbContext>(options, poolSize);
                break;
            case ContextRegistration.PooledFactoryWithScopedContext:
                services.AddPooledDbContextFactory<TestDbContext>(options, poolSize);
                services.AddScoped(sp => sp.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext());
                break;
            case ContextRegistration.Factory:
                services.AddDbContextFactory<TestDbContext>(options);
                break;
            case ContextRegistration.FactoryScoped:
                services.AddDbContextFactory<TestDbContext>(options, ServiceLifetime.Scoped);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(registration));
        }
    }

    /// <summary>
    /// The context the application would use in this scope: the scoped registration, or a context created from the
    /// factory (disposed with the returned lease).
    /// </summary>
    public ContextLease Lease(IServiceScope scope)
    {
        switch (Registration)
        {
            case ContextRegistration.AddDbContext:
            case ContextRegistration.AddDbContextPool:
            case ContextRegistration.PooledFactoryWithScopedContext:
                return new ContextLease(scope.ServiceProvider.GetRequiredService<TestDbContext>(), owned: false);
            default:
                return new ContextLease(scope.ServiceProvider.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext(), owned: true);
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

    public int SeedOutboxRow()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "INSERT INTO \"OutboxMessages\" (\"Type\", \"Payload\", \"Created\", \"LockExpiresAt\") VALUES ('t', '{}', $created, $lock) RETURNING \"Id\"";
        command.Parameters.AddWithValue("$created", DateTime.UtcNow);
        command.Parameters.AddWithValue("$lock", DateTime.UtcNow.AddMinutes(5));
        return Convert.ToInt32(command.ExecuteScalar());
    }

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}

public sealed class ContextLease : IDisposable
{
    private readonly bool _owned;

    public ContextLease(TestDbContext db, bool owned)
    {
        Db = db;
        _owned = owned;
    }

    public TestDbContext Db { get; }

    public void Dispose()
    {
        if (_owned) Db.Dispose();
    }
}
