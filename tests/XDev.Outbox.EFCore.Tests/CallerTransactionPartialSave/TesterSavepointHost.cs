using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Variant of <see cref="SavepointHost"/> for the adversarial (tester) suite: optional pooled registration, optional
/// retrying execution strategy, and a <see cref="SavepointSabotage"/> interceptor that throws in or suppresses the
/// N-th savepoint creation/release and can cancel a token at a savepoint event.
/// </summary>
public sealed class TesterSavepointHost : IDisposable
{
    private TesterSavepointHost(SqliteConnection connection, ServiceProvider services, RecordingQueue<int> queue, FaultInjector faults, SavepointProbe probe, SavepointSabotage sabotage, LogCapture log)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
        Probe = probe;
        Sabotage = sabotage;
        Log = log;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingQueue<int> Queue { get; }
    public FaultInjector Faults { get; }
    public SavepointProbe Probe { get; }
    public SavepointSabotage Sabotage { get; }
    public LogCapture Log { get; }

    public static TesterSavepointHost Create(bool pooled = false, bool retrying = false)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingQueue<int>();
        var faults = new FaultInjector();
        var probe = new SavepointProbe();
        var sabotage = new SavepointSabotage();
        var log = new LogCapture();
        var loggerFactory = LoggerFactory.Create(b => b.AddProvider(log));

        void Configure(DbContextOptionsBuilder options)
        {
            options.UseSqlite(connection, sqlite =>
            {
                if (retrying) sqlite.ExecutionStrategy(d => new TestRetryingExecutionStrategy(d));
            });
            options.UseLoggerFactory(loggerFactory);
            options.AddInterceptors(
                new FaultCommandInterceptor(faults),
                new FaultCommitInterceptor(faults),
                new ProbeCommandInterceptor(probe),
                new ProbeTransactionInterceptor(probe),
                new SabotageTransactionInterceptor(sabotage));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        }

        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        if (pooled) services.AddDbContextPool<AdvDbContext>(Configure, poolSize: 1);
        else services.AddDbContext<AdvDbContext>(Configure);
        services.AddOutbox<int>(typeof(TesterSavepointHost).Assembly);
        services.AddEFCoreOutboxStore<int, AdvDbContext>();

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using (var scope = provider.CreateScope())
        {
            scope.ServiceProvider.GetRequiredService<AdvDbContext>().Database.EnsureCreated();
        }
        faults.ExecutedCommands.Clear();
        return new TesterSavepointHost(connection, provider, queue, faults, probe, sabotage, log);
    }

    public (IServiceScope Scope, AdvDbContext Db, IOutbox Outbox) CreateScope()
    {
        var scope = Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AdvDbContext>();
        return (scope, db, db.Outbox);
    }

    /// <summary>Direct read on the host connection. Do not call while a transaction is open (use the context).</summary>
    public long Scalar(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }

    public List<(int Id, string Payload)> OutboxRows()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Payload\" FROM \"OutboxMessages\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, string)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    public List<(int Id, string Email)> Users()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Email\" FROM \"Users\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, string)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}

public sealed class SabotageException : Exception
{
    public SabotageException(string message) : base(message)
    {
    }
}

/// <summary>
/// Sabotages savepoint operations by ordinal (counted since the last <see cref="Reset"/>): throws in the N-th
/// CreatingSavepoint / ReleasingSavepoint, suppresses the N-th savepoint creation (EF then does not run it), or
/// cancels a token when the N-th savepoint was created.
/// </summary>
public sealed class SavepointSabotage
{
    private readonly object _gate = new();
    private int _creates;
    private int _releases;

    public int ThrowOnCreate { get; set; }
    public int ThrowOnRelease { get; set; }
    public int SuppressCreate { get; set; }
    public int CancelOnCreating { get; set; }
    public int CancelOnCreated { get; set; }
    public CancellationTokenSource? CancelSource { get; set; }
    public int Fired { get; private set; }

    public void Reset()
    {
        lock (_gate)
        {
            _creates = 0;
            _releases = 0;
            ThrowOnCreate = ThrowOnRelease = SuppressCreate = CancelOnCreating = CancelOnCreated = 0;
            CancelSource = null;
            Fired = 0;
        }
    }

    internal InterceptionResult Creating(InterceptionResult result)
    {
        int n;
        lock (_gate) n = ++_creates;
        if (n == CancelOnCreating) CancelSource?.Cancel();
        if (n == ThrowOnCreate)
        {
            Fired++;
            throw new SabotageException($"Injected failure creating savepoint #{n}");
        }
        if (n == SuppressCreate)
        {
            Fired++;
            return InterceptionResult.Suppress();
        }
        return result;
    }

    internal void Created()
    {
        int n;
        lock (_gate) n = _creates;
        if (n == CancelOnCreated) CancelSource?.Cancel();
    }

    internal InterceptionResult Releasing(InterceptionResult result)
    {
        int n;
        lock (_gate) n = ++_releases;
        if (n == ThrowOnRelease)
        {
            Fired++;
            throw new SabotageException($"Injected failure releasing savepoint #{n}");
        }
        return result;
    }
}

public sealed class SabotageTransactionInterceptor : DbTransactionInterceptor
{
    private readonly SavepointSabotage _sabotage;

    public SabotageTransactionInterceptor(SavepointSabotage sabotage) => _sabotage = sabotage;

    public override InterceptionResult CreatingSavepoint(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        => _sabotage.Creating(result);

    public override ValueTask<InterceptionResult> CreatingSavepointAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(_sabotage.Creating(result));

    public override void CreatedSavepoint(DbTransaction transaction, TransactionEventData eventData) => _sabotage.Created();

    public override Task CreatedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        _sabotage.Created();
        return Task.CompletedTask;
    }

    public override InterceptionResult ReleasingSavepoint(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        => _sabotage.Releasing(result);

    public override ValueTask<InterceptionResult> ReleasingSavepointAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(_sabotage.Releasing(result));
}
