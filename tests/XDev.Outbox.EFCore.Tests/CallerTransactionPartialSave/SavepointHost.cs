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
/// SQLite in-memory host for <see cref="AdvDbContext"/> with the standard fault injector plus: failures after a
/// command ran, cancellation at a chosen point, a transaction-event recorder, failures injected into rollbacks to a
/// savepoint, and a captured log.
/// </summary>
public sealed class SavepointHost : IDisposable
{
    private SavepointHost(SqliteConnection connection, ServiceProvider services, RecordingQueue<int> queue, FaultInjector faults, SavepointProbe probe, LogCapture log)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
        Probe = probe;
        Log = log;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingQueue<int> Queue { get; }
    public FaultInjector Faults { get; }
    public SavepointProbe Probe { get; }
    public LogCapture Log { get; }

    public static SavepointHost Create()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingQueue<int>();
        var faults = new FaultInjector();
        var probe = new SavepointProbe();
        var log = new LogCapture();
        var loggerFactory = LoggerFactory.Create(b => b.AddProvider(log));

        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddDbContext<AdvDbContext>(options =>
        {
            options.UseSqlite(connection);
            options.UseLoggerFactory(loggerFactory);
            options.AddInterceptors(
                new FaultCommandInterceptor(faults),
                new FaultCommitInterceptor(faults),
                new ProbeCommandInterceptor(probe),
                new ProbeTransactionInterceptor(probe));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        });
        services.AddOutbox<int>(typeof(SavepointHost).Assembly);
        services.AddEFCoreOutboxStore<int, AdvDbContext>();

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using (var scope = provider.CreateScope())
        {
            scope.ServiceProvider.GetRequiredService<AdvDbContext>().Database.EnsureCreated();
        }
        return new SavepointHost(connection, provider, queue, faults, probe, log);
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

    public void Exec(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
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

    public List<int> UserIds()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\" FROM \"Users\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read()) ids.Add(reader.GetInt32(0));
        return ids;
    }

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}

/// <summary>
/// Records transaction events (savepoint events carry no name in EF) and commands, fails a command after it ran,
/// cancels a token at a chosen point and fails rollbacks to a savepoint.
/// </summary>
public sealed class SavepointProbe
{
    private readonly object _gate = new();
    private readonly List<string> _events = new();
    private Func<string, bool>? _failAfter;
    private Func<string, bool>? _cancelAfterCommand;
    private CancellationTokenSource? _cancelSource;
    private bool _cancelOnFirstRelease;
    private int _rollbackToSavepointFailures;

    public IReadOnlyList<string> Events
    {
        get { lock (_gate) return _events.ToArray(); }
    }

    public int AfterExecutionFailuresFired { get; private set; }
    public int RollbackToSavepointFailuresFired { get; private set; }

    public void ClearEvents()
    {
        lock (_gate) _events.Clear();
    }

    /// <summary>The next command matching <paramref name="predicate"/> runs, then the call fails.</summary>
    public void FailAfterExecution(Func<string, bool> predicate)
    {
        lock (_gate) _failAfter = predicate;
    }

    /// <summary>Cancels <paramref name="source"/> once a command matching <paramref name="predicate"/> has run.</summary>
    public void CancelAfterCommand(Func<string, bool> predicate, CancellationTokenSource source)
    {
        lock (_gate)
        {
            _cancelAfterCommand = predicate;
            _cancelSource = source;
        }
    }

    /// <summary>Cancels <paramref name="source"/> when the first savepoint is released (EF's own, after the INSERTs).</summary>
    public void CancelOnFirstSavepointRelease(CancellationTokenSource source)
    {
        lock (_gate)
        {
            _cancelOnFirstRelease = true;
            _cancelSource = source;
        }
    }

    public void FailRollbacksToSavepoint(int times = 1)
    {
        lock (_gate) _rollbackToSavepointFailures = times;
    }

    internal void Record(string name)
    {
        lock (_gate) _events.Add(name);
    }

    internal void OnCommandExecuted(DbCommand command)
    {
        CancellationTokenSource? cancel = null;
        var fail = false;
        lock (_gate)
        {
            _events.Add("Command: " + command.CommandText);
            if (_cancelAfterCommand != null && _cancelAfterCommand(command.CommandText))
            {
                _cancelAfterCommand = null;
                cancel = _cancelSource;
            }
            if (_failAfter != null && _failAfter(command.CommandText))
            {
                _failAfter = null;
                AfterExecutionFailuresFired++;
                fail = true;
            }
        }
        cancel?.Cancel();
        if (fail) throw new InjectedFailureException("Injected failure after execution: " + command.CommandText);
    }

    internal void OnSavepointReleased()
    {
        CancellationTokenSource? cancel = null;
        lock (_gate)
        {
            _events.Add("ReleasedSavepoint");
            if (_cancelOnFirstRelease)
            {
                _cancelOnFirstRelease = false;
                cancel = _cancelSource;
            }
        }
        cancel?.Cancel();
    }

    internal void OnRollingBackToSavepoint()
    {
        lock (_gate)
        {
            _events.Add("RollingBackToSavepoint");
            if (_rollbackToSavepointFailures <= 0) return;
            _rollbackToSavepointFailures--;
            RollbackToSavepointFailuresFired++;
        }
        throw new RollbackToSavepointFailureException();
    }
}

public sealed class RollbackToSavepointFailureException : Exception
{
    public RollbackToSavepointFailureException() : base("Injected rollback-to-savepoint failure")
    {
    }
}

public sealed class ProbeCommandInterceptor : DbCommandInterceptor
{
    private readonly SavepointProbe _probe;

    public ProbeCommandInterceptor(SavepointProbe probe) => _probe = probe;

    public override DbDataReader ReaderExecuted(DbCommand command, CommandExecutedEventData eventData, DbDataReader result)
    {
        Executed(command, result);
        return result;
    }

    public override ValueTask<DbDataReader> ReaderExecutedAsync(DbCommand command, CommandExecutedEventData eventData, DbDataReader result, CancellationToken cancellationToken = default)
    {
        Executed(command, result);
        return ValueTask.FromResult(result);
    }

    public override int NonQueryExecuted(DbCommand command, CommandExecutedEventData eventData, int result)
    {
        _probe.OnCommandExecuted(command);
        return result;
    }

    public override ValueTask<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        _probe.OnCommandExecuted(command);
        return ValueTask.FromResult(result);
    }

    public override object? ScalarExecuted(DbCommand command, CommandExecutedEventData eventData, object? result)
    {
        _probe.OnCommandExecuted(command);
        return result;
    }

    public override ValueTask<object?> ScalarExecutedAsync(DbCommand command, CommandExecutedEventData eventData, object? result, CancellationToken cancellationToken = default)
    {
        _probe.OnCommandExecuted(command);
        return ValueTask.FromResult(result);
    }

    private void Executed(DbCommand command, DbDataReader reader)
    {
        try
        {
            _probe.OnCommandExecuted(command);
        }
        catch
        {
            // The statement ran; closing the reader finishes it before the failure surfaces.
            reader.Dispose();
            throw;
        }
    }
}

public sealed class ProbeTransactionInterceptor : DbTransactionInterceptor
{
    private readonly SavepointProbe _probe;

    public ProbeTransactionInterceptor(SavepointProbe probe) => _probe = probe;

    public override void CreatedSavepoint(DbTransaction transaction, TransactionEventData eventData) => _probe.Record("CreatedSavepoint");

    public override Task CreatedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        _probe.Record("CreatedSavepoint");
        return Task.CompletedTask;
    }

    public override InterceptionResult RollingBackToSavepoint(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        _probe.OnRollingBackToSavepoint();
        return result;
    }

    public override ValueTask<InterceptionResult> RollingBackToSavepointAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
    {
        _probe.OnRollingBackToSavepoint();
        return ValueTask.FromResult(result);
    }

    public override void RolledBackToSavepoint(DbTransaction transaction, TransactionEventData eventData) => _probe.Record("RolledBackToSavepoint");

    public override Task RolledBackToSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        _probe.Record("RolledBackToSavepoint");
        return Task.CompletedTask;
    }

    public override void ReleasedSavepoint(DbTransaction transaction, TransactionEventData eventData) => _probe.OnSavepointReleased();

    public override Task ReleasedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        _probe.OnSavepointReleased();
        return Task.CompletedTask;
    }

    public override void TransactionFailed(DbTransaction transaction, TransactionErrorEventData eventData) => _probe.Record("Failed:" + eventData.Action);

    public override Task TransactionFailedAsync(DbTransaction transaction, TransactionErrorEventData eventData, CancellationToken cancellationToken = default)
    {
        _probe.Record("Failed:" + eventData.Action);
        return Task.CompletedTask;
    }
}

public sealed record LogEntry(string Category, LogLevel Level, EventId EventId, string Message, Exception? Exception);

/// <summary>Captures every log entry of the contexts it is given to.</summary>
public sealed class LogCapture : ILoggerProvider
{
    private readonly object _gate = new();
    private readonly List<LogEntry> _entries = new();

    public IReadOnlyList<LogEntry> Entries
    {
        get { lock (_gate) return _entries.ToArray(); }
    }

    public IReadOnlyList<LogEntry> WithEventId(int id) => Entries.Where(e => e.EventId.Id == id).ToList();

    public ILogger CreateLogger(string categoryName) => new CaptureLogger(this, categoryName);

    public void Dispose()
    {
    }

    private void Add(LogEntry entry)
    {
        lock (_gate) _entries.Add(entry);
    }

    private sealed class CaptureLogger : ILogger
    {
        private readonly LogCapture _owner;
        private readonly string _category;

        public CaptureLogger(LogCapture owner, string category)
        {
            _owner = owner;
            _category = category;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _owner.Add(new LogEntry(_category, logLevel, eventId, formatter(state, exception), exception));
    }
}
