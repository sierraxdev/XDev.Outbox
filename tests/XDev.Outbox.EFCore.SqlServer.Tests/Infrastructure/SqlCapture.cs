using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

/// <summary>Injected failure treated as transient by <see cref="SmokeRetryingExecutionStrategy"/>.</summary>
public sealed class InjectedTransientException : Exception
{
    public InjectedTransientException(string message) : base(message)
    {
    }
}

public sealed record ParameterRecord(string Name, DbType DbType, string SqlDbType, int Size, int? ValueLength, object? Value);

public sealed record CommandRecord(string Sql, IReadOnlyList<ParameterRecord> Parameters)
{
    public override string ToString()
        => Sql + Environment.NewLine + string.Join(Environment.NewLine, Parameters.Select(p => $"  {p.Name}: {p.SqlDbType} size={p.Size} dbType={p.DbType} valueLength={p.ValueLength?.ToString() ?? "-"}"));
}

public enum FaultTiming
{
    /// <summary>The command is never sent to the server.</summary>
    BeforeExecution,

    /// <summary>The command ran on the server (its effects exist in the transaction), then the call fails.</summary>
    AfterExecution,
}

/// <summary>Recognizes the statements failures are injected into (SQL Server text: INSERT/MERGE/UPDATE on [table]).</summary>
public static class SqlMatch
{
    public static bool WritesTo(string sql, string table)
        => Regex.IsMatch(sql, @"(INSERT\s+INTO|MERGE(\s+INTO)?)\s+(\[\w+\]\.)?\[" + Regex.Escape(table) + @"\]", RegexOptions.IgnoreCase);

    public static bool IsUserInsert(string sql) => WritesTo(sql, "Users");
    public static bool IsOutboxInsert(string sql) => WritesTo(sql, "OutboxMessages");
    public static bool IsOutboxPayloadUpdate(string sql) => Regex.IsMatch(sql, @"^\s*UPDATE\s+(\[\w+\]\.)?\[OutboxMessages\]\s+SET\s+\[Payload\]", RegexOptions.IgnoreCase);
}

/// <summary>Records every command (text + parameter typing) and injects failures into commands and commits.</summary>
public sealed class FaultInjector
{
    private readonly object _gate = new();
    private readonly List<CommandRecord> _commands = new();
    private readonly List<string> _transactionEvents = new();
    private Func<string, bool>? _predicate;
    private FaultTiming _timing;
    private int _commandFailuresRemaining;
    private int _commitFailuresRemaining;
    private DbCommand? _failAfter;

    public int CommandFailuresFired { get; private set; }
    public int CommitFailuresFired { get; private set; }

    public IReadOnlyList<CommandRecord> Commands
    {
        get { lock (_gate) return _commands.ToArray(); }
    }

    /// <summary>Transaction interceptor events in order (savepoint events carry no name in EF).</summary>
    public IReadOnlyList<string> TransactionEvents
    {
        get { lock (_gate) return _transactionEvents.ToArray(); }
    }

    public void FailCommands(Func<string, bool> predicate, FaultTiming timing, int times = 1)
    {
        lock (_gate)
        {
            _predicate = predicate;
            _timing = timing;
            _commandFailuresRemaining = times;
        }
    }

    public void FailCommits(int times = 1)
    {
        lock (_gate) _commitFailuresRemaining = times;
    }

    public void ClearLog()
    {
        lock (_gate)
        {
            _commands.Clear();
            _transactionEvents.Clear();
        }
    }

    internal void OnExecuting(DbCommand command)
    {
        lock (_gate)
        {
            _commands.Add(new CommandRecord(command.CommandText, command.Parameters.Cast<DbParameter>().Select(Describe).ToList()));
            if (_predicate == null || _commandFailuresRemaining <= 0 || !_predicate(command.CommandText)) return;

            _commandFailuresRemaining--;
            CommandFailuresFired++;
            if (_timing == FaultTiming.BeforeExecution)
            {
                throw new InjectedTransientException("Injected failure before execution: " + command.CommandText);
            }
            _failAfter = command;
        }
    }

    /// <summary>True when the command ran and must now fail (AfterExecution).</summary>
    internal bool ShouldFailAfter(DbCommand command)
    {
        lock (_gate)
        {
            if (!ReferenceEquals(_failAfter, command)) return false;
            _failAfter = null;
            return true;
        }
    }

    internal void OnTransactionEvent(string name)
    {
        lock (_gate) _transactionEvents.Add(name);
    }

    internal void OnCommitting()
    {
        lock (_gate)
        {
            _transactionEvents.Add("Committing");
            if (_commitFailuresRemaining <= 0) return;
            _commitFailuresRemaining--;
            CommitFailuresFired++;
        }
        throw new InjectedTransientException("Injected commit failure");
    }

    private static ParameterRecord Describe(DbParameter parameter)
    {
        var sqlDbType = parameter is SqlParameter sql ? sql.SqlDbType.ToString() : "?";
        var value = parameter.Value is DBNull ? null : parameter.Value;
        return new ParameterRecord(parameter.ParameterName, parameter.DbType, sqlDbType, parameter.Size, value is string s ? s.Length : null, value);
    }
}

public sealed class FaultCommandInterceptor : DbCommandInterceptor
{
    private readonly FaultInjector _faults;

    public FaultCommandInterceptor(FaultInjector faults) => _faults = faults;

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        _faults.OnExecuting(command);
        return result;
    }

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
    {
        _faults.OnExecuting(command);
        return ValueTask.FromResult(result);
    }

    public override DbDataReader ReaderExecuted(DbCommand command, CommandExecutedEventData eventData, DbDataReader result)
    {
        if (_faults.ShouldFailAfter(command))
        {
            // Closing the reader drains the batch, so every statement of the command has run on the server.
            result.Dispose();
            throw new InjectedTransientException("Injected failure after execution: " + command.CommandText);
        }
        return result;
    }

    public override async ValueTask<DbDataReader> ReaderExecutedAsync(DbCommand command, CommandExecutedEventData eventData, DbDataReader result, CancellationToken cancellationToken = default)
    {
        if (_faults.ShouldFailAfter(command))
        {
            await result.DisposeAsync();
            throw new InjectedTransientException("Injected failure after execution: " + command.CommandText);
        }
        return result;
    }

    public override InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        _faults.OnExecuting(command);
        return result;
    }

    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        _faults.OnExecuting(command);
        return ValueTask.FromResult(result);
    }

    public override int NonQueryExecuted(DbCommand command, CommandExecutedEventData eventData, int result)
    {
        if (_faults.ShouldFailAfter(command)) throw new InjectedTransientException("Injected failure after execution: " + command.CommandText);
        return result;
    }

    public override ValueTask<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        if (_faults.ShouldFailAfter(command)) throw new InjectedTransientException("Injected failure after execution: " + command.CommandText);
        return ValueTask.FromResult(result);
    }

    public override InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        _faults.OnExecuting(command);
        return result;
    }

    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<object> result, CancellationToken cancellationToken = default)
    {
        _faults.OnExecuting(command);
        return ValueTask.FromResult(result);
    }
}

public sealed class FaultTransactionInterceptor : DbTransactionInterceptor
{
    private readonly FaultInjector _faults;

    public FaultTransactionInterceptor(FaultInjector faults) => _faults = faults;

    public override InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        _faults.OnCommitting();
        return result;
    }

    public override ValueTask<InterceptionResult> TransactionCommittingAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
    {
        _faults.OnCommitting();
        return ValueTask.FromResult(result);
    }

    public override void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData)
        => _faults.OnTransactionEvent("RolledBack");

    public override Task TransactionRolledBackAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        _faults.OnTransactionEvent("RolledBack");
        return Task.CompletedTask;
    }

    public override void CreatedSavepoint(DbTransaction transaction, TransactionEventData eventData)
        => _faults.OnTransactionEvent("CreatedSavepoint");

    public override Task CreatedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        _faults.OnTransactionEvent("CreatedSavepoint");
        return Task.CompletedTask;
    }

    public override void RolledBackToSavepoint(DbTransaction transaction, TransactionEventData eventData)
        => _faults.OnTransactionEvent("RolledBackToSavepoint");

    public override Task RolledBackToSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        _faults.OnTransactionEvent("RolledBackToSavepoint");
        return Task.CompletedTask;
    }

    public override void ReleasedSavepoint(DbTransaction transaction, TransactionEventData eventData)
        => _faults.OnTransactionEvent("ReleasedSavepoint");

    public override Task ReleasedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        _faults.OnTransactionEvent("ReleasedSavepoint");
        return Task.CompletedTask;
    }
}

/// <summary>
/// The real SQL Server retrying strategy (EnableRetryOnFailure), with no delay and with the injected failures added
/// to SQL Server's transient error detection (a real <see cref="SqlException"/> cannot be constructed by a test).
/// </summary>
public sealed class SmokeRetryingExecutionStrategy : SqlServerRetryingExecutionStrategy
{
    public SmokeRetryingExecutionStrategy(ExecutionStrategyDependencies dependencies)
        : base(dependencies, maxRetryCount: 3, maxRetryDelay: TimeSpan.Zero, errorNumbersToAdd: null)
    {
    }

    protected override bool ShouldRetryOn(Exception exception)
    {
        for (var e = exception; e != null; e = e.InnerException)
        {
            if (e is InjectedTransientException) return true;
        }
        return base.ShouldRetryOn(exception);
    }

    protected override TimeSpan? GetNextDelay(Exception lastException)
        => base.GetNextDelay(lastException) is null ? null : TimeSpan.Zero;
}
