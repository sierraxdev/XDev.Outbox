using System.Data.Common;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace XDev.Outbox.EFCore.Tests.Infrastructure;

public sealed class InjectedFailureException : Exception
{
    public InjectedFailureException(string message) : base(message)
    {
    }
}

/// <summary>Recognizes the SQL statements tests inject failures into.</summary>
public static class SqlMatch
{
    private static readonly Regex OutboxInsert = new(@"INSERT\s+INTO\s+\S*OutboxMessages", RegexOptions.IgnoreCase);
    private static readonly Regex OutboxUpdate = new(@"UPDATE\s+\S*OutboxMessages", RegexOptions.IgnoreCase);
    private static readonly Regex UserInsert = new(@"INSERT\s+INTO\s+\S*Users", RegexOptions.IgnoreCase);

    public static bool IsOutboxInsert(string sql) => OutboxInsert.IsMatch(sql);
    public static bool IsOutboxUpdate(string sql) => OutboxUpdate.IsMatch(sql);
    public static bool IsUserInsert(string sql) => UserInsert.IsMatch(sql);
}

/// <summary>Per-test switchboard for injected failures.</summary>
public sealed class FaultInjector
{
    private readonly object _gate = new();
    private Func<string, bool>? _commandPredicate;
    private int _commandFailuresRemaining;
    private int _commitFailuresRemaining;

    public int CommandFailuresFired { get; private set; }
    public int CommitFailuresFired { get; private set; }
    public List<string> ExecutedCommands { get; } = new();

    public void FailCommands(Func<string, bool> predicate, int times = 1)
    {
        lock (_gate)
        {
            _commandPredicate = predicate;
            _commandFailuresRemaining = times;
        }
    }

    public void FailCommits(int times = 1)
    {
        lock (_gate) _commitFailuresRemaining = times;
    }

    public void Clear()
    {
        lock (_gate)
        {
            _commandPredicate = null;
            _commandFailuresRemaining = 0;
            _commitFailuresRemaining = 0;
        }
    }

    internal void OnCommand(DbCommand command)
    {
        lock (_gate)
        {
            ExecutedCommands.Add(command.CommandText);
            if (_commandPredicate != null && _commandFailuresRemaining > 0 && _commandPredicate(command.CommandText))
            {
                _commandFailuresRemaining--;
                CommandFailuresFired++;
                throw new InjectedFailureException("Injected command failure: " + command.CommandText);
            }
        }
    }

    internal void OnCommitting()
    {
        lock (_gate)
        {
            if (_commitFailuresRemaining > 0)
            {
                _commitFailuresRemaining--;
                CommitFailuresFired++;
                throw new InjectedFailureException("Injected commit failure");
            }
        }
    }
}

public sealed class FaultCommandInterceptor : DbCommandInterceptor
{
    private readonly FaultInjector _faults;

    public FaultCommandInterceptor(FaultInjector faults) => _faults = faults;

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        _faults.OnCommand(command);
        return base.ReaderExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
    {
        _faults.OnCommand(command);
        return base.ReaderExecutingAsync(command, eventData, result, cancellationToken);
    }

    public override InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        _faults.OnCommand(command);
        return base.NonQueryExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        _faults.OnCommand(command);
        return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
    }

    public override InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        _faults.OnCommand(command);
        return base.ScalarExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<object> result, CancellationToken cancellationToken = default)
    {
        _faults.OnCommand(command);
        return base.ScalarExecutingAsync(command, eventData, result, cancellationToken);
    }
}

public sealed class FaultCommitInterceptor : DbTransactionInterceptor
{
    private readonly FaultInjector _faults;

    public FaultCommitInterceptor(FaultInjector faults) => _faults = faults;

    public override InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        _faults.OnCommitting();
        return base.TransactionCommitting(transaction, eventData, result);
    }

    public override ValueTask<InterceptionResult> TransactionCommittingAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
    {
        _faults.OnCommitting();
        return base.TransactionCommittingAsync(transaction, eventData, result, cancellationToken);
    }
}

/// <summary>Retrying strategy used only by retry tests: retries injected failures, with no delay.</summary>
public sealed class TestRetryingExecutionStrategy : ExecutionStrategy
{
    public TestRetryingExecutionStrategy(ExecutionStrategyDependencies dependencies)
        : base(dependencies, maxRetryCount: 3, maxRetryDelay: TimeSpan.Zero)
    {
    }

    protected override bool ShouldRetryOn(Exception exception)
    {
        for (var e = exception; e != null; e = e.InnerException)
        {
            if (e is InjectedFailureException) return true;
        }
        return false;
    }

    protected override TimeSpan? GetNextDelay(Exception lastException)
        => base.GetNextDelay(lastException) is null ? null : TimeSpan.Zero;
}
