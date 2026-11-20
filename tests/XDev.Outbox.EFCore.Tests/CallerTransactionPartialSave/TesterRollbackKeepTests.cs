using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Adversarial (tester) checks of fix-rollback-event-loss-and-retry-identity-conflict, bug 1: never-saved events are
/// kept by every end of a caller transaction without a commit (explicit Rollback, failed commit, failed rollback),
/// as by a dispose. Looks for duplicate or phantom rows/dispatches, events kept across several transactions,
/// savepoints, a failing rollback, pooled leases and a retrying strategy around a user transaction.
/// </summary>
public class TesterRollbackKeepTests
{
    public static TheoryData<bool> Async() => new() { false, true };

    public static TheoryData<bool, bool> AsyncFlag()
    {
        var data = new TheoryData<bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(async, flag);
        return data;
    }

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static async Task Rollback(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.RollbackAsync(); else tx.Rollback();
    }

    private static async Task DisposeTx(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.DisposeAsync(); else tx.Dispose();
    }

    private static User AddUserWithEvent(AdvDbContext db, string email, out UserCreated evt)
    {
        var user = new User { Email = email };
        db.Users.Add(user);
        evt = new UserCreated { User = user };
        db.Outbox.AddEvent(evt);
        return user;
    }

    /// <summary>A save whose outbox payload UPDATE runs and then fails (undone by the library's savepoint in a caller transaction).</summary>
    private static async Task FailingSave(RollbackTesterHost host, AdvDbContext db, bool async, bool flag = true)
    {
        host.Probe.FailAfterExecution(SqlMatch.IsOutboxUpdate);
        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
        Assert.True(ExceptionChain.Contains<InjectedFailureException>(error), error?.ToString());
    }

    /// <summary>Committed users are exactly <paramref name="emails"/>, one outbox row each carrying its user's key, each row dispatched exactly once.</summary>
    private static void AssertCommittedWithOneRowEach(RollbackTesterHost host, params string[] emails)
    {
        var users = host.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        Assert.Equal(
            users.OrderBy(u => u.Email).Select(u => (u.Id, u.Email)),
            rows.Select(r => Payload.User(r.Payload)).Select(p => (p.GetProperty("id").GetInt32(), p.GetProperty("email").GetString()!)).OrderBy(x => x.Item2));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id).OrderBy(x => x));
    }

    // ---------------- Phantom events ----------------

    /// <summary>
    /// HAZARD (pinned, behavior of the approved fix): after a failed save and an explicit Rollback the event stays
    /// buffered; if the application then discards its business changes with ChangeTracker.Clear() (but not the
    /// event buffer), the next save of anything persists and dispatches the event without its user (the payload
    /// carries the default key 0). Before the fix the explicit Rollback dropped it; a dispose of the transaction kept
    /// it already (pinned below, same outcome), so this is the dispose behavior extended to Rollback.
    /// The application must call Outbox.ClearEvents() together with ChangeTracker.Clear().
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task FailedSaveThenEndWithoutCommit_AppClearsChangeTrackerOnly_NextSavePersistsOrphanEventWithoutItsUser(bool async, bool explicitRollback)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        var tx = await Begin(db, async);
        AddUserWithEvent(db, "discarded", out var evt);
        await FailingSave(host, db, async);
        if (explicitRollback) await Rollback(tx, async);
        await DisposeTx(tx, async);

        db.ChangeTracker.Clear();
        if (explicitRollback) Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        db.Users.Add(new User { Email = "unrelated" });
        await Save.Invoke(db, async);

        var users = host.Users();
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(new[] { "unrelated" }, users.Select(u => u.Email));
        var payloadUser = Payload.User(row.Payload);
        Assert.Equal("discarded", payloadUser.GetProperty("email").GetString());
        Assert.Equal(0, payloadUser.GetProperty("id").GetInt32());
        Assert.Single(host.Queue.Enqueued);
    }

    /// <summary>The documented remedy: clearing the buffer with the tracker leaves no orphan row.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task FailedSaveThenRollback_AppClearsTrackerAndEvents_NextSavePersistsNoOutboxRow(bool async)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "discarded", out _);
            await FailingSave(host, db, async);
            await Rollback(tx, async);
        }

        db.ChangeTracker.Clear();
        db.Outbox.ClearEvents();
        db.Users.Add(new User { Email = "unrelated" });
        await Save.Invoke(db, async);

        Assert.Single(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// A flag=true save (accepted) whose event instance the application adds a second time before rolling back: the
    /// saved copy is dropped (B1, accepted) and the second copy, never saved, is kept. The application re-adds the
    /// user (detach, reset the never-committed key, Add); the retry writes exactly one row. When the event is
    /// re-added only after the rollback, the outcome is the same.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task AcceptedSaveThenRollback_SameEventInstanceReAdded_RetryPersistsExactlyOneRow(bool async, bool reAddBeforeRollback)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            var user = AddUserWithEvent(db, "a", out var evt);
            await Save.Invoke(db, async, true);
            if (reAddBeforeRollback) db.Outbox.AddEvent(evt);
            await Rollback(tx, async);

            Assert.Equal(reAddBeforeRollback ? new object[] { evt } : Array.Empty<object>(), db.Outbox.GetEvents());
            if (!reAddBeforeRollback) db.Outbox.AddEvent(evt);

            db.Entry(user).State = EntityState.Detached;
            user.Id = 0;
            db.Users.Add(user);
        }

        using (var retry = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            await Commit(retry, async);
        }

        AssertCommittedWithOneRowEach(host, "a");
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// The event of a failed save survives four transactions that end without commit (rollback without a save, a
    /// second failed save and rollback, a failed commit, a dispose), then is persisted and dispatched exactly once.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task NeverSavedEventKeptAcrossSeveralUncommittedTransactions_PersistedAndDispatchedOnce(bool async)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        AddUserWithEvent(db, "a", out var evt);

        using (var tx = await Begin(db, async))
        {
            await FailingSave(host, db, async);
            await Rollback(tx, async);
        }
        Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        using (var tx = await Begin(db, async))
        {
            await Rollback(tx, async);
        }
        Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        using (var tx = await Begin(db, async))
        {
            await FailingSave(host, db, async, flag: false);
            await Rollback(tx, async);
        }
        Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        using (var tx = await Begin(db, async))
        {
            host.Faults.FailCommits();
            Assert.NotNull(await Record.ExceptionAsync(() => Commit(tx, async)));
        }
        Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        using (var tx = await Begin(db, async))
        {
            // disposed without commit or rollback
        }

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);

        using (var tx = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "a");
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// Pending (flag=false), failed and added-after-the-last-save events kept by a rollback are persisted by an owned
    /// save of the same context (no transaction): one row each, dispatched once, by that save.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task RollbackThenOwnedSave_KeptEventsPersistedOnceWithTheirUsers(bool async, bool flag)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "pending", out _);
            await Save.Invoke(db, async, false);
            AddUserWithEvent(db, "failed", out _);
            await FailingSave(host, db, async, flag: false);
            AddUserWithEvent(db, "later", out _);
            await Rollback(tx, async);
        }
        Assert.Equal(3, db.Outbox.GetEvents().Count);

        await Save.Invoke(db, async, flag);

        AssertCommittedWithOneRowEach(host, "pending", "failed", "later");
        Assert.Empty(db.Outbox.GetEvents());

        // Nothing is persisted or dispatched again by a later save.
        db.Users.Add(new User { Email = "next" });
        if (!flag) db.ChangeTracker.AcceptAllChanges();
        await Save.Invoke(db, async);
        Assert.Equal(3, host.OutboxRows().Count);
        Assert.Equal(3, host.Queue.Enqueued.Count);
    }

    /// <summary>
    /// tx: save A (flag), savepoint, save B (flag), add C (never saved), RollbackToSavepoint, then Rollback. flag=false:
    /// A and B pending (kept by B1) and C kept. flag=true: A and B accepted (dropped), C kept. A retry in a new
    /// transaction persists exactly the users whose changes are still pending, each with one row.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task SavepointRollbackThenFullRollback_KeptEventsMatchPendingChanges_RetryPersistsEachOnce(bool async, bool flag)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        User a, b;
        object aEvt, bEvt, cEvt;

        using (var tx = await Begin(db, async))
        {
            a = AddUserWithEvent(db, "a", out var ae);
            aEvt = ae;
            await Save.Invoke(db, async, flag);
            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            b = AddUserWithEvent(db, "b", out var be);
            bEvt = be;
            await Save.Invoke(db, async, flag);
            AddUserWithEvent(db, "c", out var ce);
            cEvt = ce;
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");

            Assert.Equal(flag ? new[] { aEvt, cEvt } : new[] { aEvt, bEvt, cEvt }, db.Outbox.GetEvents());
            await Rollback(tx, async);
        }

        Assert.Equal(flag ? new[] { cEvt } : new[] { aEvt, bEvt, cEvt }, db.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);
        if (flag)
        {
            // Accepted changes are not saved again (plain EF); their never-committed keys are detached by the app.
            db.Entry(a).State = EntityState.Detached;
            db.Entry(b).State = EntityState.Detached;
        }

        using (var retry = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            await Commit(retry, async);
        }

        if (flag) AssertCommittedWithOneRowEach(host, "c");
        else AssertCommittedWithOneRowEach(host, "a", "b", "c");
        Assert.Empty(db.Outbox.GetEvents());
    }

    // ---------------- Failed rollback ----------------

    public enum RollbackFailure
    {
        /// <summary>Throws in TransactionRollingBack: the database rollback does not run (dispose rolls back).</summary>
        BeforeRollback,

        /// <summary>Throws in TransactionRolledBack: the database rollback ran, then the call fails.</summary>
        AfterRollback,
    }

    public static TheoryData<RollbackFailure, bool> FailureAsync()
    {
        var data = new TheoryData<RollbackFailure, bool>();
        foreach (var failure in Enum.GetValues<RollbackFailure>())
        foreach (var async in new[] { false, true })
            data.Add(failure, async);
        return data;
    }

    /// <summary>
    /// The caller's Rollback fails (EF reports TransactionFailed with action Rollback). Accepted (flag=true) event
    /// dropped, pending (flag=false), failed-save and later events kept; nothing dispatched; a retry in a new
    /// transaction persists each pending user with exactly one row, dispatched once.
    /// </summary>
    [Theory]
    [MemberData(nameof(FailureAsync))]
    public async Task RollbackFails_SavedFollowB1AndNeverSavedKept_RetryPersistsEachOnce(RollbackFailure failure, bool async)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        var tx = await Begin(db, async);
        var accepted = AddUserWithEvent(db, "accepted", out var acceptedEvt);
        await Save.Invoke(db, async, true);
        AddUserWithEvent(db, "pending", out var pendingEvt);
        await Save.Invoke(db, async, false);
        AddUserWithEvent(db, "failed", out var failedEvt);
        await FailingSave(host, db, async, flag: false);
        AddUserWithEvent(db, "later", out var laterEvt);

        if (failure == RollbackFailure.BeforeRollback) host.RollbackSabotage.ThrowOnRollingBack = 1;
        else host.RollbackSabotage.ThrowOnRolledBack = 1;
        var error = await Record.ExceptionAsync(() => Rollback(tx, async));
        Assert.IsType<RollbackSabotageException>(error);
        Assert.Contains("Failed:Rollback", host.Probe.Events);
        Assert.Equal(new object[] { pendingEvt, failedEvt, laterEvt }, db.Outbox.GetEvents());
        await DisposeTx(tx, async);

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(new object[] { pendingEvt, failedEvt, laterEvt }, db.Outbox.GetEvents());

        db.Entry(accepted).State = EntityState.Detached;
        using (var retry = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            await Commit(retry, async);
        }

        AssertCommittedWithOneRowEach(host, "pending", "failed", "later");
        Assert.DoesNotContain(acceptedEvt, db.Outbox.GetEvents());
        Assert.Empty(db.Outbox.GetEvents());
    }

    // ---------------- Pooled contexts ----------------

    /// <summary>
    /// An event kept by a rollback (failed save) on a pooled context is not seen by the next lease of the same
    /// instance: its buffer is empty, its saves persist and dispatch only its own event.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task PooledContext_EventKeptByRollback_NextLeaseOfSameInstanceNeverSeesIt(bool async)
    {
        using var host = RollbackTesterHost.Create(pooled: true);
        AdvDbContext first;
        {
            var (scope, db) = host.CreateScope();
            using var _s = scope;
            first = db;
            using (var tx = await Begin(db, async))
            {
                AddUserWithEvent(db, "lease1", out var evt);
                await FailingSave(host, db, async);
                await Rollback(tx, async);
            }
            Assert.Single(db.Outbox.GetEvents());
        }

        {
            var (scope, db) = host.CreateScope();
            using var _s = scope;
            Assert.Same(first, db);
            Assert.Empty(db.Outbox.GetEvents());
            Assert.Empty(db.ChangeTracker.Entries());

            db.Users.Add(new User { Email = "plain" });
            await Save.Invoke(db, async);
            Assert.Empty(host.OutboxRows());

            using (var tx = await Begin(db, async))
            {
                AddUserWithEvent(db, "lease2", out _);
                await Save.Invoke(db, async);
                await Commit(tx, async);
            }
        }

        var rows = host.OutboxRows();
        var row = Assert.Single(rows);
        Assert.Equal("lease2", Payload.User(row.Payload).GetProperty("email").GetString());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
    }

    // ---------------- Retrying execution strategy around a user transaction ----------------

    public enum StrategyFailure
    {
        /// <summary>The commit of the user transaction fails once (EF: TransactionFailed, action Commit).</summary>
        Commit,

        /// <summary>The save fails once after its writes (undone by the library's savepoint); the lambda's transaction is disposed.</summary>
        SaveAfterWrites,

        /// <summary>The save fails once after its writes; the lambda rolls back explicitly in a catch.</summary>
        SaveAfterWritesExplicitRollback,
    }

    public static TheoryData<StrategyFailure, bool, bool> StrategyFailureAsync()
    {
        var data = new TheoryData<StrategyFailure, bool, bool>();
        foreach (var failure in Enum.GetValues<StrategyFailure>())
        foreach (var async in new[] { false, true })
        foreach (var withOrder in new[] { false, true })
            data.Add(failure, async, withOrder);
        return data;
    }

    /// <summary>
    /// The lambda EF documents for a retrying strategy with a user transaction: BeginTransaction, SaveChanges(false),
    /// Commit (optionally an explicit Rollback in a catch). Returns the exception that escaped the strategy.
    /// </summary>
    private static async Task<Exception?> RunInStrategy(DbContext db, bool async, bool explicitRollback, Action onAttempt)
    {
        var strategy = db.Database.CreateExecutionStrategy();
        return await Record.ExceptionAsync(async () =>
        {
            if (async)
            {
                await strategy.ExecuteAsync(async () =>
                {
                    onAttempt();
                    await using var tx = await db.Database.BeginTransactionAsync();
                    try
                    {
                        await db.SaveChangesAsync(false);
                        await tx.CommitAsync();
                    }
                    catch when (explicitRollback)
                    {
                        await tx.RollbackAsync();
                        throw;
                    }
                });
            }
            else
            {
                strategy.Execute(() =>
                {
                    onAttempt();
                    using var tx = db.Database.BeginTransaction();
                    try
                    {
                        db.SaveChanges(false);
                        tx.Commit();
                    }
                    catch when (explicitRollback)
                    {
                        tx.Rollback();
                        throw;
                    }
                });
            }
        });
    }

    private static bool IsForeignKeyFailure(Exception? error)
        => error is DbUpdateException && error.InnerException?.Message.Contains("FOREIGN KEY", StringComparison.Ordinal) == true;

    /// <summary>
    /// Plain-EF baseline (no event, so the library passes through): the commit fails once and the strategy reruns the
    /// lambda. A lone user is inserted again (its retained key is sent explicitly). With a dependent order, plain EF's
    /// retry fails on SQLite with a FOREIGN KEY error (pre-existing EF behavior for SaveChanges(false) retried after
    /// the transaction failed; not caused by the library).
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Baseline_PlainEf_RetryingStrategyWithUserTransaction_CommitFailsOnce(bool async, bool withOrder)
    {
        using var host = RollbackTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var user = new User { Email = "a" };
        if (withOrder) user.Orders.Add(new Order { Description = "o" });
        db.Users.Add(user);
        host.Faults.FailCommits();

        var attempts = 0;
        var error = await RunInStrategy(db, async, explicitRollback: false, () => attempts++);

        Assert.Equal(2, attempts);
        if (withOrder)
        {
            Assert.True(IsForeignKeyFailure(error), error?.ToString());
            Assert.Empty(host.Users());
        }
        else
        {
            Assert.Null(error);
            Assert.Single(host.Users());
        }
    }

    /// <summary>
    /// The documented pattern with an event: the first attempt fails, the strategy reruns the lambda; the user and
    /// the event are committed and dispatched exactly once, with the final key in the payload. Commit failure with a
    /// dependent order: the retry fails exactly like plain EF (see the baseline); nothing is committed or dispatched
    /// and the event stays buffered (saved with flag = false, released by the failed commit).
    /// </summary>
    [Theory]
    [MemberData(nameof(StrategyFailureAsync))]
    public async Task RetryingStrategyWithUserTransaction_FirstAttemptFails_EventPersistedAndDispatchedOnce(StrategyFailure failure, bool async, bool withOrder)
    {
        using var host = RollbackTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var user = AddUserWithEvent(db, "a", out var evt);
        if (withOrder) user.Orders.Add(new Order { Description = "o" });

        if (failure == StrategyFailure.Commit) host.Faults.FailCommits();
        else host.Probe.FailAfterExecution(SqlMatch.IsOutboxUpdate);

        var attempts = 0;
        var error = await RunInStrategy(db, async, failure == StrategyFailure.SaveAfterWritesExplicitRollback, () => attempts++);
        Assert.Equal(2, attempts);

        if (failure == StrategyFailure.Commit && withOrder)
        {
            Assert.True(IsForeignKeyFailure(error), error?.ToString());
            Assert.Empty(host.Users());
            Assert.Empty(host.OutboxRows());
            Assert.Empty(host.Queue.Enqueued);
            Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());
            return;
        }

        Assert.Null(error);
        db.ChangeTracker.AcceptAllChanges();
        AssertCommittedWithOneRowEach(host, "a");
        Assert.Equal(withOrder ? 1 : 0, host.Scalar("SELECT COUNT(*) FROM \"Orders\""));
        Assert.Equal(user.Id, host.Users().Single().Id);
        Assert.Empty(db.Outbox.GetEvents());
    }
}

public sealed class RollbackSabotageException : Exception
{
    public RollbackSabotageException(string message) : base(message)
    {
    }
}

/// <summary>Throws in the N-th TransactionRollingBack / TransactionRolledBack (counted separately, 1-based, one-shot).</summary>
public sealed class RollbackSabotage
{
    private int _rollingBack;
    private int _rolledBack;

    public int ThrowOnRollingBack { get; set; }
    public int ThrowOnRolledBack { get; set; }

    internal void RollingBack()
    {
        if (++_rollingBack == ThrowOnRollingBack)
        {
            ThrowOnRollingBack = 0;
            _rollingBack = 0;
            throw new RollbackSabotageException("Injected failure in TransactionRollingBack");
        }
    }

    internal void RolledBack()
    {
        if (++_rolledBack == ThrowOnRolledBack)
        {
            ThrowOnRolledBack = 0;
            _rolledBack = 0;
            throw new RollbackSabotageException("Injected failure in TransactionRolledBack");
        }
    }
}

public sealed class RollbackSabotageInterceptor : DbTransactionInterceptor
{
    private readonly RollbackSabotage _sabotage;

    public RollbackSabotageInterceptor(RollbackSabotage sabotage) => _sabotage = sabotage;

    public override InterceptionResult TransactionRollingBack(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        _sabotage.RollingBack();
        return result;
    }

    public override ValueTask<InterceptionResult> TransactionRollingBackAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
    {
        _sabotage.RollingBack();
        return ValueTask.FromResult(result);
    }

    public override void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData) => _sabotage.RolledBack();

    public override Task TransactionRolledBackAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        _sabotage.RolledBack();
        return Task.CompletedTask;
    }
}

/// <summary>
/// SQLite in-memory host for <see cref="AdvDbContext"/> like <see cref="TesterSavepointHost"/>, plus a
/// <see cref="RollbackSabotage"/> interceptor (registered before the library's interceptor).
/// </summary>
public sealed class RollbackTesterHost : IDisposable
{
    private RollbackTesterHost(SqliteConnection connection, ServiceProvider services, RecordingQueue<int> queue, FaultInjector faults, SavepointProbe probe, RollbackSabotage rollbackSabotage)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
        Probe = probe;
        RollbackSabotage = rollbackSabotage;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingQueue<int> Queue { get; }
    public FaultInjector Faults { get; }
    public SavepointProbe Probe { get; }
    public RollbackSabotage RollbackSabotage { get; }

    public static RollbackTesterHost Create(bool pooled = false, bool retrying = false)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingQueue<int>();
        var faults = new FaultInjector();
        var probe = new SavepointProbe();
        var rollbackSabotage = new RollbackSabotage();

        void Configure(DbContextOptionsBuilder options)
        {
            options.UseSqlite(connection, sqlite =>
            {
                if (retrying) sqlite.ExecutionStrategy(d => new TestRetryingExecutionStrategy(d));
            });
            options.AddInterceptors(
                new FaultCommandInterceptor(faults),
                new FaultCommitInterceptor(faults),
                new ProbeCommandInterceptor(probe),
                new ProbeTransactionInterceptor(probe),
                new RollbackSabotageInterceptor(rollbackSabotage));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        }

        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        if (pooled) services.AddDbContextPool<AdvDbContext>(Configure, poolSize: 1);
        else services.AddDbContext<AdvDbContext>(Configure);
        services.AddOutbox<int>(typeof(RollbackTesterHost).Assembly);
        services.AddEFCoreOutboxStore<int, AdvDbContext>();

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using (var scope = provider.CreateScope())
        {
            scope.ServiceProvider.GetRequiredService<AdvDbContext>().Database.EnsureCreated();
        }
        faults.ExecutedCommands.Clear();
        return new RollbackTesterHost(connection, provider, queue, faults, probe, rollbackSabotage);
    }

    public (IServiceScope Scope, AdvDbContext Db) CreateScope()
    {
        var scope = Services.CreateScope();
        return (scope, scope.ServiceProvider.GetRequiredService<AdvDbContext>());
    }

    /// <summary>Direct read on the host connection. Do not call while a transaction is open.</summary>
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
