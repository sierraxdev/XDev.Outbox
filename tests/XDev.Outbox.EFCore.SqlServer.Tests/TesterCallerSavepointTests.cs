#pragma warning disable EF1001 // IStateManager: used only to observe the identity map (item 8 investigation).
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Adversarial (tester) checks of the D5 fix on SQL Server, where ReleaseSavepoint is a no-op: every SAVE TRANSACTION
/// of the transaction stays, names accumulate, and ROLLBACK TRANSACTION name targets the most recent savepoint of
/// that name. Also: external transactions (UseTransaction), pooled contexts, Guid keys, savepoint counts per save,
/// cancellation, and the pre-existing identity-map defect (item 8) on SQL Server.
/// </summary>
public class TesterCallerSavepointTests : IClassFixture<SmokeDatabaseFixture>
{
    private const int NotUndoneEventId = 41002;
    private static readonly TimeSpan NoHang = TimeSpan.FromSeconds(60);

    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterCallerSavepointTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost(LogCapture? log = null, Registration registration = Registration.AddDbContext)
        => SmokeHost<int, SmokeDbContext>.Create(
            _fixture,
            new SmokeHostOptions { Registration = registration, PoolSize = 1 },
            services =>
            {
                if (log != null) services.AddSingleton<ILoggerProvider>(log);
            });

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static async Task CreateSavepoint(IDbContextTransaction tx, string name, bool async)
    {
        if (async) await tx.CreateSavepointAsync(name); else tx.CreateSavepoint(name);
    }

    private static async Task RollbackToSavepoint(IDbContextTransaction tx, string name, bool async)
    {
        if (async) await tx.RollbackToSavepointAsync(name); else tx.RollbackToSavepoint(name);
    }

    private static async Task<(int Users, int Outbox)> CountsInTransaction<TKey>(SmokeContextBase<TKey> db) where TKey : struct
        => (await db.Users.AsNoTracking().CountAsync(), await db.Set<OutboxMessage<TKey>>().AsNoTracking().CountAsync());

    private static User AddUserWithEvent<TKey>(SmokeContextBase<TKey> db, string email, params string[] orders) where TKey : struct
    {
        var user = Graph.NewUser(email, orders);
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });
        return user;
    }

    /// <summary>Committed users are exactly <paramref name="emails"/>; one outbox row each with Rule A keys; each dispatched once after commit.</summary>
    private static void AssertCommittedAndDispatchedOnce<TKey, TContext>(SmokeHost<TKey, TContext> host, params string[] emails)
        where TKey : struct
        where TContext : SmokeContextBase<TKey>
    {
        var users = host.Database.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        GraphAssert.RowsMatch(rows, users, host.Database.Orders());
        Assert.Equal(rows.Select(r => r.Id).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id.ToString()!).OrderBy(x => x));
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
    }

    // ---------------- 1. Accumulating savepoint names ----------------

    /// <summary>
    /// The caller's savepoint is named like the library's, then a successful save and a failing save run (all three
    /// SAVE TRANSACTION __XDevOutboxSavePoint stay on SQL Server). The failing save's rollback must target its own
    /// (most recent) savepoint: the first save's rows survive; the retry and commit persist both once.
    /// </summary>
    [SqlServerTheory]
    [InlineData("__XDevOutboxSavePoint", false)]
    [InlineData("__XDevOutboxSavePoint", true)]
    [InlineData("__EFSavePoint", false)]
    [InlineData("__EFSavePoint", true)]
    [InlineData("caller", false)]
    [InlineData("caller", true)]
    public async Task AccumulatedSavepoints_FailureAfterEarlierSave_RollsBackOnlyTheFailedSave(string callerName, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;

        await using (var tx = await Begin(db, async))
        {
            await CreateSavepoint(tx, callerName, async);
            AddUserWithEvent(db, "first@test", "f1");
            await Save.Invoke(db, async);

            AddUserWithEvent(db, "second@test", "s1");
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);
            Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            Assert.Equal((1, 1), await CountsInTransaction(db));
            Assert.Equal(1, await db.Orders.AsNoTracking().CountAsync());

            await Save.Invoke(db, async);
            Assert.Equal((2, 2), await CountsInTransaction(db));
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "first@test", "second@test");
        _output.WriteLine("Transaction events: " + string.Join(", ", host.Faults.TransactionEvents));
    }

    public enum CallerSavepointPosition
    {
        BeforeFirstSave,
        BetweenSaves,
    }

    /// <summary>
    /// Caller savepoint with its own name, two saves (the second fails after its writes and is retried), then the
    /// caller rolls back to its savepoint: the existence check releases exactly the undone messages and the commit
    /// dispatches exactly the surviving rows.
    /// </summary>
    [SqlServerTheory]
    [InlineData(CallerSavepointPosition.BeforeFirstSave, false)]
    [InlineData(CallerSavepointPosition.BeforeFirstSave, true)]
    [InlineData(CallerSavepointPosition.BetweenSaves, false)]
    [InlineData(CallerSavepointPosition.BetweenSaves, true)]
    public async Task CallerSavepoint_TwoSavesSecondFailedAndRetried_CallerRollback_CommitsAndDispatchesSurvivorsOnly(CallerSavepointPosition position, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;

        await using (var tx = await Begin(db, async))
        {
            if (position == CallerSavepointPosition.BeforeFirstSave) await CreateSavepoint(tx, "callerSp", async);
            AddUserWithEvent(db, "first@test", "f1");
            await Save.Invoke(db, async);

            if (position == CallerSavepointPosition.BetweenSaves) await CreateSavepoint(tx, "callerSp", async);
            AddUserWithEvent(db, "second@test", "s1");
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);
            Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            await Save.Invoke(db, async);

            await RollbackToSavepoint(tx, "callerSp", async);
            Assert.Equal(position == CallerSavepointPosition.BeforeFirstSave ? (0, 0) : (1, 1), await CountsInTransaction(db));
            await Commit(tx, async);
        }

        if (position == CallerSavepointPosition.BeforeFirstSave)
        {
            Assert.Empty(host.Database.Users());
            Assert.Empty(host.OutboxRows());
            Assert.Empty(host.Queue.Enqueued);
        }
        else
        {
            AssertCommittedAndDispatchedOnce(host, "first@test");
        }
    }

    /// <summary>
    /// Name collision hazard: the caller names its savepoint "__XDevOutboxSavePoint", writes something without events,
    /// then saves with events. Because SQL Server keeps the library's (released) savepoint, the caller's
    /// ROLLBACK TRANSACTION __XDevOutboxSavePoint targets the library's most recent one and does not undo the plain
    /// write made after the caller's savepoint. Same class of hazard as naming a savepoint "__EFSavePoint" with plain
    /// EF. Pinned here as observed behavior (reserved name), not as desired behavior.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerSavepointNamedLikeLibrary_RollbackAfterLibrarySave_TargetsLibrarySavepoint(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;

        await using (var tx = await Begin(db, async))
        {
            await CreateSavepoint(tx, "__XDevOutboxSavePoint", async);
            db.Users.Add(Graph.NewUser("plain@test"));
            await Save.Invoke(db, async); // no events: plain EF save, no library savepoint

            AddUserWithEvent(db, "evented@test");
            await Save.Invoke(db, async);
            Assert.Equal(2, await db.Users.AsNoTracking().CountAsync());

            await RollbackToSavepoint(tx, "__XDevOutboxSavePoint", async);
            // Observed: only the evented save is undone (the library's savepoint is the most recent of that name).
            Assert.Equal(new[] { "plain@test" }, await db.Users.AsNoTracking().Select(u => u.Email).ToListAsync());
            await Commit(tx, async);
        }

        Assert.Equal(new[] { "plain@test" }, host.Database.Users().Select(u => u.Email));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
    }

    // ---------------- 2. UseTransaction, pooled context, Guid key ----------------

    [SqlServerTheory]
    [InlineData(FaultTiming.BeforeExecution, false)]
    [InlineData(FaultTiming.BeforeExecution, true)]
    [InlineData(FaultTiming.AfterExecution, false)]
    [InlineData(FaultTiming.AfterExecution, true)]
    public async Task UseTransactionWithExternalSqlTransaction_PayloadUpdateFailsThenRetry_PersistsAndDispatchesOnce(FaultTiming timing, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;
        var connection = db.Database.GetDbConnection();
        await connection.OpenAsync();

        await using (var external = await connection.BeginTransactionAsync())
        {
            var tx = async ? await db.Database.UseTransactionAsync(external) : db.Database.UseTransaction(external);
            Assert.NotNull(tx);
            Assert.True(tx!.SupportsSavepoints);

            AddUserWithEvent(db, "external@test", "e1");
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, timing);
            Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            Assert.Contains("RolledBackToSavepoint", host.Faults.TransactionEvents);
            Assert.Equal((0, 0), await CountsInTransaction(db));

            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "external@test");
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PooledContext_SecondLeaseFailedSaveRetriedInSameTransaction_PersistsAndDispatchesOnlyItsRowsOnce(bool async)
    {
        using var host = CreateHost(registration: Registration.AddDbContextPool);
        Guid instance;

        var lease1 = host.CreateScope();
        try
        {
            instance = lease1.Db.ContextId.InstanceId;
            await Begin(lease1.Db, async); // abandoned
            AddUserWithEvent(lease1.Db, "lease1@test", "l1");
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);
            Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(lease1.Db, async)));
        }
        finally
        {
            if (async) await lease1.DisposeAsync(); else lease1.Dispose();
        }

        await using (var lease2 = host.CreateScope())
        {
            var db = lease2.Db;
            Assert.Equal(instance, db.ContextId.InstanceId);
            Assert.Null(db.Database.CurrentTransaction);
            Assert.Empty(db.Outbox.GetEvents());

            await using (var tx = await Begin(db, async))
            {
                AddUserWithEvent(db, "lease2@test", "l2");
                host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);
                Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
                Assert.Equal((0, 0), await CountsInTransaction(db));
                await Save.Invoke(db, async);
                await Commit(tx, async);
            }
        }

        AssertCommittedAndDispatchedOnce(host, "lease2@test");
    }

    // ---------------- 7. Savepoints per save ----------------

    /// <summary>
    /// SQL Server savepoints are created through SqlTransaction.Save (no DbCommand), so they are counted with the
    /// transaction interceptor. Owned save: EF's savepoint only (inside the library's transaction). Caller save with
    /// events: EF's + the library's. Caller save without events: EF's only.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SavepointsPerSave_OwnedUnchanged_CallerWithEventsAddsExactlyOne(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;

        host.Faults.ClearLog();
        AddUserWithEvent(db, "owned@test", "o1");
        await Save.Invoke(db, async);
        var owned = host.Faults.TransactionEvents.Count(e => e == "CreatedSavepoint");
        var ownedSaveCommands = host.Faults.Commands.Count(c => c.Sql.Contains("SAVE TRAN", StringComparison.OrdinalIgnoreCase));

        await using (var tx = await Begin(db, async))
        {
            host.Faults.ClearLog();
            AddUserWithEvent(db, "caller@test", "c1");
            await Save.Invoke(db, async);
            var caller = host.Faults.TransactionEvents.Count(e => e == "CreatedSavepoint");

            host.Faults.ClearLog();
            db.Users.Add(Graph.NewUser("plain@test"));
            await Save.Invoke(db, async);
            var plain = host.Faults.TransactionEvents.Count(e => e == "CreatedSavepoint");

            _output.WriteLine($"CreatedSavepoint per save: owned={owned}, caller with events={caller}, caller without events={plain}; SAVE TRAN DbCommands (owned)={ownedSaveCommands}");
            Assert.Equal(1, owned);
            Assert.Equal(owned + 1, caller);
            Assert.Equal(1, plain);
            await Commit(tx, async);
        }
        Assert.Equal(0, ownedSaveCommands);
    }

    // ---------------- 5. Cancellation ----------------

    public enum CancelAt
    {
        /// <summary>The token is cancelled before SaveChangesAsync is called.</summary>
        BeforeSave,

        /// <summary>Cancelled just as the Users/OutboxMessages INSERT is sent (in flight inside EF's save).</summary>
        DuringOutboxInsert,

        /// <summary>Cancelled just as the payload UPDATE is sent (after EF's save completed).</summary>
        DuringPayloadUpdate,
    }

    [SqlServerTheory]
    [InlineData(CancelAt.BeforeSave)]
    [InlineData(CancelAt.DuringOutboxInsert)]
    [InlineData(CancelAt.DuringPayloadUpdate)]
    public async Task SaveAsync_CancelledAroundTheLibrarySavepoint_NoPartialStateNoHangAndRetryPersistsOnce(CancelAt point)
    {
        var log = new LogCapture();
        using var cts = new CancellationTokenSource();
        using var host = CreateHost(log);
        await using var scope = host.CreateScope();
        var db = scope.Db;

        var user = AddUserWithEvent(db, "cancel@test", "c1");
        await using (var tx = await db.Database.BeginTransactionAsync())
        {
            if (point == CancelAt.BeforeSave)
            {
                cts.Cancel();
            }
            else
            {
                Func<string, bool> match = point == CancelAt.DuringOutboxInsert ? SqlMatch.IsOutboxInsert : SqlMatch.IsOutboxPayloadUpdate;
                // Never fails a command: the predicate only cancels the token as the matching command is sent.
                host.Faults.FailCommands(sql =>
                {
                    if (match(sql)) cts.Cancel();
                    return false;
                }, FaultTiming.BeforeExecution);
            }

            var error = await Record.ExceptionAsync(() => db.SaveChangesAsync(cts.Token).WaitAsync(NoHang));
            Assert.True(cts.IsCancellationRequested);
            _output.WriteLine($"{point}: {error?.GetType().Name}: {error?.Message} / inner: {error?.InnerException?.GetType().Name}: {error?.InnerException?.Message}");
            Assert.NotNull(error);
            Assert.False(error is TimeoutException, "the save hung");

            // No partial state, whatever exception surfaced.
            Assert.Empty(log.WithEventId(NotUndoneEventId));
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, user.Id);
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            Assert.Single(db.Outbox.GetEvents());

            host.Faults.FailCommands(_ => false, FaultTiming.BeforeExecution, times: 0);
            await db.SaveChangesAsync(CancellationToken.None).WaitAsync(NoHang);
            await tx.CommitAsync();
        }

        AssertCommittedAndDispatchedOnce(host, "cancel@test");
    }

    // ---------------- 8. Pre-existing identity-map defect on SQL Server ----------------

    /// <summary>
    /// Item 8 on SQL Server: EF's save fails after the Users+OutboxMessages batch ran (the Orders batch fails), so EF
    /// discards the outbox row's identity value. The library's detach leaves that value in the identity map (stale
    /// entry, as on SQLite), but SQL Server does not reuse identity values after a rollback, so the retry gets a new
    /// value and succeeds: the defect is latent on SQL Server identity keys.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task EfSaveFailsAfterOutboxBatchRan_StaleIdentityMapEntryButRetrySucceeds(bool callerTransaction, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;
        AddUserWithEvent(db, "stale@test", "s1");
        host.Faults.FailCommands(sql => SqlMatch.WritesTo(sql, "Orders"), FaultTiming.BeforeExecution);

        var tx = callerTransaction ? await Begin(db, async) : null;
        try
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            Assert.NotNull(error);
            Assert.Contains(host.Faults.Commands, c => SqlMatch.IsOutboxInsert(c.Sql));

            var stateManager = db.GetService<IStateManager>();
            var key = db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindPrimaryKey()!;
            var stale = Enumerable.Range(1, 1000).Select(i => stateManager.TryGetEntry(key, new object[] { i })).FirstOrDefault(e => e != null);
            _output.WriteLine($"Stale outbox identity-map entry after the failed save: {(stale == null ? "none" : stale.EntityState.ToString())}");
            Assert.NotNull(stale);
            Assert.Equal(EntityState.Detached, stale!.EntityState);

            Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            if (tx != null) await Commit(tx, async);
        }
        finally
        {
            if (tx != null) await tx.DisposeAsync();
        }

        AssertCommittedAndDispatchedOnce(host, "stale@test");
    }
}

/// <summary>D5 with a Guid outbox key (own database: its tables differ from the int-key context's).</summary>
public class TesterGuidKeyCallerSavepointTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public TesterGuidKeyCallerSavepointTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static async Task<(int Users, int Outbox)> CountsInTransaction(GuidDbContext db)
        => (await db.Users.AsNoTracking().CountAsync(), await db.Set<OutboxMessage<Guid>>().AsNoTracking().CountAsync());

    private static User AddUserWithEvent(GuidDbContext db, string email, params string[] orders)
    {
        var user = Graph.NewUser(email, orders);
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });
        return user;
    }

    [SqlServerTheory]
    [InlineData(FaultTiming.BeforeExecution, false)]
    [InlineData(FaultTiming.BeforeExecution, true)]
    [InlineData(FaultTiming.AfterExecution, false)]
    [InlineData(FaultTiming.AfterExecution, true)]
    public async Task GuidKey_PayloadUpdateFailsInCallerTransaction_RetryPersistsAndDispatchesOnce(FaultTiming timing, bool async)
    {
        using var host = SmokeHost<Guid, GuidDbContext>.Create(_fixture);
        await using var scope = host.CreateScope();
        var db = scope.Db;

        await using (var tx = await Begin(db, async))
        {
            var user = AddUserWithEvent(db, "guid@test", "g1");
            db.Outbox.AddEvent(new OrderPlaced { Order = user.Orders[0] });
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, timing);
            Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<Guid>>());

            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        var users = host.Database.Users();
        var rows = host.OutboxRows();
        Assert.Single(users);
        Assert.Equal(2, rows.Count);
        GraphAssert.RowsMatch(rows, users, host.Database.Orders());
        Assert.Equal(rows.Select(r => Guid.Parse(r.Id)).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id).OrderBy(x => x));
    }

}
