using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Adversarial checks of the D5 fix (library savepoint "__XDevOutboxSavePoint" around a save with events inside a
/// caller-owned transaction) on SQLite: savepoint name collisions and nesting with caller savepoints, the
/// existence check after caller rollbacks, external transactions (UseTransaction), pooled contexts, user
/// interceptors that throw in or suppress savepoint operations, and cancellation around the savepoint.
/// </summary>
public class TesterAdversarialSavepointTests
{
    private const int NotUndoneEventId = 41002;
    private static readonly TimeSpan NoHang = TimeSpan.FromSeconds(30);

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

    private static async Task CreateSavepoint(IDbContextTransaction tx, string name, bool async)
    {
        if (async) await tx.CreateSavepointAsync(name); else tx.CreateSavepoint(name);
    }

    private static async Task RollbackToSavepoint(IDbContextTransaction tx, string name, bool async)
    {
        if (async) await tx.RollbackToSavepointAsync(name); else tx.RollbackToSavepoint(name);
    }

    private static async Task<(int Users, int Outbox)> CountsInTransaction(AdvDbContext db)
        => (await db.Users.AsNoTracking().CountAsync(), await db.Set<OutboxMessage<int>>().AsNoTracking().CountAsync());

    /// <summary>The next payload UPDATE runs, then the call fails (its row change is in the transaction).</summary>
    private static void FailNextPayloadUpdateAfterExecution(TesterSavepointHost host) => host.Probe.FailAfterExecution(SqlMatch.IsOutboxUpdate);

    private static User AddUserWithEvent(AdvDbContext db, string email)
    {
        var user = new User { Email = email };
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });
        return user;
    }

    /// <summary>Committed users are exactly <paramref name="emails"/>, one outbox row each with the user's key, each dispatched once.</summary>
    private static void AssertCommittedAndDispatchedOnce(TesterSavepointHost host, params string[] emails)
    {
        var users = host.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        Assert.Equal(users.Select(u => u.Id).OrderBy(x => x), rows.Select(r => Payload.User(r.Payload).GetProperty("id").GetInt32()).OrderBy(x => x));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id).OrderBy(x => x));
    }

    // ---------------- 1. Savepoint name collisions and nesting ----------------

    /// <summary>
    /// The caller's own savepoint uses the library's (or EF's) reserved name. SQLite resolves ROLLBACK TO/RELEASE to the
    /// most recent savepoint of that name, so the library's rollback must hit its own savepoint, not the caller's,
    /// and the caller's later rollback must hit the caller's.
    /// </summary>
    [Theory]
    [InlineData("__XDevOutboxSavePoint", false, false)]
    [InlineData("__XDevOutboxSavePoint", false, true)]
    [InlineData("__XDevOutboxSavePoint", true, false)]
    [InlineData("__XDevOutboxSavePoint", true, true)]
    [InlineData("__EFSavePoint", false, false)]
    [InlineData("__EFSavePoint", true, true)]
    public async Task CallerSavepointWithReservedName_FailedSaveRetried_ThenCallerRollsBackToIt_NothingCommittedOrDispatched(string name, bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, _) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            await CreateSavepoint(tx, name, async);
            var user = AddUserWithEvent(db, "a");
            FailNextPayloadUpdateAfterExecution(host);

            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            Assert.Equal((0, 0), await CountsInTransaction(db));

            await Save.Invoke(db, async, flag);
            Assert.Equal((1, 1), await CountsInTransaction(db));

            // The caller's savepoint must still exist (the library released only its own).
            await RollbackToSavepoint(tx, name, async);
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(flag ? EntityState.Unchanged : EntityState.Added, db.Entry(user).State);

            await Commit(tx, async);
        }

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
    }

    [Theory]
    [InlineData("__XDevOutboxSavePoint", false)]
    [InlineData("__XDevOutboxSavePoint", true)]
    [InlineData("__EFSavePoint", false)]
    [InlineData("__EFSavePoint", true)]
    public async Task CallerSavepointWithReservedName_FailedSaveRetried_ThenCommit_PersistsAndDispatchesOnce(string name, bool async)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            await CreateSavepoint(tx, name, async);
            AddUserWithEvent(db, "a");
            FailNextPayloadUpdateAfterExecution(host);
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "a");
        Assert.Empty(outbox.GetEvents());
    }

    public enum CallerSavepointPosition
    {
        /// <summary>Before both saves: the caller's rollback undoes both.</summary>
        BeforeFirstSave,

        /// <summary>Between the saves: the caller's rollback undoes only the second (failed, then retried) save.</summary>
        BetweenSaves,
    }

    /// <summary>
    /// Two saves with events; the second fails after its writes and is retried; then the caller rolls back to its own
    /// savepoint. The existence check (run because the rollback happens outside a save) must release exactly the
    /// messages whose rows were undone, and the commit must dispatch exactly the committed rows.
    /// </summary>
    [Theory]
    [InlineData(CallerSavepointPosition.BeforeFirstSave, false, false)]
    [InlineData(CallerSavepointPosition.BeforeFirstSave, false, true)]
    [InlineData(CallerSavepointPosition.BeforeFirstSave, true, false)]
    [InlineData(CallerSavepointPosition.BeforeFirstSave, true, true)]
    [InlineData(CallerSavepointPosition.BetweenSaves, false, false)]
    [InlineData(CallerSavepointPosition.BetweenSaves, false, true)]
    [InlineData(CallerSavepointPosition.BetweenSaves, true, false)]
    [InlineData(CallerSavepointPosition.BetweenSaves, true, true)]
    public async Task TwoSaves_SecondFailsAndIsRetried_CallerRollsBackToItsSavepoint_CommitsAndDispatchesExactlyTheSurvivingRows(CallerSavepointPosition position, bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            if (position == CallerSavepointPosition.BeforeFirstSave) await CreateSavepoint(tx, "caller", async);
            AddUserWithEvent(db, "first");
            await Save.Invoke(db, async, flag);
            if (!flag) db.ChangeTracker.AcceptAllChanges();

            if (position == CallerSavepointPosition.BetweenSaves) await CreateSavepoint(tx, "caller", async);
            AddUserWithEvent(db, "second");
            FailNextPayloadUpdateAfterExecution(host);
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            Assert.Equal((1, 1), await CountsInTransaction(db));
            await Save.Invoke(db, async, flag);
            Assert.Equal((2, 2), await CountsInTransaction(db));

            await RollbackToSavepoint(tx, "caller", async);
            Assert.Equal(position == CallerSavepointPosition.BeforeFirstSave ? (0, 0) : (1, 1), await CountsInTransaction(db));

            await Commit(tx, async);
        }

        if (position == CallerSavepointPosition.BeforeFirstSave)
        {
            Assert.Empty(host.Users());
            Assert.Empty(host.OutboxRows());
            Assert.Empty(host.Queue.Enqueued);
        }
        else
        {
            AssertCommittedAndDispatchedOnce(host, "first");
        }
        Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
    }

    /// <summary>
    /// A caller savepoint created between a successful library save and its own later save shares no name with the
    /// library's; the library's rollback inside the failing save must not undo the earlier save's rows (the caller's
    /// savepoint, created later, sits between them).
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task ThreeSavesWithFailures_EachFailedSaveUndoesOnlyItself(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            for (var i = 1; i <= 3; i++)
            {
                AddUserWithEvent(db, "u" + i);
                FailNextPayloadUpdateAfterExecution(host);
                Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
                Assert.Equal((i - 1, i - 1), await CountsInTransaction(db));
                await Save.Invoke(db, async, flag);
                Assert.Equal((i, i), await CountsInTransaction(db));
                if (!flag) db.ChangeTracker.AcceptAllChanges();
            }
            Assert.Empty(host.Queue.Enqueued);
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "u1", "u2", "u3");
        Assert.Empty(outbox.GetEvents());
    }

    // ---------------- 2. External transaction (UseTransaction) and pooled contexts ----------------

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task UseTransactionWithExternalDbTransaction_PayloadUpdateFailsThenRetry_PersistsAndDispatchesOnce(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        using (var external = host.Connection.BeginTransaction())
        {
            var tx = async ? await db.Database.UseTransactionAsync(external) : db.Database.UseTransaction(external);
            Assert.NotNull(tx);

            AddUserWithEvent(db, "a");
            FailNextPayloadUpdateAfterExecution(host);
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            Assert.Contains("RolledBackToSavepoint", host.Probe.Events);
            Assert.Equal((0, 0), await CountsInTransaction(db));

            await Save.Invoke(db, async, flag);
            // Committed through EF's wrapper so the dispatch interceptor observes the commit.
            await Commit(tx!, async);
        }

        AssertCommittedAndDispatchedOnce(host, "a");
        Assert.Empty(outbox.GetEvents());
        Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
    }

    /// <summary>
    /// Pooled context, second lease: the first lease leaves a failed save in an abandoned caller transaction; the
    /// second lease of the same instance fails after EF's save, retries and commits.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task PooledContext_SecondLeaseFailedSaveRetriedInSameTransaction_PersistsAndDispatchesOnlyItsRowsOnce(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create(pooled: true);
        Guid instance;

        var (scope1, db1, _) = host.CreateScope();
        using (scope1)
        {
            instance = db1.ContextId.InstanceId;
            var tx = await Begin(db1, async);
            AddUserWithEvent(db1, "lease1");
            FailNextPayloadUpdateAfterExecution(host);
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db1, async, flag)));
            // Abandoned: the scope ends without commit/rollback/dispose of the transaction.
        }

        var (scope2, db2, outbox2) = host.CreateScope();
        using (scope2)
        {
            Assert.Equal(instance, db2.ContextId.InstanceId);
            Assert.Empty(outbox2.GetEvents());
            Assert.Null(db2.Database.CurrentTransaction);

            using (var tx = await Begin(db2, async))
            {
                AddUserWithEvent(db2, "lease2");
                FailNextPayloadUpdateAfterExecution(host);
                Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db2, async, flag)));
                Assert.Equal((0, 0), await CountsInTransaction(db2));
                await Save.Invoke(db2, async, flag);
                await Commit(tx, async);
            }
            Assert.Empty(outbox2.GetEvents());
        }

        AssertCommittedAndDispatchedOnce(host, "lease2");
        Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
    }

    // ---------------- 3. User interceptors on savepoint operations ----------------

    /// <summary>
    /// A user interceptor throws when the library creates its savepoint (the first savepoint of the save): nothing
    /// was written, the interceptor's exception surfaces unchanged, the tracker is as before, no warning; the save
    /// can be retried in the same transaction.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task InterceptorThrowsCreatingLibrarySavepoint_OriginalExceptionNothingWrittenRetrySucceeds(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            var user = AddUserWithEvent(db, "a");
            host.Sabotage.Reset();
            host.Sabotage.ThrowOnCreate = 1;

            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.IsType<SabotageException>(error);
            Assert.Equal(1, host.Sabotage.Fired);
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, user.Id);
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            Assert.Single(outbox.GetEvents());
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Empty(host.Log.WithEventId(NotUndoneEventId));

            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "a");
    }

    /// <summary>
    /// A user interceptor throws when the library releases its savepoint after a fully successful attempt (EF's
    /// release is #1, the library's is #2). The library treats it as a failed save: it rolls back to its savepoint,
    /// restores the tracker and rethrows the interceptor's exception; nothing stays in the transaction.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task InterceptorThrowsReleasingLibrarySavepoint_SaveFailsUndoneAndRetrySucceeds(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            var user = AddUserWithEvent(db, "a");
            host.Sabotage.Reset();
            host.Sabotage.ThrowOnRelease = 2;
            host.Probe.ClearEvents();

            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.IsType<SabotageException>(error);
            Assert.Contains("RolledBackToSavepoint", host.Probe.Events);
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, user.Id);
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Empty(host.Queue.Enqueued);
            Assert.Empty(host.Log.WithEventId(NotUndoneEventId));

            host.Sabotage.Reset();
            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "a");
        Assert.Empty(outbox.GetEvents());
    }

    /// <summary>
    /// The payload UPDATE fails, the rollback to the library's savepoint succeeds, and the best-effort release after it
    /// throws (EF's release is #1, the library's best-effort release is #2): the release error is swallowed, the
    /// original exception surfaces, no warning, nothing is left and the retry (whose releases succeed) persists once.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task BestEffortReleaseAfterRollbackThrows_OriginalExceptionNoWarningRetrySucceeds(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            var user = AddUserWithEvent(db, "a");
            host.Sabotage.Reset();
            host.Sabotage.ThrowOnRelease = 2;
            FailNextPayloadUpdateAfterExecution(host);

            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.IsType<InjectedFailureException>(error);
            Assert.Equal(1, host.Sabotage.Fired);
            Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(EntityState.Added, db.Entry(user).State);

            host.Sabotage.Reset();
            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "a");
        Assert.Empty(outbox.GetEvents());
    }

    /// <summary>
    /// A user interceptor suppresses the creation of the library's savepoint (EF reports it created but never runs
    /// SAVEPOINT). The release then fails (no such savepoint), the rollback fails too: the library must log warning
    /// 41002 and keep the guidance (roll back the transaction), the tracker is restored, and rolling back then saving
    /// again persists everything once.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task InterceptorSuppressesLibrarySavepoint_WarningLoggedRollbackThenRetryPersistsOnce(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        User user;
        using (var tx = await Begin(db, async))
        {
            user = AddUserWithEvent(db, "a");
            host.Sabotage.Reset();
            host.Sabotage.SuppressCreate = 1;

            // The save's writes all succeeded; the release of the never-created savepoint fails and that error surfaces.
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.Contains("no such savepoint", Assert.IsType<Microsoft.Data.Sqlite.SqliteException>(error).Message);
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, user.Id);
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            Assert.Empty(host.Queue.Enqueued);
            var warning = Assert.Single(host.Log.WithEventId(NotUndoneEventId));
            Assert.Contains("no such savepoint", Assert.IsType<Microsoft.Data.Sqlite.SqliteException>(warning.Exception).Message);
            Assert.Equal((1, 1), await CountsInTransaction(db)); // not undone, as the warning says

            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        host.Sabotage.Reset();
        Assert.Single(outbox.GetEvents()); // never saved: kept by the rollback, follows the still-Added user

        using (var tx = await Begin(db, async))
        {
            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }
        AssertCommittedAndDispatchedOnce(host, "a");
    }

    // ---------------- 5. Cancellation around the savepoint ----------------

    public enum CancelAt
    {
        /// <summary>The token is cancelled before SaveChangesAsync is called.</summary>
        BeforeSave,

        /// <summary>Cancelled while the library's savepoint is being created (CreatingSavepoint).</summary>
        CreatingLibrarySavepoint,

        /// <summary>Cancelled right after the library's savepoint was created, before EF's save.</summary>
        AfterLibrarySavepointCreated,

        /// <summary>Cancelled after the payload UPDATE ran: only the library's release still sees the token.</summary>
        AfterPayloadUpdate,
    }

    [Theory]
    [InlineData(CancelAt.BeforeSave, false)]
    [InlineData(CancelAt.BeforeSave, true)]
    [InlineData(CancelAt.CreatingLibrarySavepoint, false)]
    [InlineData(CancelAt.CreatingLibrarySavepoint, true)]
    [InlineData(CancelAt.AfterLibrarySavepointCreated, false)]
    [InlineData(CancelAt.AfterLibrarySavepointCreated, true)]
    [InlineData(CancelAt.AfterPayloadUpdate, false)]
    [InlineData(CancelAt.AfterPayloadUpdate, true)]
    public async Task SaveAsync_CancelledAroundTheLibrarySavepoint_NoPartialStateNoHangAndRetryPersistsOnce(CancelAt point, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        var user = AddUserWithEvent(db, "a");

        using var cts = new CancellationTokenSource();
        await using (var tx = await db.Database.BeginTransactionAsync())
        {
            host.Sabotage.Reset();
            host.Sabotage.CancelSource = cts;
            switch (point)
            {
                case CancelAt.BeforeSave: cts.Cancel(); break;
                case CancelAt.CreatingLibrarySavepoint: host.Sabotage.CancelOnCreating = 1; break;
                case CancelAt.AfterLibrarySavepointCreated: host.Sabotage.CancelOnCreated = 1; break;
                case CancelAt.AfterPayloadUpdate: host.Probe.CancelAfterCommand(SqlMatch.IsOutboxUpdate, cts); break;
            }

            var error = await Record.ExceptionAsync(() => db.SaveChangesAsync(flag, cts.Token).WaitAsync(NoHang));
            Assert.True(cts.IsCancellationRequested);
            Assert.IsAssignableFrom<OperationCanceledException>(error);

            // No partial state: nothing in the transaction, tracker as before the save, event still buffered.
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, user.Id);
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            Assert.Single(outbox.GetEvents());
            Assert.Empty(host.Log.WithEventId(NotUndoneEventId));

            host.Sabotage.Reset();
            await db.SaveChangesAsync(flag, CancellationToken.None).WaitAsync(NoHang);
            await tx.CommitAsync();
        }

        AssertCommittedAndDispatchedOnce(host, "a");
    }

    // ---------------- Never-saved events on a transaction end without commit (fix-rollback-event-loss) ----------------

    /// <summary>
    /// After a failed save in a caller transaction the tracker is restored (the user stays Added) and no message is
    /// pending, so the event is never saved. An explicit Rollback (what warning 41002 tells the caller to do) keeps it
    /// buffered exactly like a dispose without Rollback, so saving again in a new transaction commits the user WITH its
    /// outbox row, dispatched once. (Before the fix, the explicit Rollback dropped the event and the retry committed
    /// the user without an outbox row.)
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FailedSave_CallerRollsBackOrDisposes_RetryInNewTransactionCommitsUserWithOutboxRowDispatchedOnce(bool explicitRollback, bool async)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        var user = AddUserWithEvent(db, "a");
        var tx = await Begin(db, async);
        FailNextPayloadUpdateAfterExecution(host);
        Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
        Assert.Equal(EntityState.Added, db.Entry(user).State);
        Assert.Single(outbox.GetEvents());
        if (explicitRollback)
        {
            if (async) await tx.RollbackAsync(); else tx.Rollback();
            Assert.Single(outbox.GetEvents()); // never saved: kept by the rollback
        }
        if (async) await tx.DisposeAsync(); else tx.Dispose();

        using (var retry = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            await Commit(retry, async);
        }

        AssertCommittedAndDispatchedOnce(host, "a");
    }

    // ---------------- 6. Existence check after a failed-then-retried save ----------------

    /// <summary>
    /// Pending messages of an earlier save, a failed-then-retried save, and a caller rollback to a savepoint created
    /// after the earlier save: the retried save's message is released (its event follows its business changes),
    /// the earlier one is kept and is the only one dispatched; the commit writes exactly its rows.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task PendingFromEarlierSave_FailedThenRetriedSave_CallerRollback_ReleasesOnlyTheUndoneMessage(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        UserCreated secondEvent;
        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "first");
            await Save.Invoke(db, async, flag);
            if (!flag) db.ChangeTracker.AcceptAllChanges();

            await CreateSavepoint(tx, "sp", async);
            var second = new User { Email = "second" };
            db.Users.Add(second);
            secondEvent = new UserCreated { User = second };
            outbox.AddEvent(secondEvent);
            FailNextPayloadUpdateAfterExecution(host);
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            await Save.Invoke(db, async, flag);

            var before = host.Faults.ExecutedCommands.Count;
            await RollbackToSavepoint(tx, "sp", async);
            var existenceQueries = host.Faults.ExecutedCommands.Skip(before).Count(sql => sql.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) && sql.Contains("OutboxMessages"));
            Assert.Equal(1, existenceQueries);

            // B1: the released message's event stays buffered only when its changes were not accepted.
            var buffered = outbox.GetEvents();
            if (flag) Assert.DoesNotContain(secondEvent, buffered);
            else Assert.Contains(secondEvent, buffered);

            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "first");
    }
}
