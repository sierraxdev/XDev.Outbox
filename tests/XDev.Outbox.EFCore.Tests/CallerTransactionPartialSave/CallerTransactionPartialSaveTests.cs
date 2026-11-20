using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// D5: an outbox save inside a caller-owned transaction that fails after EF's own save (payload re-serialization,
/// payload UPDATEs, completion) must leave nothing in the transaction, so retrying the save in the same transaction
/// persists and dispatches everything exactly once. The library wraps such a save in its own savepoint
/// (<c>__XDevOutboxSavePoint</c>) when the transaction supports savepoints and <c>AutoSavepointsEnabled</c> is true;
/// otherwise a failure after EF's save logs warning 41002 and the caller must roll back.
/// </summary>
public class CallerTransactionPartialSaveTests
{
    private const int NotUndoneEventId = 41002;

    public static TheoryData<bool, bool> AsyncFlag()
    {
        var data = new TheoryData<bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(async, flag);
        return data;
    }

    public enum PostInsertFailure
    {
        FirstPayloadUpdateBeforeExecution,
        FirstPayloadUpdateAfterExecution,
        SecondPayloadUpdateBeforeExecution,
        SecondPayloadUpdateAfterExecution,
    }

    public static TheoryData<PostInsertFailure, bool, bool> FailureAsyncFlag()
    {
        var data = new TheoryData<PostInsertFailure, bool, bool>();
        foreach (var failure in Enum.GetValues<PostInsertFailure>())
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(failure, async, flag);
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

    /// <summary>Counts read inside the open transaction (through the context's connection).</summary>
    private static async Task<(int Users, int Outbox)> CountsInTransaction(AdvDbContext db)
        => (await db.Users.AsNoTracking().CountAsync(), await db.Set<OutboxMessage<int>>().AsNoTracking().CountAsync());

    private static void FailPayloadUpdate(SavepointHost host, int occurrence, bool afterExecution)
    {
        var updates = 0;
        bool Predicate(string sql) => SqlMatch.IsOutboxUpdate(sql) && ++updates == occurrence;
        if (afterExecution) host.Probe.FailAfterExecution(Predicate);
        else host.Faults.FailCommands(Predicate);
    }

    private static int FailuresFired(SavepointHost host) => host.Faults.CommandFailuresFired + host.Probe.AfterExecutionFailuresFired;

    /// <summary>Every user row exists once, each outbox payload carries its user's database key, each row was dispatched once.</summary>
    private static void AssertPersistedOnce(SavepointHost host, IOutbox outbox, int users)
    {
        var userIds = host.UserIds();
        var rows = host.OutboxRows();
        Assert.Equal(users, userIds.Count);
        Assert.Equal(users, rows.Count);
        Assert.Equal(userIds, rows.Select(r => Payload.User(r.Payload).GetProperty("id").GetInt32()));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id));
        Assert.Empty(outbox.GetEvents());
    }

    // ---------------- Failure after the base save, retry in the same transaction ----------------

    [Theory]
    [MemberData(nameof(FailureAsyncFlag))]
    public async Task PayloadUpdateFails_RetryInTheSameTransactionThenCommit_PersistsAndDispatchesOnce(PostInsertFailure failure, bool async, bool flag)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var a = new User { Email = "a" };
        var b = new User { Email = "b" };
        db.Users.AddRange(a, b);
        outbox.AddEvent(new UserCreated { User = a }, new UserCreated { User = b });
        var occurrence = failure is PostInsertFailure.SecondPayloadUpdateBeforeExecution or PostInsertFailure.SecondPayloadUpdateAfterExecution ? 2 : 1;
        FailPayloadUpdate(host, occurrence, failure is PostInsertFailure.FirstPayloadUpdateAfterExecution or PostInsertFailure.SecondPayloadUpdateAfterExecution);

        using (var tx = await Begin(db, async))
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.IsType<InjectedFailureException>(error);
            Assert.Equal(1, FailuresFired(host));

            // The library rolled back to its savepoint: nothing of the failed save is left in the transaction.
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(EntityState.Added, db.Entry(a).State);
            Assert.Equal(EntityState.Added, db.Entry(b).State);
            Assert.Equal(0, a.Id);
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            Assert.Equal(2, outbox.GetEvents().Count);

            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }

        AssertPersistedOnce(host, outbox, users: 2);
        Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
    }

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task PayloadReserializationThrows_RetryInTheSameTransactionThenCommit_PersistsAndDispatchesOnce(bool async, bool flag)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new ExplodingAfterKeyEvent { User = user });

        using (var tx = await Begin(db, async))
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.IsType<InvalidOperationException>(error);
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, user.Id);

            // The application replaces the bad event and retries.
            outbox.ClearEvents();
            outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }

        AssertPersistedOnce(host, outbox, users: 1);
    }

    public enum CancelPoint
    {
        /// <summary>Inside EF's save: later reads/commands of the save see the cancelled token.</summary>
        AfterOutboxInsertExecuted,

        /// <summary>After EF's save completed: the payload UPDATE is the first call to see the cancelled token.</summary>
        AfterEfSavepointReleased,
    }

    [Theory]
    [InlineData(CancelPoint.AfterOutboxInsertExecuted, false)]
    [InlineData(CancelPoint.AfterOutboxInsertExecuted, true)]
    [InlineData(CancelPoint.AfterEfSavepointReleased, false)]
    [InlineData(CancelPoint.AfterEfSavepointReleased, true)]
    public async Task SaveAsync_CancelledAfterTheInserts_StillRollsBackAndRetryPersistsOnce(CancelPoint point, bool flag)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });

        using var cts = new CancellationTokenSource();
        using (var tx = await db.Database.BeginTransactionAsync())
        {
            host.Probe.ClearEvents();
            if (point == CancelPoint.AfterOutboxInsertExecuted) host.Probe.CancelAfterCommand(SqlMatch.IsOutboxInsert, cts);
            else host.Probe.CancelOnFirstSavepointRelease(cts);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.SaveChangesAsync(flag, cts.Token));
            Assert.True(cts.IsCancellationRequested);
            Assert.Contains("RolledBackToSavepoint", host.Probe.Events);

            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(EntityState.Added, db.Entry(user).State);

            await db.SaveChangesAsync(flag, CancellationToken.None);
            await tx.CommitAsync();
        }

        AssertPersistedOnce(host, outbox, users: 1);
    }

    public enum OtherEntity
    {
        /// <summary>Without the rollback the retried DELETE affects no row (DbUpdateConcurrencyException).</summary>
        Deleted,

        /// <summary>Without the rollback the retried INSERT violates the primary key.</summary>
        ClientGuidKey,
    }

    [Theory]
    [InlineData(OtherEntity.Deleted, false, false)]
    [InlineData(OtherEntity.Deleted, false, true)]
    [InlineData(OtherEntity.Deleted, true, false)]
    [InlineData(OtherEntity.Deleted, true, true)]
    [InlineData(OtherEntity.ClientGuidKey, false, false)]
    [InlineData(OtherEntity.ClientGuidKey, false, true)]
    [InlineData(OtherEntity.ClientGuidKey, true, false)]
    [InlineData(OtherEntity.ClientGuidKey, true, true)]
    public async Task PayloadUpdateFailsWithOtherEntityInTheSave_RetryInTheSameTransactionSucceeds(OtherEntity other, bool async, bool flag)
    {
        using var host = SavepointHost.Create();
        if (other == OtherEntity.Deleted) host.Exec("INSERT INTO \"Users\" (\"Email\") VALUES ('seeded')");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var docId = Guid.NewGuid();
        if (other == OtherEntity.Deleted) db.Users.Remove(await db.Users.SingleAsync());
        else db.Docs.Add(new Doc { Id = docId, Title = "t" });
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        FailPayloadUpdate(host, 1, afterExecution: false);

        using (var tx = await Begin(db, async))
        {
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            await Commit(tx, async);
        }

        AssertPersistedOnce(host, outbox, users: 1);
        if (other == OtherEntity.Deleted) Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\" WHERE \"Email\" = 'seeded'"));
        else Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Docs\""));
    }

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task TwoSaves_SecondFailsAfterTheInsertsThenRetries_EarlierSaveKeptAndAllDispatchedOnce(bool async, bool flag)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await Begin(db, async))
        {
            var first = new User { Email = "first" };
            db.Users.Add(first);
            outbox.AddEvent(new UserCreated { User = first });
            await Save.Invoke(db, async, flag);
            // Plain EF: after SaveChanges(false) the caller accepts, or the next save writes the changes again.
            if (!flag) db.ChangeTracker.AcceptAllChanges();

            var second = new User { Email = "second" };
            db.Users.Add(second);
            outbox.AddEvent(new UserCreated { User = second });
            FailPayloadUpdate(host, 1, afterExecution: true);

            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            Assert.Equal((1, 1), await CountsInTransaction(db));
            Assert.Equal(2, outbox.GetEvents().Count);

            await Save.Invoke(db, async, flag);
            Assert.Empty(host.Queue.Enqueued);
            await Commit(tx, async);
        }

        AssertPersistedOnce(host, outbox, users: 2);
    }

    // ---------------- Caller savepoints ----------------

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task FailedSaveAfterCallerSavepoint_CallerRollsBackToItsSavepoint_NothingPersistedOrDispatched(bool retryBeforeRollback, bool async, bool flag)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        var evt = new UserCreated { User = user };
        int existenceQueries;
        using (var tx = await Begin(db, async))
        {
            if (async) await tx.CreateSavepointAsync("before"); else tx.CreateSavepoint("before");

            db.Users.Add(user);
            outbox.AddEvent(evt);
            FailPayloadUpdate(host, 1, afterExecution: true);
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            if (retryBeforeRollback) await Save.Invoke(db, async, flag);

            var before = host.Faults.ExecutedCommands.Count;
            if (async) await tx.RollbackToSavepointAsync("before"); else tx.RollbackToSavepoint("before");
            existenceQueries = host.Faults.ExecutedCommands.Skip(before).Count(IsOutboxSelect);

            if (retryBeforeRollback)
            {
                // B1: the released message's event follows its business changes.
                Assert.Equal(flag ? 0 : 1, outbox.GetEvents().Count);
                Assert.Equal(flag ? EntityState.Unchanged : EntityState.Added, db.Entry(user).State);
            }
            else
            {
                Assert.Same(evt, Assert.Single(outbox.GetEvents()));
                Assert.Equal(EntityState.Added, db.Entry(user).State);
            }

            await Commit(tx, async);
        }

        // The caller's rollback after a successful save checks which pending rows survived; without one there is nothing to check.
        Assert.Equal(retryBeforeRollback ? 1 : 0, existenceQueries);
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        // Committing drops never-saved events; a released, not accepted event stays buffered (B1).
        Assert.Equal(retryBeforeRollback && !flag ? 1 : 0, outbox.GetEvents().Count);
    }

    // ---------------- Interceptor events ----------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SavesInCallerTransaction_LibrarySavepointWrapsEachSaveAndItsRollbackRunsNoExistenceQuery(bool async)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await Begin(db, async))
        {
            var first = new User { Email = "first" };
            db.Users.Add(first);
            outbox.AddEvent(new UserCreated { User = first });
            host.Probe.ClearEvents();
            await Save.Invoke(db, async);

            // Library savepoint, EF's savepoint around its own save, EF's release, library release.
            Assert.Equal(new[] { "CreatedSavepoint", "CreatedSavepoint", "ReleasedSavepoint", "ReleasedSavepoint" }, TransactionEvents(host));

            var second = new User { Email = "second" };
            db.Users.Add(second);
            outbox.AddEvent(new UserCreated { User = second });
            FailPayloadUpdate(host, 1, afterExecution: true);
            host.Probe.ClearEvents();
            var before = host.Faults.ExecutedCommands.Count;

            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));

            Assert.Equal(
                new[] { "CreatedSavepoint", "CreatedSavepoint", "ReleasedSavepoint", "RollingBackToSavepoint", "RolledBackToSavepoint", "ReleasedSavepoint" },
                TransactionEvents(host));
            // The rollback to the library's savepoint happens inside the save: the pending message of the first save
            // is not checked (the savepoint postdates it).
            Assert.DoesNotContain(host.Faults.ExecutedCommands.Skip(before), IsOutboxSelect);

            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        AssertPersistedOnce(host, outbox, users: 2);
    }

    // ---------------- Rollback failure and fallback (warning 41002) ----------------

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task RollbackToLibrarySavepointFails_OriginalExceptionRethrownAndWarningLogged(bool async, bool flag)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        FailPayloadUpdate(host, 1, afterExecution: false);
        host.Probe.FailRollbacksToSavepoint();

        using (var tx = await Begin(db, async))
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.IsType<InjectedFailureException>(error);
            Assert.Equal(1, host.Probe.RollbackToSavepointFailuresFired);

            var warning = Assert.Single(host.Log.WithEventId(NotUndoneEventId));
            Assert.Equal(LogLevel.Warning, warning.Level);
            Assert.Equal("XDev.Outbox.EFCore.CallerTransactionSaveNotUndone", warning.EventId.Name);
            Assert.Equal("Microsoft.EntityFrameworkCore.Database.Transaction", warning.Category);
            Assert.IsType<RollbackToSavepointFailureException>(warning.Exception);

            // The tracker is restored anyway; the rows of the failed save are still in the transaction.
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal((1, 1), await CountsInTransaction(db));

            // What the warning asks for.
            await Rollback(tx, async);
        }

        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        // The failed save left no pending message, so its event was never saved: the rollback keeps it (it follows
        // the still-Added user).
        Assert.Single(outbox.GetEvents());

        using (var tx = await Begin(db, async))
        {
            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }
        AssertPersistedOnce(host, outbox, users: 1);
    }

    /// <summary>
    /// Q2: with AutoSavepointsEnabled = false the library creates no savepoint (as EF creates none). A failure after
    /// EF's save then leaves the rows in the caller's transaction (pinned) and logs warning 41002; rolling back and
    /// saving again persists everything once.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task AutoSavepointsDisabled_NoLibrarySavepoint_FailureAfterEfSaveLogsWarningAndRollbackThenRetryPersistsOnce(bool async, bool flag)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;
        db.Database.AutoSavepointsEnabled = false;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        FailPayloadUpdate(host, 1, afterExecution: true);

        using (var tx = await Begin(db, async))
        {
            host.Probe.ClearEvents();
            Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));

            Assert.Empty(TransactionEvents(host));
            var warning = Assert.Single(host.Log.WithEventId(NotUndoneEventId));
            Assert.Equal(LogLevel.Warning, warning.Level);
            Assert.Equal("XDev.Outbox.EFCore.CallerTransactionSaveNotUndone", warning.EventId.Name);
            Assert.Equal((1, 1), await CountsInTransaction(db)); // fallback: nothing undone
            Assert.Equal(EntityState.Added, db.Entry(user).State);

            await Rollback(tx, async);
        }

        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(host.OutboxRows());
        Assert.Single(outbox.GetEvents()); // never saved: kept by the rollback

        using (var tx = await Begin(db, async))
        {
            await Save.Invoke(db, async, flag);
            await Commit(tx, async);
        }

        AssertPersistedOnce(host, outbox, users: 1);
        Assert.Single(host.Log.WithEventId(NotUndoneEventId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AutoSavepointsDisabled_FailureBeforeEfSaveCompleted_NoWarning(bool async)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;
        db.Database.AutoSavepointsEnabled = false;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        using (var tx = await Begin(db, async))
        {
            // EF wraps a failure of its own save in DbUpdateException.
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async))));
            await Rollback(tx, async);
        }

        Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SuccessfulAndFailedSavesWithSavepoints_LogNoWarning(bool async)
    {
        using var host = SavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        // The outbox INSERT is EF's first command here: it fails before anything was written.
        host.Faults.FailCommands(SqlMatch.IsOutboxInsert);

        using (var tx = await Begin(db, async))
        {
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async))));
            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        AssertPersistedOnce(host, outbox, users: 1);
        Assert.Empty(host.Log.WithEventId(NotUndoneEventId));
    }

    private static bool IsOutboxSelect(string sql)
        => sql.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) && sql.Contains("OutboxMessages", StringComparison.Ordinal);

    private static List<string> TransactionEvents(SavepointHost host)
        => host.Probe.Events.Where(e => !e.StartsWith("Command: ", StringComparison.Ordinal)).ToList();
}
