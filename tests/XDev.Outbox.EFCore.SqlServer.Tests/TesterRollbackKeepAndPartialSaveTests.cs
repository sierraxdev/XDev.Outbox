#pragma warning disable EF1001 // IStateManager: used only to observe the identity map.
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Adversarial (tester) checks of fix-rollback-event-loss-and-retry-identity-conflict on SQL Server: never-saved
/// events kept by an explicit rollback inside a retrying strategy's user transaction, a rollback followed by an owned
/// save, and retries after EF's own save failed after its Users/OutboxMessages batch ran (several users and events,
/// owned, caller and retrying-strategy paths, AutoDetectChangesEnabled on and off).
/// </summary>
public class TesterRollbackKeepAndPartialSaveTests : IClassFixture<SmokeDatabaseFixture>
{
    private const int UserCount = 5;
    private readonly SmokeDatabaseFixture _fixture;

    public TesterRollbackKeepAndPartialSaveTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    private SmokeHost<int, SmokeDbContext> CreateHost(bool retrying = false)
        => SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RetryingStrategy = retrying });

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static User AddUserWithEvent(SmokeDbContext db, string email, params string[] orders)
    {
        var user = Graph.NewUser(email, orders);
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });
        return user;
    }

    private static void AssertCommittedAndDispatchedOnce(SmokeHost<int, SmokeDbContext> host, params string[] emails)
    {
        var users = host.Database.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        GraphAssert.RowsMatch(rows, users, host.Database.Orders());
        Assert.Equal(rows.Select(r => r.Id).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id.ToString()).OrderBy(x => x));
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
    }

    private static void AssertNoStaleOutboxIdentityEntry(DbContext db)
    {
        var stateManager = db.GetService<IStateManager>();
        var key = db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindPrimaryKey()!;
        Assert.All(Enumerable.Range(1, 200), id => Assert.Null(stateManager.TryGetEntry(key, new object[] { id })));
        Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
    }

    /// <summary>
    /// SQL Server retrying strategy with the documented user-transaction pattern; the first attempt's save fails after
    /// its writes and the lambda rolls back explicitly in a catch (before the fix the explicit rollback dropped the
    /// event, so the retry committed the user without an outbox row). The retry commits user, orders and event once.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RetryingStrategy_SaveFailsAndLambdaRollsBackExplicitly_RetryCommitsEventOnce(bool async)
    {
        using var host = CreateHost(retrying: true);
        await using var scope = host.CreateScope();
        var db = scope.Db;
        AddUserWithEvent(db, "strategy@test", "s1", "s2");
        host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);

        var attempts = 0;
        var strategy = db.Database.CreateExecutionStrategy();
        if (async)
        {
            await strategy.ExecuteAsync(async () =>
            {
                attempts++;
                await using var tx = await db.Database.BeginTransactionAsync();
                try
                {
                    await db.SaveChangesAsync(false);
                    await tx.CommitAsync();
                }
                catch
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
                attempts++;
                using var tx = db.Database.BeginTransaction();
                try
                {
                    db.SaveChanges(false);
                    tx.Commit();
                }
                catch
                {
                    tx.Rollback();
                    throw;
                }
            });
        }
        db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(2, attempts);
        Assert.Equal(1, host.Faults.CommandFailuresFired);
        AssertCommittedAndDispatchedOnce(host, "strategy@test");
        Assert.Equal(2, host.Database.Orders().Count);
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>A failed save, an explicit rollback, then an owned save of the same context: one row, dispatched once.</summary>
    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedSaveRollbackThenOwnedSave_EventPersistedOnceWithItsUser(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;

        await using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "owned@test", "o1");
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);
            Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }
        Assert.Single(db.Outbox.GetEvents());

        await Save.Invoke(db, async);

        AssertCommittedAndDispatchedOnce(host, "owned@test");
        Assert.Empty(db.Outbox.GetEvents());
    }

    public enum SavePath
    {
        Owned,
        Caller,
        OwnedRetryingStrategy,
    }

    public enum FailedBatch
    {
        /// <summary>The Users+OutboxMessages batch runs, then the call fails (no generated value is read back).</summary>
        UsersAndOutboxAfterExecution,

        /// <summary>The Users+OutboxMessages batch ran (keys propagated), the Orders batch runs and then fails.</summary>
        OrdersAfterExecution,

        /// <summary>The Users+OutboxMessages batch ran (keys propagated), the Orders batch is never sent.</summary>
        OrdersBeforeExecution,
    }

    public static TheoryData<SavePath, FailedBatch, bool, bool> PartialSaveMatrix()
    {
        var data = new TheoryData<SavePath, FailedBatch, bool, bool>();
        foreach (var path in Enum.GetValues<SavePath>())
        foreach (var batch in Enum.GetValues<FailedBatch>())
        foreach (var async in new[] { false, true })
        foreach (var autoDetect in new[] { true, false })
        {
            if (path == SavePath.OwnedRetryingStrategy && !autoDetect) continue;
            data.Add(path, batch, async, autoDetect);
        }
        return data;
    }

    [SqlServerTheory]
    [MemberData(nameof(PartialSaveMatrix))]
    public async Task PartialEfSaveWithSeveralUsersAndEvents_RetryPersistsEachOnceWithFinalKeys(SavePath path, FailedBatch batch, bool async, bool autoDetect)
    {
        using var host = CreateHost(retrying: path == SavePath.OwnedRetryingStrategy);
        await using var scope = host.CreateScope();
        var db = scope.Db;
        db.ChangeTracker.AutoDetectChangesEnabled = autoDetect;
        var emails = Enumerable.Range(1, UserCount).Select(i => $"p{i}@test").ToArray();
        foreach (var email in emails) AddUserWithEvent(db, email, email + "-o1", email + "-o2");

        switch (batch)
        {
            case FailedBatch.UsersAndOutboxAfterExecution:
                host.Faults.FailCommands(sql => SqlMatch.IsUserInsert(sql) || SqlMatch.IsOutboxInsert(sql), FaultTiming.AfterExecution);
                break;
            case FailedBatch.OrdersAfterExecution:
                host.Faults.FailCommands(sql => SqlMatch.WritesTo(sql, "Orders"), FaultTiming.AfterExecution);
                break;
            case FailedBatch.OrdersBeforeExecution:
                host.Faults.FailCommands(sql => SqlMatch.WritesTo(sql, "Orders"), FaultTiming.BeforeExecution);
                break;
        }

        var tx = path == SavePath.Caller ? await Begin(db, async) : null;
        try
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            if (path == SavePath.OwnedRetryingStrategy)
            {
                Assert.Null(error);
            }
            else
            {
                Assert.True(error is InjectedTransientException || error?.InnerException is InjectedTransientException, error?.ToString());
                Assert.Empty(host.Queue.Enqueued);
                AssertNoStaleOutboxIdentityEntry(db);
                if (!autoDetect) db.ChangeTracker.DetectChanges();
                Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            }
            Assert.Equal(1, host.Faults.CommandFailuresFired);
            if (tx != null) await Commit(tx, async);
        }
        finally
        {
            if (tx != null) await tx.DisposeAsync();
        }

        AssertCommittedAndDispatchedOnce(host, emails);
        Assert.Equal(UserCount * 2, host.Database.Orders().Count);
        Assert.Empty(db.Outbox.GetEvents());
    }
}
