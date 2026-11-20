#pragma warning disable EF1001 // IStateManager: used only to observe the identity map.
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Retry after EF's own save failed partway (fix-rollback-event-loss-and-retry-identity-conflict, bug 2): an earlier
/// command of the save ran (the outbox INSERT got its identity value), then a later one failed (Users or Orders
/// INSERT). EF discards the store-generated key (the entry's key goes back to its temporary value) but its identity
/// map keeps the entry under the discarded value until a DetectChanges. Before the fix, RecoverFailedAttempt
/// detached the outbox entries without one, so EF removed them under their temporary key and the discarded key stayed
/// in the identity map; SQLite hands out the same rowid again, so the retry threw "The instance of entity type
/// 'OutboxMessage&lt;int&gt;' cannot be tracked because another instance with the same key value for {'Id'} is already
/// being tracked" (owned, caller and retrying-strategy paths). The recovery now detects key changes of each outbox
/// entry before detaching it, also when AutoDetectChangesEnabled is false.
/// </summary>
public class RetryAfterPartialEfSaveTests
{
    public static TheoryData<bool, bool, bool> AsyncFlagAutoDetect()
    {
        var data = new TheoryData<bool, bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
        foreach (var autoDetectChanges in new[] { true, false })
            data.Add(async, flag, autoDetectChanges);
        return data;
    }

    private static bool IsOrderInsert(string sql) => sql.Contains("INSERT INTO \"Orders\"", StringComparison.Ordinal);

    /// <summary>No outbox entity is left in EF's identity map under a key the database could hand out again.</summary>
    private static void AssertNoStaleOutboxIdentityEntry(DbContext db)
    {
        var stateManager = db.GetService<IStateManager>();
        var key = db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindPrimaryKey()!;
        Assert.All(Enumerable.Range(1, 20), id => Assert.Null(stateManager.TryGetEntry(key, new object[] { id })));
    }

    private static async Task<(int Users, int Outbox)> CountsInTransaction(AdvDbContext db)
        => (await db.Users.AsNoTracking().CountAsync(), await db.Set<OutboxMessage<int>>().AsNoTracking().CountAsync());

    [Theory]
    [MemberData(nameof(AsyncFlagAutoDetect))]
    public async Task OwnedSave_UserInsertFailsAfterOutboxInsertRan_ApplicationRetry_PersistsAndDispatchesOnce(bool async, bool flag, bool autoDetectChanges)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        db.ChangeTracker.AutoDetectChangesEnabled = autoDetectChanges;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
        Assert.True(ExceptionChain.Contains<InjectedFailureException>(error));
        Assert.Contains(host.Faults.ExecutedCommands, SqlMatch.IsOutboxInsert); // the outbox INSERT ran first
        AssertNoStaleOutboxIdentityEntry(db);

        Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    [Theory]
    [MemberData(nameof(AsyncFlagAutoDetect))]
    public async Task CallerTransaction_UserInsertFailsAfterOutboxInsertRan_RetryInSameTransaction_PersistsAndDispatchesOnce(bool async, bool flag, bool autoDetectChanges)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        db.ChangeTracker.AutoDetectChangesEnabled = autoDetectChanges;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        await using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(error));
            Assert.Equal((0, 0), await CountsInTransaction(db));
            AssertNoStaleOutboxIdentityEntry(db);

            Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    /// <summary>The retrying execution strategy's retry hits the same collision, which is not transient, so the save fails.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSaveWithRetryingStrategy_UserInsertFailsOnceAfterOutboxInsertRan_RetrySucceeds(bool async)
    {
        using var host = TesterSavepointHost.Create(retrying: true);
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
        Assert.Equal(1, host.Faults.CommandFailuresFired);
        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    /// <summary>
    /// The Users and outbox INSERTs ran, then the Orders INSERT failed: EF discards the keys of the user and of the
    /// outbox message. The library leaves no stale outbox entry; the user stays tracked (EF fixes its identity-map
    /// entry at the retry's DetectChanges, as for plain EF), and the retry persists the user, its order and the
    /// event once. With AutoDetectChangesEnabled = false the application calls ChangeTracker.DetectChanges() before
    /// retrying, as plain EF requires for the business entities.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlagAutoDetect))]
    public async Task OrdersInsertFailsAfterUserAndOutboxInsertsRan_RetryPersistsGraphAndDispatchesOnce(bool async, bool callerTransaction, bool autoDetectChanges)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        db.ChangeTracker.AutoDetectChangesEnabled = autoDetectChanges;
        var user = new User { Email = "a" };
        user.Orders.Add(new Order { Description = "o" });
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(IsOrderInsert);

        await using (var tx = callerTransaction ? (async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction()) : null)
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(error));
            Assert.Contains(host.Faults.ExecutedCommands, SqlMatch.IsOutboxInsert);
            Assert.Contains(host.Faults.ExecutedCommands, SqlMatch.IsUserInsert);
            AssertNoStaleOutboxIdentityEntry(db);

            if (!autoDetectChanges) db.ChangeTracker.DetectChanges();
            Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            if (tx != null)
            {
                if (async) await tx.CommitAsync(); else tx.Commit();
            }
        }

        var users = host.Users();
        var rows = host.OutboxRows();
        Assert.Single(users);
        Assert.Single(rows);
        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Orders\""));
        Assert.Equal(users[0].Id, Payload.User(rows[0].Payload).GetProperty("id").GetInt32());
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id));
    }

    /// <summary>
    /// Guid outbox key (client-generated, never discarded by EF): the Users INSERT fails, the retry persists and
    /// dispatches once, owned and caller paths, with and without AutoDetectChangesEnabled.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlagAutoDetect))]
    public async Task GuidOutboxKey_UserInsertFails_RetryPersistsAndDispatchesOnce(bool async, bool callerTransaction, bool autoDetectChanges)
    {
        using var host = AdvHost<GuidOutboxDbContext, Guid>.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        db.ChangeTracker.AutoDetectChangesEnabled = autoDetectChanges;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        await using (var tx = callerTransaction ? (async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction()) : null)
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(error));
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<Guid>>());

            Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            if (tx != null)
            {
                if (async) await tx.CommitAsync(); else tx.Commit();
            }
        }

        var rows = host.OutboxRows();
        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Single(rows);
        Assert.Equal(Guid.Parse(rows[0].Id), Assert.Single(host.Queue.Enqueued).Id);
        Assert.Equal(user.Id, Payload.User(rows[0].Payload).GetProperty("id").GetInt32());
    }
}
